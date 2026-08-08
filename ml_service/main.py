"""ML prediction service — the blog's stream validator + inference in one process.

Blog mapping (docs/blog/data-system-summary.md):
- step 3  "stream validation": every CDC event is validated against the data
  contract (contracts/listing_event_v1.json) BEFORE it may flow downstream.
- step 4  violations go to the dead letter topic (see dlq_tools.py for the
  alerting/recovery apps that consume it).
- step 10 the validated event is scored with the SAME sklearn Pipeline that was
  trained offline (train/serve parity — preprocessing travels inside the model).
- model   loaded from the MLflow registry's @production alias, i.e. only models
  that passed the promotion gate in train.py are ever served here.
"""
import os
import json
import jsonschema
import mlflow
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
import logging
from typing import Optional, Dict, Any
import time
from pathlib import Path
import boto3
from botocore.client import Config
import warnings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

KAFKA_TOPIC_LISTINGS = 'cars-db.public.listings'
KAFKA_TOPIC_PREDICTIONS = 'cars.public.predictions'
KAFKA_TOPIC_DLQ = 'cars-db.public.listings.dlq'

MODEL_NAME = 'car_price_predictor'
MODEL_ALIAS = 'production'

MODEL_FEATURES = ['model', 'year', 'transmission', 'mileage',
                  'fuelType', 'tax', 'mpg', 'engineSize']

# Feature group name in the feature store (see feature_store.py).
FEATURE_GROUP = 'listing_features'


class CarPricePredictor:
    def __init__(self):
        self.model = None
        self.model_version = None
        self.contract_validator = None
        self.consumer = None
        self.producer = None
        # Online feature store (blog step 8.1: real-time feature ingestion).
        # Best-effort: if the store cannot be initialised the service still
        # predicts — the store is an observability/parity aid here, not a
        # hard dependency of the serving path.
        try:
            from feature_store import FeatureStore
            self.feature_store = FeatureStore(root=os.getenv('FEATURE_STORE_ROOT') or None)
        except Exception as e:
            logger.warning(f"Feature store unavailable, continuing without it: {e}")
            self.feature_store = None

    def setup_minio(self):
        """Configure MinIO credentials"""
        os.environ['AWS_ACCESS_KEY_ID'] = 'minio'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'minio123'
        os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'http://localhost:9000'
        
        boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minio',
            aws_secret_access_key='minio123',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        logger.info("MinIO credentials configured")

    def load_contract(self) -> None:
        """Load the data contract (JSON Schema) used to validate listing events"""
        contract_path = Path(
            os.getenv('CONTRACT_PATH',
                      str(Path(__file__).parent.parent / 'contracts' / 'listing_event_v1.json'))
        )
        with open(contract_path, encoding='utf-8') as f:
            schema = json.load(f)
        self.contract_validator = jsonschema.Draft202012Validator(schema)
        logger.info(f"Loaded data contract from {contract_path}")

    def load_model(self) -> None:
        """Load the production model from the MLflow registry"""
        try:
            logger.info("Loading model from MLflow registry...")

            self.setup_minio()
            mlflow.set_tracking_uri("http://localhost:5000")

            model_uri = f"models:/{MODEL_NAME}@{MODEL_ALIAS}"
            max_retries = 5
            for i in range(max_retries):
                try:
                    self.model = mlflow.pyfunc.load_model(model_uri)
                    break
                except Exception:
                    if i == max_retries - 1:
                        raise
                    logger.warning(f"Failed to load model, retrying... ({i+1}/{max_retries})")
                    time.sleep(5)

            client = mlflow.tracking.MlflowClient()
            self.model_version = client.get_model_version_by_alias(
                MODEL_NAME, MODEL_ALIAS).version
            logger.info(f"Loaded {model_uri} (version {self.model_version})")

        except Exception as e:
            logger.error(f"Error loading model: {str(e)}")
            raise

    def setup_kafka(self) -> None:
        """Initialize Kafka consumer and producer"""
        try:
            KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
            
            self.consumer = KafkaConsumer(
                KAFKA_TOPIC_LISTINGS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='car_price_predictor',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                consumer_timeout_ms=1000
            )
            
            self.producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                retries=5
            )
            
            logger.info("Kafka consumer and producer initialized")
            
        except Exception as e:
            logger.error(f"Error setting up Kafka: {str(e)}")
            raise

    def preprocess_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """Build the model input frame; encoding/scaling live inside the pipeline"""
        try:
            return pd.DataFrame([{col: data[col] for col in MODEL_FEATURES}])
        except Exception as e:
            logger.error(f"Error preprocessing data: {str(e)}")
            raise

    def validate_event(self, car_data: Dict[str, Any]) -> Optional[str]:
        """Validate a listing event against the data contract; return error text or None"""
        errors = list(self.contract_validator.iter_errors(car_data))
        if errors:
            return "; ".join(
                f"{'/'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}"
                for e in errors
            )
        return None

    def send_to_dlq(self, data: Any, error: str) -> None:
        """Publish a contract-violating message to the dead letter topic"""
        dlq_record = {
            'error': error,
            'message': data,
            'failed_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        }
        self.producer.send(KAFKA_TOPIC_DLQ, value=dlq_record)
        self.producer.flush()
        logger.warning(f"Sent message to {KAFKA_TOPIC_DLQ}: {error}")

    def process_message(self, message: Any) -> Optional[Dict[str, Any]]:
        """Validate an incoming Kafka message against the contract and return a prediction"""
        try:
            data = message.value if isinstance(message.value, dict) else json.loads(message.value)

            if 'payload' in data and isinstance(data['payload'], dict) and 'after' in data['payload']:
                car_data = data['payload']['after']
            else:
                car_data = data

            # Debezium delete/tombstone events carry no new row state; nothing to predict.
            if car_data is None:
                return None

            error = self.validate_event(car_data)
            if error:
                self.send_to_dlq(data, error)
                return None

            # Blog step 8.1: real-time feature ingestion. The validated event's
            # features are upserted into the online store so they can be served
            # later with train/serve parity (blog step 10). Best-effort — a
            # store hiccup must never block a prediction.
            if self.feature_store is not None:
                try:
                    self.feature_store.ingest_online(
                        FEATURE_GROUP, {k: car_data.get(k) for k in ['id'] + MODEL_FEATURES})
                except Exception as e:
                    logger.warning(f"Online feature ingestion failed (continuing): {e}")

            processed_data = self.preprocess_data(car_data)
            prediction = self.model.predict(processed_data)[0]

            car_data['predicted_price'] = float(prediction)
            car_data['prediction_timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')
            car_data['model_version'] = self.model_version

            return car_data

        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            return None

    def run(self) -> None:
        """Main processing loop"""
        logger.info("Starting to consume messages...")
        
        while True:
            try:
                messages = self.consumer.poll(timeout_ms=1000)
                
                for topic_partition, msgs in messages.items():
                    for message in msgs:
                        result = self.process_message(message)
                        
                        if result:
                            self.producer.send(KAFKA_TOPIC_PREDICTIONS, value=result)
                            self.producer.flush()
                            
                            logger.info(
                                f"Processed car: {result.get('model', 'Unknown')} "
                                f"({result.get('year', 'Unknown')}). "
                                f"Predicted price: £{result['predicted_price']:,.2f}"
                            )
                
                time.sleep(0.1)
                    
            except KeyboardInterrupt:
                logger.info("Stopping the service...")
                break
                
            except Exception as e:
                logger.error(f"Error in processing loop: {str(e)}")
                time.sleep(5)  
        
        try:
            self.consumer.close()
            self.producer.close()
            logger.info("Kafka connections closed")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
        
        logger.info("Service stopped")

def main():
    """Main entry point"""
    try:
        load_dotenv()
        
        predictor = CarPricePredictor()

        predictor.load_contract()

        predictor.load_model()

        predictor.setup_kafka()
        
        predictor.run()
        
    except Exception as e:
        logger.error(f"Application failed: {str(e)}")
        raise
    
    finally:
        logger.info("Application shutdown complete")

if __name__ == "__main__":
    main()