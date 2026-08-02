import os
import logging
import warnings
from pathlib import Path
from typing import Optional

import boto3
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from botocore.client import Config
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

warnings.filterwarnings('ignore', category=UserWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

MODEL_NAME = "car_price_predictor"
CATEGORICAL_FEATURES = ['model', 'transmission', 'fuelType']
NUMERIC_FEATURES = ['year', 'mileage', 'tax', 'mpg', 'engineSize']

PARAMS = {
    'n_estimators': 100,
    'max_depth': 10,
    'min_samples_split': 2,
    'min_samples_leaf': 1,
    'random_state': 42
}


def setup_minio():
    """Setup MinIO connection and ensure bucket exists"""
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url='http://localhost:9000',
            aws_access_key_id='minio',
            aws_secret_access_key='minio123',
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )

        try:
            s3_client.head_bucket(Bucket='mlflow')
            logger.info("MLflow bucket exists")
        except Exception:
            s3_client.create_bucket(Bucket='mlflow')
            logger.info("Created MLflow bucket")

    except Exception as e:
        logger.error(f"Error setting up MinIO: {str(e)}")
        raise


def build_pipeline(params: dict) -> Pipeline:
    """Build a single train/serve pipeline: preprocessing travels with the model"""
    preprocessor = ColumnTransformer([
        ('categorical', OneHotEncoder(handle_unknown='ignore', sparse_output=False),
         CATEGORICAL_FEATURES),
        ('numeric', StandardScaler(), NUMERIC_FEATURES),
    ])
    return Pipeline([
        ('preprocessor', preprocessor),
        ('model', RandomForestRegressor(**params)),
    ])


def get_production_rmse(client: mlflow.tracking.MlflowClient) -> Optional[float]:
    """RMSE of the model currently behind the 'production' alias, or None"""
    try:
        version = client.get_model_version_by_alias(MODEL_NAME, 'production')
        run = client.get_run(version.run_id)
        return run.data.metrics.get('rmse')
    except Exception:
        return None


def train_model():
    mlflow.set_tracking_uri("http://localhost:5000")

    os.environ['AWS_ACCESS_KEY_ID'] = 'minio'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'minio123'
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = 'http://localhost:9000'

    setup_minio()
    mlflow.set_experiment("car-price-prediction")

    current_dir = Path(__file__).parent.parent
    data_path = current_dir / 'data' / 'ford.csv'

    logger.info(f"Loading data from {data_path}")
    if not data_path.exists():
        raise FileNotFoundError(f"Data file not found at {data_path}")

    df = pd.read_csv(data_path)
    X = df.drop('price', axis=1)
    y = df['price']

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42)

    pipeline = build_pipeline(PARAMS)

    logger.info("Training Random Forest pipeline...")
    with mlflow.start_run() as run:
        pipeline.fit(X_train, y_train)

        y_pred = pipeline.predict(X_test)
        metrics = {
            'rmse': float(np.sqrt(mean_squared_error(y_test, y_pred))),
            'mae': float(mean_absolute_error(y_test, y_pred)),
            'r2': float(r2_score(y_test, y_pred))
        }
        logger.info(f"Model metrics: {metrics}")

        mlflow.log_params(PARAMS)
        mlflow.log_metrics(metrics)

        signature = mlflow.models.signature.infer_signature(
            X_train, pipeline.predict(X_train))

        mlflow.sklearn.log_model(
            pipeline,
            "model",
            registered_model_name=MODEL_NAME,
            signature=signature
        )
        new_run_id = run.info.run_id
        new_rmse = metrics['rmse']

    # Model handover: tag as staging, then promote only if it beats production.
    client = mlflow.tracking.MlflowClient()
    new_version = client.search_model_versions(
        f"name='{MODEL_NAME}' and run_id='{new_run_id}'")[0]

    client.set_registered_model_alias(MODEL_NAME, 'staging', new_version.version)
    logger.info(f"Model version {new_version.version} tagged as 'staging'")

    prod_rmse = get_production_rmse(client)
    if prod_rmse is None or new_rmse < prod_rmse:
        client.set_registered_model_alias(MODEL_NAME, 'production', new_version.version)
        logger.info(
            f"Promoted version {new_version.version} to 'production' "
            f"(rmse {new_rmse:.2f} vs production {prod_rmse})"
        )
    else:
        logger.info(
            f"Challenger lost: rmse {new_rmse:.2f} >= production {prod_rmse:.2f}; "
            f"version {new_version.version} stays in 'staging'"
        )


if __name__ == "__main__":
    try:
        train_model()
    except Exception as e:
        logger.error(f"Training failed: {str(e)}")
        raise
