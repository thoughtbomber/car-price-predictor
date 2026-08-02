# Services Overview

This project is an end-to-end **car price prediction platform**: users add car listings through a web UI, an ML model predicts prices asynchronously over a Kafka event pipeline, and results flow back into the database for display. Below is a summary of every service and how they relate.

## Infrastructure Services (docker-compose)

### 1. PostgreSQL (`postgres`)
- Image: `postgres:15`, port `5432`
- The system of record. Hosts two databases (created by `postgres/init.sql`):
  - `cars_db` — application data, with a `listings` table (car details + `predicted_price` column)
  - `mlflow` — MLflow's backend store (experiment metadata, runs, metrics)
- Started with `wal_level=logical` so Debezium can perform change data capture (CDC) on the `listings` table.

### 2. Zookeeper (`zookeeper`)
- Image: `confluentinc/cp-zookeeper:7.4.0`, port `2181`
- Coordination service required by the Kafka broker.

### 3. Kafka (`kafka`)
- Image: `confluentinc/cp-kafka:7.4.0`, port `9092` (host) / `29092` (internal)
- The event backbone. Key topics:
  - `cars-db.public.listings` — CDC events for every insert/update to the `listings` table (produced by Debezium; it is the *only* publisher)
  - `cars-db.public.listings.dlq` — dead letter topic: listing events that fail contract validation
  - `cars.public.predictions` — prediction results produced by the ML service

### 4. Debezium Connect (`connect`)
- Image: `debezium/connect:2.4`, port `8083`
- Kafka Connect worker running the PostgreSQL connector (configured in `debezium-connector-config.json`). It tails Postgres's write-ahead log and streams every change to `public.listings` into the `cars-db.public.listings` Kafka topic. This decouples the database from downstream consumers.

### 5. MinIO (`minio`)
- Image: `minio/minio`, ports `9000` (API), `9001` (console)
- S3-compatible object storage. Holds the `mlflow` bucket where MLflow stores model artifacts (trained model binaries, pickled encoders/scaler).

### 6. MLflow (`mlflow`)
- Built from `mlflow/Dockerfile`, port `5000`
- Experiment tracking and model registry. Uses **PostgreSQL** (`mlflow` DB) as its backend store and **MinIO** (`s3://mlflow/`) as its artifact store. On startup it creates the MinIO bucket, then serves the tracking UI/API.

### 7. Redpanda Console (`kafka-ui`)
- Image: `redpandadata/console:v2.4.3`, port `8080`
- Web UI for inspecting Kafka topics, messages, and the Connect cluster.

### 8. Adminer (`adminer`)
- Image: `adminer:latest`, port `8081`
- Web UI for browsing/editing the PostgreSQL databases.

## Application Services (run separately)

### 9. Backend API (`backend/main.py`)
- FastAPI app, port `8000`
- REST interface to the `listings` table: `GET /cars`, `POST /cars`, `GET /cars/{id}`, `GET /health`
- On `POST /cars`: validates input (Pydantic — the same data contract as `contracts/listing_event_v1.json`, enforced at the producing application) and inserts into Postgres. Debezium CDC takes it from there; the backend never publishes to Kafka.
- Runs a background **Kafka consumer thread** on `cars.public.predictions`: when a prediction arrives, it writes `predicted_price` back into the corresponding row in Postgres and appends the full event (features + `model_version`) to the `predictions_log` table for monitoring and lineage.

### 10. ML Service (`ml_service/`)
- **`train.py`** — offline training script. Reads `data/ford.csv` and trains a single sklearn **Pipeline** (`ColumnTransformer`: OneHotEncoder `handle_unknown='ignore'` for categoricals, StandardScaler for numerics → `RandomForestRegressor`), so preprocessing travels inside the model artifact (train/serve parity, no separate pickles). Logs params/metrics/model to **MLflow** (artifacts land in **MinIO**), registers the model as `car_price_predictor`, tags the new version `staging`, and promotes it to the `production` alias **only if its RMSE beats the current production model** (champion/challenger gate).
- **`main.py`** — online inference service. Loads the model from the registry via `models:/car_price_predictor@production`, consumes listings from `cars-db.public.listings` (unwraps the Debezium CDC envelope), **validates each event against the data contract** (`contracts/listing_event_v1.json`, JSON Schema), routes violations to the `cars-db.public.listings.dlq` dead letter topic, predicts a price, and produces the result (including `model_version`) to `cars.public.predictions`.
- **`monitor.py`** — drift monitor. Compares feature distributions in the `predictions_log` table against the training data (`data/ford.csv`) using PSI (threshold 0.2), prints a report, and logs it to the `drift-monitoring` MLflow experiment.

### 11. Streamlit Frontend (`streamlit_app/app.py`)
- Streamlit dashboard (default port `8501`)
- Talks **only** to the Backend API (`http://localhost:8000`): a sidebar form to add cars (`POST /cars`), plus a dashboard with metrics, Plotly charts (price distribution, price vs mileage, predicted vs actual), filterable listings table, and CSV export (`GET /cars`).

## How They Work Together

### Runtime / inference flow
```
Streamlit UI ──POST /cars──▶ Backend API ──insert──▶ PostgreSQL (listings)
                                                         │ CDC (WAL)
                                                         ▼
                              Kafka topic: cars-db.public.listings ◀── Debezium
                                                         │
                                                         ▼
                              ML Service (consume → validate contract → predict)
                                  │              │
                                  │              └─ invalid ─▶ DLQ topic: cars-db.public.listings.dlq
                                  ▼
                          Kafka topic: cars.public.predictions
                                  │
                                  ▼
                          Backend consumer thread ──update predicted_price──▶ PostgreSQL
                                  └──────────────append features + model_version──▶ predictions_log
                                  │
Streamlit UI ◀──GET /cars─────────┘ (dashboard now shows predicted prices)
```

### Training flow
```
data/ford.csv ──▶ train.py ──▶ MLflow (metrics, params, model registry)
                                    │
                                    ├─ backend store: PostgreSQL (mlflow DB)
                                    └─ artifacts: MinIO (s3://mlflow/)
                                            │
                                            ▼
                              ML Service loads latest model + artifacts for inference
```

## Relationship Summary

| Service | Depends on | Used by |
|---|---|---|
| postgres | — | backend, mlflow, debezium, adminer |
| zookeeper | — | kafka |
| kafka | zookeeper | connect, backend, ml_service, kafka-ui |
| connect (Debezium) | kafka, postgres | ml_service (via topics) |
| minio | — | mlflow, ml_service |
| mlflow | minio, postgres | ml_service (train + inference) |
| kafka-ui | kafka, connect | humans |
| adminer | postgres | humans |
| backend | postgres, kafka | streamlit_app |
| ml_service | kafka, mlflow, minio | backend (via topics) |
| streamlit_app | backend | end users |
