# Car Price Predictor — Workflow & Data Flow

This document describes how the components in this repository interact, both at
runtime (the prediction loop) and during model training.

## 1. Component Overview

```mermaid
graph TB
    subgraph "User Layer"
        UI["Streamlit UI<br/>streamlit_app/app.py<br/>:8501"]
        ADMIN["Adminer (DB UI)<br/>:8081"]
        KUI["Redpanda Console (Kafka UI)<br/>:8080"]
    end

    subgraph "Application Layer"
        API["FastAPI Backend<br/>backend/main.py<br/>:8000"]
        ML["ML Prediction Service<br/>ml_service/main.py"]
        TRAIN["Training Script<br/>ml_service/train.py"]
        MON["Drift Monitor<br/>ml_service/monitor.py"]
    end

    subgraph "Streaming Layer"
        DEB["Debezium Connect (CDC)<br/>:8083"]
        KAFKA["Kafka Broker<br/>:9092"]
        ZK["Zookeeper<br/>:2181"]
    end

    subgraph "Storage Layer"
        PG[("PostgreSQL :5432<br/>cars_db.listings<br/>+ predictions_log<br/>+ mlflow backend store")]
        MINIO[("MinIO (S3) :9000/:9001<br/>model artifacts")]
        CSV[/"data/ford.csv<br/>training dataset"/]
    end

    subgraph "ML Platform"
        MLFLOW["MLflow Server :5000<br/>tracking + registry"]
    end

    UI -->|"HTTP GET/POST /cars"| API
    API -->|"SQL (SQLAlchemy)"| PG
    API -->|"consume: predictions"| KAFKA
    PG -->|"WAL (logical replication)"| DEB
    DEB -->|"CDC events → topic<br/>cars-db.public.listings"| KAFKA
    KAFKA -->|"consume: listings"| ML
    ML -->|"contract violations → DLQ topic<br/>cars-db.public.listings.dlq"| KAFKA
    ML -->|"produce: predictions → topic<br/>cars.public.predictions"| KAFKA
    ML -->|"load @production model alias"| MLFLOW
    TRAIN -->|"log params/metrics/model,<br/>staging → production gate"| MLFLOW
    MON -->|"read predictions_log"| PG
    MON -->|"log drift metrics"| MLFLOW
    MLFLOW -->|"backend store (runs metadata)"| PG
    MLFLOW -->|"artifact store (model binaries)"| MINIO
    CSV --> TRAIN
    ZK --- KAFKA
    ADMIN --> PG
    KUI --> KAFKA
```

## 2. Runtime Prediction Loop (sequence)

This is the main data flow: a car added by a user travels through the system
and comes back enriched with a `predicted_price`.

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant UI as Streamlit (:8501)
    participant API as FastAPI (:8000)
    participant PG as PostgreSQL (cars_db.listings)
    participant DEB as Debezium CDC
    participant K as Kafka
    participant ML as ML Service
    participant MF as MLflow / MinIO

    User->>UI: Fill "Add New Car" form
    UI->>API: POST /cars (car JSON)
    API->>PG: INSERT INTO listings (predicted_price = NULL)
    API-->>UI: 200 OK (car with id)
    PG-->>DEB: WAL change event (INSERT)
    DEB->>K: CDC event → topic cars-db.public.listings

    K->>ML: consume listing (Debezium envelope)
    Note over ML: unwraps payload.after, validates against the<br/>data contract (contracts/listing_event_v1.json)
    ML->>K: contract violation → DLQ topic cars-db.public.listings.dlq
    ML->>MF: (at startup) load models:/car_price_predictor@production
    ML->>ML: pipeline preprocessing + model.predict()
    ML->>K: produce {id, predicted_price, model_version, ...} → topic cars.public.predictions

    K->>API: background consumer thread polls predictions
    API->>PG: UPDATE listings SET predicted_price WHERE id = car_id
    API->>PG: INSERT INTO predictions_log (features, model_version)

    User->>UI: Refresh dashboard
    UI->>API: GET /cars
    API->>PG: SELECT * FROM listings
    API-->>UI: cars incl. predicted_price
    UI-->>User: charts + table with predictions
```

## 3. Training Pipeline (offline)

```mermaid
flowchart LR
    CSV[/"data/ford.csv"/] --> P["ml_service/train.py"]
    P --> CT["ColumnTransformer:<br/>OneHotEncoder (handle_unknown='ignore')<br/>+ StandardScaler"]
    CT --> RF["sklearn Pipeline:<br/>preprocessor + RandomForestRegressor<br/>train/test split 80/20"]
    RF --> METRICS["metrics: RMSE, MAE, R²"]
    RF --> MLFLOW["MLflow Server :5000"]
    METRICS --> MLFLOW
    MLFLOW --> PG[("PostgreSQL mlflow DB<br/>runs, params, metrics")]
    MLFLOW --> MINIO[("MinIO s3://mlflow/<br/>pipeline artifacts")]
    MLFLOW --> STAGE["Model Registry:<br/>new version → alias 'staging'"]
    STAGE --> GATE{"RMSE beats current<br/>production?"}
    GATE -->|"yes"| REG["promote → alias 'production'"]
    GATE -->|"no"| STAY["stays in staging"]
```

## Key Details

- **One path into the listings topic**: Debezium streams every committed insert
  from the Postgres WAL into `cars-db.public.listings`
  (`debezium-connector-config.json`). The backend does **not** publish to Kafka
  itself, so each car produces exactly one event — and changes made outside the
  API (e.g. SQL in Adminer) are captured too.
- **Data contract + dead letter topic**: the ML service validates every event
  against `contracts/listing_event_v1.json` (JSON Schema, versioned in git —
  the same contract the backend enforces at the API edge via Pydantic).
  Violations go to `cars-db.public.listings.dlq` with the error attached;
  nothing is silently dropped.
- **Topics**:
  - `cars-db.public.listings` — new/changed car listings (input to ML service)
  - `cars-db.public.listings.dlq` — contract-violating events (dead letter)
  - `cars.public.predictions` — prediction results (input to backend consumer)
- **Train/serve parity**: preprocessing lives inside a single sklearn
  `Pipeline` (`ColumnTransformer` + `RandomForestRegressor`) that is logged to
  MLflow as one artifact — there is no separate encoder/scaler to keep in sync.
  `OneHotEncoder(handle_unknown='ignore')` also makes the service robust to
  car models never seen in training.
- **Model lifecycle**: `train.py` tags each new version `staging` and promotes
  it to the `production` alias only if its RMSE beats the current production
  model. The ML service loads `models:/car_price_predictor@production` and
  stamps every prediction with the `model_version` it used (lineage).
- **Monitoring**: every prediction is also written to the `predictions_log`
  table by the backend consumer. `ml_service/monitor.py` compares its feature
  distributions against `data/ford.csv` using PSI (threshold 0.2) and logs the
  report to the `drift-monitoring` MLflow experiment.
- **Backend consumer** (`backend/main.py`): a daemon thread started at FastAPI
  startup that listens on `cars.public.predictions` and writes
  `predicted_price` back into Postgres, closing the loop.
- **Streamlit** only talks to the FastAPI REST API; it never touches Kafka,
  Postgres, or MLflow directly.
- **Admin/observability UIs**: Adminer (:8081) for Postgres, Redpanda Console
  (:8080) for Kafka/Connect, MLflow UI (:5000), MinIO console (:9001).
- **Seeding data**: `add_cars.sh` POSTs a batch of sample cars to
  `POST /cars`, each of which flows through the loop above.
