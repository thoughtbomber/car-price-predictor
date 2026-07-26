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
    end

    subgraph "Streaming Layer"
        DEB["Debezium Connect (CDC)<br/>:8083"]
        KAFKA["Kafka Broker<br/>:9092"]
        ZK["Zookeeper<br/>:2181"]
    end

    subgraph "Storage Layer"
        PG[("PostgreSQL :5432<br/>cars_db.listings<br/>+ mlflow backend store")]
        MINIO[("MinIO (S3) :9000/:9001<br/>model artifacts")]
        CSV[/"data/ford.csv<br/>training dataset"/]
    end

    subgraph "ML Platform"
        MLFLOW["MLflow Server :5000<br/>tracking + registry"]
    end

    UI -->|"HTTP GET/POST /cars"| API
    API -->|"SQL (SQLAlchemy)"| PG
    API -->|"produce: new listing"| KAFKA
    API -->|"consume: predictions"| KAFKA
    PG -->|"WAL (logical replication)"| DEB
    DEB -->|"CDC events → topic<br/>cars-db.public.listings"| KAFKA
    KAFKA -->|"consume: listings"| ML
    ML -->|"produce: predictions → topic<br/>cars.public.predictions"| KAFKA
    ML -->|"load latest model run"| MLFLOW
    TRAIN -->|"log params/metrics/model"| MLFLOW
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
    API->>K: produce car JSON → topic cars-db.public.listings
    PG-->>DEB: WAL change event (INSERT)
    DEB->>K: CDC event → topic cars-db.public.listings

    K->>ML: consume listing (raw JSON or Debezium envelope)
    Note over ML: unwraps payload.after if present,<br/>validates required fields
    ML->>MF: (at startup) load latest model run,<br/>label_encoders.pkl, scaler.pkl
    ML->>ML: preprocess (label-encode + scale)<br/>model.predict()
    ML->>K: produce {id, predicted_price, ...} → topic cars.public.predictions

    K->>API: background consumer thread polls predictions
    API->>PG: UPDATE listings SET predicted_price WHERE id = car_id

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
    P --> ENC["LabelEncoder<br/>(model, transmission, fuelType)"]
    P --> SC["StandardScaler<br/>(year, mileage, tax, mpg, engineSize)"]
    ENC --> RF["RandomForestRegressor<br/>train/test split 80/20"]
    SC --> RF
    RF --> METRICS["metrics: RMSE, MAE, R²"]
    RF --> MLFLOW["MLflow Server :5000"]
    ENC -.->|"label_encoders.pkl<br/>(saved next to train.py + logged)"| MLFLOW
    SC -.->|"scaler.pkl<br/>(saved next to train.py + logged)"| MLFLOW
    METRICS --> MLFLOW
    MLFLOW --> PG[("PostgreSQL mlflow DB<br/>runs, params, metrics")]
    MLFLOW --> MINIO[("MinIO s3://mlflow/<br/>model binaries + artifacts")]
    MLFLOW --> REG["Model Registry:<br/>car_price_predictor → alias 'production'"]
```

## Key Details

- **Two paths into the listings topic**: the backend publishes directly to
  `cars-db.public.listings` after an insert (`backend/main.py:180`), and
  Debezium independently streams the same insert from the Postgres WAL into the
  same topic (`debezium-connector-config.json`). The ML service tolerates both
  raw JSON and the Debezium envelope (`ml_service/main.py:151-154`).
- **Topics**:
  - `cars-db.public.listings` — new/changed car listings (input to ML service)
  - `cars.public.predictions` — prediction results (input to backend consumer)
- **ML service startup** (`ml_service/main.py`): loads `label_encoders.pkl` and
  `scaler.pkl` from disk, then pulls the most recent run's model from MLflow
  (`runs:/{run_id}/model`), whose artifacts live in MinIO.
- **Backend consumer** (`backend/main.py:124-161`): a daemon thread started at
  FastAPI startup that listens on `cars.public.predictions` and writes
  `predicted_price` back into Postgres, closing the loop.
- **Streamlit** only talks to the FastAPI REST API; it never touches Kafka,
  Postgres, or MLflow directly.
- **Admin/observability UIs**: Adminer (:8081) for Postgres, Redpanda Console
  (:8080) for Kafka/Connect, MLflow UI (:5000), MinIO console (:9001).
- **Seeding data**: `add_cars.sh` POSTs a batch of sample cars to
  `POST /cars`, each of which flows through the loop above.
