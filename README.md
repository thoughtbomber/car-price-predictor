# Car Price Predictor

A machine learning system for predicting car prices using MLflow and Streamlit. This project implements a complete pipeline for predicting car prices with an interactive web interface and real-time visualization.

> **Teaching branch** (`feat/production-ml-pipeline`): this branch doubles as a
> guided tour of the blog architecture in `docs/blog/data-system-summary.md`
> (data contracts, CDC, stream validation, DLQ, feature store, model registry,
> drift monitoring). Every module is annotated with the blog concept it
> implements, and there is a clickable walkthrough of the whole system:
>
> **→ Open `docs/interactive/index.html` in a browser** (double-click — no
> server needed) for an interactive architecture diagram with animated data
> flows and a blog-component ↔ code ↔ test mapping table.

## Architecture

- **Frontend**: Streamlit application for data visualization and interaction
- **Backend**: FastAPI REST API for data management
- **ML Pipeline**: MLflow for model management and serving
- **Storage**: PostgreSQL for data storage, MinIO for model artifacts
  
```mermaid
 graph TB
    UI[Streamlit UI:8501] --> API[FastAPI:8000]
    API --> DB[(PostgreSQL:5432)]
    DB --> DEB[Debezium:8083]
    DEB --> KAFKA[Kafka:9092]
    KAFKA --> ML[ML Service]
    ML --> MLFLOW[MLflow:5000]
    MLFLOW --> MINIO[(MinIO:9000)]
    ML --> KAFKA
    KAFKA --> API
    API --> DB

    ADMIN[Adminer:8081] --> DB
    KAFKAUI[Kafka UI:8080] --> KAFKA
    ZK[Zookeeper:2181] --> KAFKA
    
    subgraph "User Interface"
        UI
        ADMIN
        KAFKAUI
    end

    subgraph "Storage"
        DB
        MINIO
    end

    subgraph "Processing"
        KAFKA
        DEB
        ML
        MLFLOW
        ZK
    end

    style UI fill:#2563eb,stroke:#1d4ed8,color:#fff
    style API fill:#2563eb,stroke:#1d4ed8,color:#fff
    style DB fill:#059669,stroke:#047857,color:#fff
    style MINIO fill:#059669,stroke:#047857,color:#fff
    style KAFKA fill:#4b5563,stroke:#374151,color:#fff
    style ML fill:#7c3aed,stroke:#6d28d9,color:#fff
    style MLFLOW fill:#7c3aed,stroke:#6d28d9,color:#fff

```


## Prerequisites

- Docker and Docker Compose
- Python 3.9+ (for local development)

## Quick Start

1. Clone the repository:
```bash
git clone https://github.com/Stefen-Taime/car-price-predictor
cd car-price-predictor
```

2. Start the services:
```bash
docker-compose up --build
```

3. Train the initial model:
```bash
cd ml_service
python train.py
```

4. Start the FastAPI backend:
```bash
cd backend
uvicorn main:app --reload
```

5. Start the Streamlit frontend:
```bash
cd streamlit_app
streamlit run app.py
```

6. Access the applications:
- Streamlit UI: http://localhost:8501
- FastAPI Docs: http://localhost:8000/docs
- MLflow UI: http://localhost:5000
- MinIO Console: http://localhost:9001
- Kafka UI: http://localhost:8080

## Project Structure

```
.
├── backend/                # FastAPI backend service
│   ├── main.py            # Main API application
│   └── requirements.txt   # Python dependencies
├── data/                  # Training data
│   └── ford.csv          # Sample car data
├── ml_service/           # ML training and prediction service
│   ├── train.py         # Model training script (Pipeline + staging→production gate)
│   ├── main.py          # Prediction service (contract validation + DLQ + online feature ingestion)
│   ├── monitor.py       # Drift monitoring report (PSI vs training data)
│   ├── contract_registry.py  # Contract Registry: loads versioned contracts (Schema+SLA+Semantics+Lineage)
│   ├── feature_store.py # Feature Store: online (SQLite) + offline (CSV) with 4 APIs
│   ├── dlq_tools.py     # DLQ Alerting & Recovery apps (--alert / --recover)
│   ├── batch_validation.py   # Scheduled batch SLA validation job (+ alert path)
│   └── tests/           # Unit + offline integration tests (pytest)
├── contracts/            # Data contracts (JSON Schema + x-sla/x-semantics/x-lineage, versioned)
│   └── listing_event_v1.json
├── docs/
│   └── interactive/index.html  # Interactive architecture explainer (open in a browser)
├── mlflow/               # MLflow service configuration
├── postgres/            # PostgreSQL initialization scripts
├── streamlit_app/       # Streamlit frontend application
│   ├── app.py          # Main Streamlit application
│   └── requirements.txt # Python dependencies
├── docker-compose.yml   # Docker services configuration
└── README.md
```

## Features

- Real-time car price predictions
- Interactive data visualization with Streamlit
- RESTful API with FastAPI
- ML model versioning and tracking with MLflow
- Beautiful charts with Plotly
- Scalable architecture
- API documentation with Swagger UI

## Development

### Backend Development

```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### Frontend Development

```bash
cd streamlit_app
pip install -r requirements.txt
streamlit run app.py
```

### ML Service Development

```bash
cd ml_service
pip install -r requirements.txt
python train.py
```

### Drift Monitoring

After some predictions have been served, compare their feature distributions
against the training data (PSI, threshold 0.2) and log the report to MLflow:

```bash
cd ml_service
python monitor.py
```

Note: the `predictions_log` table is created by `postgres/init.sql`, which only
runs on a fresh database volume. For an existing deployment either recreate the
volume (`docker compose down -v` — this wipes all data) or create the table
manually via Adminer.

### Teaching Components (blog architecture)

These standalone scripts implement the remaining components from
`docs/blog/data-system-summary.md`. They are covered by `ml_service/tests/`
and runnable on demand:

```bash
cd ml_service

# Batch SLA validation (blog step 7): check landed predictions against the
# contract's x-sla block (freshness/completeness/volume/ranges); alerts + MLflow log.
python batch_validation.py

# DLQ apps (blog step 4): tail the dead-letter topic and alert / repair+republish.
python dlq_tools.py --alert
python dlq_tools.py --recover
```

The **Contract Registry** (`contract_registry.py`) and **Feature Store**
(`feature_store.py`, online SQLite + offline CSV under `data/feature_store/`)
are libraries used by the services above rather than standalone scripts. Run
the whole offline flow — validation → DLQ → recovery → feature store →
training parity → SLA check → drift — via the integration test:

```bash
cd ml_service
python -m pytest tests -v
```

## API Documentation

The API documentation is available at `http://localhost:8000/docs` when the backend service is running. The following endpoints are available:

- `GET /cars`: List all cars
- `POST /cars`: Add a new car
- `GET /cars/{car_id}`: Get car details
- `GET /health`: Check service health

## Data Flow

1. User submits car data through Streamlit interface
2. Data is sent to FastAPI backend
3. ML service makes predictions using MLflow
4. Results are stored in PostgreSQL
5. Updated data is displayed in Streamlit UI

## Technologies Used

- **Backend**:
  - FastAPI for REST API
  - Pydantic for data validation
  - SQLAlchemy for database ORM

- **Frontend**:
  - Streamlit for UI
  - Plotly for data visualization
  - Pandas for data manipulation

- **ML Pipeline**:
  - MLflow for model management
  - scikit-learn for ML models
  - PostgreSQL for data storage
  - MinIO for artifact storage

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request


## Acknowledgments

- Ford Used Car Dataset
- MLflow for ML model management
- FastAPI for the backend API
- Streamlit for the interactive UI
