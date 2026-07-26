# Data Pipelines in Machine Learning Systems — Combined Summary

*Source: blog text (`data system.txt`) + accompanying diagram (`data system diagram.gif`), by Aurimas Griciūnas. The diagram supplements the text with components the text does not mention; both are merged below.*

## Core Idea

Data quality and integrity must be enforced **upstream** of ML training and inference pipelines. Fixing quality problems downstream causes unavoidable failure at scale. Most of the work happens at the Data Lake / LakeHouse layer. The same architecture also applies to LLM-based systems.

## Key Concepts (from the diagram)

- **Data Contract** — the agreement between Data Producer and Data Consumer, composed of four parts:
  - **Schema** — structure of the data
  - **SLA** — quality/service guarantees (freshness, completeness, etc.)
  - **Semantics** — meaning of the fields
  - **Lineage** — where the data comes from
- Producers emit **raw data**; consumers receive **validated data**. Everything in between exists to enforce that transition.

## The End-to-End Flow (numbered steps from the text, enriched with diagram detail)

1. **Schema version control** — schema changes are implemented in version control; once approved, they are pushed to the applications generating the data, the databases holding it, and a central **Data Contract Registry** (holding Schema + SLA). *Diagram: both the emitting services and the CDC'd databases connect to the Contract Registry.*
2. **Application events** — services push generated events directly to Kafka topics (also covers IoT fleets and website activity tracking).
   - **2.1 CDC streams** — raw data topics fed by Change Data Capture from operational databases (e.g. user DB, finance DB).
3. **Stream validation** — a Flink application consumes the raw streams and validates each event against the schemas in the Contract Registry.
4. **Dead Letter path** — data that fails the contract goes to an **Invalidated / Dead Letter Topic**. *Diagram adds: two downstream apps — a **Recovery app** (reprocess/fix failed events) and an **Alerting app** (notify on failures).*
5. **Validated topic** — data that meets the contract is pushed to a Validated Data Topic.
6. **Object storage landing** — validated data is persisted to object storage (data lake) for additional validation.
7. **Scheduled batch validation** — on a schedule, data in object storage is validated against additional SLAs in the Data Contracts, then pushed to the **Data Warehouse** to be transformed and modeled for analytical purposes. *Diagram adds: an **alerting** path when this batch validation fails.*
8. **Feature Store** — modeled, curated data is pushed to the Feature Store for further feature engineering.
   - **8.1 Real-time features** — ingested into the Feature Store directly from the Validated Data Topic (step 5), skipping the batch path. *Note: quality is harder to guarantee here since SLA checks are difficult to perform in real time.*
   - *Diagram adds: the Feature Store exposes four APIs — **Real-Time Feature Serving**, **Real-Time Feature Ingestion**, **Batch Feature Ingestion**, and **Batch Feature Serving** — plus a **Feature Retrieval → Feature Validation** step before features are consumed.*
9. **ML training** — high-quality data from the Feature Store feeds the training pipelines. *Diagram expands this into a pipeline: **Model Training → Model Validation → Model Handover**, all tracked by an **ML Metadata Store** (metadata/lineage across the pipeline).*
10. **Inference / feature serving** — the same features used for training are served for inference, guaranteeing train/serve parity.

## Deployment & Serving Layer (diagram only)

The diagram extends beyond the text into how models reach production:

- **Model Registry** — versioned models with **STAGING → PRODUCTION** promotion.
- **Container Registry** — production models are packaged as containers.
- **Model serving** — containers deployed on Kubernetes behind a **Load Balancer**, serving predictions to Product Apps (via gRPC/REST).
- **ML Metadata Store** — connects training, validation, handover, registry, and serving, tracking artifact and metadata movement throughout.

## Caveats (from the text)

- Real-time feature quality is hard to guarantee — SLA checks are difficult to perform on streaming paths.
- **Data and concept drift** are silent failures that cannot be captured in a Data Contract; they require separate monitoring.
