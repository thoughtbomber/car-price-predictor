"""Batch SLA validation — blog step 7 (see docs/blog/data-system-summary.md).

The stream validator (ml_service/main.py) checks each event against the
contract SCHEMA in real time. But some promises can only be checked on a whole
batch of landed data: "no nulls in any feature column", "the newest row is
fresh", "the stream didn't silently stop". Those live in the contract's
``x-sla`` block, and this scheduled job enforces them — the blog's "scheduled
batch validation against additional SLAs in the Data Contracts", with the
diagram's **alerting path** when validation fails.

Data source: ``cars_db.predictions_log`` — the table of validated, served
predictions (this repo's stand-in for the data lake landing zone). Only data
that passed stream validation ever reaches it, which is exactly the blog's
"validated data lands in object storage, then gets batch-validated".

Run on demand (a cron/scheduler would run it in production)::

    python batch_validation.py
"""
import logging
import os
from pathlib import Path
from typing import List, Optional

import mlflow
import pandas as pd
import psycopg2

from contract_registry import ContractRegistry, DataContract, SlaBreach

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv(
    'DATABASE_URL', 'postgresql://postgres:postgres123@localhost:5432/cars_db')


def load_landed_data(database_url: str = DATABASE_URL) -> pd.DataFrame:
    """Read the validated-data landing zone (predictions_log)."""
    conn = psycopg2.connect(database_url)
    try:
        return pd.read_sql(
            "SELECT model, year, transmission, mileage, \"fuelType\", tax, mpg, "
            "\"engineSize\", predicted_at FROM predictions_log", conn)
    finally:
        conn.close()


def run_batch_validation(
    rows: pd.DataFrame,
    contract: DataContract,
    alert_sink=None,
) -> List[SlaBreach]:
    """Validate one landed batch against the contract SLA and alert on breaches.

    Returns the list of breaches (empty = batch is healthy). ``alert_sink`` is
    injectable for tests; defaults to logging ALERT lines — the blog diagram's
    "alerting app on batch validation failure".
    """
    alert = alert_sink or (lambda msg: logger.warning(msg))
    breaches = contract.validate_sla(rows)

    print(f"\nBatch SLA validation — contract '{contract.name}' ({len(rows)} rows)")
    print("-" * 55)
    if not breaches:
        print("All SLA checks passed (freshness, completeness, volume, ranges)")
        logger.info("Batch passed all SLA checks")
    for breach in breaches:
        print(f"BREACH  {breach}")
        alert(f"ALERT [sla-breach] contract={contract.name} {breach}")
    print("-" * 55)
    return breaches


def log_to_mlflow(rows: pd.DataFrame, breaches: List[SlaBreach]) -> None:
    """Track batch-validation history next to training runs and drift reports."""
    try:
        mlflow.set_tracking_uri("http://localhost:5000")
        mlflow.set_experiment("batch-validation")
        with mlflow.start_run(run_name="sla-check"):
            mlflow.log_param('rows_checked', len(rows))
            mlflow.log_metric('sla_breaches', len(breaches))
            for i, breach in enumerate(breaches):
                mlflow.log_param(f'breach_{i}', str(breach))
        logger.info("Validation result logged to MLflow (experiment 'batch-validation')")
    except Exception as e:
        logger.warning(f"Could not log to MLflow (report above is still valid): {e}")


def main() -> None:
    contract = ContractRegistry().get('listing_event')
    rows = load_landed_data()
    breaches = run_batch_validation(rows, contract)
    log_to_mlflow(rows, breaches)
    # Exit code doubles as a cron-friendly signal for schedulers.
    raise SystemExit(1 if breaches else 0)


if __name__ == '__main__':
    main()
