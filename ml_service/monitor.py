"""Drift monitoring: compare serving data against training data.

Blog mapping (docs/blog/data-system-summary.md): "Data and concept drift are
silent failures that cannot be captured in a Data Contract; they require
separate monitoring." This script IS that separate monitoring.

Reference distribution: data/ford.csv (what the model was trained on).
Current distribution:   cars_db.predictions_log (what the model has actually seen).

Computes the Population Stability Index (PSI) per feature — PSI > 0.2 is a
common "significant drift" threshold — prints a report, and logs the metrics
to MLflow (experiment "drift-monitoring") so a tracking trail builds up.

Run on demand:  python monitor.py
"""
import logging
import os
from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

NUMERIC_FEATURES = ['year', 'mileage', 'tax', 'mpg', 'engineSize']
CATEGORICAL_FEATURES = ['model', 'transmission', 'fuelType']

PSI_DRIFT_THRESHOLD = 0.2
DATABASE_URL = os.getenv(
    'DATABASE_URL', 'postgresql://postgres:postgres123@localhost:5432/cars_db')


def psi_numeric(reference: pd.Series, current: pd.Series, bins: int = 10) -> float:
    """PSI for a numeric feature, binned on reference quantiles"""
    edges = np.unique(np.quantile(reference.dropna(), np.linspace(0, 1, bins + 1)))
    if len(edges) < 2:
        return 0.0
    edges[0], edges[-1] = -np.inf, np.inf
    ref_pct = np.histogram(reference, bins=edges)[0] / max(len(reference), 1)
    cur_pct = np.histogram(current, bins=edges)[0] / max(len(current), 1)
    return _psi_from_shares(ref_pct, cur_pct)


def psi_categorical(reference: pd.Series, current: pd.Series) -> float:
    """PSI for a categorical feature, over the union of categories"""
    categories = reference.value_counts().index.union(current.value_counts().index)
    ref_pct = reference.value_counts(normalize=True).reindex(categories, fill_value=0).values
    cur_pct = current.value_counts(normalize=True).reindex(categories, fill_value=0).values
    return _psi_from_shares(ref_pct, cur_pct)


def _psi_from_shares(ref_pct: np.ndarray, cur_pct: np.ndarray, eps: float = 1e-6) -> float:
    ref_pct = np.clip(ref_pct, eps, None)
    cur_pct = np.clip(cur_pct, eps, None)
    return float(np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct)))


def load_reference() -> pd.DataFrame:
    data_path = Path(__file__).parent.parent / 'data' / 'ford.csv'
    return pd.read_csv(data_path)


def load_current() -> pd.DataFrame:
    conn = psycopg2.connect(DATABASE_URL)
    try:
        return pd.read_sql(
            "SELECT model, year, transmission, mileage, \"fuelType\", tax, mpg, "
            "\"engineSize\" FROM predictions_log", conn)
    finally:
        conn.close()


def main():
    reference = load_reference()
    current = load_current()
    logger.info(f"Reference rows: {len(reference)}, serving rows: {len(current)}")

    if current.empty:
        logger.warning("predictions_log is empty — nothing to compare yet. Add some cars first.")
        return

    report = {}
    for feature in NUMERIC_FEATURES:
        report[feature] = psi_numeric(reference[feature], current[feature])
    for feature in CATEGORICAL_FEATURES:
        report[feature] = psi_categorical(reference[feature], current[feature])

    print("\nDrift report (PSI, threshold {:.1f})".format(PSI_DRIFT_THRESHOLD))
    print("-" * 45)
    drifted = []
    for feature, value in sorted(report.items(), key=lambda kv: kv[1], reverse=True):
        flag = "DRIFT" if value > PSI_DRIFT_THRESHOLD else "ok"
        if value > PSI_DRIFT_THRESHOLD:
            drifted.append(feature)
        print(f"{feature:<15} {value:>8.4f}   {flag}")
    print("-" * 45)

    if drifted:
        logger.warning(f"Drift detected in: {', '.join(drifted)}")
    else:
        logger.info("No significant drift detected")

    # Log to MLflow so the drift history is tracked alongside training runs.
    try:
        mlflow.set_tracking_uri("http://localhost:5000")
        mlflow.set_experiment("drift-monitoring")
        with mlflow.start_run(run_name="drift-report"):
            mlflow.log_params({
                'reference_rows': len(reference),
                'serving_rows': len(current),
                'threshold': PSI_DRIFT_THRESHOLD,
            })
            mlflow.log_metrics({f"psi_{k}": v for k, v in report.items()})
            mlflow.log_metric('psi_max', max(report.values()))
            mlflow.log_metric('drifted_features', len(drifted))
        logger.info("Drift metrics logged to MLflow (experiment 'drift-monitoring')")
    except Exception as e:
        logger.warning(f"Could not log to MLflow (report above is still valid): {e}")


if __name__ == "__main__":
    main()
