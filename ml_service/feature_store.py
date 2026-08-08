"""Feature Store — blog step 8 (see docs/blog/data-system-summary.md).

The blog's feature store exposes four APIs:

- **Real-Time Feature Ingestion** — features land straight from the validated
  stream (blog step 8.1). Fast, but the blog warns: SLA checks are hard in real
  time, so quality guarantees are weaker on this path.
- **Real-Time Feature Serving** — low-latency point lookup for ONE entity at
  inference time ("give me the features for listing 42").
- **Batch Feature Ingestion** — curated data lands in the offline store after
  batch validation (blog step 7 → 8). This is the high-quality path.
- **Batch Feature Serving** — whole datasets for training ("give me everything
  as a DataFrame").

Why a feature store at all? **Train/serve parity**: the same store feeds both
training (batch) and inference (online), so the features the model learns from
and the features it sees in production cannot silently diverge.

This teaching implementation keeps the machinery minimal so the concepts show
through:

- **Online store**  → SQLite table keyed by entity id (stand-in for Redis/DynamoDB).
- **Offline store** → one CSV per feature group under ``data/feature_store/``
  (stand-in for Parquet files in a data lake / warehouse).

Usage::

    store = FeatureStore()
    store.ingest_online('listing_features', {'id': 1, 'model': 'Fiesta', ...})
    store.serve_online('listing_features', 1)
    store.ingest_batch('listing_features', df)
    store.serve_batch('listing_features')
"""
import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_ROOT = Path(__file__).parent.parent / 'data' / 'feature_store'
ENTITY_KEY = 'id'


class FeatureStore:
    """A minimal two-tier feature store: online (SQLite) + offline (CSV files)."""

    def __init__(self, root: Optional[Path] = None, db_path: Optional[Path] = None):
        self.root = Path(root or DEFAULT_ROOT)
        self.root.mkdir(parents=True, exist_ok=True)
        # The online store is one SQLite file next to the offline CSVs.
        self.db_path = Path(db_path or self.root / 'online_store.db')
        self._init_db()

    # ------------------------------------------------------------------
    # Online path (blog: Real-Time Feature Ingestion / Serving)
    # ------------------------------------------------------------------
    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                # One row per (feature group, entity). The feature payload is
                # stored as JSON — a real store would use typed columns.
                "CREATE TABLE IF NOT EXISTS online_features ("
                "  feature_group TEXT NOT NULL,"
                "  entity_id     TEXT NOT NULL,"
                "  features      TEXT NOT NULL,"
                "  updated_at    TEXT DEFAULT CURRENT_TIMESTAMP,"
                "  PRIMARY KEY (feature_group, entity_id)"
                ")"
            )

    def ingest_online(self, feature_group: str, features: Dict[str, Any]) -> None:
        """Upsert one entity's features — called per validated stream event.

        NOTE (blog caveat): this real-time path skips SLA batch checks, exactly
        like the blog's step 8.1 — speed is traded for weaker quality guarantees.
        """
        entity_id = features.get(ENTITY_KEY)
        if entity_id is None:
            raise ValueError(f"online ingestion requires an '{ENTITY_KEY}' entity key")
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO online_features "
                "(feature_group, entity_id, features, updated_at) "
                "VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                (feature_group, str(entity_id), json.dumps(features, default=str)),
            )
        logger.debug(f"online ingest [{feature_group}] entity {entity_id}")

    def serve_online(self, feature_group: str, entity_id: Any) -> Optional[Dict[str, Any]]:
        """Point lookup of one entity's features at inference time."""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT features FROM online_features "
                "WHERE feature_group = ? AND entity_id = ?",
                (feature_group, str(entity_id)),
            ).fetchone()
        return json.loads(row[0]) if row else None

    # ------------------------------------------------------------------
    # Offline path (blog: Batch Feature Ingestion / Serving)
    # ------------------------------------------------------------------
    def _offline_path(self, feature_group: str) -> Path:
        return self.root / f'{feature_group}.csv'

    def ingest_batch(self, feature_group: str, rows: pd.DataFrame) -> Path:
        """Persist a curated batch (e.g. training features after validation).

        This is the blog's step 7 → 8 hand-off: only data that passed the batch
        SLA validation should reach the offline store, so training always sees
        the highest-quality slice of the data.
        """
        path = self._offline_path(feature_group)
        rows.to_csv(path, index=False)
        logger.info(f"batch ingest [{feature_group}] {len(rows)} rows -> {path}")
        return path

    def serve_batch(self, feature_group: str) -> pd.DataFrame:
        """Read a whole feature group as a DataFrame — the training-time API."""
        path = self._offline_path(feature_group)
        if not path.exists():
            raise FileNotFoundError(
                f"No offline features for '{feature_group}' at {path}")
        return pd.read_csv(path)

    # ------------------------------------------------------------------
    # Convenience: mirror one entity into the offline store
    # ------------------------------------------------------------------
    def promote_online_to_batch(self, feature_group: str) -> pd.DataFrame:
        """Fold the whole online store into the offline store.

        Teaching helper: in production a scheduled job curates stream-landed
        data through batch validation before it becomes training data. Here we
        simply dump the online rows so tests and demos can show both tiers
        serving the SAME features (train/serve parity).
        """
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT features FROM online_features WHERE feature_group = ?",
                (feature_group,),
            ).fetchall()
        df = pd.DataFrame([json.loads(r[0]) for r in rows])
        if not df.empty:
            self.ingest_batch(feature_group, df)
        return df
