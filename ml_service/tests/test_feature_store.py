"""Unit tests for the Feature Store (blog step 8: the four feature-store APIs)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pytest

from feature_store import FeatureStore

FEATURES_1 = {"id": 1, "model": "Fiesta", "year": 2020, "transmission": "Manual",
              "mileage": 10000, "fuelType": "Petrol", "tax": 150.0, "mpg": 50.0,
              "engineSize": 1.0}
FEATURES_2 = {"id": 2, "model": "Focus", "year": 2019, "transmission": "Automatic",
              "mileage": 20000, "fuelType": "Diesel", "tax": 145.0, "mpg": 60.0,
              "engineSize": 1.5}


@pytest.fixture
def store(tmp_path):
    return FeatureStore(root=tmp_path)


# --- Real-Time Feature Ingestion / Serving (online path) ---

def test_online_round_trip(store):
    store.ingest_online("listing_features", FEATURES_1)
    assert store.serve_online("listing_features", 1) == FEATURES_1


def test_online_upsert_overwrites(store):
    store.ingest_online("listing_features", FEATURES_1)
    updated = dict(FEATURES_1, mileage=11000)
    store.ingest_online("listing_features", updated)
    assert store.serve_online("listing_features", 1)["mileage"] == 11000


def test_online_serve_missing_entity_returns_none(store):
    assert store.serve_online("listing_features", 999) is None


def test_online_ingest_requires_entity_key(store):
    with pytest.raises(ValueError, match="entity key"):
        store.ingest_online("listing_features", {"model": "Fiesta"})


# --- Batch Feature Ingestion / Serving (offline path) ---

def test_batch_round_trip(store):
    df = pd.DataFrame([FEATURES_1, FEATURES_2])
    store.ingest_batch("listing_features", df)
    served = store.serve_batch("listing_features")
    assert len(served) == 2
    assert set(served.columns) == set(df.columns)


def test_batch_serve_missing_group_raises(store):
    with pytest.raises(FileNotFoundError):
        store.serve_batch("nope")


# --- Train/serve parity: both tiers must serve the SAME features ---

def test_promote_online_to_batch_parity(store):
    store.ingest_online("listing_features", FEATURES_1)
    store.ingest_online("listing_features", FEATURES_2)

    df = store.promote_online_to_batch("listing_features")
    assert len(df) == 2

    served_batch = store.serve_batch("listing_features")
    served_online = store.serve_online("listing_features", 1)
    row = served_batch[served_batch["id"] == 1].iloc[0]
    # The feature the model trains on equals the feature it is served.
    assert row["model"] == served_online["model"]
    assert row["mileage"] == served_online["mileage"]
    assert set(served_batch.columns) == set(served_online.keys())
