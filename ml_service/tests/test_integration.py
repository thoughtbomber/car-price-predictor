"""Offline integration test: the blog's components working together, no Docker needed.

Flow under test (mirrors docs/interactive/index.html):

    raw event -> stream validation (main.py) -> DLQ on violation
              -> RecoveryApp repairs & republishes (dlq_tools.py)
              -> re-validated event scored by the model
              -> features ingested online (feature_store.py)
              -> promoted to the offline store (train/serve parity)
              -> sklearn Pipeline from train.py predicts on served features
              -> SLA batch validation passes on served output (batch_validation.py)
              -> drift PSI computable on served output (monitor.py)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pytest
from unittest.mock import MagicMock

from main import CarPricePredictor, KAFKA_TOPIC_DLQ, MODEL_FEATURES, FEATURE_GROUP
from contract_registry import ContractRegistry
from dlq_tools import RecoveryApp, KAFKA_TOPIC_LISTINGS
from batch_validation import run_batch_validation
from feature_store import FeatureStore
from monitor import psi_numeric
from train import build_pipeline, PARAMS

VALID_CAR = {
    "id": 1, "model": "Fiesta", "year": 2020, "price": 12000.0,
    "transmission": "Manual", "mileage": 10000, "fuelType": "Petrol",
    "tax": 150.0, "mpg": 50.0, "engineSize": 1.0, "predicted_price": None,
}


def kafka_msg(value):
    return MagicMock(value=value)


@pytest.fixture
def predictor(tmp_path, monkeypatch):
    monkeypatch.setenv("FEATURE_STORE_ROOT", str(tmp_path))
    p = CarPricePredictor()
    p.load_contract()
    p.producer = MagicMock()
    p.model = MagicMock()
    p.model.predict.return_value = [15000.0]
    p.model_version = "3"
    return p


def test_full_teaching_flow(predictor, tmp_path):
    # 1) A contract-violating event is rejected to the DLQ (blog steps 3-4).
    broken = dict(VALID_CAR, transmission="manual")  # enum casing violation
    assert predictor.process_message(kafka_msg(dict(broken))) is None
    predictor.producer.send.assert_called_once()
    topic = predictor.producer.send.call_args[0][0]
    dlq_record = predictor.producer.send.call_args[1]["value"]
    assert topic == KAFKA_TOPIC_DLQ

    # 2) The recovery app repairs it and republishes to the listings topic.
    republished = []
    recovery = RecoveryApp(contract=ContractRegistry().get("listing_event"),
                           republish=lambda t, e: republished.append((t, e)))
    fixed = recovery.handle_record(dlq_record)
    assert fixed is not None
    assert republished[0][0] == KAFKA_TOPIC_LISTINGS

    # 3) The recovered event now passes stream validation and gets scored.
    predictor.producer.reset_mock()
    result = predictor.process_message(kafka_msg(republished[0][1]))
    assert result is not None
    assert result["predicted_price"] == 15000.0
    predictor.producer.send.assert_not_called()  # nothing went back to the DLQ

    # 4) Its features were ingested into the online feature store (blog 8.1).
    served_online = predictor.feature_store.serve_online(FEATURE_GROUP, fixed["id"])
    assert served_online is not None
    assert served_online["transmission"] == "Manual"
    assert set(served_online.keys()) == {"id"} | set(MODEL_FEATURES)

    # 5) Promote online -> offline: batch serving yields the SAME features
    #    (train/serve parity, blog step 10).
    store = FeatureStore(root=tmp_path)
    store.promote_online_to_batch(FEATURE_GROUP)
    served_batch = store.serve_batch(FEATURE_GROUP)
    assert len(served_batch) == 1
    assert served_batch.iloc[0]["mileage"] == served_online["mileage"]

    # 6) The training pipeline (train.py) can train on repo data and score the
    #    served features unchanged — one pipeline for train and serve.
    ford = pd.read_csv(Path(__file__).resolve().parent.parent.parent
                       / "data" / "ford.csv").head(50)
    pipeline = build_pipeline({**PARAMS, "n_estimators": 5})
    pipeline.fit(ford.drop("price", axis=1), ford["price"])
    price = pipeline.predict(served_batch[MODEL_FEATURES])[0]
    assert price > 0

    # 7) The served output passes the batch SLA validation (blog step 7).
    landed = pd.DataFrame([{
        **{f: served_online[f] for f in MODEL_FEATURES},
        "predicted_at": pd.Timestamp.now(tz="UTC"),
    }])
    alerts = []
    breaches = run_batch_validation(
        landed, ContractRegistry().get("listing_event"), alert_sink=alerts.append)
    assert breaches == []
    assert alerts == []

    # 8) Drift monitoring can compare served features against training data.
    psi = psi_numeric(ford["mileage"], landed["mileage"])
    assert psi >= 0.0
