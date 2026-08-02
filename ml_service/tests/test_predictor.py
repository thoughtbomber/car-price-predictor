import os

import pandas as pd
import pytest
from unittest.mock import MagicMock

from main import CarPricePredictor, KAFKA_TOPIC_DLQ, MODEL_FEATURES

VALID_CAR = {
    "id": 1,
    "model": "Fiesta",
    "year": 2020,
    "price": 12000.0,
    "transmission": "Manual",
    "mileage": 10000,
    "fuelType": "Petrol",
    "tax": 150.0,
    "mpg": 50.0,
    "engineSize": 1.0,
    "predicted_price": None,
}


def kafka_msg(value):
    return MagicMock(value=value)


@pytest.fixture
def predictor():
    p = CarPricePredictor()
    p.load_contract()  # reads contracts/listing_event_v1.json, no external services needed
    p.producer = MagicMock()
    p.model = MagicMock()
    p.model.predict.return_value = [15000.0]
    p.model_version = "3"
    return p


def test_valid_raw_json_accepted(predictor):
    result = predictor.process_message(kafka_msg(dict(VALID_CAR)))

    assert result is not None
    assert result["predicted_price"] == 15000.0
    assert result["model_version"] == "3"
    predictor.producer.send.assert_not_called()  # nothing went to the DLQ


def test_valid_debezium_envelope_accepted(predictor):
    envelope = {"payload": {"before": None, "after": dict(VALID_CAR), "op": "c"}}

    result = predictor.process_message(kafka_msg(envelope))

    assert result is not None
    assert result["id"] == VALID_CAR["id"]


def test_missing_field_goes_to_dlq(predictor):
    bad = {k: v for k, v in VALID_CAR.items() if k != "mpg"}

    result = predictor.process_message(kafka_msg(dict(bad)))

    assert result is None
    predictor.producer.send.assert_called_once()
    topic, payload = predictor.producer.send.call_args[0][0], predictor.producer.send.call_args[1]["value"]
    assert topic == KAFKA_TOPIC_DLQ
    assert "mpg" in payload["error"]
    assert payload["message"] == bad


def test_invalid_enum_goes_to_dlq(predictor):
    bad = dict(VALID_CAR, transmission="Flappy-Paddle")

    result = predictor.process_message(kafka_msg(bad))

    assert result is None
    predictor.producer.send.assert_called_once()
    assert predictor.producer.send.call_args[0][0] == KAFKA_TOPIC_DLQ


def test_debezium_delete_event_skipped_without_dlq(predictor):
    tombstone = {"payload": {"before": dict(VALID_CAR), "after": None, "op": "d"}}

    result = predictor.process_message(kafka_msg(tombstone))

    assert result is None
    predictor.producer.send.assert_not_called()


def test_preprocess_passthrough_uses_model_features(predictor):
    df = predictor.preprocess_data(dict(VALID_CAR, extra_field="ignored"))

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == MODEL_FEATURES
    assert not df.isnull().values.any()


@pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1",
                    reason="integration test: needs MLflow + a trained production model")
def test_integration_predict_with_real_model():
    p = CarPricePredictor()
    p.load_contract()
    p.load_model()

    result = p.process_message(kafka_msg(dict(VALID_CAR)))

    assert result is not None
    assert result["predicted_price"] > 0
