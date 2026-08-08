"""Unit tests for the DLQ Alerting & Recovery apps (blog step 4)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

from contract_registry import ContractRegistry
from dlq_tools import AlertingApp, RecoveryApp, KAFKA_TOPIC_LISTINGS

VALID_CAR = {
    "id": 1, "model": "Fiesta", "year": 2020, "price": 12000.0,
    "transmission": "Manual", "mileage": 10000, "fuelType": "Petrol",
    "tax": 150.0, "mpg": 50.0, "engineSize": 1.0, "predicted_price": None,
}


def dlq_record(message, error="schema violation"):
    return {"error": error, "message": message, "failed_at": "2026-01-01 00:00:00"}


@pytest.fixture
def contract():
    return ContractRegistry().get("listing_event")


# --- Alerting app ---

def test_alerting_emits_structured_alert():
    alerts = []
    app = AlertingApp(alert_sink=alerts.append)

    app.handle_record(dlq_record({"id": 7}, error="mpg is required"))

    assert app.alerts_raised == 1
    assert "ALERT" in alerts[0]
    assert "mpg is required" in alerts[0]
    assert '"id": 7' in alerts[0]


# --- Recovery app ---

def test_recovery_fixes_casing_and_numeric_strings(contract):
    republished = []
    app = RecoveryApp(contract=contract,
                      republish=lambda t, e: republished.append((t, e)))
    broken = dict(VALID_CAR, transmission="manual", mileage="10000")

    result = app.handle_record(dlq_record(broken))

    assert result is not None
    assert result["transmission"] == "Manual"
    assert result["mileage"] == 10000
    assert republished == [(KAFKA_TOPIC_LISTINGS, result)]
    assert app.unrecoverable == []


def test_recovery_leaves_unfixable_events_alone(contract):
    republished = []
    app = RecoveryApp(contract=contract,
                      republish=lambda t, e: republished.append((t, e)))
    # Missing 'mpg' — recovery must NOT invent business data.
    broken = {k: v for k, v in VALID_CAR.items() if k != "mpg"}
    record = dlq_record(broken, error="mpg is required")

    result = app.handle_record(record)

    assert result is None
    assert republished == []
    assert app.unrecoverable == [record]


def test_recovery_handles_missing_payload(contract):
    app = RecoveryApp(contract=contract, republish=lambda t, e: None)
    record = dlq_record(None)

    assert app.handle_record(record) is None
    assert app.unrecoverable == [record]
