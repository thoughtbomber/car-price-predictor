"""Unit tests for batch SLA validation (blog step 7 + its alerting path)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pytest

from batch_validation import run_batch_validation
from contract_registry import ContractRegistry


@pytest.fixture(scope="module")
def contract():
    return ContractRegistry().get("listing_event")


def _batch(**overrides):
    data = {
        "model": ["Fiesta", "Focus"],
        "year": [2020, 2019],
        "transmission": ["Manual", "Automatic"],
        "mileage": [10000, 20000],
        "fuelType": ["Petrol", "Diesel"],
        "tax": [150.0, 145.0],
        "mpg": [50.0, 60.0],
        "engineSize": [1.0, 1.5],
        "predicted_at": [pd.Timestamp.now(tz="UTC")] * 2,
    }
    data.update(overrides)
    return pd.DataFrame(data)


def test_healthy_batch_passes_without_alerts(contract):
    alerts = []
    breaches = run_batch_validation(_batch(), contract, alert_sink=alerts.append)
    assert breaches == []
    assert alerts == []


def test_null_features_raise_completeness_alert(contract):
    alerts = []
    batch = _batch()
    batch.loc[1, "tax"] = None

    breaches = run_batch_validation(batch, contract, alert_sink=alerts.append)

    assert any(b.rule == "completeness.tax" for b in breaches)
    # The blog diagram's alert path: every breach must fire an alert.
    assert len(alerts) == len(breaches)
    assert "ALERT" in alerts[0] and "completeness.tax" in alerts[0]


def test_stale_batch_raises_freshness_alert(contract):
    alerts = []
    batch = _batch(predicted_at=[pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=2)] * 2)

    breaches = run_batch_validation(batch, contract, alert_sink=alerts.append)

    assert any(b.rule == "freshness" for b in breaches)
    assert any("freshness" in a for a in alerts)


def test_empty_batch_raises_volume_alert(contract):
    alerts = []
    breaches = run_batch_validation(_batch().iloc[0:0], contract, alert_sink=alerts.append)
    assert [b.rule for b in breaches] == ["volume"]
    assert len(alerts) == 1


def test_out_of_range_values_raise_range_alert(contract):
    alerts = []
    batch = _batch(mileage=[10000, -5])

    breaches = run_batch_validation(batch, contract, alert_sink=alerts.append)

    assert any(b.rule == "ranges.mileage" for b in breaches)
