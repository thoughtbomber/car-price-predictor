"""Unit tests for the Contract Registry (blog step 1: Schema + SLA + Semantics + Lineage)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import pytest

from contract_registry import ContractRegistry

VALID_CAR = {
    "id": 1, "model": "Fiesta", "year": 2020, "price": 12000.0,
    "transmission": "Manual", "mileage": 10000, "fuelType": "Petrol",
    "tax": 150.0, "mpg": 50.0, "engineSize": 1.0, "predicted_price": None,
}


@pytest.fixture(scope="module")
def registry():
    return ContractRegistry()  # reads the real contracts/ directory


@pytest.fixture(scope="module")
def contract(registry):
    return registry.get("listing_event")  # latest version


def test_registry_discovers_versioned_contracts(registry):
    assert "listing_event_v1" in registry.list_contracts()
    assert registry.get("listing_event", 1).name == "ListingEvent"


def test_schema_validation_accepts_valid_event(contract):
    assert contract.validate(dict(VALID_CAR)) == []


def test_schema_validation_rejects_bad_event(contract):
    bad = {k: v for k, v in VALID_CAR.items() if k != "mpg"}
    errors = contract.validate(bad)
    assert errors and "mpg" in errors[0]


def test_contract_carries_all_four_parts(contract):
    # A data contract = Schema + SLA + Semantics + Lineage (the blog's definition).
    assert contract.sla()["freshness"]["max_age_hours"] == 24
    assert "mileage" in contract.sla()["completeness"]["fields"]
    assert "GBP" in contract.semantics()["price"]
    assert contract.lineage()["topic"] == "cars-db.public.listings"


def _healthy_batch():
    return pd.DataFrame({
        "model": ["Fiesta", "Focus"],
        "year": [2020, 2019],
        "transmission": ["Manual", "Automatic"],
        "mileage": [10000, 20000],
        "fuelType": ["Petrol", "Diesel"],
        "tax": [150.0, 145.0],
        "mpg": [50.0, 60.0],
        "engineSize": [1.0, 1.5],
        "predicted_at": [pd.Timestamp.now(tz="UTC")] * 2,
    })


def test_sla_accepts_healthy_batch(contract):
    assert contract.validate_sla(_healthy_batch()) == []


def test_sla_flags_empty_batch(contract):
    breaches = contract.validate_sla(_healthy_batch().iloc[0:0])
    assert [b.rule for b in breaches] == ["volume"]


def test_sla_flags_nulls(contract):
    batch = _healthy_batch()
    batch.loc[0, "mileage"] = None
    breaches = contract.validate_sla(batch)
    assert any(b.rule == "completeness.mileage" for b in breaches)


def test_sla_flags_stale_data(contract):
    batch = _healthy_batch()
    batch["predicted_at"] = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=72)
    breaches = contract.validate_sla(batch)
    assert any(b.rule == "freshness" for b in breaches)


def test_sla_flags_out_of_range_values(contract):
    batch = _healthy_batch()
    batch.loc[0, "engineSize"] = 0.0  # SLA: exclusiveMin 0
    breaches = contract.validate_sla(batch)
    assert any(b.rule == "ranges.engineSize" for b in breaches)
