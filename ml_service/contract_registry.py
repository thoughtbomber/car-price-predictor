"""Contract Registry — blog step 1 (see docs/blog/data-system-summary.md).

In the blog architecture, schema changes are version-controlled and pushed to a
central *Data Contract Registry* holding Schema + SLA. Both data producers and
data consumers fetch the contract from there, so "what a valid event looks like"
has exactly one source of truth.

In this teaching repo the registry is deliberately simple: versioned JSON files
in ``contracts/`` (git IS the version control) plus this loader class. A full
platform would swap the file listing for an HTTP schema-registry API — the
consumer code below would not change.

A complete data contract has four parts, all stored in one document:

- **Schema**   — the JSON Schema proper (structure & types). Enforced per-event.
- **SLA**      — ``x-sla``: freshness / completeness / volume / range guarantees.
                 Enforced *in batch* on landed data by ``batch_validation.py`` —
                 you cannot check "the stream is fresh" one event at a time.
- **Semantics**— ``x-semantics``: what each field MEANS (units, conventions).
- **Lineage**  — ``x-lineage``: producer, source table, CDC path, topic.

Usage::

    registry = ContractRegistry()                 # loads contracts/*.json
    contract = registry.get("listing_event", 1)   # a single versioned contract
    errors = contract.validate(event)             # schema check, per event
    breaches = contract.validate_sla(df)          # SLA check, per batch
"""
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import jsonschema
import pandas as pd

logger = logging.getLogger(__name__)

DEFAULT_CONTRACTS_DIR = Path(__file__).parent.parent / 'contracts'


@dataclass
class SlaBreach:
    """One violated SLA rule — what failed, where, and by how much."""
    rule: str          # e.g. 'completeness.mileage', 'freshness', 'volume'
    detail: str        # human-readable explanation for the alert

    def __str__(self) -> str:
        return f"{self.rule}: {self.detail}"


class DataContract:
    """One versioned contract: schema validation + SLA batch validation."""

    def __init__(self, document: Dict[str, Any]):
        self.document = document
        self.name: str = document.get('title', '<unnamed>')
        self._validator = jsonschema.Draft202012Validator(document)

    # -- Schema (part 1 of the contract) ------------------------------------
    # Checked per event, in the stream, BEFORE the data is allowed downstream.
    def validate(self, event: Dict[str, Any]) -> List[str]:
        """Return a list of schema errors for one event ([] means valid)."""
        return [
            f"{'/'.join(str(p) for p in e.absolute_path) or '<root>'}: {e.message}"
            for e in self._validator.iter_errors(event)
        ]

    def is_valid(self, event: Dict[str, Any]) -> bool:
        return not self.validate(event)

    # -- SLA (part 2 of the contract) ---------------------------------------
    # Checked per batch, on landed data, by batch_validation.py.
    def sla(self) -> Dict[str, Any]:
        return self.document.get('x-sla', {})

    def validate_sla(self, rows: pd.DataFrame) -> List[SlaBreach]:
        """Check a landed batch against the SLA; return all breaches found."""
        sla = self.sla()
        breaches: List[SlaBreach] = []
        if not sla:
            return breaches

        # Volume: an empty batch means the stream silently stopped.
        min_rows = sla.get('volume', {}).get('min_rows', 0)
        if len(rows) < min_rows:
            breaches.append(SlaBreach(
                'volume', f"only {len(rows)} rows landed, SLA requires >= {min_rows}"))
            return breaches  # nothing else is checkable on an empty batch

        # Completeness: no nulls allowed in the listed feature columns.
        comp = sla.get('completeness', {})
        max_null = comp.get('max_null_fraction', 0.0)
        for field in comp.get('fields', []):
            if field not in rows.columns:
                breaches.append(SlaBreach(
                    f'completeness.{field}', 'column missing from landed batch'))
                continue
            null_frac = float(rows[field].isna().mean())
            if null_frac > max_null:
                breaches.append(SlaBreach(
                    f'completeness.{field}',
                    f"{null_frac:.1%} nulls, SLA allows at most {max_null:.1%}"))

        # Freshness: the newest row proves the pipeline is still alive.
        fresh = sla.get('freshness', {})
        field, max_age = fresh.get('field'), fresh.get('max_age_hours')
        if field and max_age is not None:
            if field not in rows.columns:
                breaches.append(SlaBreach('freshness', f"timestamp column '{field}' missing"))
            else:
                stamps = pd.to_datetime(rows[field], utc=True, errors='coerce')
                newest = stamps.max()
                if pd.isna(newest):
                    breaches.append(SlaBreach(
                        'freshness', f"no parseable timestamps in '{field}'"))
                else:
                    age = datetime.now(timezone.utc) - newest.to_pydatetime()
                    if age > timedelta(hours=max_age):
                        breaches.append(SlaBreach(
                            'freshness',
                            f"newest row is {age} old, SLA allows {max_age}h"))

        # Ranges: landed values must stay inside the promised bounds.
        for field, bounds in sla.get('ranges', {}).items():
            if field not in rows.columns:
                continue
            values = pd.to_numeric(rows[field], errors='coerce')
            if 'min' in bounds:
                bad = values < bounds['min']
                if bounds.get('exclusiveMin'):
                    bad = values <= bounds['min']
                if bool(bad.any()):
                    breaches.append(SlaBreach(
                        f'ranges.{field}',
                        f"{int(bad.sum())} rows below min {bounds['min']}"))
            if 'max' in bounds and bool((values > bounds['max']).any()):
                breaches.append(SlaBreach(
                    f'ranges.{field}',
                    f"{int((values > bounds['max']).sum())} rows above max {bounds['max']}"))

        return breaches

    # -- Semantics & lineage (parts 3 & 4) ----------------------------------
    # Not machine-checked — they are the human-readable half of the contract.
    def semantics(self) -> Dict[str, str]:
        return self.document.get('x-semantics', {})

    def lineage(self) -> Dict[str, Any]:
        return self.document.get('x-lineage', {})


class ContractRegistry:
    """The central registry: one place every producer/consumer fetches contracts from.

    Blog mapping: "schema changes are implemented in version control; once
    approved, they are pushed to ... a central Data Contract Registry". Here,
    git + the contracts/ directory play both roles.
    """

    def __init__(self, contracts_dir: Optional[Path] = None):
        self.contracts_dir = Path(contracts_dir or DEFAULT_CONTRACTS_DIR)
        self._contracts: Dict[str, DataContract] = {}
        self._load_all()

    def _load_all(self) -> None:
        """Load every ``<name>_v<version>.json`` contract found in the directory."""
        for path in sorted(self.contracts_dir.glob('*_v*.json')):
            document = json.loads(path.read_text(encoding='utf-8'))
            stem = path.stem  # e.g. 'listing_event_v1'
            name, _, version = stem.rpartition('_v')
            self._contracts[f'{name}_v{version}'] = DataContract(document)
            logger.info(f"Registered contract '{name}' version {version} from {path.name}")

    def get(self, name: str, version: Optional[int] = None) -> DataContract:
        """Fetch a contract by name; latest version unless ``version`` is given."""
        if version is not None:
            key = f'{name}_v{version}'
            if key not in self._contracts:
                raise KeyError(f"Contract '{key}' not found in {self.contracts_dir}")
            return self._contracts[key]
        versions = sorted(
            (k for k in self._contracts if k.startswith(f'{name}_v')),
            key=lambda k: int(k.rsplit('_v', 1)[1]),
        )
        if not versions:
            raise KeyError(f"No contract named '{name}' in {self.contracts_dir}")
        return self._contracts[versions[-1]]

    def list_contracts(self) -> List[str]:
        return sorted(self._contracts)
