"""DLQ Alerting & Recovery apps — blog step 4 (see docs/blog/data-system-summary.md).

When the ML service (ml_service/main.py) rejects an event that violates the
data contract, it does not drop it — it publishes the event plus the error to
the dead letter topic ``cars-db.public.listings.dlq``. The blog diagram hangs
two small applications off that topic:

- **AlertingApp** — notifies a human that bad data arrived. Here it logs a
  structured ALERT line per failed event (a real deployment would page
  Slack/PagerDuty — the handling logic is identical).
- **RecoveryApp** — tries to *repair* failed events and re-inject them into the
  pipeline. Here it applies safe, mechanical fixes (trim whitespace, coerce
  numeric strings, normalise enum casing), re-validates against the contract,
  and republishes recovered events to the listings topic. Events it cannot fix
  are reported so a human can decide.

Both consume the same DLQ record shape produced by ``CarPricePredictor.send_to_dlq``::

    {"error": "<schema error text>", "message": <original event>, "failed_at": "..."}

Run them standalone (teaching demos)::

    python dlq_tools.py --alert      # tail the DLQ and raise alerts
    python dlq_tools.py --recover    # tail the DLQ and attempt repairs

Kafka I/O is injected so the classes are fully testable offline.
"""
import argparse
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional

from contract_registry import ContractRegistry, DataContract

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

KAFKA_TOPIC_LISTINGS = 'cars-db.public.listings'
KAFKA_TOPIC_DLQ = 'cars-db.public.listings.dlq'

# Casing used by the contract enums — lets recovery fix 'manual' -> 'Manual'.
ENUM_FIXES = {
    'transmission': {'manual': 'Manual', 'automatic': 'Automatic', 'semi-auto': 'Semi-Auto'},
    'fuelType': {'petrol': 'Petrol', 'diesel': 'Diesel', 'hybrid': 'Hybrid', 'electric': 'Electric'},
}
# Fields the schema declares numeric — lets recovery coerce "15000" -> 15000.
NUMERIC_FIELDS = ['id', 'year', 'price', 'mileage', 'tax', 'mpg', 'engineSize']


class AlertingApp:
    """Consumes the DLQ and raises an alert for every failed event."""

    def __init__(self, alert_sink: Optional[Callable[[str], None]] = None):
        # Injectable sink keeps the app testable; defaults to a warning log.
        self.alert_sink = alert_sink or (lambda msg: logger.warning(msg))
        self.alerts_raised = 0

    def handle_record(self, record: Dict[str, Any]) -> str:
        """Build and emit the alert for one DLQ record; returns the alert text."""
        alert = (
            f"ALERT [data-contract-violation] "
            f"failed_at={record.get('failed_at', '?')} "
            f"error={record.get('error', '?')} "
            f"event={json.dumps(record.get('message'), default=str)}"
        )
        self.alert_sink(alert)
        self.alerts_raised += 1
        return alert


class RecoveryApp:
    """Consumes the DLQ, repairs what it safely can, republishes the rest as failures."""

    def __init__(
        self,
        contract: Optional[DataContract] = None,
        republish: Optional[Callable[[str, Dict[str, Any]], None]] = None,
    ):
        # The contract is the arbiter of "fixed": a repaired event is only
        # republished if it passes schema validation again.
        self.contract = contract or ContractRegistry().get('listing_event')
        # Injectable publisher (topic, event) — defaults set up in run().
        self.republish = republish
        self.recovered: List[Dict[str, Any]] = []
        self.unrecoverable: List[Dict[str, Any]] = []

    @staticmethod
    def attempt_fix(event: Dict[str, Any]) -> Dict[str, Any]:
        """Apply only SAFE, mechanical repairs — never invent business data.

        Examples of safe fixes: trimming whitespace, coercing "15000" -> 15000
        for numeric fields, fixing enum casing. Inventing a missing price or a
        missing mileage would be a data-quality lie, so those stay broken.
        """
        fixed = dict(event)
        for key, value in list(fixed.items()):
            if isinstance(value, str):
                fixed[key] = value.strip()
        for field in NUMERIC_FIELDS:
            value = fixed.get(field)
            if isinstance(value, str):
                try:
                    fixed[field] = float(value) if '.' in value else int(value)
                except ValueError:
                    pass  # not coercible — leave it for the validator to reject
        for field, mapping in ENUM_FIXES.items():
            value = fixed.get(field)
            if isinstance(value, str) and value.lower() in mapping:
                fixed[field] = mapping[value.lower()]
        return fixed

    def handle_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Try to recover one DLQ record; republish on success, report on failure."""
        original = record.get('message')
        if not isinstance(original, dict):
            self.unrecoverable.append(record)
            logger.error(f"UNRECOVERABLE (no event payload): {record.get('error')}")
            return None

        candidate = self.attempt_fix(original)
        errors = self.contract.validate(candidate)

        if errors:
            self.unrecoverable.append(record)
            logger.error(
                f"UNRECOVERABLE event id={original.get('id')}: "
                f"still invalid after repair — {'; '.join(errors)}"
            )
            return None

        self.recovered.append(candidate)
        if self.republish:
            self.republish(KAFKA_TOPIC_LISTINGS, candidate)
        logger.info(
            f"RECOVERED event id={candidate.get('id')} "
            f"(was: {record.get('error')}) -> republished to {KAFKA_TOPIC_LISTINGS}"
        )
        return candidate


def _run(alert: bool, recover: bool) -> None:
    """Standalone teaching runner: tail the DLQ with the selected app(s)."""
    from kafka import KafkaConsumer  # imported here so tests never need Kafka

    consumer = KafkaConsumer(
        KAFKA_TOPIC_DLQ,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='dlq-tools',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )
    alerting = AlertingApp() if alert else None
    recovery = RecoveryApp() if recover else None
    logger.info(f"Listening on {KAFKA_TOPIC_DLQ} (alert={alert}, recover={recover})")

    try:
        while True:
            for _tp, messages in consumer.poll(timeout_ms=1000).items():
                for message in messages:
                    if alerting:
                        alerting.handle_record(message.value)
                    if recovery:
                        recovery.handle_record(message.value)
            time.sleep(0.1)
    except KeyboardInterrupt:
        logger.info("Stopping DLQ tools")
    finally:
        consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--alert', action='store_true', help='run the alerting app')
    parser.add_argument('--recover', action='store_true', help='run the recovery app')
    args = parser.parse_args()
    if not (args.alert or args.recover):
        parser.error('choose at least one of --alert / --recover')
    _run(args.alert, args.recover)
