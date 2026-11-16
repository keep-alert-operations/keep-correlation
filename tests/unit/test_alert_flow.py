"""The steps of one alert change inside correlation, as log records."""

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from keep.correlation import consumer, runtime
from keep.observability import alert_fields, flow as alert_flow
from keep.observability.testing import capture_records
from keep.shared.errors import unavailable

SNAPSHOT = {"application": "billing", "component": "database", "severity": "critical"}


class Database:
    """Answers the statements a source-caused changed event reaches."""

    def __init__(self, *, duplicate=False, journal=True, value=b"{}"):
        self.duplicate, self.journal, self.value = duplicate, journal, value

    @contextmanager
    def transaction(self, *args, **kwargs):
        yield SimpleNamespace(execute=lambda sql, params=None: self.execute(sql))

    def execute(self, sql):
        row = None
        if "SELECT 1 FROM kafka_correlation_events" in sql:
            row = {"?column?": 1} if self.duplicate else None
        elif "FROM kafka_source_results r" in sql:
            row = {"delivery_id": "delivery-1", "changed_envelope": self.value, "retired_at": None}
        elif "FROM projection_work p" in sql:
            row = {"snapshot": SNAPSHOT, "change_order": 5 if self.journal else None}
        elif "correlation_consumer_state" in sql:
            row = {"pending": False}
        return SimpleNamespace(fetchone=lambda: row)


@pytest.fixture
def records():
    return capture_records("correlation", "correlation")


@pytest.fixture
def change(monkeypatch):
    tenant = uuid4()
    envelope = SimpleNamespace(
        tenant_id=str(tenant),
        alert_id=str(uuid4()),
        event_id=str(uuid4()),
        cause="source",
        change_order="5",
        projection_seq="9",
        native_version="v2",
        recorded_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    )
    monkeypatch.setattr(consumer, "parse_json", lambda value: value)
    monkeypatch.setattr(
        consumer, "ChangedEnvelope", SimpleNamespace(model_validate=lambda value: envelope)
    )
    monkeypatch.setattr(
        runtime, "prepare_tenant", lambda *args, **kwargs: SimpleNamespace(maintenance=None)
    )
    monkeypatch.setattr(consumer, "coordinator", lambda conn, scope: None)
    message = SimpleNamespace(
        value=lambda: b"{}",
        key=lambda: tenant.bytes,
        topic=lambda: "changed",
        partition=lambda: 4,
        offset=lambda: 30,
    )
    return envelope, lambda db: consumer.correlate_record(db, None, None, message)


def deciding(monkeypatch, attachments=(), fail=None):
    """Stand in for the correlation pass: note each attachment as the shared code does."""

    def turn(*args, **kwargs):
        for alert_id, outcome in attachments:
            alert_flow.note(
                "alert_incident_attached",
                alert=alert_fields(SNAPSHOT, alert_id=alert_id),
                incident_id="incident-1",
                rule_id="rule-1",
                outcome=outcome,
            )
        if fail:
            raise fail
        return 1

    monkeypatch.setattr(runtime, "worker_turn", turn)


def actions(records):
    return [(r["event"]["sequence"], r["event"]["action"]) for r in records]


def test_a_change_that_creates_an_incident_leaves_its_steps_in_order(
    records, change, monkeypatch
):
    """Requirements: REV-74; observability 3."""
    envelope, correlate = change
    deciding(monkeypatch, [(envelope.alert_id, "incident_created"), ("other", "incident_created")])
    correlate(Database())
    consumed, own, other, ended = records()
    assert actions([consumed, own, other, ended]) == [
        (1, "alert_change_consumed"),
        (2, "alert_incident_attached"),
        (3, "alert_incident_attached"),
        (4, "alert_correlated"),
    ]
    for record in (consumed, own, ended):
        assert record["keep"]["alert"]["id"] == envelope.alert_id
        assert record["keep"]["changed_event_id"] == envelope.event_id
        assert record["keep"]["kafka"]["partition"] == 4
        assert record["service"]["name"] == "correlation"
    # The delivery that caused the change links these records to the processor's.
    assert own["keep"]["delivery_id"] == ended["keep"]["delivery_id"] == "delivery-1"
    assert own["keep"]["incident_id"] == "incident-1" and own["keep"]["rule_id"] == "rule-1"
    assert other["keep"]["alert"]["id"] == "other"
    assert ended["keep"]["outcome"] == "incident_created"
    assert ended["keep"]["alert"]["application"] == "billing"
    assert consumed["keep"]["cause"] == "source"


def test_a_change_no_rule_acts_on_ends_with_no_incident(records, change, monkeypatch):
    """Requirements: REV-74; observability 3."""
    _, correlate = change
    deciding(monkeypatch)
    correlate(Database())
    consumed, ended = records()
    assert ended["event"]["action"] == "alert_correlated"
    assert ended["keep"]["outcome"] == "no_incident"
    assert ended["keep"]["count"] == 1


@pytest.mark.parametrize(
    ("database", "cause", "reason"),
    [
        (Database(duplicate=True), "source", "duplicate"),
        (Database(), "manual", "not_source"),
        (Database(journal=False), "source", "unchanged"),
    ],
)
def test_a_change_that_needs_no_evaluation_says_why(
    records, change, monkeypatch, database, cause, reason
):
    """Requirements: REV-74; observability 3."""
    envelope, correlate = change
    envelope.cause = cause
    deciding(monkeypatch, fail=AssertionError("must not be evaluated"))
    correlate(database)
    consumed, skipped = records()
    assert skipped["event"]["action"] == "alert_correlation_skipped"
    assert skipped["keep"]["reason"] == reason


def test_a_rolled_back_decision_leaves_no_attachment_record(records, change, monkeypatch):
    """Requirements: REV-74; observability 3. Nothing is recorded as done until committed."""
    envelope, correlate = change
    deciding(
        monkeypatch,
        [(envelope.alert_id, "incident_created")],
        fail=unavailable("correlation_capacity"),
    )
    with pytest.raises(Exception):
        correlate(Database())
    assert [r["event"]["action"] for r in records()] == ["alert_change_consumed"]
