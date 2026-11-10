"""A changed event is consumed in one transaction under the tenant coordinator."""

from contextlib import contextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest

from keep.correlation import consumer, runtime


class Database:
    """Answer only the statements a non-source changed event reaches."""

    def __init__(self, *, duplicate=False):
        self.duplicate = duplicate
        self.transactions = []

    @contextmanager
    def transaction(self, *args, **kwargs):
        statements = []
        self.transactions.append(statements)
        yield SimpleNamespace(execute=lambda sql, params=None: self.execute(statements, sql))

    def execute(self, statements, sql):
        statements.append(sql)
        if "SELECT 1 FROM kafka_correlation_events" in sql:
            return SimpleNamespace(fetchone=lambda: {"?column?": 1} if self.duplicate else None)
        return SimpleNamespace(fetchone=lambda: None)


@pytest.fixture
def record(monkeypatch):
    tenant, events = uuid4(), []
    envelope = SimpleNamespace(
        tenant_id=str(tenant), alert_id=str(uuid4()), event_id=str(uuid4()), cause="manual"
    )
    monkeypatch.setattr(consumer, "parse_json", lambda value: value)
    monkeypatch.setattr(
        consumer, "ChangedEnvelope", SimpleNamespace(model_validate=lambda value: envelope)
    )
    monkeypatch.setattr(
        runtime,
        "prepare_tenant",
        lambda *args, **kwargs: events.append("prepare") or SimpleNamespace(maintenance=None),
    )
    monkeypatch.setattr(runtime, "worker_turn", lambda *args, **kwargs: events.append("turn"))
    monkeypatch.setattr(
        consumer, "coordinator", lambda conn, scope: conn.execute("SELECT coordinator")
    )
    message = SimpleNamespace(
        value=lambda: b"{}",
        key=lambda: tenant.bytes,
        topic=lambda: "changed",
        partition=lambda: 0,
        offset=lambda: 3,
    )
    settings = SimpleNamespace(environment="test")
    return lambda db: consumer.correlate_record(db, settings, None, message), events


def test_one_transaction_consumes_under_the_coordinator_without_a_gate(record):
    """Requirements: REV-39."""
    correlate, events = record
    db = Database()
    correlate(db)
    # prepare_tenant keeps its own read-only snapshot, and offsets/positions are
    # written by the transport. Correlation is always on, so no gate is read.
    assert events == ["prepare"]
    assert len(db.transactions) == 1
    statements = db.transactions[0]
    assert statements[0] == "SELECT coordinator"
    assert not any("capability_gates" in sql for sql in statements)
    assert "INSERT INTO kafka_correlation_events" in statements[-1]


def test_replayed_event_is_recognized_and_only_reasserts_its_identity(record, monkeypatch):
    correlate, events = record
    db = Database(duplicate=True)
    envelope = consumer.ChangedEnvelope.model_validate(None)
    monkeypatch.setattr(envelope, "cause", "source")
    correlate(db)
    # A source replay after a lost offset commit skips proof checks and decisions.
    assert events == ["prepare"]
    statements = db.transactions[0]
    assert not any("kafka_source_results" in sql or "projection_work" in sql for sql in statements)
    assert "ON CONFLICT DO NOTHING" in statements[-1]
