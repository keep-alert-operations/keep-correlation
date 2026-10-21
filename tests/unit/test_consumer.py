"""A changed event is consumed in one transaction that also checks the gate."""

from contextlib import contextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest

from appchi.correlation import consumer, runtime
from appchi.shared.errors import AppError


class Database:
    """Answer only the statements a non-source changed event reaches."""

    def __init__(self, *, ready=True, duplicate=False):
        self.ready, self.duplicate = ready, duplicate
        self.transactions = []

    @contextmanager
    def transaction(self, *args, **kwargs):
        statements = []
        self.transactions.append(statements)
        yield SimpleNamespace(execute=lambda sql, params=None: self.execute(statements, sql))

    def execute(self, statements, sql):
        statements.append(sql)
        if "capability_gates" in sql:
            return SimpleNamespace(fetchone=lambda: {"ready": self.ready})
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


def test_one_transaction_reads_the_gate_under_the_coordinator_and_consumes(record):
    correlate, events = record
    db = Database()
    correlate(db)
    # prepare_tenant keeps its own read-only snapshot; the separate gate
    # transaction is gone, and offsets/positions are written by the transport.
    assert events == ["prepare"]
    assert len(db.transactions) == 1
    statements = db.transactions[0]
    assert statements[0] == "SELECT coordinator"
    assert "capability_gates" in statements[1]
    assert "INSERT INTO kafka_correlation_events" in statements[-1]


def test_closed_gate_raises_before_consuming(record):
    correlate, events = record
    db = Database(ready=False)
    with pytest.raises(AppError) as raised:
        correlate(db)
    assert raised.value.code == "correlation_not_ready"
    assert len(db.transactions) == 1
    assert not any("kafka_correlation_events" in sql for sql in db.transactions[0])


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
