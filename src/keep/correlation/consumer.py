from dataclasses import replace
import logging
from uuid import UUID
from keep.event_models import ChangedEnvelope
from keep.ingestion.native import parse_json
from keep.kafka_changed import changed_flow, seconds_since
from keep.observability import alert_fields
from keep.shared.errors import unavailable
from keep.shared.transactions import coordinator

log = logging.getLogger(__name__)


def correlate_record(db, settings, profile, message):
    """Correlate one changed event and record its steps (observability 3).

    The steps are consumed, each alert the decision attached, and how correlation
    ended. Those describing a PostgreSQL effect are written after the commit.
    """
    envelope = ChangedEnvelope.model_validate(parse_json(message.value()))
    tenant = UUID(envelope.tenant_id)
    if message.key() != tenant.bytes:
        raise unavailable("changed_tenant_key_invalid")
    flow = changed_flow(log, "correlation", message, envelope)
    with flow:
        reason, passes = _correlate(db, profile, message, envelope, tenant, flow)
    # Committed: each attachment the decision made is written, then how it ended.
    flow.settle()
    attached = flow.recorded("alert_incident_attached")
    if reason:
        flow.step("alert_correlation_skipped", reason=reason, duration=flow.elapsed())
    else:
        created = any(step.get("outcome") == "incident_created" for step in attached)
        flow.step(
            "alert_correlated",
            outcome="incident_created" if created else "attached" if attached else "no_incident",
            count=passes,
            # Change commit to the end of the correlation pass (the 2-second objective).
            delay_seconds=seconds_since(getattr(envelope, "recorded_at", None)),
            duration=flow.elapsed(),
        )
    return envelope


def _correlate(db, profile, message, envelope, tenant, flow):
    """Run the correlation transaction; return (why it was not evaluated, passes)."""
    from keep.correlation.runtime import prepare_tenant, worker_turn

    reason, passes = None, 0
    prepared = prepare_tenant(db, tenant, profile, alert_ids=(envelope.alert_id,))
    with db.transaction(tenant) as conn:
        coordinator(conn, tenant)
        if prepared.maintenance:
            from keep.maintenance.runtime import InlineCorrelation, lock_catalog

            catalog = replace(
                prepared.maintenance, inline_correlation=InlineCorrelation(db, profile, prepared)
            )
            lock_catalog(conn, catalog, targets=[envelope.alert_id])
        duplicate = conn.execute(
            "SELECT 1 FROM kafka_correlation_events WHERE event_id=%s", (envelope.event_id,)
        ).fetchone()
        if duplicate:
            reason = "duplicate"
        elif envelope.cause != "source":
            reason = "not_source"
        if not duplicate and envelope.cause == "source":
            source = conn.execute(
                "SELECT r.delivery_id,r.changed_envelope,i.retired_at FROM kafka_source_results r LEFT JOIN alert_identities i ON i.tenant_id=r.tenant_id AND i.id=r.alert_id WHERE r.tenant_id=%s AND r.changed_event_id=%s",
                (tenant, envelope.event_id),
            ).fetchone()
            if source:
                # The delivery that caused this change: one ID across the services.
                flow.know(delivery_id=source.get("delivery_id"))
            if not source or (
                not source["retired_at"]
                and (
                    source["changed_envelope"] is None
                    or bytes(source["changed_envelope"]) != message.value()
                )
            ):
                raise unavailable("correlation_envelope_proof_invalid")
            proof = conn.execute(
                "SELECT p.snapshot,j.* FROM projection_work p LEFT JOIN correlation_journal j ON j.tenant_id=p.tenant_id AND j.alert_id=p.alert_id AND j.change_order=%s AND j.after_snapshot->>'history_id'=p.snapshot->>'history_id' WHERE p.tenant_id=%s AND p.seq=%s",
                (int(envelope.change_order), tenant, int(envelope.projection_seq)),
            ).fetchone()
            if not proof:
                retired = conn.execute(
                    "SELECT retired_at FROM alert_identities WHERE tenant_id=%s AND id=%s",
                    (tenant, envelope.alert_id),
                ).fetchone()
                if not retired or not retired["retired_at"]:
                    raise unavailable("correlation_transition_proof_missing")
                reason = "retired"
            elif proof["change_order"] is None:
                flow.know(alert=alert_fields(proof.get("snapshot")))
                reason = "unchanged"
            else:
                flow.know(alert=alert_fields(proof.get("snapshot")))
                passes = 1
                event = dict(proof, stream="kafka")
                worker_turn(
                    db,
                    tenant,
                    profile,
                    prepared_input=prepared,
                    input_event=event,
                    transaction=conn,
                    kafka=True,
                )
                # Candidate arbitration completes before this Kafka input advances.
                while (
                    state := conn.execute(
                        "SELECT pending FROM correlation_consumer_state WHERE tenant_id=%s",
                        (tenant,),
                    ).fetchone()
                ) and state["pending"]:
                    passes += 1
                    if not worker_turn(
                        db, tenant, profile, prepared_input=prepared, transaction=conn, kafka=True
                    ):
                        break
        conn.execute(
            "INSERT INTO kafka_correlation_events(event_id,tenant_id,topic,partition,input_offset) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (envelope.event_id, tenant, message.topic(), message.partition(), message.offset()),
        )
        if not duplicate and envelope.cause == "source" and prepared.maintenance:
            from keep.maintenance.runtime import publish_inline_effects

            publish_inline_effects(conn, {"tenant_id": tenant, "id": envelope.alert_id})
    return reason, passes
