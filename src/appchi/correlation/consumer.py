from dataclasses import replace
from uuid import UUID
from appchi.event_models import ChangedEnvelope
from appchi.ingestion.native import parse_json
from appchi.shared.errors import unavailable
from appchi.shared.transactions import coordinator


def correlate_record(db, settings, profile, message):
    envelope = ChangedEnvelope.model_validate(parse_json(message.value()))
    tenant = UUID(envelope.tenant_id)
    if message.key() != tenant.bytes:
        raise unavailable("changed_tenant_key_invalid")
    from appchi.correlation.runtime import prepare_tenant, worker_turn

    prepared = prepare_tenant(
        db,
        tenant,
        profile,
        allow_local_preview=settings.environment == "development",
        alert_ids=(envelope.alert_id,),
    )
    with db.transaction(tenant) as conn:
        coordinator(conn, tenant)
        # The gate is read under the coordinator, so it holds for this whole
        # consumption; a closed gate rolls back and retries the input.
        gate = conn.execute(
            "SELECT ready FROM capability_gates WHERE capability='correlation'"
        ).fetchone()
        if not gate or not gate["ready"]:
            raise unavailable("correlation_not_ready")
        if prepared.maintenance:
            from appchi.maintenance.runtime import InlineCorrelation, lock_catalog

            catalog = replace(
                prepared.maintenance, inline_correlation=InlineCorrelation(db, profile, prepared)
            )
            lock_catalog(conn, catalog, targets=[envelope.alert_id])
        duplicate = conn.execute(
            "SELECT 1 FROM kafka_correlation_events WHERE event_id=%s", (envelope.event_id,)
        ).fetchone()
        if not duplicate and envelope.cause == "source":
            source = conn.execute(
                "SELECT r.changed_envelope,i.retired_at FROM kafka_source_results r LEFT JOIN alert_identities i ON i.tenant_id=r.tenant_id AND i.id=r.alert_id WHERE r.tenant_id=%s AND r.changed_event_id=%s",
                (tenant, envelope.event_id),
            ).fetchone()
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
            elif proof["change_order"] is not None:
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
                    if not worker_turn(
                        db, tenant, profile, prepared_input=prepared, transaction=conn, kafka=True
                    ):
                        break
        conn.execute(
            "INSERT INTO kafka_correlation_events(event_id,tenant_id,topic,partition,input_offset) VALUES(%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (envelope.event_id, tenant, message.topic(), message.partition(), message.offset()),
        )
        if not duplicate and envelope.cause == "source" and prepared.maintenance:
            from appchi.maintenance.runtime import publish_inline_effects

            publish_inline_effects(conn, {"tenant_id": tenant, "id": envelope.alert_id})
    return envelope
