"""Explicit-v5 entity exclusion, not entity launch/completion or recovery authority.

One acknowledged intent COMMIT permits only original-bound read-only input checks
in this locked session. Reopen and acknowledgment uncertainty preserve BLOCKED
ownership; no entity history may masquerade as completed raw compaction.
"""

import json

from .authority import validate_anchor
from .coordinator import _Clock
from .entity_execution import recheck_entity_execution, validate_entity_receipt
from .entity_verification import (
    recheck_input_observation,
    validate_input_observation,
    verify_entity_inputs_prepared,
)
from .ledger import COLUMNS, _validate_block_reason
from .protocol import CoordinationError, canonical, digest, identifier, timestamp
from .supervisor import _publish
from .verification import _read

MAX_ENTITY_INTENT_BYTES = 128 * 1024
MAX_ENTITY_RUNS = 10_000
MAX_ENTITY_HISTORY_BYTES = 64 * 1024 * 1024
MAX_ENTITY_AUDIT_BYTES = 16 * 1024
INTENT_FIELDS = {
    "version",
    "operation",
    "ledger_id",
    "binding",
    "run_id",
    "cli_run_id",
    "entity_command",
    "hour_start",
    "hour_end",
    "started_at",
    "raw_next_hour",
    "reason",
    "confirmed",
    "execution",
    "input_observation",
    "authority_before",
    "authority_proof_sha256",
    "schema_upgrade_sha256",
}


def _budget(ledger, *, records=0, size=0):
    # SQL computes sizes before any payload is returned to Python. No pruning.
    count, total, largest = ledger._connection.execute(
        "SELECT count(*),coalesce(sum(octet_length(encode(intent_json))),0),"
        "coalesce(max(octet_length(encode(intent_json))),0) FROM entity_runs"
    ).fetchone()
    audits, audit_total, audit_largest, completions, completion_largest = (
        ledger._connection.execute(
            "SELECT count(*),coalesce(sum(octet_length(encode(details_json))),0),"
            "coalesce(max(CASE WHEN kind NOT IN ('entity_completion_record','entity_resolution_record') "
            "THEN octet_length(encode(details_json)) ELSE 0 END),0),"
            "count(*) FILTER (WHERE kind='entity_completion_record'),"
            "coalesce(max(CASE WHEN kind='entity_completion_record' "
            "THEN octet_length(encode(details_json)) ELSE 0 END),0) FROM events "
            "WHERE kind LIKE 'entity_%' AND kind<>'entity_history_initialized'"
        ).fetchone()
    )
    from .entity_publication import _budget as publication_budget

    publications, publication_total = publication_budget(ledger)
    from .entity_history import MAX_ENTITY_COMPLETION_BYTES, MAX_ENTITY_COMPLETIONS
    from .entity_resolution import _budget as resolution_budget

    resolutions = resolution_budget(ledger)
    recovery_acks = ledger._connection.execute(
        "SELECT count(*) FROM events WHERE kind='entity_resolution_unresolved' "
        "AND json_extract_string(details_json,'$.resolution_sha256') IS NULL"
    ).fetchone()[0]

    if (
        count + records > MAX_ENTITY_RUNS
        # Keep the closed unresolved allowance exactly unchanged. Large completion
        # records are separate; only retained records add their small audit capacity.
        or audits - completions - resolutions
        > 2 * MAX_ENTITY_RUNS
        + publications
        + 2 * completions
        + 2 * resolutions
        + recovery_acks
        or recovery_acks > completions
        or completions > MAX_ENTITY_COMPLETIONS
        or completion_largest > MAX_ENTITY_COMPLETION_BYTES
        or total + audit_total + publication_total + size > MAX_ENTITY_HISTORY_BYTES
        or largest > MAX_ENTITY_INTENT_BYTES
        or audit_largest > MAX_ENTITY_AUDIT_BYTES
    ):
        raise CoordinationError("Entity intent history exceeds budget; never prune")


def _upgrade_identity(ledger):
    rows = ledger._connection.execute(
        "SELECT observed_at,details_json FROM events WHERE kind='schema_upgraded' "
        "AND json_extract_string(details_json,'$.to')='5' LIMIT 2"
    ).fetchall()
    if len(rows) != 1:
        raise CoordinationError("Entity intent requires its original v5 upgrade audit")
    return rows[0][0], digest(json.loads(rows[0][1]))


def _authority_identity(ledger):
    row = ledger._connection.execute(
        "SELECT proof_sha256 FROM authority_checks ORDER BY sequence DESC LIMIT 1"
    ).fetchone()
    return row[0] if row else None


def validate_intent(ledger, run):
    """Validate retained typed JSON only; never reconstruct or reread old artifacts."""
    try:
        payload = run["intent_json"]
        if len(payload.encode()) > MAX_ENTITY_INTENT_BYTES:
            raise ValueError
        intent = json.loads(payload)
        if (
            not isinstance(intent, dict)
            or set(intent) != INTENT_FIELDS
            or type(intent["version"]) is not int
            or intent["version"] != 1
            or intent["operation"] != "entity-only-intent"
            or intent["confirmed"] is not True
            or canonical(intent) != payload
            or digest(intent) != run["intent_sha256"]
            or intent["ledger_id"] != ledger.ledger_id
            or canonical(intent["binding"]) != ledger.binding
            or any(
                intent[k] != run[k]
                for k in (
                    "run_id",
                    "cli_run_id",
                    "entity_command",
                    "hour_start",
                    "hour_end",
                    "started_at",
                )
            )
        ):
            raise ValueError
        identifier(intent["run_id"])
        identifier(intent["cli_run_id"])
        _validate_block_reason(intent["reason"])
        timestamp(intent["raw_next_hour"], hour=True)
        started = timestamp(intent["started_at"])
        execution = intent["execution"]
        if not isinstance(execution, dict) or set(execution) != {
            "path",
            "sha256",
            "receipt",
        }:
            raise ValueError
        receipt = validate_entity_receipt(execution["receipt"])
        observation = intent["input_observation"]
        core = validate_input_observation(
            observation, intent["binding"], ledger.ledger_id
        )
        upgraded, upgrade_hash = _upgrade_identity(ledger)
        if (
            execution["path"]
            != str(
                ledger.directory
                / "entity-executions"
                / run["run_id"]
                / "execution.json"
            )
            or execution["sha256"] != digest(receipt)
            or any(
                receipt[k] != intent[k]
                for k in (
                    "run_id",
                    "cli_run_id",
                    "entity_command",
                    "hour_start",
                    "hour_end",
                    "binding",
                )
            )
            or observation["receipt"] != receipt
            or observation["request"]["original"] is not None
            or timestamp(receipt["created_at"]) > timestamp(observation["observed_at"])
            or timestamp(observation["observed_at"]) > started
            or timestamp(upgraded) > started
            or intent["schema_upgrade_sha256"] != upgrade_hash
            or core["authority_high_water"] != intent["authority_before"]
        ):
            raise ValueError
        if intent["authority_before"] is not None:
            validate_anchor(intent["authority_before"], intent["binding"])
        from .entity_history import validate_original_scope

        validate_original_scope(ledger, intent)
        return intent
    except (TypeError, ValueError, KeyError, RecursionError):
        raise CoordinationError(
            "Stored typed entity intent/identity conflict"
        ) from None


def validate_history(ledger):
    """Bounded same-transaction keyset scans, including newly staged rows/audits.

    Materialize each SQL-limited page before per-run lookups replace its result.
    DuckDB cursor() creates another connection, not a transaction-local cursor.
    Original publications and typed completion records remain distinct immutable history.
    """
    _budget(ledger)
    connection = ledger._connection
    count = 0
    last_run = None
    while True:
        batch = connection.execute(
            "SELECT * FROM entity_runs "
            + ("WHERE run_id>? " if last_run is not None else "")
            + "ORDER BY run_id LIMIT 256",
            [last_run] if last_run is not None else [],
        ).fetchall()
        if not batch:
            break
        last_run = batch[-1][0]
        for row in batch:
            run = dict(zip(COLUMNS["entity_runs"], row, strict=True))
            intent = validate_intent(ledger, run)
            state = ledger.snapshot()
            from .entity_history import retained_record

            completion = retained_record(ledger, run["run_id"])
            completed = run["state"] == "COMPLETED"
            if not completed:
                count += 1
            if (
                run["state"] not in {"INTENT", "BLOCKED", "COMPLETED"}
                or completed
                and (completion is None or run["block_reason"])
                or not completed
                and (
                    state["active_run_id"] != run["run_id"]
                    or state["mode"]
                    != ("WRITING" if run["state"] == "INTENT" else "BLOCKED")
                    or state["block_reason"] != run["block_reason"]
                    or state["readers"]
                )
                or ledger._connection.execute(
                    "SELECT count(*) FROM compaction_runs WHERE run_id IN (?,?) OR cli_run_id IN (?,?)",
                    [
                        run["run_id"],
                        run["cli_run_id"],
                        run["run_id"],
                        run["cli_run_id"],
                    ],
                ).fetchone()[0]
            ):
                raise CoordinationError("Typed entity owner/history conflict")
            audits = ledger._connection.execute(
                "SELECT observed_at,details_json FROM events WHERE kind='entity_intent' AND identity=? LIMIT 2",
                [run["run_id"]],
            ).fetchall()
            if audits != [
                (
                    run["started_at"],
                    canonical({"intent_sha256": run["intent_sha256"]}),
                )
            ]:
                raise CoordinationError("Entity intent audit/identity conflict")
            from .entity_resolution import validate_owner

            validate_owner(ledger, run, intent, completion)
    from .entity_history import validate_history as validate_completions

    validate_completions(ledger)
    from .entity_resolution import validate_history as validate_resolutions

    validate_resolutions(ledger)
    from .entity_publication import validate_history as validate_publications

    validate_publications(ledger)
    # An extra/orphan audit cannot hide behind per-run lookups. Page by the
    # unique event identity, not kind/run, so duplicates are never skipped.
    last_event = None
    while True:
        batch = connection.execute(
            "SELECT event_id,kind,identity FROM events WHERE kind LIKE 'entity_%' "
            "AND kind<>'entity_history_initialized' "
            + ("AND event_id>? " if last_event is not None else "")
            + "ORDER BY event_id LIMIT 256",
            [last_event] if last_event is not None else [],
        ).fetchall()
        if not batch:
            break
        last_event = batch[-1][0]
        for _, kind, run_id in batch:
            if (
                kind
                not in {
                    "entity_intent",
                    "entity_unresolved",
                    "entity_publication",
                    "entity_completion_record",
                    "entity_completed",
                    "entity_recovered",
                    "entity_resolution_record",
                    "entity_resolved",
                    "entity_resolution_unresolved",
                }
                or ledger._connection.execute(
                    "SELECT count(*) FROM entity_runs WHERE run_id=?", [run_id]
                ).fetchone()[0]
                != 1
                or kind
                in {
                    "entity_completed",
                    "entity_recovered",
                    "entity_resolution_record",
                    "entity_resolved",
                    "entity_resolution_unresolved",
                }
                and ledger._connection.execute(
                    "SELECT count(*) FROM events WHERE kind='entity_completion_record' AND identity=?",
                    [run_id],
                ).fetchone()[0]
                != 1
                or kind == "entity_publication"
                and ledger._connection.execute(
                    "SELECT count(*) FROM entity_evidence WHERE run_id=?", [run_id]
                ).fetchone()[0]
                != 1
            ):
                raise CoordinationError("Unsupported/orphan entity history audit")
    if count > 1:
        raise CoordinationError("Multiple unresolved entity intents require recovery")


def block_entity(ledger, run_id, now, reason):
    """Retain exact originals, with no cursor/high-water/admission transition."""
    identifier(run_id)
    timestamp(now)
    _validate_block_reason(reason)
    ledger._entity_live_intent = None
    ledger._validate()
    run = ledger.entity_run(run_id)
    if (
        run is None
        or run["state"] != "INTENT"
        or ledger.snapshot()["active_run_id"] != run_id
    ):
        raise CoordinationError("Only the active typed entity intent may be blocked")
    details = {"reason": reason, "intent_sha256": run["intent_sha256"]}
    _budget(ledger, size=len(canonical(details).encode()))
    if timestamp(now) < timestamp(run["started_at"]):
        raise CoordinationError("Entity block clock predates original intent")
    with ledger._transaction():
        ledger._connection.execute(
            "UPDATE entity_runs SET state='BLOCKED',block_reason=? WHERE run_id=?",
            [reason, run_id],
        )
        ledger._connection.execute(
            "UPDATE dataset_state SET mode='BLOCKED',block_reason=? WHERE singleton=1",
            [reason],
        )
        ledger._event("entity_unresolved", run_id, now, details)
        ledger._validate()


def recheck_live_original(ledger, prepared, run_id, original, now):
    """Read-only post-COMMIT checks require acknowledged, same-session typed ownership."""
    identifier(run_id)
    run = ledger.entity_run(run_id)
    if (
        run is None
        or run["state"] != "INTENT"
        or getattr(ledger, "_entity_live_intent", None)
        != (run_id, run["intent_sha256"])
    ):
        raise CoordinationError(
            "Entity inputs require this session's acknowledged typed intent"
        )
    intent = validate_intent(ledger, run)
    receipt = recheck_entity_execution(prepared, ledger)
    if (
        intent["execution"]
        != {"path": str(prepared.path), "sha256": prepared.sha256, "receipt": receipt}
        or original is None
        or canonical(original) != canonical(intent["input_observation"])
        or timestamp(now) < timestamp(run["started_at"])
        or _read(prepared.path.parent / "intent.json", MAX_ENTITY_INTENT_BYTES)
        != canonical(intent).encode()
    ):
        raise CoordinationError(
            "Post-COMMIT entity inputs do not bind the retained original"
        )
    recheck_input_observation(ledger, prepared, intent["input_observation"])
    return intent


def begin_entity_prepared(
    ledger, prepared, now, *, clock, confirmed=False, reason=None
):
    """Commit typed exclusion only; this API never launches an entity operation."""
    if confirmed is not True:
        raise CoordinationError("Typed entity intent requires explicit confirmation")
    _validate_block_reason(reason)
    if not callable(clock):
        raise CoordinationError("Typed entity intent requires a fresh evolving clock")
    observed = _Clock(now, clock)
    run_id = None
    try:
        ledger._require_entity_schema()
        ledger._validate()
        state, anchor = ledger.snapshot(), ledger.authority_high_water()
        if (
            state["mode"] not in {"OPEN", "DRAINING"}
            or state["active_run_id"] is not None
            or state["readers"]
        ):
            raise CoordinationError(
                "Entity intent requires unowned zero-reader exclusion"
            )
        from .maintenance import no_pending

        no_pending(ledger)
        receipt = recheck_entity_execution(prepared, ledger)
        run_id = receipt["run_id"]
        current = observed.observe()
        upgraded, upgrade_hash = _upgrade_identity(ledger)
        floor = ledger._completion_clock_floor()
        if (
            timestamp(current) < timestamp(receipt["created_at"])
            or timestamp(current) < timestamp(upgraded)
            or floor is not None
            and timestamp(current) < timestamp(floor)
        ):
            raise CoordinationError("Entity intent clock predates retained evidence")
        if ledger.entity_run(run_id) is not None:
            raise CoordinationError(
                "Entity intent identity already exists; never resume"
            )
        _budget(ledger, records=1)
        inputs = verify_entity_inputs_prepared(ledger, prepared, current)
        current = observed.observe()
        intent = {
            "version": 1,
            "operation": "entity-only-intent",
            "ledger_id": ledger.ledger_id,
            "binding": prepared.binding,
            **{
                k: receipt[k]
                for k in (
                    "run_id",
                    "cli_run_id",
                    "entity_command",
                    "hour_start",
                    "hour_end",
                )
            },
            "started_at": current,
            "raw_next_hour": state["next_hour"],
            "reason": reason,
            "confirmed": True,
            "execution": {
                "path": str(prepared.path),
                "sha256": prepared.sha256,
                "receipt": receipt,
            },
            "input_observation": inputs,
            "authority_before": anchor,
            "authority_proof_sha256": _authority_identity(ledger),
            "schema_upgrade_sha256": upgrade_hash,
        }
        size = len(canonical(intent).encode())
        if size > MAX_ENTITY_INTENT_BYTES:
            raise CoordinationError("Typed entity intent exceeds record budget")
        # Reserve one bounded unresolved audit before creating ownership.
        _budget(ledger, records=1, size=size + MAX_ENTITY_AUDIT_BYTES + 256)
        recheck_input_observation(ledger, prepared, inputs)
        if ledger.snapshot() != state or ledger.authority_high_water() != anchor:
            raise CoordinationError(
                "Entity original scope changed before intent COMMIT"
            )
        _publish(prepared.path.parent / "intent.json", intent)
        with ledger._transaction(kind="entity"):
            ledger._connection.execute(
                "INSERT INTO entity_runs VALUES (?,?,?,?,?,'INTENT',?,?,?,'')",
                [
                    receipt["run_id"],
                    receipt["cli_run_id"],
                    receipt["entity_command"],
                    receipt["hour_start"],
                    receipt["hour_end"],
                    current,
                    canonical(intent),
                    digest(intent),
                ],
            )
            ledger._connection.execute(
                "UPDATE dataset_state SET mode='WRITING',active_run_id=?,block_reason='' WHERE singleton=1",
                [run_id],
            )
            ledger._event(
                "entity_intent", run_id, current, {"intent_sha256": digest(intent)}
            )
            ledger._validate()
        ledger._entity_live_intent = (run_id, digest(intent))
        return json.loads(canonical(intent))
    except BaseException as error:
        state = ledger.snapshot()
        if (
            run_id is not None
            and state["mode"] == "WRITING"
            and state["active_run_id"] == run_id
        ):
            block_entity(
                ledger,
                run_id,
                observed.last,
                "Typed entity intent acknowledgment uncertain; preserve originals and require audited recovery",
            )
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        if isinstance(error, CoordinationError):
            raise
        raise CoordinationError(
            "Typed entity intent refused/uncertain; details suppressed"
        ) from None
