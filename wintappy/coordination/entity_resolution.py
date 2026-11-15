"""Immutable acknowledgment dispositions; retained evidence is never a release token.

History authenticates bounded transaction-local JSON only. The writer alone supplies
a freshly supervised, actual-original-bound observation and a same-session token.
Completion/publication/authority originals and the v5 schema remain unchanged.
"""

import json

from .ledger import _validate_block_reason
from .protocol import CoordinationError, canonical, digest, timestamp

MAX_ENTITY_RESOLUTION_BYTES = 128 * 1024
MAX_ENTITY_RESOLUTIONS = 10_000
RECORD_KIND = "entity_resolution_record"
UNRESOLVED_KIND = "entity_resolution_unresolved"
PROOF_FIELDS = {
    "version",
    "operation",
    "ledger_id",
    "binding",
    "run_id",
    "cli_run_id",
    "intent_sha256",
    "completion_sha256",
    "publication_before_sha256",
    "blocked_by_sha256",
    "resolved_at",
    "next_hour",
    "mode",
    "reason",
    "confirmed_cannot_resume",
    "verification",
    "observation",
}


def _budget(ledger, *, records=0, size=0):
    count, largest = ledger._connection.execute(
        "SELECT count(*),coalesce(max(octet_length(encode(details_json))),0) "
        "FROM events WHERE kind=?",
        [RECORD_KIND],
    ).fetchone()
    if (
        count + records > MAX_ENTITY_RESOLUTIONS
        or largest > MAX_ENTITY_RESOLUTION_BYTES
        or size > MAX_ENTITY_RESOLUTION_BYTES
    ):
        raise CoordinationError("Entity resolution history exceeds budget; never prune")
    return count


def _decode(run_id, observed_at, payload):
    try:
        retained = json.loads(payload)
        if (
            len(payload.encode()) > MAX_ENTITY_RESOLUTION_BYTES
            or not isinstance(retained, dict)
            or set(retained) != {"sequence", "previous_sha256", "proof_sha256", "proof"}
            or type(retained["sequence"]) is not int
            or not 1 <= retained["sequence"] <= MAX_ENTITY_RESOLUTIONS
            or canonical(retained) != payload
            or not isinstance(retained["proof"], dict)
            or set(retained["proof"]) != PROOF_FIELDS
            or retained["proof_sha256"] != digest(retained["proof"])
            or retained["proof"]["run_id"] != run_id
            or retained["proof"]["resolved_at"] != observed_at
        ):
            raise ValueError
        return retained
    except (TypeError, ValueError, KeyError, RecursionError):
        raise CoordinationError("Stored entity resolution record conflict") from None


def _block(kind, run_id, observed_at, details):
    # Include the clock and kind, not merely mutually rehashable reason strings.
    return {
        "kind": kind,
        "run_id": run_id,
        "observed_at": observed_at,
        "details": details,
    }


def _uncertainty(ledger, run, completion, resolution):
    rows = ledger._connection.execute(
        "SELECT observed_at,details_json FROM events WHERE kind=? AND identity=? "
        "AND json_extract_string(details_json,'$.resolution_sha256') IS NOT DISTINCT FROM ? LIMIT 2",
        [
            UNRESOLVED_KIND,
            run["run_id"],
            resolution["proof_sha256"] if resolution else None,
        ],
    ).fetchall()
    if not rows:
        return None
    if len(rows) != 1:
        raise CoordinationError("Duplicate entity acknowledgment uncertainty audit")
    observed_at, payload = rows[0]
    details = json.loads(payload)
    expected = {
        "reason": details.get("reason"),
        "intent_sha256": run["intent_sha256"],
        "completion_sha256": completion["proof_sha256"],
        "resolution_sha256": resolution["proof_sha256"] if resolution else None,
    }
    floor = (
        resolution["proof"]["resolved_at"]
        if resolution
        else completion["proof"]["completed_at"]
    )
    if payload != canonical(expected) or timestamp(observed_at) < timestamp(floor):
        raise CoordinationError("Entity acknowledgment uncertainty/original conflict")
    _validate_block_reason(expected["reason"])
    return _block(UNRESOLVED_KIND, run["run_id"], observed_at, expected)


def _audit(retained):
    proof = retained["proof"]
    return {
        "proof_sha256": retained["proof_sha256"],
        "sequence": retained["sequence"],
        "previous_sha256": retained["previous_sha256"],
        "completion_sha256": proof["completion_sha256"],
        "blocked_by_sha256": proof["blocked_by_sha256"],
        "next_hour": proof["next_hour"],
        "mode": proof["mode"],
    }


def validate_owner(ledger, run, intent, completion):
    """Authenticate original block and every disposition/uncertainty without artifacts.

    An original recovery block is consumed by completion, not overwritten by later
    acknowledgment loss. Each subsequent disposition consumes exactly one retained
    blocker, and its uncertain COMMIT supplies a new immutable blocker.
    """
    from .entity_completion import validate_completion_observation

    try:
        run_id = run["run_id"]
        blocks = ledger._connection.execute(
            "SELECT observed_at,details_json FROM events WHERE kind='entity_unresolved' "
            "AND identity=? LIMIT 2",
            [run_id],
        ).fetchall()
        base = None
        if blocks:
            if len(blocks) != 1:
                raise ValueError
            at, payload = blocks[0]
            details = json.loads(payload)
            if payload != canonical(
                {"reason": details.get("reason"), "intent_sha256": run["intent_sha256"]}
            ) or timestamp(at) < timestamp(intent["started_at"]):
                raise ValueError
            _validate_block_reason(details["reason"])
            base = _block("entity_unresolved", run_id, at, details)
        if completion is None:
            if run["state"] == "INTENT":
                if base is not None or run["block_reason"]:
                    raise ValueError
            elif base is None or run["block_reason"] != base["details"]["reason"]:
                raise ValueError
            if ledger._connection.execute(
                "SELECT count(*) FROM events WHERE identity=? AND kind IN (?,?,?)",
                [run_id, RECORD_KIND, "entity_resolved", UNRESOLVED_KIND],
            ).fetchone()[0]:
                raise ValueError
            return base

        original = completion["proof"]
        completed_at = timestamp(original["completed_at"])
        recovering = original["recovery"] is not None
        if recovering:
            if base is None or timestamp(base["observed_at"]) > completed_at:
                raise ValueError
            pending = _uncertainty(ledger, run, completion, None)
        else:
            if base is not None and timestamp(base["observed_at"]) < completed_at:
                raise ValueError
            pending = base
            if _uncertainty(ledger, run, completion, None) is not None:
                raise ValueError
        uncertainties = int(recovering and pending is not None)
        count, last_sequence, last_observation = (
            0,
            0,
            original["observation"]["observations_directory"],
        )
        while True:
            batch = ledger._connection.execute(
                "SELECT observed_at,details_json FROM events WHERE kind=? AND identity=? "
                "AND try_cast(json_extract_string(details_json,'$.sequence') AS BIGINT)>? "
                "ORDER BY try_cast(json_extract_string(details_json,'$.sequence') AS BIGINT) LIMIT 256",
                [RECORD_KIND, run_id, last_sequence],
            ).fetchall()
            if not batch:
                break
            for at, payload in batch:
                retained = _decode(run_id, at, payload)
                proof = retained["proof"]
                observation = proof["observation"]
                if (
                    pending is None
                    or type(proof["version"]) is not int
                    or proof["version"] != 1
                    or proof["operation"] != "entity-completion-resolution"
                    or proof["ledger_id"] != ledger.ledger_id
                    or canonical(proof["binding"]) != ledger.binding
                    or proof["cli_run_id"] != run["cli_run_id"]
                    or proof["intent_sha256"] != run["intent_sha256"]
                    or proof["completion_sha256"] != completion["proof_sha256"]
                    or proof["publication_before_sha256"]
                    != original["publication_before_sha256"]
                    or proof["blocked_by_sha256"] != digest(pending)
                    or proof["next_hour"] != intent["raw_next_hour"]
                    or proof["mode"] != "DRAINING"
                    or proof["confirmed_cannot_resume"] is not True
                    or timestamp(observation["observed_at"])
                    < timestamp(pending["observed_at"])
                    or timestamp(at) < timestamp(observation["observed_at"])
                    or observation["observations_directory"]
                    in {
                        last_observation,
                        original["observation"]["observations_directory"],
                    }
                    or any(
                        observation[k] != original["observation"][k]
                        for k in (
                            "receipt",
                            "intent_sha256",
                            "prelaunch",
                            "execution_supervision",
                            "execution_result",
                            "request",
                        )
                    )
                ):
                    raise ValueError
                _validate_block_reason(proof["reason"])
                core = validate_completion_observation(
                    observation, proof["binding"], ledger.ledger_id, intent=intent
                )
                if core != proof["verification"] or core != original["verification"]:
                    raise ValueError
                audits = ledger._connection.execute(
                    "SELECT observed_at,details_json FROM events WHERE kind='entity_resolved' "
                    "AND identity=? AND json_extract_string(details_json,'$.proof_sha256')=? LIMIT 2",
                    [run_id, retained["proof_sha256"]],
                ).fetchall()
                if audits != [(at, canonical(_audit(retained)))]:
                    raise ValueError
                pending = _uncertainty(ledger, run, completion, retained)
                uncertainties += int(pending is not None)
                count += 1
                last_sequence, last_observation = (
                    retained["sequence"],
                    observation["observations_directory"],
                )
        measured = ledger._connection.execute(
            "SELECT count(*) FILTER (WHERE kind=?),count(*) FILTER (WHERE kind='entity_resolved'),"
            "count(*) FILTER (WHERE kind=?) FROM events WHERE identity=?",
            [RECORD_KIND, UNRESOLVED_KIND, run_id],
        ).fetchone()
        if measured != (count, count, uncertainties):
            raise ValueError
        if run["state"] == "COMPLETED":
            if pending is not None:
                raise ValueError
        elif (
            run["state"] != "BLOCKED"
            or pending is None
            or run["block_reason"] != pending["details"]["reason"]
            or timestamp(pending["observed_at"]) < completed_at
            or ledger.authority_high_water()
            != original["verification"]["authority_high_water"]
            or ledger.snapshot()["next_hour"] != intent["raw_next_hour"]
        ):
            raise ValueError
        return pending
    except (TypeError, ValueError, KeyError, AttributeError, RecursionError):
        raise CoordinationError(
            "Entity completion acknowledgment/block/resolution conflict"
        ) from None


def validate_history(ledger):
    """Global sequence-keyset chain, with exact count equality and materialized pages."""
    expected = _budget(ledger)
    count, predecessor, last_clock = 0, "", None
    while True:
        batch = ledger._connection.execute(
            "SELECT identity,observed_at,details_json FROM events WHERE kind=? "
            "AND try_cast(json_extract_string(details_json,'$.sequence') AS BIGINT)>? "
            "ORDER BY try_cast(json_extract_string(details_json,'$.sequence') AS BIGINT) LIMIT 256",
            [RECORD_KIND, count],
        ).fetchall()
        if not batch:
            break
        for run_id, at, payload in batch:
            retained = _decode(run_id, at, payload)
            count += 1
            if (
                retained["sequence"] != count
                or retained["previous_sha256"] != predecessor
                or last_clock is not None
                and timestamp(at) < last_clock
            ):
                raise CoordinationError("Entity resolution chain/clock conflict")
            predecessor, last_clock = retained["proof_sha256"], timestamp(at)
    if count != expected:
        raise CoordinationError("Entity resolution history count conflict")


def blocked_original(ledger, run, intent, completion, now):
    pending = validate_owner(ledger, run, intent, completion)
    if pending is None:
        raise CoordinationError("Entity resolution has no unresolved original audit")
    if timestamp(now) < timestamp(pending["observed_at"]):
        raise CoordinationError("Entity recovery clock predates retained entity block")
    return pending


def block_acknowledgment(ledger, run, completion, now):
    """Restore exclusion after actual COMMIT; never replace the original block."""
    from . import entity_intent

    ledger._validate()
    row = ledger._connection.execute(
        "SELECT observed_at,details_json FROM events WHERE kind=? AND identity=? "
        "ORDER BY cast(json_extract_string(details_json,'$.sequence') AS BIGINT) DESC LIMIT 1",
        [RECORD_KIND, run["run_id"]],
    ).fetchone()
    resolution = _decode(run["run_id"], *row) if row else None
    floor = (
        resolution["proof"]["resolved_at"]
        if resolution
        else completion["proof"]["completed_at"]
    )
    if timestamp(now) < timestamp(floor):
        raise CoordinationError(
            "Entity acknowledgment clock predates committed evidence"
        )
    reason = "Entity resolution acknowledgment uncertain; preserve immutable evidence and require a new audited resolution"
    details = {
        "reason": reason,
        "intent_sha256": run["intent_sha256"],
        "completion_sha256": completion["proof_sha256"],
        "resolution_sha256": resolution["proof_sha256"] if resolution else None,
    }
    if len(canonical(details).encode()) > entity_intent.MAX_ENTITY_AUDIT_BYTES:
        raise CoordinationError("Entity acknowledgment audit exceeds budget")
    entity_intent._budget(ledger, size=len(canonical(details).encode()))
    with ledger._transaction():
        ledger._connection.execute(
            "UPDATE entity_runs SET state='BLOCKED',block_reason=? WHERE run_id=?",
            [reason, run["run_id"]],
        )
        ledger._connection.execute(
            "UPDATE dataset_state SET mode='BLOCKED',active_run_id=?,block_reason=? WHERE singleton=1",
            [run["run_id"], reason],
        )
        ledger._event(UNRESOLVED_KIND, run["run_id"], now, details)
        ledger._validate()


def complete_resolution(
    ledger, prepared, intent, completion, pending, observation, now, reason
):
    """Internal fresh writer seam; callers cannot resolve with retained dictionaries."""
    from . import entity_completion, entity_intent

    ledger._validate()
    run = ledger.entity_run(intent["run_id"])
    if (
        run["state"] != "BLOCKED"
        or ledger.snapshot()["active_run_id"] != run["run_id"]
        or ledger.snapshot()["readers"]
        or getattr(ledger, "_entity_ack_resolution", None)
        != (
            run["run_id"],
            digest(observation),
            completion["proof_sha256"],
            digest(pending),
        )
        or blocked_original(ledger, run, intent, completion, now) != pending
    ):
        raise CoordinationError(
            "Entity acknowledgment resolution requires fresh locked evidence"
        )
    core = entity_completion.validate_completion_observation(
        observation, prepared.binding, ledger.ledger_id, intent=intent
    )
    if core != completion["proof"]["verification"]:
        raise CoordinationError(
            "Fresh entity resolution differs from immutable completion"
        )
    proof = {
        "version": 1,
        "operation": "entity-completion-resolution",
        "ledger_id": ledger.ledger_id,
        "binding": prepared.binding,
        "run_id": run["run_id"],
        "cli_run_id": run["cli_run_id"],
        "intent_sha256": run["intent_sha256"],
        "completion_sha256": completion["proof_sha256"],
        "publication_before_sha256": completion["proof"]["publication_before_sha256"],
        "blocked_by_sha256": digest(pending),
        "resolved_at": now,
        "next_hour": intent["raw_next_hour"],
        "mode": "DRAINING",
        "reason": reason,
        "confirmed_cannot_resume": True,
        "verification": core,
        "observation": observation,
    }
    tail = ledger._connection.execute(
        "SELECT identity,observed_at,details_json FROM events WHERE kind=? ORDER BY cast(json_extract_string(details_json,'$.sequence') AS BIGINT) DESC LIMIT 1",
        [RECORD_KIND],
    ).fetchone()
    previous = _decode(*tail) if tail else None
    retained = {
        "sequence": previous["sequence"] + 1 if previous else 1,
        "previous_sha256": previous["proof_sha256"] if previous else "",
        "proof_sha256": digest(proof),
        "proof": proof,
    }
    _budget(ledger, records=1, size=len(canonical(retained).encode()))
    details = _audit(retained)
    if len(canonical(details).encode()) > entity_intent.MAX_ENTITY_AUDIT_BYTES:
        raise CoordinationError("Entity resolution audit exceeds budget")
    entity_intent._budget(
        ledger,
        size=len(canonical(retained).encode())
        + 2 * entity_intent.MAX_ENTITY_AUDIT_BYTES,
    )
    with ledger._transaction(kind="entity_resolution"):
        ledger._event(RECORD_KIND, run["run_id"], now, retained)
        ledger._event("entity_resolved", run["run_id"], now, details)
        ledger._connection.execute(
            "UPDATE entity_runs SET state='COMPLETED',block_reason='' WHERE run_id=?",
            [run["run_id"]],
        )
        ledger._connection.execute(
            "UPDATE dataset_state SET mode='DRAINING',active_run_id=NULL,block_reason='' WHERE singleton=1"
        )
        ledger._validate()
    ledger._entity_ack_resolution = None
    return {**ledger.snapshot(), "entity_run_id": run["run_id"]}
