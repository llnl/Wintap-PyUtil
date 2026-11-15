"""Distinct typed completion records; original publications are never replaced.

Only the writer's fresh observation seam inserts records. Reopen authenticates
bounded retained JSON, original authority/cursor predecessors and atomic links;
it never reopens old artifacts or interprets current data as historical evidence.
"""

import json

from .authority import MAX_AUTHORITY_BYTES, validate_anchor
from .ledger import COLUMNS, _validate_block_reason
from .protocol import CoordinationError, canonical, digest, identifier, timestamp

MAX_ENTITY_COMPLETION_BYTES = 128 * 1024
MAX_ENTITY_COMPLETIONS = 10_000
RECORD_KIND = "entity_completion_record"
PROOF_FIELDS = {
    "version",
    "operation",
    "ledger_id",
    "binding",
    "run_id",
    "cli_run_id",
    "receipt",
    "intent_sha256",
    "completed_at",
    "next_hour",
    "mode",
    "verification",
    "observation",
    "authority_before_sha256",
    "raw_cursor_before_sha256",
    "publication_before_sha256",
    "recovery",
}


def retained_record(ledger, run_id):
    """Internal lookup: callers must first authenticate shared SQL budgets/history."""
    rows = ledger._connection.execute(
        "SELECT observed_at,details_json FROM events WHERE kind=? AND identity=? LIMIT 2",
        [RECORD_KIND, run_id],
    ).fetchall()
    if not rows:
        return None
    if len(rows) != 1 or len(rows[0][1].encode()) > MAX_ENTITY_COMPLETION_BYTES:
        raise CoordinationError("Entity completion record conflict or exceeds budget")
    try:
        record = json.loads(rows[0][1])
        if (
            not isinstance(record, dict)
            or set(record) != {"sequence", "previous_sha256", "proof_sha256", "proof"}
            or type(record["sequence"]) is not int
            or not 1 <= record["sequence"] <= MAX_ENTITY_COMPLETIONS
            or canonical(record) != rows[0][1]
            or record["proof_sha256"] != digest(record["proof"])
            or rows[0][0] != record["proof"]["completed_at"]
            or record["proof"]["run_id"] != run_id
        ):
            raise ValueError
        return record
    except (TypeError, ValueError, KeyError, RecursionError):
        raise CoordinationError("Stored typed completion record conflict") from None


def raw_cursor_identity(ledger, intent):
    """Bind the original raw-history prefix, not today's cursor or aggregate."""
    first = ledger._connection.execute("SELECT first_hour FROM metadata").fetchone()[0]
    hour = intent["raw_next_hour"]
    if hour == first:
        return None
    rows = ledger._connection.execute(
        "SELECT proof_sha256,CASE WHEN octet_length(encode(proof_json))<=131072 "
        "THEN proof_json ELSE NULL END FROM completion_evidence "
        "WHERE json_extract_string(proof_json,'$.next_hour')=? LIMIT 2",
        [hour],
    ).fetchall()
    if len(rows) != 1 or rows[0][1] is None:
        raise CoordinationError("Entity original raw cursor has no bounded predecessor")
    proof = json.loads(rows[0][1])
    if (
        digest(proof) != rows[0][0]
        or canonical(proof) != rows[0][1]
        or timestamp(proof["completed_at"]) > timestamp(intent["started_at"])
        or proof["next_hour"] != hour
    ):
        raise CoordinationError("Entity original raw cursor predecessor conflict")
    return rows[0][0]


def validate_original_scope(ledger, intent):
    """Historical success keeps original evidence; unresolved originals stay current."""
    original_hash = intent["authority_proof_sha256"]
    before = intent["authority_before"]
    if before is None:
        if original_hash is not None:
            raise CoordinationError(
                "Entity genesis has unexpected authority predecessor"
            )
    else:
        rows = ledger._connection.execute(
            "SELECT CASE WHEN octet_length(encode(proof_json))<=? "
            "THEN proof_json ELSE NULL END FROM authority_checks "
            "WHERE proof_sha256=? LIMIT 2",
            [MAX_AUTHORITY_BYTES, original_hash],
        ).fetchall()
        if len(rows) != 1 or rows[0][0] is None:
            raise CoordinationError("Entity original authority predecessor missing")
        original = json.loads(rows[0][0])
        if (
            digest(original) != original_hash
            or canonical(original) != rows[0][0]
            or original["result"]["authority_high_water"] != before
            or timestamp(original["observed_at"]) > timestamp(intent["started_at"])
        ):
            raise CoordinationError("Entity original authority predecessor conflict")
        validate_anchor(before, intent["binding"])
    retained = retained_record(ledger, intent["run_id"])
    if retained is None:
        from .entity_intent import _authority_identity

        if (
            intent["raw_next_hour"] != ledger.snapshot()["next_hour"]
            or before != ledger.authority_high_water()
            or original_hash != _authority_identity(ledger)
        ):
            raise CoordinationError("Unresolved entity original scope changed")
    else:
        proof = retained["proof"]
        if (
            proof["authority_before_sha256"] != original_hash
            or proof["raw_cursor_before_sha256"] != raw_cursor_identity(ledger, intent)
            or timestamp(ledger.snapshot()["next_hour"], hour=True)
            < timestamp(intent["raw_next_hour"], hour=True)
        ):
            raise CoordinationError("Historical entity original scope conflict")


def authority_reference(proof):
    """Different typed contract; never pass entity receipts to public validate_proof."""
    return {
        "version": 1,
        "operation": "entity-completion-authority",
        "ledger_id": proof["ledger_id"],
        "binding": proof["binding"],
        "run_id": proof["run_id"],
        "intent_sha256": proof["intent_sha256"],
        "completion_sha256": digest(proof),
        "observed_at": proof["completed_at"],
        "authority_before_sha256": proof["authority_before_sha256"],
        "result": {
            "authority_high_water": proof["verification"]["authority_high_water"]
        },
    }


def validate_authority_reference(ledger, proof, previous, previous_hash):
    from .entity_completion import validate_completion_observation

    if len(canonical(proof).encode()) > MAX_AUTHORITY_BYTES:
        raise CoordinationError("Typed authority reference exceeds budget")
    run_id = proof.get("run_id")
    identifier(run_id)
    retained = retained_record(ledger, run_id)
    if retained is None or proof != authority_reference(retained["proof"]):
        raise CoordinationError("Typed authority has no atomic completion record")
    rows = ledger._connection.execute(
        "SELECT intent_json FROM entity_runs WHERE run_id=? LIMIT 2",
        [run_id],
    ).fetchall()
    if len(rows) != 1:
        raise CoordinationError("Typed authority has no original intent")
    intent = json.loads(rows[0][0])
    completed = retained["proof"]
    core = validate_completion_observation(
        completed["observation"],
        completed["binding"],
        ledger.ledger_id,
        intent=intent,
    )
    if (
        completed["verification"] != core
        or completed["intent_sha256"] != digest(intent)
        or proof["authority_before_sha256"] != (previous_hash or None)
        or intent["authority_before"] != previous
    ):
        raise CoordinationError(
            "Typed authority original predecessor/completion conflict"
        )
    return validate_anchor(core["authority_high_water"], completed["binding"], previous)


def validate_completion_record(ledger, retained, run, intent):
    from .entity_completion import validate_completion_observation

    try:
        proof = retained["proof"]
        if (
            not isinstance(proof, dict)
            or set(proof) != PROOF_FIELDS
            or type(proof["version"]) is not int
            or proof["version"] != 1
            or proof["operation"] != "entity-only-completion"
            or proof["ledger_id"] != ledger.ledger_id
            or canonical(proof["binding"]) != ledger.binding
            or proof["run_id"] != run["run_id"]
            or proof["cli_run_id"] != run["cli_run_id"]
            or proof["receipt"] != intent["execution"]["receipt"]
            or proof["intent_sha256"] != run["intent_sha256"]
            or proof["next_hour"] != intent["raw_next_hour"]
            or proof["mode"] != "DRAINING"
            or proof["authority_before_sha256"] != intent["authority_proof_sha256"]
            or proof["raw_cursor_before_sha256"] != raw_cursor_identity(ledger, intent)
            or timestamp(proof["completed_at"])
            < timestamp(proof["observation"]["observed_at"])
        ):
            raise ValueError
        core = validate_completion_observation(
            proof["observation"],
            proof["binding"],
            ledger.ledger_id,
            intent=intent,
        )
        if proof["verification"] != core:
            raise ValueError
        publication = ledger._connection.execute(
            "SELECT proof_sha256 FROM entity_evidence WHERE run_id=?",
            [run["run_id"]],
        ).fetchone()
        if proof["publication_before_sha256"] != (
            publication[0] if publication else None
        ):
            raise ValueError
        authority = authority_reference(proof)
        if ledger._connection.execute(
            "SELECT proof_json FROM authority_checks WHERE proof_sha256=? LIMIT 2",
            [digest(authority)],
        ).fetchall() != [(canonical(authority),)]:
            raise ValueError
        details = {
            "proof_sha256": digest(proof),
            "sequence": retained["sequence"],
            "previous_sha256": retained["previous_sha256"],
            "authority_sha256": digest(authority),
            "next_hour": proof["next_hour"],
            "mode": proof["mode"],
        }
        if ledger._connection.execute(
            "SELECT observed_at,details_json FROM events WHERE kind='entity_completed' "
            "AND identity=? LIMIT 2",
            [run["run_id"]],
        ).fetchall() != [(proof["completed_at"], canonical(details))]:
            raise ValueError
        recovery = proof["recovery"]
        recovered = ledger._connection.execute(
            "SELECT observed_at,details_json FROM events WHERE kind='entity_recovered' "
            "AND identity=? LIMIT 2",
            [run["run_id"]],
        ).fetchall()
        if recovery is None:
            if recovered:
                raise ValueError
        else:
            if (
                not isinstance(recovery, dict)
                or set(recovery) != {"reason", "confirmed_cannot_resume"}
                or recovery["confirmed_cannot_resume"] is not True
                or recovered
                != [
                    (
                        proof["completed_at"],
                        canonical({**recovery, "proof_sha256": digest(proof)}),
                    )
                ]
            ):
                raise ValueError
            _validate_block_reason(recovery["reason"])
        return proof
    except (TypeError, ValueError, KeyError, RecursionError):
        raise CoordinationError(
            "Stored typed completion/original/audit conflict"
        ) from None


def validate_history(ledger):
    """Materialized same-connection sequence-keyset pages, including staged rows."""
    expected = ledger._connection.execute(
        "SELECT count(*) FROM events WHERE kind=?",
        [RECORD_KIND],
    ).fetchone()[0]
    count, previous_hash, previous_completed = 0, "", None
    while True:
        batch = ledger._connection.execute(
            "SELECT identity,details_json FROM events WHERE kind=? "
            "AND try_cast(json_extract_string(details_json,'$.sequence') AS BIGINT)>? "
            "ORDER BY try_cast(json_extract_string(details_json,'$.sequence') AS BIGINT) LIMIT 256",
            [RECORD_KIND, count],
        ).fetchall()
        if not batch:
            break
        for run_id, _ in batch:
            count += 1
            retained = retained_record(ledger, run_id)
            if (
                retained["sequence"] != count
                or retained["previous_sha256"] != previous_hash
            ):
                raise CoordinationError("Entity completion chain conflict")
            rows = ledger._connection.execute(
                "SELECT * FROM entity_runs WHERE run_id=? LIMIT 2",
                [run_id],
            ).fetchall()
            if len(rows) != 1:
                raise CoordinationError("Orphan typed completion record")
            run = dict(zip(COLUMNS["entity_runs"], rows[0], strict=True))
            intent = json.loads(run["intent_json"])
            proof = validate_completion_record(ledger, retained, run, intent)
            if (
                previous_completed is not None
                and timestamp(intent["started_at"]) < previous_completed
            ):
                raise CoordinationError("Entity completion history clock conflict")
            previous_hash, previous_completed = (
                digest(proof),
                timestamp(proof["completed_at"]),
            )
    if count != expected:
        raise CoordinationError("Entity completion history count conflict")


def complete_observation(ledger, prepared, intent, observation, now, *, recovery=None):
    """Internal fresh writer seam; no public caller-dictionary completion API."""
    from . import entity_completion, entity_intent

    ledger._validate()
    run = ledger.entity_run(intent["run_id"])
    recovering = recovery is not None
    if (
        run["state"] != ("BLOCKED" if recovering else "INTENT")
        or ledger.snapshot()["active_run_id"] != intent["run_id"]
        or ledger.snapshot()["readers"]
        or not recovering
        and ledger._entity_live_intent != (run["run_id"], run["intent_sha256"])
        or recovering
        and getattr(ledger, "_entity_clean_recovery", None)
        != (run["run_id"], digest(observation))
        or retained_record(ledger, run["run_id"]) is not None
    ):
        raise CoordinationError(
            "Entity completion requires fresh original live/recovery evidence"
        )
    core = entity_completion.validate_completion_observation(
        observation,
        prepared.binding,
        ledger.ledger_id,
        intent=intent,
    )
    if not recovering:
        entity_completion.recheck_completion_observation(ledger, prepared, observation)
    proof = {
        "version": 1,
        "operation": "entity-only-completion",
        "ledger_id": ledger.ledger_id,
        "binding": prepared.binding,
        "run_id": run["run_id"],
        "cli_run_id": run["cli_run_id"],
        "receipt": prepared.receipt,
        "intent_sha256": run["intent_sha256"],
        "completed_at": now,
        "next_hour": intent["raw_next_hour"],
        "mode": "DRAINING",
        "verification": core,
        "observation": observation,
        "authority_before_sha256": intent["authority_proof_sha256"],
        "raw_cursor_before_sha256": raw_cursor_identity(ledger, intent),
        "publication_before_sha256": None,
        "recovery": recovery,
    }
    publication = ledger._connection.execute(
        "SELECT proof_sha256 FROM entity_evidence WHERE run_id=?",
        [run["run_id"]],
    ).fetchone()
    proof["publication_before_sha256"] = publication[0] if publication else None
    tail = ledger._connection.execute(
        "SELECT details_json FROM events WHERE kind=? "
        "ORDER BY cast(json_extract_string(details_json,'$.sequence') AS BIGINT) DESC LIMIT 1",
        [RECORD_KIND],
    ).fetchone()
    previous = json.loads(tail[0]) if tail else None
    retained = {
        "sequence": previous["sequence"] + 1 if previous else 1,
        "previous_sha256": previous["proof_sha256"] if previous else "",
        "proof_sha256": digest(proof),
        "proof": proof,
    }
    if (
        len(canonical(retained).encode()) > MAX_ENTITY_COMPLETION_BYTES
        or retained["sequence"] > MAX_ENTITY_COMPLETIONS
    ):
        raise CoordinationError("Entity completion exceeds record/count budget")
    entity_intent._budget(
        ledger,
        size=len(canonical(retained).encode())
        + 2 * entity_intent.MAX_ENTITY_AUDIT_BYTES,
    )
    authority = authority_reference(proof)
    with ledger._transaction(kind="entity_completion"):
        ledger._insert_entity_authority_check(authority)
        ledger._event(RECORD_KIND, run["run_id"], now, retained)
        ledger._connection.execute(
            "UPDATE entity_runs SET state='COMPLETED',block_reason='' WHERE run_id=?",
            [run["run_id"]],
        )
        ledger._connection.execute(
            "UPDATE dataset_state SET mode='DRAINING',active_run_id=NULL,block_reason='' WHERE singleton=1"
        )
        ledger._event(
            "entity_completed",
            run["run_id"],
            now,
            {
                "proof_sha256": digest(proof),
                "sequence": retained["sequence"],
                "previous_sha256": retained["previous_sha256"],
                "authority_sha256": digest(authority),
                "next_hour": proof["next_hour"],
                "mode": proof["mode"],
            },
        )
        if recovering:
            ledger._event(
                "entity_recovered",
                run["run_id"],
                now,
                {**recovery, "proof_sha256": digest(proof)},
            )
        ledger._validate()
    ledger._entity_live_intent = None
    ledger._entity_clean_recovery = None
    ledger._authority_checked = digest(authority)
    return {**ledger.snapshot(), "entity_run_id": run["run_id"]}
