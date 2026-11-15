"""Immutable original-generation observations, not entity completion or permission.

Retention obtains fresh current-only standalone evidence under acknowledged typed
exclusion. Ordinary history/accessors authenticate bounded retained JSON only; they
never reconstruct an observation from runtime files or today's aggregate. Neither
successful retention nor an accessor advances authority, the raw cursor or admission.
"""

import json

from . import entity_completion, entity_intent
from .coordinator import _Clock
from .ledger import COLUMNS
from .protocol import CoordinationError, canonical, digest, identifier, timestamp

MAX_ENTITY_PUBLICATION_BYTES = 128 * 1024
MAX_ENTITY_PUBLICATIONS = 10_000
PUBLICATION_FIELDS = {
    "version",
    "operation",
    "ledger_id",
    "binding",
    "run_id",
    "cli_run_id",
    "intent_sha256",
    "recorded_at",
    "observation",
    "completion",
}


def _budget(ledger, *, records=0, size=0):
    """Compute SQL sizes before returning any publication or owner payload."""
    count, total, largest = ledger._connection.execute(
        "SELECT count(*),coalesce(sum(octet_length(encode(proof_json))),0),"
        "coalesce(max(octet_length(encode(proof_json))),0) FROM entity_evidence"
    ).fetchone()
    if (
        count + records > MAX_ENTITY_PUBLICATIONS
        or largest > MAX_ENTITY_PUBLICATION_BYTES
        or size > MAX_ENTITY_PUBLICATION_BYTES
        or records
        and MAX_ENTITY_PUBLICATION_BYTES <= 0
    ):
        raise CoordinationError(
            "Entity publication history exceeds budget; never prune"
        )
    return count, total


def _audit(sequence, intent_sha256, proof_sha256, previous_sha256):
    return {
        "sequence": sequence,
        "intent_sha256": intent_sha256,
        "proof_sha256": proof_sha256,
        "previous_sha256": previous_sha256,
    }


def _validate_publication(ledger, row, previous_recorded):
    """Contract/retained-original authentication only, without any filesystem reads."""
    sequence, run_id, payload, sha256, predecessor = row
    try:
        proof = json.loads(payload)
        if (
            not isinstance(proof, dict)
            or set(proof) != PUBLICATION_FIELDS
            or type(proof["version"]) is not int
            or proof["version"] != 1
            or proof["operation"] != "entity-original-publication"
            or canonical(proof) != payload
            or digest(proof) != sha256
            or proof["ledger_id"] != ledger.ledger_id
            or canonical(proof["binding"]) != ledger.binding
            or proof["run_id"] != run_id
        ):
            raise ValueError
        identifier(run_id)
        rows = ledger._connection.execute(
            "SELECT * FROM entity_runs WHERE run_id=? LIMIT 2", [run_id]
        ).fetchall()
        if len(rows) != 1:
            raise ValueError
        run = dict(zip(COLUMNS["entity_runs"], rows[0], strict=True))
        intent = entity_intent.validate_intent(ledger, run)
        if (
            run["state"] not in {"INTENT", "BLOCKED", "COMPLETED"}
            or proof["cli_run_id"] != run["cli_run_id"]
            or proof["intent_sha256"] != run["intent_sha256"]
        ):
            raise ValueError
        core = entity_completion.validate_completion_observation(
            proof["observation"], proof["binding"], ledger.ledger_id, intent=intent
        )
        recorded = timestamp(proof["recorded_at"])
        if (
            canonical(proof["completion"]) != canonical(core)
            or recorded < timestamp(proof["observation"]["observed_at"])
            or previous_recorded is not None
            and recorded < timestamp(previous_recorded)
        ):
            raise ValueError
        audits = ledger._connection.execute(
            "SELECT observed_at,details_json FROM events "
            "WHERE kind='entity_publication' AND identity=? LIMIT 2",
            [run_id],
        ).fetchall()
        if audits != [
            (
                proof["recorded_at"],
                canonical(_audit(sequence, run["intent_sha256"], sha256, predecessor)),
            )
        ]:
            raise ValueError
        if run["state"] == "BLOCKED":
            blocks = ledger._connection.execute(
                "SELECT observed_at FROM events "
                "WHERE kind='entity_unresolved' AND identity=? LIMIT 2",
                [run_id],
            ).fetchall()
            if len(blocks) != 1 or timestamp(blocks[0][0]) < recorded:
                raise ValueError
        return proof["recorded_at"]
    except (TypeError, ValueError, KeyError, AttributeError, RecursionError):
        raise CoordinationError(
            "Stored entity publication/original/audit conflict"
        ) from None


def validate_history(ledger):
    """Same-connection, SQL-limited keyset pages, materialized before nested SQL.

    The caller has checked combined typed budgets before any owner/payload fetch.
    Publications keep their original observations after separately authenticated typed
    completion; this history never itself advances authority or grants permission.
    """
    expected_count, _ = _budget(ledger)
    count, previous_hash, previous_recorded, last_sequence = 0, "", None, None
    while True:
        batch = ledger._connection.execute(
            "SELECT * FROM entity_evidence "
            + ("WHERE sequence>? " if last_sequence is not None else "")
            + "ORDER BY sequence LIMIT 256",
            [last_sequence] if last_sequence is not None else [],
        ).fetchall()
        if not batch:
            break
        last_sequence = batch[-1][0]
        for row in batch:
            count += 1
            sequence, _, payload, sha256, predecessor = row
            if (
                sequence != count
                or predecessor != previous_hash
                or len(payload.encode()) > MAX_ENTITY_PUBLICATION_BYTES
            ):
                raise CoordinationError(
                    "Entity publication chain invalid or exceeds budget"
                )
            previous_recorded = _validate_publication(ledger, row, previous_recorded)
            previous_hash = sha256
    if count != expected_count:
        raise CoordinationError("Entity publication history count conflict")


def get_retained_entity_publication(ledger, run_id):
    """Return an authenticated copy of the exact original core, never today's state."""
    identifier(run_id)
    ledger._require_entity_schema()
    ledger._validate()
    row = ledger._connection.execute(
        "SELECT proof_json FROM entity_evidence WHERE run_id=?", [run_id]
    ).fetchone()
    proof = json.loads(row[0]) if row else None
    if proof is None:
        raise CoordinationError(
            "Original entity publication is not retained; never reconstruct"
        )
    return json.loads(canonical(proof["completion"]))


def retain_entity_publication_prepared(
    ledger, prepared, now, *, clock, original, prelaunch
):
    """Retain fresh independently verified current evidence only, without completion.

    Caller dictionaries cannot stand in for a completion observation. Refusals before
    COMMIT leave ownership/acknowledgment unchanged; a committed lost acknowledgment
    retains the publication and blocks the original typed owner for audited recovery.
    """
    if not callable(clock):
        raise CoordinationError("Entity publication requires a fresh evolving clock")
    observed = _Clock(now, clock)
    run_id, proof_sha256 = None, None
    try:
        ledger._require_entity_schema()
        ledger._validate()
        _budget(ledger, records=1)
        # Reserve bounded publication/audit and a possible lost-ack block audit
        # before a verifier launch. Shared history never silently prunes originals.
        entity_intent._budget(
            ledger,
            size=MAX_ENTITY_PUBLICATION_BYTES
            + 2 * entity_intent.MAX_ENTITY_AUDIT_BYTES,
        )
        receipt, intent, _ = entity_completion._live(
            ledger, prepared, original, prelaunch, now
        )
        run_id = receipt["run_id"]
        if ledger.entity_evidence(run_id) is not None:
            raise CoordinationError(
                "Original entity publication already retained; immutable"
            )
        state, anchor = ledger.snapshot(), ledger.authority_high_water()
        acknowledgment = ledger._entity_live_intent
        current = observed.observe()
        observation = entity_completion.verify_entity_completion_prepared(
            ledger, prepared, current, original=original, prelaunch=prelaunch
        )
        core = entity_completion.recheck_completion_observation(
            ledger, prepared, observation
        )
        recorded = observed.observe()
        proof = {
            "version": 1,
            "operation": "entity-original-publication",
            "ledger_id": ledger.ledger_id,
            "binding": prepared.binding,
            "run_id": run_id,
            "cli_run_id": receipt["cli_run_id"],
            "intent_sha256": digest(intent),
            "recorded_at": recorded,
            "observation": observation,
            "completion": core,
        }
        payload, proof_sha256 = canonical(proof), digest(proof)
        _budget(ledger, records=1, size=len(payload.encode()))
        tail = ledger._connection.execute(
            "SELECT sequence,proof_sha256 FROM entity_evidence ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        sequence, predecessor = (tail[0] + 1, tail[1]) if tail else (1, "")
        details = _audit(sequence, digest(intent), proof_sha256, predecessor)
        audit_size = len(canonical(details).encode())
        if audit_size > entity_intent.MAX_ENTITY_AUDIT_BYTES:
            raise CoordinationError("Entity publication audit exceeds budget")
        entity_intent._budget(
            ledger,
            size=len(payload.encode())
            + audit_size
            + entity_intent.MAX_ENTITY_AUDIT_BYTES,
        )
        if (
            ledger.snapshot() != state
            or ledger.authority_high_water() != anchor
            or ledger._entity_live_intent != acknowledgment
        ):
            raise CoordinationError("Entity publication scope changed before COMMIT")
        with ledger._transaction(kind="entity_publication"):
            ledger._connection.execute(
                "INSERT INTO entity_evidence VALUES (?,?,?,?,?)",
                [sequence, run_id, payload, proof_sha256, predecessor],
            )
            ledger._event("entity_publication", run_id, recorded, details)
            ledger._validate()
        return json.loads(payload)
    except BaseException as error:
        # _transaction's after-COMMIT hook can fail before returning to this API.
        # Inspect the exact staged identity under the same lock, never erase it or
        # manufacture a successful completion/authority transition from its presence.
        if (
            run_id is not None
            and proof_sha256 is not None
            and ledger._connection.execute(
                "SELECT proof_sha256 FROM entity_evidence WHERE run_id=?", [run_id]
            ).fetchone()
            == (proof_sha256,)
        ):
            entity_intent.block_entity(
                ledger,
                run_id,
                observed.last,
                "Entity publication acknowledgment uncertain; preserve originals and require audited recovery",
            )
        if isinstance(error, (KeyboardInterrupt, SystemExit, CoordinationError)):
            raise
        raise CoordinationError(
            "Entity publication refused/uncertain; details suppressed"
        ) from None
