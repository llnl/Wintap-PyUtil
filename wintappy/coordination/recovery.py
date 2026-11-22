"""Explicit audited recovery; missing original evidence can never become success.

Resolution supports original clean complete runs and separately audited exact
checkpoint/deletion repair after a recorded exit-2 failure. Missing original
migrations and unresolved entity publications remain blocked.
"""

import json

from .completion import _build_completion_proof
from .coordinator import _Clock
from .execution import (
    PreparedExecution,
    load_execution,
    recheck_execution,
    resolve_storage,
)
from .ledger import _validate_block_reason
from .protocol import CoordinationError, identifier, make_cancellation, timestamp


def _confirmation(confirmed, reason):
    if confirmed is not True:
        raise CoordinationError(
            "Explicit confirmation that original processes cannot resume is required"
        )
    _validate_block_reason(reason)


def release_reader(ledger, job_id, now, *, confirmed=False, reason=None):
    _confirmation(confirmed, reason)
    identifier(job_id)
    timestamp(now)
    ledger._validate()
    with ledger._transaction():
        job = ledger.job(job_id)
        if job is None or job["state"] not in {"WAITING", "GRANTED"}:
            raise CoordinationError(
                "Reader recovery requires the original outstanding job"
            )
        request = json.loads(job["request_json"])
        # Operator cancellation, not an invented DBT exit/shutdown observation.
        from .protocol import canonical, digest

        terminal = make_cancellation(request, now)
        ledger._ingest(canonical(terminal), now, operator_recovery=True)
        ledger._event(
            "reader_recovered",
            job_id,
            now,
            {
                "reason": reason,
                "confirmed_cannot_resume": True,
                "request_sha256": job["request_sha256"],
                "grant_id": job["grant_id"],
                "terminal_sha256": digest(terminal),
            },
        )
    return ledger.snapshot()


def load_original_execution(ledger, run_id, storage_config):
    from pathlib import Path

    identifier(run_id)
    run = ledger.writer_run(run_id)
    if run is None:
        raise CoordinationError("Original writer record required")
    intent = json.loads(run["intent_json"])
    execution = intent.get("execution")
    if not execution:
        raise CoordinationError(
            "Original execution receipt is missing; recovery remains blocked"
        )
    receipt = load_execution(execution["path"], execution["sha256"])
    binding = receipt["binding"]
    storage = receipt["settings"]["storage"]
    credentials = {}
    if storage:
        current, credentials = resolve_storage(
            binding,
            {
                **storage_config,
                "region": storage["region"],
                "endpoint_url": storage["endpoint_url"],
                "extension_directory": storage["extension_directory"],
            },
        )
        if current != storage:
            raise CoordinationError(
                "Recovery storage scope differs from original execution"
            )
    prepared = PreparedExecution(
        Path(execution["path"]),
        execution["sha256"],
        receipt,
        tuple(receipt["command"]),
        binding,
        credentials,
    )
    recheck_execution(prepared, ledger)
    return prepared


def load_original_entity_execution(ledger, run_id, storage_config):
    """Authenticate the blocked typed owner's actual receipt; never prepare a run."""
    from pathlib import Path

    from .entity_execution import load_entity_execution, recheck_entity_execution
    from .entity_intent import validate_intent

    identifier(run_id)
    if not isinstance(storage_config, dict) or set(storage_config) - {
        "profile",
        "region",
        "endpoint_url",
        "extension_directory",
    }:
        raise CoordinationError(
            "Entity recovery storage configuration must be allowlisted"
        )
    run = ledger.entity_run(run_id)
    state = ledger.snapshot()
    if (
        run is None
        or run["state"] != "BLOCKED"
        or state["mode"] != "BLOCKED"
        or state["active_run_id"] != run_id
        or state["readers"]
        or ledger._entity_live_intent is not None
    ):
        raise CoordinationError("Original blocked typed entity owner required")
    intent = validate_intent(ledger, run)
    execution = intent["execution"]
    receipt = load_entity_execution(execution["path"], execution["sha256"])
    if receipt != execution["receipt"]:
        raise CoordinationError(
            "Actual original entity execution receipt differs from intent"
        )
    prepared = PreparedExecution(
        Path(execution["path"]),
        execution["sha256"],
        receipt,
        tuple(receipt["command"]),
        receipt["binding"],
        {},
    )
    # Validate original scope, path and runtime before resolving any credentials.
    recheck_entity_execution(prepared, ledger)
    storage = receipt["settings"]["storage"]
    credentials = {}
    if storage:
        current, credentials = resolve_storage(
            receipt["binding"],
            {
                **storage_config,
                "region": storage["region"],
                "endpoint_url": storage["endpoint_url"],
                "extension_directory": storage["extension_directory"],
            },
        )
        if current != storage:
            raise CoordinationError(
                "Entity recovery storage scope differs from original execution"
            )
    return prepared._replace(credentials=credentials)


def repair_checkpoints(
    ledger, prepared, now, *, confirmed=False, reason=None, replace_checkpoints=False
):
    from .maintenance import repair

    return repair(
        ledger,
        prepared,
        now,
        confirmed=confirmed,
        reason=reason,
        replace_checkpoints=replace_checkpoints,
    )


def repair_deletions(
    ledger, prepared, now, *, confirmed=False, reason=None, replace_checkpoints=False
):
    from .maintenance import repair

    return repair(
        ledger,
        prepared,
        now,
        confirmed=confirmed,
        reason=reason,
        replace_checkpoints=replace_checkpoints,
        operation="deletions-repair",
    )


def resolve_hour(
    ledger, prepared, now, *, clock, confirmed=False, reason=None, maintenance_id=None
):
    _confirmation(confirmed, reason)
    if not callable(clock):
        raise CoordinationError("Recovery requires a fresh evolving clock")
    observed = _Clock(now, clock)
    state = ledger.snapshot()
    receipt = recheck_execution(prepared, ledger)
    if (
        state["mode"] != "BLOCKED"
        or state["readers"]
        or state["active_run_id"] != receipt["run_id"]
        or state["next_hour"] != receipt["hour_start"]
    ):
        raise CoordinationError(
            "Recovery requires the exact original blocked hour/run under exclusion"
        )
    ledger._completion_recovery = None
    try:
        from .maintenance import no_pending

        operations = no_pending(ledger)
        recovery = None
        if maintenance_id is not None:
            recovery = operations.get(identifier(maintenance_id))
            if (
                recovery is None
                or recovery["finished"] is None
                or recovery["receipt"]["execution"] != receipt
            ):
                raise CoordinationError(
                    "Resolution requires the exact completed maintenance"
                )
        proof = _build_completion_proof(ledger, prepared, observed, recovery=recovery)
        # Repaired proofs retain the actual original failure and separate repair;
        # missing migration phases or PID observations can never become success.
        if ledger.snapshot() != state:
            raise CoordinationError("Recovery disposition changed during verification")
        ledger._completion_recovery = (receipt["run_id"], reason)
        return ledger._complete_writer(proof, _recovery=reason)
    finally:
        ledger._completion_recovery = None
        ledger._completion_live_run = None
