"""Receipt-bound narrow maintenance on an already blocked original writer.

The original intent keeps exclusion through crashes and repairs. Maintenance never
advances the cursor; only fresh independent repaired-phase resolution can do that.
"""

import hashlib
import json
from pathlib import Path
from uuid import uuid4

from .execution import execution_environment, recheck_execution, validate_receipt
from .ledger import _mkdir, _validate_block_reason
from .protocol import CoordinationError, canonical, digest, identifier, timestamp
from .supervisor import _publish, run_supervised

MAX_MAINTENANCE_BYTES = 128 * 1024
MAX_MAINTENANCE_RUNS = 10_000
MAX_MAINTENANCE_TOTAL_BYTES = 64 * 1024 * 1024
HISTORY_PAGE_SIZE = 256
AUDIT_KINDS = (
    "kind IN ('maintenance_intent','maintenance_finished','maintenance_abandoned')"
)


def command_for(receipt):
    base = receipt["execution"]
    original = base["command"]
    command = original[:5] + [receipt["operation"]]
    # No arbitrary CLI dispatch, scope changes, budgets or credential lookup.
    removed = {
        "--target-size-bytes",
        "--grace-minutes",
    }
    if receipt["operation"] == "checkpoints-repair":
        removed.update(
            ("--delete-confirm-attempts", "--delete-confirm-initial-delay-seconds")
        )
    index = 6
    while index < len(original):
        option = original[index]
        if option == "--delete-source":
            index += 1
            continue
        value = original[index + 1]
        if option == "--run-id":
            value = receipt["cli_run_id"]
        elif option == "--coordinator-run-id":
            value = receipt["maintenance_id"]
        if option not in removed:
            command.extend([option, value])
        index += 2
    command.extend(["--repair-run-id", base["cli_run_id"], "--confirm-repair"])
    if receipt["replace_checkpoints"]:
        command.append("--confirm-replace-checkpoints")
    return command


def validate_maintenance(receipt, run, binding):
    if (
        not isinstance(receipt, dict)
        or set(receipt)
        != {
            "version",
            "operation",
            "maintenance_id",
            "cli_run_id",
            "execution",
            "execution_sha256",
            "created_at",
            "reason",
            "confirmed_cannot_resume",
            "replace_checkpoints",
            "command",
            "observations_directory",
        }
        or type(receipt["version"]) is not int
        or receipt["operation"] not in {"checkpoints-repair", "deletions-repair"}
        or receipt["version"]
        != (2 if receipt["operation"] == "deletions-repair" else 1)
        or receipt["confirmed_cannot_resume"] is not True
        or type(receipt["replace_checkpoints"]) is not bool
        or len(canonical(receipt).encode()) > MAX_MAINTENANCE_BYTES
    ):
        raise CoordinationError("Invalid bounded maintenance receipt")
    identifier(receipt["maintenance_id"])
    identifier(receipt["cli_run_id"])
    _validate_block_reason(receipt["reason"])
    base = validate_receipt(receipt["execution"])
    intent = json.loads(run["intent_json"])
    expected_path = (
        Path(base["observations_directory"]).parent
        / "maintenance"
        / receipt["maintenance_id"]
        / "observations"
    )
    if (
        receipt["maintenance_id"]
        in {receipt["cli_run_id"], base["run_id"], base["cli_run_id"]}
        or receipt["cli_run_id"] in {base["run_id"], base["cli_run_id"]}
        or base["binding"] != binding
        or intent.get("execution", {}).get("receipt") != base
        or receipt["execution_sha256"] != digest(base)
        or receipt["operation"] == "checkpoints-repair"
        and not base["settings"]["entity"]
        or base["settings"]["delete_source"] is not True
        or timestamp(receipt["created_at"]) < timestamp(run["started_at"])
        or receipt["observations_directory"] != str(expected_path)
        or receipt["command"] != command_for(receipt)
    ):
        raise CoordinationError(
            "Maintenance is not bound to original blocked execution"
        )
    return receipt


def validate_finished(receipt, finished):
    from .completion import _clean

    if (
        not isinstance(finished, dict)
        or set(finished) != {"receipt_sha256", "supervision", "report"}
        or finished["receipt_sha256"] != digest(receipt)
        or not isinstance(finished["report"], dict)
        or len(canonical(finished).encode()) > MAX_MAINTENANCE_BYTES
    ):
        raise CoordinationError("Invalid retained maintenance outcome")
    _clean(finished["supervision"], receipt["execution"], receipt["command"])
    report = finished["report"]
    captured = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode()
    if (
        report.get("status") != "repaired"
        or report.get("run_id") != receipt["cli_run_id"]
        or report.get("coordinator_run_id") != receipt["maintenance_id"]
        or report.get("repair_run_id") != receipt["execution"]["cli_run_id"]
        or finished["supervision"]["stdout"]["sha256"]
        != hashlib.sha256(captured).hexdigest()
        or finished["supervision"]["stdout"]["bytes_saved"] != len(captured)
    ):
        raise CoordinationError("Maintenance report/capture/scope mismatch")


def _budget(ledger, *, records=0, size=0):
    count, total, largest = ledger._connection.execute(
        "SELECT count(*),coalesce(sum(octet_length(encode(details_json))),0),"
        "coalesce(max(octet_length(encode(details_json))),0) FROM events WHERE "
        + AUDIT_KINDS
    ).fetchone()
    if (
        count + records > MAX_MAINTENANCE_RUNS * 2
        or total + size > MAX_MAINTENANCE_TOTAL_BYTES
        or largest > MAX_MAINTENANCE_BYTES
    ):
        raise CoordinationError(
            "Maintenance history exceeds bounded budgets; never prune"
        )


def history(ledger):
    """Validate bounded pages, including pending state; no runtime files on reopen."""
    _budget(ledger)  # Refuse oversized JSON before it reaches the Python client.
    cursor = ledger._connection.cursor()
    operations, identities = {}, set()
    try:
        cursor.execute(
            "SELECT kind,identity,observed_at,details_json FROM events WHERE "
            + AUDIT_KINDS
            + " ORDER BY observed_at,CASE WHEN kind='maintenance_intent' THEN 0 ELSE 1 END"
        )
        while rows := cursor.fetchmany(HISTORY_PAGE_SIZE):
            for kind, identity, now, payload in rows:
                identifier(identity)
                timestamp(now)
                value = json.loads(payload)
                if not isinstance(value, dict) or canonical(value) != payload:
                    raise CoordinationError("Maintenance audit shape/bytes changed")
                if kind == "maintenance_intent":
                    validate_maintenance(
                        value,
                        ledger.writer_run(value["execution"]["run_id"]),
                        json.loads(ledger.binding),
                    )
                    run = ledger.writer_run(value["execution"]["run_id"])
                    if (
                        run is None
                        or now != value["created_at"]
                        or identity != value["maintenance_id"]
                        or identities & {identity, value["cli_run_id"]}
                    ):
                        raise CoordinationError(
                            "Duplicate/mismatched maintenance intent"
                        )
                    identities.update((identity, value["cli_run_id"]))
                    operations[identity] = {
                        "receipt": value,
                        "finished": None,
                        "abandoned": None,
                    }
                else:
                    item = operations.get(identity)
                    if (
                        item is None
                        or item["finished"] is not None
                        or item["abandoned"] is not None
                        or timestamp(now) < timestamp(item["receipt"]["created_at"])
                    ):
                        raise CoordinationError("Maintenance terminal audit conflict")
                    run = ledger.writer_run(item["receipt"]["execution"]["run_id"])
                    proof = ledger.completion_evidence(run["run_id"])
                    if proof is not None and timestamp(now) > timestamp(
                        proof["completed_at"]
                    ):
                        raise CoordinationError(
                            "Maintenance audit postdates original completion"
                        )
                    if kind == "maintenance_finished":
                        validate_finished(item["receipt"], value)
                        item["finished"] = value
                    else:
                        if (
                            set(value)
                            != {"reason", "confirmed_cannot_resume", "receipt_sha256"}
                            or value["confirmed_cannot_resume"] is not True
                            or value["receipt_sha256"] != digest(item["receipt"])
                        ):
                            raise CoordinationError("Invalid maintenance abandonment")
                        _validate_block_reason(value["reason"])
                        item["abandoned"] = value
        if len(operations) > MAX_MAINTENANCE_RUNS:
            raise CoordinationError("Maintenance run budget exhausted; never prune")
        pending = [
            item
            for item in operations.values()
            if item["finished"] is None and item["abandoned"] is None
        ]
        state = ledger.snapshot()
        if len(pending) > 1 or any(
            state["mode"] != "BLOCKED"
            or state["active_run_id"] != item["receipt"]["execution"]["run_id"]
            for item in pending
        ):
            raise CoordinationError(
                "Outstanding maintenance lost original blocked exclusion"
            )
        return operations
    except (TypeError, ValueError, KeyError, RecursionError):
        raise CoordinationError("Malformed retained maintenance history") from None
    finally:
        cursor.close()


def no_pending(ledger):
    operations = history(ledger)
    if any(
        item["finished"] is None and item["abandoned"] is None
        for item in operations.values()
    ):
        raise CoordinationError(
            "Unresolved maintenance needs audited cannot-resume abandonment"
        )
    return operations


def repair(
    ledger,
    prepared,
    now,
    *,
    confirmed=False,
    reason=None,
    replace_checkpoints=False,
    operation="checkpoints-repair",
):
    from .completion import _recheck_archive
    from .recovery import _confirmation
    from .verification import _observations, _read

    _confirmation(confirmed, reason)
    ledger._validate()
    operations = no_pending(ledger)
    if (
        len(operations) >= MAX_MAINTENANCE_RUNS
        or type(replace_checkpoints) is not bool
        or operation not in {"checkpoints-repair", "deletions-repair"}
    ):
        raise CoordinationError("Invalid maintenance scope/budget")
    base = recheck_execution(prepared, ledger)
    state = ledger.snapshot()
    if (
        state["mode"] != "BLOCKED"
        or state["readers"]
        or state["active_run_id"] != base["run_id"]
        or state["next_hour"] != base["hour_start"]
    ):
        raise CoordinationError(
            "Maintenance requires the exact blocked original writer"
        )
    original = _observations(
        Path(base["observations_directory"]), base, prepared.command, successful=False
    )
    report = original[2]
    evidence = report.get("phase_evidence")
    if (
        report.get("status")
        not in (
            {"completed_with_deletion_errors", "error"}
            if operation == "deletions-repair"
            else {"error"}
        )
        or report.get("run_id") != base["cli_run_id"]
        or report.get("coordinator_run_id") != base["run_id"]
        or not isinstance(evidence, dict)
        or set(evidence)
        != {"directory", "last_sha256", "records", "migration_complete"}
        or evidence["migration_complete"] is not (operation == "checkpoints-repair")
        or evidence["directory"]
        != str(Path(base["settings"]["run_evidence_directory"]) / base["cli_run_id"])
    ):
        raise CoordinationError("Maintenance requires original failed phase evidence")
    _recheck_archive(
        base,
        {
            "archive_records": evidence["records"],
            "archive_sha256": evidence["last_sha256"],
        },
    )
    _budget(ledger, records=2, size=2 * MAX_MAINTENANCE_BYTES)
    receipt = {
        "version": 2 if operation == "deletions-repair" else 1,
        "operation": operation,
        "maintenance_id": uuid4().hex,
        "cli_run_id": uuid4().hex,
        "execution": base,
        "execution_sha256": prepared.sha256,
        "created_at": now,
        "reason": reason,
        "confirmed_cannot_resume": True,
        "replace_checkpoints": replace_checkpoints,
    }
    directory = prepared.path.parent / "maintenance" / receipt["maintenance_id"]
    receipt["observations_directory"] = str(directory / "observations")
    receipt["command"] = command_for(receipt)
    validate_maintenance(receipt, ledger.writer_run(base["run_id"]), prepared.binding)
    _mkdir(directory)
    path = directory / "receipt.json"
    _publish(path, receipt)  # Durable receipt before intent COMMIT, before any launch.
    with ledger._transaction(kind="maintenance"):
        ledger._event("maintenance_intent", receipt["maintenance_id"], now, receipt)
    if (
        _read(path, MAX_MAINTENANCE_BYTES) != canonical(receipt).encode()
        or recheck_execution(prepared, ledger) != base
    ):
        raise CoordinationError("Committed maintenance receipt changed; remain blocked")
    settings = base["settings"]
    run_supervised(
        receipt["command"],
        receipt["observations_directory"],
        timeout_seconds=settings["supervision_timeout_seconds"],
        stop_grace_seconds=settings["stop_grace_seconds"],
        shutdown_seconds=settings["shutdown_seconds"],
        max_log_bytes=settings["max_log_bytes"],
        cwd=base["cwd"],
        env=execution_environment(prepared),
    )
    observed = _observations(
        Path(receipt["observations_directory"]), base, receipt["command"]
    )
    finished = {
        "receipt_sha256": digest(receipt),
        "supervision": observed[0],
        "report": observed[2],
    }
    validate_finished(receipt, finished)
    if (
        _read(path, MAX_MAINTENANCE_BYTES) != canonical(receipt).encode()
        or recheck_execution(prepared, ledger) != base
        or ledger.snapshot() != state
    ):
        raise CoordinationError(
            "Maintenance evidence/disposition changed; remain blocked"
        )
    with ledger._transaction(kind="maintenance_finished"):
        ledger._event("maintenance_finished", receipt["maintenance_id"], now, finished)
    return receipt["maintenance_id"]


def abandon(ledger, maintenance_id, now, *, confirmed=False, reason=None):
    from .recovery import _confirmation

    _confirmation(confirmed, reason)
    timestamp(now)
    ledger._validate()
    item = history(ledger).get(identifier(maintenance_id))
    if (
        item is None
        or item["finished"] is not None
        or item["abandoned"] is not None
        or ledger.snapshot()["mode"] != "BLOCKED"
        or ledger.snapshot()["active_run_id"] != item["receipt"]["execution"]["run_id"]
        or timestamp(now) < timestamp(item["receipt"]["created_at"])
    ):
        raise CoordinationError(
            "Only outstanding maintenance under its original block can be abandoned"
        )
    with ledger._transaction():
        ledger._event(
            "maintenance_abandoned",
            maintenance_id,
            now,
            {
                "reason": reason,
                "confirmed_cannot_resume": True,
                "receipt_sha256": digest(item["receipt"]),
            },
        )
    return ledger.snapshot()
