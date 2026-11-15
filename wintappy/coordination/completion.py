"""Bounded immutable completion proofs, authenticated independently of report flags.

Only the uninterrupted qualified writer adapter can commit a clean live intent.
Retained proof validation never reopens old runtime artifacts or grants permission.
"""

import hashlib
import json
import re
from pathlib import Path

from .authority import validate_anchor, validate_proof
from .execution import recheck_execution, validate_intent_execution
from .protocol import CoordinationError, canonical, digest, identifier, timestamp
from .supervisor import _validate_result

MAX_HISTORY_BYTES = 128 * 1024
MAX_COMPLETED_RUNS = 10_000


def _manifest(receipt, command):
    settings = receipt["settings"]
    return {
        "command": list(command),
        "cwd": receipt["cwd"],
        "timeout_seconds": settings["supervision_timeout_seconds"],
        "stop_grace_seconds": settings["stop_grace_seconds"],
        "shutdown_seconds": settings["shutdown_seconds"],
        "max_log_bytes": settings["max_log_bytes"],
    }


def _clean(supervision, receipt, command, *, successful=True):
    _validate_result(supervision)
    if (
        supervision["status"] != "exited"
        or supervision["exit_code"] != (0 if successful else 2)
        or supervision["children_stopped"] is not True
        or any(supervision[s]["truncated"] for s in ("stdout", "stderr"))
        or supervision["command_sha256"] != digest(_manifest(receipt, command))
        or any(
            supervision[s]["bytes_saved"] > receipt["settings"]["max_log_bytes"]
            for s in ("stdout", "stderr")
        )
        or supervision["stdout"]["bytes_saved"]
        > receipt["settings"]["max_report_bytes"]
    ):
        raise CoordinationError("Completion supervision is not clean and receipt-bound")


def _request(receipt, intent_sha256, writer_supervision, before, recovery=None):
    value = {
        "version": 1,
        "arguments": receipt["command"][5:],
        "execution_sha256": digest(receipt),
        "supervision_sha256": digest(writer_supervision),
        "intent_sha256": intent_sha256,
        "authority_high_water": before,
    }
    if recovery is not None:
        value.update(
            version=3,
            repair_run_id=recovery["receipt"]["cli_run_id"],
            repair_coordinator_run_id=recovery["receipt"]["maintenance_id"],
            repair_replace_checkpoints=recovery["receipt"]["replace_checkpoints"],
        )
        if recovery["receipt"]["operation"] == "deletions-repair":
            value.update(version=4, repair_operation="deletions-repair")
    return value


def _verifier_command(receipt, observations, request):
    return [
        receipt["runtime"]["executable"],
        "-I",
        "-B",
        "-m",
        "raw_sensor_compact.completion",
        "--request",
        str(observations.parent / "request.json"),
        "--request-sha256",
        digest(request),
    ]


def validate_completion_proof(proof, run, binding, ledger_id):
    """Validate all retained identities; external files are rechecked before COMMIT."""
    from .coordinator import writer_due

    recovery = proof.get("recovery") if isinstance(proof, dict) else None
    retry = proof.get("retry") if isinstance(proof, dict) else None
    if (
        not isinstance(proof, dict)
        or set(proof)
        != {
            "version",
            "ledger_id",
            "run_id",
            "completed_at",
            "next_hour",
            "mode",
            "verification",
            "writer_supervision",
            "authority_before",
            "authority_proof",
        }
        | ({"recovery"} if recovery is not None else set())
        | ({"retry"} if retry is not None else set())
        or type(proof["version"]) is not int
        or proof["version"]
        != (
            4
            if retry is not None and recovery is None
            else 3
            if recovery is not None and recovery.get("operation") == "deletions-repair"
            else 2
            if recovery is not None
            else 1
        )
        or proof["ledger_id"] != ledger_id
        or proof["run_id"] != run["run_id"]
        or len(canonical(proof).encode()) > MAX_HISTORY_BYTES
    ):
        raise CoordinationError("Invalid bounded completion proof")
    if retry is not None and (
        recovery is not None
        or not isinstance(retry, dict)
        or set(retry)
        != {
            "disposition_sha256",
            "prelaunch_sha256",
            "original_migration_sha256",
            "retry_migration_sha256",
        }
        or any(
            not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value)
            for value in retry.values()
        )
        or retry["original_migration_sha256"] != retry["retry_migration_sha256"]
    ):
        raise CoordinationError("Invalid original-bound retry completion identity")
    identifier(ledger_id)
    intent = json.loads(run["intent_json"])
    if digest(intent) != run["intent_sha256"] or "execution" not in intent:
        raise CoordinationError(
            "Completion requires original receipt-bound writer intent"
        )
    validate_intent_execution(intent["execution"], intent)
    receipt = intent["execution"]["receipt"]
    completed, started = timestamp(proof["completed_at"]), timestamp(run["started_at"])
    grace = receipt["settings"]["grace_minutes"]
    if (
        receipt["binding"] != binding
        or completed < started
        or not writer_due(receipt["hour_start"], run["started_at"], grace_minutes=grace)
        or proof["next_hour"] != receipt["hour_end"]
        or proof["mode"]
        != (
            "DRAINING"
            if writer_due(
                receipt["hour_end"], proof["completed_at"], grace_minutes=grace
            )
            else "OPEN"
        )
    ):
        raise CoordinationError("Completion clock/cursor/backlog scope mismatch")
    writer = proof["writer_supervision"]
    if recovery is not None:
        from .maintenance import validate_finished, validate_maintenance

        if (
            set(recovery) != {"operation", "receipt", "finished"}
            or recovery["operation"] not in {"checkpoints-repair", "deletions-repair"}
            or recovery["operation"] != recovery["receipt"].get("operation")
        ):
            raise CoordinationError("Invalid repaired-phase completion")
        validate_maintenance(recovery["receipt"], run, binding)
        validate_finished(recovery["receipt"], recovery["finished"])
        if completed < timestamp(recovery["receipt"]["created_at"]):
            raise CoordinationError("Repaired completion precedes maintenance")
    _clean(writer, receipt, receipt["command"], successful=recovery is None)
    result = proof["verification"]
    if not isinstance(result, dict):
        raise CoordinationError("Invalid completion data evidence")
    entity = receipt["settings"]["entity"]
    before = proof["authority_before"]
    if entity:
        validate_anchor(before, binding)
    elif (
        before is not None
        or proof["authority_proof"] is not None
        or result.get("checkpoint_sha256") is not None
        or result.get("checkpoint_generation_id") is not None
    ):
        raise CoordinationError("Inactive completion authority must be explicit")
    request = _request(receipt, run["intent_sha256"], writer, before, recovery)
    roots = binding["roots"]
    data_binding = {
        "raw_root": roots["raw"],
        "compacted_root": roots["compacted"],
        "entity_root": roots["entity"] if entity else "",
        "metrics_root": roots["metrics"] if entity else "",
        "namespace": entity["namespace"] if entity else "",
        "hosts": entity["hosts"] if entity else [],
        "identity_policy": entity["identity_policy"] if entity else "reject",
        "identity_max_ratio": entity["identity_max_ratio"] if entity else 0.05,
        "window_start": receipt["hour_start"],
        "window_end": receipt["hour_end"],
        "event_types": receipt["settings"]["event_types"],
    }
    expected = {
        "version": 1,
        "status": "verified",
        "run_id": run["run_id"],
        "cli_run_id": run["cli_run_id"],
        "hour_start": run["hour_start"],
        "hour_end": run["hour_end"],
        "execution_sha256": digest(receipt),
        "supervision_sha256": digest(writer),
        "intent_sha256": run["intent_sha256"],
        "request_sha256": digest(request),
        "binding_sha256": digest(data_binding),
    }
    if recovery is not None:
        expected["repair_run_id"] = recovery["receipt"]["cli_run_id"]
        expected["repair_coordinator_run_id"] = recovery["receipt"]["maintenance_id"]
        if recovery["operation"] == "deletions-repair":
            expected["repair_operation"] = "deletions-repair"
    if (
        not isinstance(result, dict)
        or set(result)
        != set(expected)
        | {
            "archive_sha256",
            "archive_records",
            "raw_source_rows",
            "raw_output_rows",
            "authority_high_water",
            "checkpoint_sha256",
            "checkpoint_generation_id",
            "verifier",
            "verifier_observations",
        }
        | (
            {"repair_archive_sha256", "repair_archive_records"}
            if recovery is not None
            else set()
        )
        or any(
            result[k] != v or type(result[k]) is not type(v)
            for k, v in expected.items()
        )
        or any(
            type(result[k]) is not int or result[k] < 0
            for k in ("archive_records", "raw_source_rows", "raw_output_rows")
        )
        or not 3 <= result["archive_records"] <= 10_000
        or result["raw_source_rows"] != result["raw_output_rows"]
    ):
        raise CoordinationError("Completion data evidence shape/scope/count conflict")
    if recovery is not None and (
        type(result["repair_archive_records"]) is not int
        or not (
            4 <= result["repair_archive_records"] <= 10_000
            if recovery["operation"] == "deletions-repair"
            else result["repair_archive_records"] == 5
        )
        or not isinstance(result["repair_archive_sha256"], str)
        or not re.fullmatch(r"[a-f0-9]{64}", result["repair_archive_sha256"])
    ):
        raise CoordinationError("Repaired archive identity/count missing")
    if recovery is not None:
        maintenance = recovery["receipt"]
        phase = recovery["finished"]["report"].get("phase_evidence")
        expected_phase = {
            "directory": str(
                Path(receipt["settings"]["run_evidence_directory"])
                / maintenance["cli_run_id"]
            ),
            "last_sha256": result["repair_archive_sha256"],
            "records": result["repair_archive_records"],
            "migration_complete": True,
        }
        if canonical(phase) != canonical(expected_phase):
            raise CoordinationError(
                "Repair archive is not bound to retained maintenance capture"
            )
    for key in ("archive_sha256", "checkpoint_sha256", "checkpoint_generation_id"):
        value = result[key]
        if not entity and key != "archive_sha256" and value is None:
            continue
        if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
            raise CoordinationError("Completion archive/checkpoint identity missing")
    if entity:
        candidate = validate_anchor(result["authority_high_water"], binding, before)
        authority = proof["authority_proof"]
        if (
            validate_proof(authority, binding, ledger_id, before) != candidate
            or authority["receipt"] != receipt
            or not started <= timestamp(authority["observed_at"]) <= completed
        ):
            raise CoordinationError(
                "Completion fresh authority and data evidence disagree"
            )
    elif result["authority_high_water"] is not None:
        raise CoordinationError("Inactive completion anchor must be explicit")
    if not isinstance(result["verifier_observations"], str):
        raise CoordinationError("Completion verifier identity missing")
    observations = Path(result["verifier_observations"])
    if (
        observations.name != "observations"
        or observations.parent.parent.name != "verifications"
        or observations.parent.parent.parent
        != Path(receipt["observations_directory"]).parent
    ):
        raise CoordinationError(
            "Completion verifier observations are outside the execution"
        )
    identifier(observations.parent.name)
    _clean(
        result["verifier"], receipt, _verifier_command(receipt, observations, request)
    )
    data = {
        k: v
        for k, v in result.items()
        if k not in {"verifier", "verifier_observations"}
    }
    captured = (canonical(data) + "\n").encode()
    if (
        len(captured) > receipt["settings"]["max_report_bytes"]
        or result["verifier"]["stdout"]["bytes_saved"] != len(captured)
        or result["verifier"]["stdout"]["sha256"]
        != hashlib.sha256(captured).hexdigest()
    ):
        raise CoordinationError(
            "Completion verifier captured evidence identity mismatch"
        )


def _recheck_archive(receipt, result):
    """Recheck only original local JSON identity, never data SQL in the main runtime."""
    from .verification import _read

    limits = receipt["settings"]["archive_limits"]
    directory = (
        Path(receipt["settings"]["run_evidence_directory"]) / receipt["cli_run_id"]
    )
    paths = []
    for path in directory.iterdir():
        if path.suffix == ".json":
            paths.append(path)
            if len(paths) > limits["records"]:
                raise CoordinationError(
                    "Original completion archive exceeds record budget"
                )
    if len(paths) != result["archive_records"]:
        raise CoordinationError("Original completion archive record count changed")
    total, previous, migrations = 0, "", []
    for sequence, path in enumerate(sorted(paths)):
        encoded = _read(path, min(limits["record_bytes"], limits["run_bytes"] - total))
        total += len(encoded)
        record = json.loads(encoded)
        if encoded != (canonical(record) + "\n").encode() or set(record) != {
            "sha256",
            "evidence_version",
            "sequence",
            "previous_sha256",
            "run_id",
            "coordinator_run_id",
            "phase",
            "data",
        }:
            raise CoordinationError("Original completion archive bytes/shape changed")
        sha256 = record.pop("sha256")
        if (
            digest(record) != sha256
            or record["previous_sha256"] != previous
            or type(record["sequence"]) is not int
            or record["sequence"] != sequence
            or record["run_id"] != receipt["cli_run_id"]
            or record["coordinator_run_id"] != receipt["run_id"]
            or record["evidence_version"] != "1"
            or not isinstance(record["phase"], str)
            or not re.fullmatch(r"[a-z_]+", record["phase"])
            or path.name != f"{sequence:06d}-{record['phase']}.json"
        ):
            raise CoordinationError(
                "Original completion archive identity/chain changed"
            )
        previous = sha256
        if record["phase"] == "migration_intent":
            migrations.append(digest(record["data"]))
    if previous != result["archive_sha256"]:
        raise CoordinationError("Original completion archive terminal identity changed")
    return migrations


def _complete_live_prepared(ledger, prepared, clock):
    """Obtain fresh evidence internally; a passed dictionary never authorizes entry."""
    return ledger._complete_writer(_build_completion_proof(ledger, prepared, clock))


def _build_completion_proof(ledger, prepared, clock, *, recovery=None):
    """Fresh original-evidence checks shared by normal success and audited recovery."""
    from .coordinator import writer_due
    from .verification import (
        _observations,
        _read,
        _verified_authority_observation,
        verify_prepared,
    )

    receipt = recheck_execution(prepared, ledger)
    run = ledger.writer_run(receipt["run_id"])
    before = ledger.authority_high_water()
    result = (
        verify_prepared(ledger, prepared, recovery=recovery)
        if recovery is not None
        else verify_prepared(ledger, prepared)
    )
    authority = None
    if receipt["settings"]["entity"]:
        authority = _verified_authority_observation(
            ledger,
            prepared,
            receipt,
            before,
            clock.observe(),
            reason="Fresh independent authority before atomic writer completion",
        )
        authority_observations = Path(authority["observations_directory"])
        authority_request = authority["request"]
        authority_command = [
            receipt["runtime"]["executable"],
            "-I",
            "-B",
            "-m",
            "raw_sensor_compact.completion",
            "--authority-only",
            "--request",
            str(authority_observations.parent / "request.json"),
            "--request-sha256",
            digest(authority_request),
        ]
        observed = _observations(authority_observations, receipt, authority_command)
        if (
            observed[0] != authority["supervision"]
            or observed[2] != authority["result"]
            or _read(authority_observations.parent / "request.json", 1024 * 1024)
            != canonical(authority_request).encode()
        ):
            raise CoordinationError("Fresh completion authority observations changed")
    original = _observations(
        Path(receipt["observations_directory"]),
        receipt,
        prepared.command,
        successful=recovery is None,
    )
    request = _request(receipt, run["intent_sha256"], original[0], before, recovery)
    observations = Path(result["verifier_observations"])
    verified = _observations(
        observations, receipt, _verifier_command(receipt, observations, request)
    )
    data = {
        k: v
        for k, v in result.items()
        if k not in {"verifier", "verifier_observations"}
    }
    if (
        verified[0] != result["verifier"]
        or verified[2] != data
        or _read(observations.parent / "request.json", 1024 * 1024)
        != canonical(request).encode()
        or recheck_execution(prepared, ledger) != receipt
        or ledger.writer_run(receipt["run_id"]) != run
        or ledger.authority_high_water() != before
    ):
        raise CoordinationError("Fresh completion evidence changed before COMMIT")
    _recheck_archive(receipt, result)
    if recovery is not None:
        from .maintenance import MAX_MAINTENANCE_BYTES, no_pending

        maintenance = recovery["receipt"]
        report = original[2]
        deletion_repaired = maintenance["operation"] == "deletions-repair"
        if report.get("status") not in (
            {"completed_with_deletion_errors", "error"}
            if deletion_repaired
            else {"error"}
        ) or canonical(report.get("phase_evidence")) != canonical(
            {
                "directory": str(
                    Path(receipt["settings"]["run_evidence_directory"])
                    / receipt["cli_run_id"]
                ),
                "last_sha256": result["archive_sha256"],
                "records": result["archive_records"],
                "migration_complete": not deletion_repaired,
            }
        ):
            raise CoordinationError(
                "Failed original capture is not bound to verified archive"
            )
        item = no_pending(ledger).get(maintenance["maintenance_id"])
        directory = Path(maintenance["observations_directory"])
        captured = _observations(directory, receipt, maintenance["command"])
        if (
            item != recovery
            or _read(directory.parent / "receipt.json", MAX_MAINTENANCE_BYTES)
            != canonical(maintenance).encode()
            or captured[0] != recovery["finished"]["supervision"]
            or captured[2] != recovery["finished"]["report"]
            or canonical(captured[2].get("phase_evidence"))
            != canonical(
                {
                    "directory": str(
                        Path(receipt["settings"]["run_evidence_directory"])
                        / maintenance["cli_run_id"]
                    ),
                    "last_sha256": result["repair_archive_sha256"],
                    "records": result["repair_archive_records"],
                    "migration_complete": True,
                }
            )
        ):
            raise CoordinationError(
                "Original maintenance evidence changed before resolution"
            )
        _recheck_archive(
            {
                **receipt,
                "cli_run_id": maintenance["cli_run_id"],
                "run_id": maintenance["maintenance_id"],
            },
            {
                "archive_records": result["repair_archive_records"],
                "archive_sha256": result["repair_archive_sha256"],
            },
        )
    from .intentional_retry import _recheck_retry_completion

    retry = _recheck_retry_completion(ledger, prepared, result)
    now = clock.observe()
    if recovery is not None:
        audits = ledger._connection.execute(
            "SELECT observed_at,details_json FROM events "
            "WHERE kind='maintenance_finished' AND identity=? LIMIT 2",
            [recovery["receipt"]["maintenance_id"]],
        ).fetchall()
        if (
            len(audits) != 1
            or audits[0][1] != canonical(recovery["finished"])
            or timestamp(audits[0][0]) > timestamp(now)
        ):
            raise CoordinationError(
                "Maintenance terminal audit is not prior to completion"
            )
    proof = {
        "version": 1,
        "ledger_id": ledger.ledger_id,
        "run_id": receipt["run_id"],
        "completed_at": now,
        "next_hour": receipt["hour_end"],
        "mode": "DRAINING"
        if writer_due(
            receipt["hour_end"], now, grace_minutes=receipt["settings"]["grace_minutes"]
        )
        else "OPEN",
        "verification": result,
        "writer_supervision": original[0],
        "authority_before": before,
        "authority_proof": authority,
    }
    if recovery is not None:
        proof.update(
            version=3 if recovery["receipt"]["operation"] == "deletions-repair" else 2,
            recovery={
                "operation": recovery["receipt"]["operation"],
                "receipt": recovery["receipt"],
                "finished": recovery["finished"],
            },
        )
    if retry is not None:
        proof.update(version=4, retry=retry)
    return proof
