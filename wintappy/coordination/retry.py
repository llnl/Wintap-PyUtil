"""Independent failed-merge preflight; no retry permission or ledger transition.

Durable intentional retry remains separate work. This prerequisite preserves the
original blocked owner, cursor, actual failure and archive, including on restart.
"""

import json
from pathlib import Path
from uuid import uuid4

from .completion import _recheck_archive
from .execution import execution_environment, recheck_execution
from .ledger import _mkdir
from .protocol import CoordinationError, canonical, digest, timestamp
from .supervisor import _publish, run_supervised
from .verification import _observations, _read

MAX_PREFLIGHT_BYTES = 128 * 1024


def _original_source_totals(receipt, archive):
    """Read bounded original intent totals, authenticated by the captured terminal.

    The main runtime reads only JSON. Current Parquet/schema/count observations
    remain the independent standalone verifier's responsibility.
    """
    settings = receipt["settings"]
    limits = settings["archive_limits"]
    directory = Path(settings["run_evidence_directory"]) / receipt["cli_run_id"]
    records, total = [], 0
    for name in ("000001-migration_intent.json", "000002-blocked.json"):
        payload = _read(
            directory / name, min(limits["record_bytes"], limits["run_bytes"] - total)
        )
        total += len(payload)
        record = json.loads(payload)
        if payload != (canonical(record) + "\n").encode():
            raise CoordinationError("Retry original intent bytes changed")
        sha256 = record.pop("sha256")
        if digest(record) != sha256:
            raise CoordinationError("Retry original intent identity changed")
        records.append((record, sha256))
    (migration, migration_sha256), (terminal, terminal_sha256) = records
    if (
        terminal_sha256 != archive["archive_sha256"]
        or terminal["previous_sha256"] != migration_sha256
    ):
        raise CoordinationError("Retry source intent is not bound to original failure")
    original = migration["data"]
    files, partitions = original["source"], original["partitions"]
    if (
        not isinstance(files, list)
        or not 1 <= len(files) <= settings["max_files"]
        or not isinstance(partitions, list)
        or not 1 <= len(partitions) <= len(files)
    ):
        raise CoordinationError("Retry original source totals exceed intent budget")
    rows = [partition["source_rows"] for partition in partitions]
    sizes = [entry["file"]["size"] for entry in files]
    if any(type(value) is not int or not 0 <= value < 2**63 for value in rows + sizes):
        raise CoordinationError("Retry original source totals require bounded integers")
    totals = {
        "source_rows": sum(rows),
        "source_files": len(files),
        "source_bytes": sum(sizes),
    }
    if any(value >= 2**63 for value in totals.values()):
        raise CoordinationError("Retry original source totals exceed supported range")
    return totals


def verify_retry_prepared(ledger, prepared, now):
    """Fresh standalone check under the original block; returned evidence grants nothing."""
    return _verify_retry_prepared(ledger, prepared, now)


def _verify_retry_prepared(ledger, prepared, now, *, retry_run_id=None):
    """Internal reobservation for the exact atomically committed retry owner only.

    Public preflight still requires the original active BLOCKED owner. The private
    post-COMMIT path requires its validated disposition and fresh INTENT owner;
    it cannot resume a restart-blocked invocation or change any permission.
    """
    try:
        ledger._validate()
        receipt = recheck_execution(prepared, ledger)
        state = ledger.snapshot()
        run = ledger.writer_run(receipt["run_id"])
        settings = receipt["settings"]
        if (
            state["mode"] != ("WRITING" if retry_run_id is not None else "BLOCKED")
            or state["readers"]
            or state["active_run_id"] != (retry_run_id or receipt["run_id"])
            or state["next_hour"] != receipt["hour_start"]
            or run is None
            or run["state"] != "BLOCKED"
            or timestamp(now) < timestamp(run["started_at"])
            or settings["entity"] is not None
            or settings["delete_source"] is not False
            or len(settings["event_types"]) != 1
        ):
            raise CoordinationError(
                "Retry preflight requires the exact raw-only blocked merge"
            )
        if retry_run_id is not None:
            new = ledger.writer_run(retry_run_id)
            links = ledger._connection.execute(
                "SELECT original_run_id FROM retry_dispositions WHERE retry_run_id=? LIMIT 2",
                [retry_run_id],
            ).fetchall()
            if (
                new is None
                or new["state"] != "INTENT"
                or timestamp(now) < timestamp(new["started_at"])
                or links != [(receipt["run_id"],)]
            ):
                raise CoordinationError(
                    "Fresh retry observation requires its exact committed owner"
                )
        intent = json.loads(run["intent_json"])
        if run["intent_sha256"] != digest(intent) or intent.get("execution") != {
            "path": str(prepared.path),
            "sha256": prepared.sha256,
            "receipt": receipt,
        }:
            raise CoordinationError("Retry original receipt/intent mismatch")
        original = _observations(
            Path(receipt["observations_directory"]),
            receipt,
            prepared.command,
            successful=False,
        )
        report = original[2]
        evidence = report.get("phase_evidence")
        roots = receipt["binding"]["roots"]
        event = settings["event_types"][0]
        hour = timestamp(receipt["hour_start"])
        parent = f"{event}/dayPK={hour:%Y%m%d}/hourPK={hour:%H}/"
        if (
            report.get("status") != "error"
            or report.get("phase") != "merge"
            or report.get("run_id") != receipt["cli_run_id"]
            or report.get("coordinator_run_id") != receipt["run_id"]
            or report.get("source_hour") != roots["raw"].rstrip("/") + "/" + parent
            or report.get("destination_hour")
            != roots["compacted"].rstrip("/") + "/" + parent
            or not isinstance(evidence, dict)
            or set(evidence)
            != {"directory", "last_sha256", "records", "migration_complete"}
            or evidence["migration_complete"] is not False
            or type(evidence["records"]) is not int
            or evidence["records"] != 3
            or evidence["directory"]
            != str(Path(settings["run_evidence_directory"]) / receipt["cli_run_id"])
        ):
            raise CoordinationError(
                "Retry requires retained actual failed-merge evidence"
            )
        archive = {
            "archive_sha256": evidence["last_sha256"],
            "archive_records": evidence["records"],
        }
        _recheck_archive(receipt, archive)
        totals = _original_source_totals(receipt, archive)
        request = {
            "version": 1,
            "operation": "retry-preflight",
            "arguments": list(prepared.command[5:]),
            "execution_sha256": prepared.sha256,
            "supervision_sha256": original[1],
            "intent_sha256": run["intent_sha256"],
            "ledger_id": ledger.ledger_id,
            "dataset_sha256": digest(prepared.binding),
            **archive,
        }
        directory = prepared.path.parent / "retry-verifications" / uuid4().hex
        _mkdir(directory)
        request_path = directory / "request.json"
        _publish(
            request_path, request
        )  # Durable exact request before read-only launch.
        command = [
            receipt["runtime"]["executable"],
            "-I",
            "-B",
            "-m",
            "raw_sensor_compact.retry_verifier",
            "--request",
            str(request_path),
            "--request-sha256",
            digest(request),
        ]
        observations = directory / "observations"
        run_supervised(
            command,
            observations,
            timeout_seconds=settings["supervision_timeout_seconds"],
            stop_grace_seconds=settings["stop_grace_seconds"],
            shutdown_seconds=settings["shutdown_seconds"],
            max_log_bytes=settings["max_log_bytes"],
            cwd=receipt["cwd"],
            env=execution_environment(prepared),
        )
        observed = _observations(observations, receipt, command)
        result = observed[2]
        binding = {
            "raw_root": roots["raw"],
            "compacted_root": roots["compacted"],
            "entity_root": "",
            "metrics_root": "",
            "namespace": "",
            "hosts": [],
            "identity_policy": "reject",
            "identity_max_ratio": 0.05,
            "window_start": receipt["hour_start"],
            "window_end": receipt["hour_end"],
            "event_types": settings["event_types"],
        }
        expected = {
            "version": 1,
            "operation": "retry-preflight",
            "status": "verified",
            "disposition": "unchanged-inputs-absent-output",
            "output_absent": True,
            "run_id": receipt["run_id"],
            "cli_run_id": receipt["cli_run_id"],
            "hour_start": receipt["hour_start"],
            "hour_end": receipt["hour_end"],
            "binding_sha256": digest(binding),
            **{
                k: v
                for k, v in request.items()
                if k not in {"arguments", "operation", "version"}
            },
            "request_sha256": digest(request),
            **totals,
        }
        if set(result) != set(expected) or any(
            type(result[k]) is not type(v) or result[k] != v
            for k, v in expected.items()
        ):
            raise CoordinationError(
                "Independent retry evidence scope/type/count mismatch"
            )
        _recheck_archive(receipt, archive)
        if (
            recheck_execution(prepared, ledger) != receipt
            or _observations(
                Path(receipt["observations_directory"]),
                receipt,
                prepared.command,
                successful=False,
            )
            != original
            or _read(request_path, 1024 * 1024) != canonical(request).encode()
            or _observations(observations, receipt, command) != observed
            or ledger.snapshot() != state
            or ledger.writer_run(receipt["run_id"]) != run
        ):
            raise CoordinationError("Retry original or verification evidence changed")
        proof = {
            "version": 1,
            "operation": "retry-preflight",
            "observed_at": now,
            "receipt": receipt,
            "request": request,
            "writer_supervision": original[0],
            "verification": result,
            "verifier": observed[0],
            "verifier_observations": str(observations),
        }
        if len(canonical(proof).encode()) > MAX_PREFLIGHT_BYTES:
            raise CoordinationError("Retry preflight exceeds byte budget")
        return proof
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        raise CoordinationError(
            "Independent retry preflight failed; details suppressed"
        ) from None
