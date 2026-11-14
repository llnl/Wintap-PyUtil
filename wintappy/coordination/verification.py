"""Receipt/shutdown-authenticated standalone data checks, not ledger completion.

All permission transitions stay with the locked main-runtime ledger. Entity-mode
invocation requires independently initialized ledger high-water; caller-provided
snapshots/anchors cannot bypass that authority gate.
"""

import hashlib
import json
import os
import re
import stat
from pathlib import Path
from uuid import uuid4

from .execution import execution_environment, recheck_execution
from .ledger import _mkdir, _now, _validate_block_reason
from .protocol import CoordinationError, canonical, digest
from .supervisor import (
    MAX_RECEIPT_BYTES,
    _parse_json,
    _publish,
    _validate_result,
    run_supervised,
)

MAX_COMPLETION_BYTES = 64 * 1024


def verify_authority_prepared(
    ledger, prepared, now, *, initialize=False, confirmed=False, reason=None
):
    """Explicit initialization or fresh anchored read-only check under one lock.

    No passed report/anchor may initialize authority. Ordinary reopening requires
    another separately supervised check; a retained row alone cannot grant access.
    """
    from .protocol import timestamp

    ledger._authority_checked = None
    timestamp(now)
    try:
        if type(initialize) is not bool:
            raise CoordinationError("Authority initialization choice must be explicit")
        receipt = recheck_execution(prepared, ledger)
        previous = ledger.authority_high_water()
        state = ledger.snapshot()
        if receipt["settings"]["entity"] is None or state["mode"] not in {
            "OPEN",
            "DRAINING",
        }:
            raise CoordinationError(
                "Authority verification requires entity scope and no unresolved writer"
            )
        if initialize:
            if confirmed is not True or previous is not None or state["readers"]:
                raise CoordinationError(
                    "Authority initialization requires explicit confirmation, "
                    "no prior baseline and zero readers"
                )
            _validate_block_reason(reason)
        else:
            if previous is None:
                raise CoordinationError(
                    "Authority high-water requires explicit verified initialization"
                )
            reason = "Fresh verification against retained authority high-water"
        proof = _verified_authority_observation(
            ledger,
            prepared,
            receipt,
            previous,
            now,
            initialize=initialize,
            reason=reason,
        )
        ledger._record_authority_check(proof)
        return ledger.authority_high_water()
    except BaseException as error:
        ledger._authority_checked = None
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        raise CoordinationError(
            "Independent authority verification failed; details suppressed"
        ) from None


def _verified_authority_observation(
    ledger, prepared, receipt, previous, now, *, initialize=False, reason
):
    """Internal read-only evidence seam; public policy and COMMIT stay with callers."""
    from .authority import validate_proof

    state = ledger.snapshot()
    request = {
        "version": 2,
        "operation": "authority",
        "arguments": list(prepared.command[5:]),
        "execution_sha256": prepared.sha256,
        "ledger_id": ledger.ledger_id,
        "dataset_sha256": digest(prepared.binding),
        "authority_high_water": previous,
    }
    directory = prepared.path.parent / "authority-verifications" / uuid4().hex
    _mkdir(directory)
    _mkdir(Path(receipt["settings"]["temp_directory"]))
    storage = receipt["settings"]["storage"]
    if storage and storage["extension_directory"]:
        _mkdir(Path(storage["extension_directory"]))
    request_path = directory / "request.json"
    _publish(request_path, request)
    command = [
        receipt["runtime"]["executable"],
        "-I",
        "-B",
        "-m",
        "raw_sensor_compact.completion",
        "--authority-only",
        "--request",
        str(request_path),
        "--request-sha256",
        digest(request),
    ]
    settings, observations = receipt["settings"], directory / "observations"
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
    proof = {
        "version": 1,
        "observed_at": now,
        "initialization": initialize,
        "reason": reason,
        "receipt": receipt,
        "execution_sha256": prepared.sha256,
        "request": request,
        "result": observed[2],
        "supervision": observed[0],
        "observations_directory": str(observations),
    }
    validate_proof(proof, prepared.binding, ledger.ledger_id, previous)
    if (
        recheck_execution(prepared, ledger) != receipt
        or ledger.snapshot() != state
        or ledger.authority_high_water() != previous
        or _read(request_path, 1024 * 1024) != canonical(request).encode()
        or _observations(observations, receipt, command) != observed
    ):
        raise CoordinationError("Authority verification evidence changed")
    return proof


def _read(path, budget):
    descriptor = None
    try:
        if any(p.is_symlink() for p in (path, *path.parents)):
            raise ValueError
        before = path.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_size > budget
        ):
            raise ValueError
        descriptor = os.open(
            path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
        )
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino, opened.st_size)
            != (before.st_dev, before.st_ino, before.st_size)
        ):
            raise ValueError
        with os.fdopen(descriptor, "rb") as source:
            descriptor = None
            payload = source.read(budget + 1)
        current = path.lstat()
        if (
            len(payload) != before.st_size
            or len(payload) > budget
            or (current.st_dev, current.st_ino, current.st_size, current.st_nlink)
            != (before.st_dev, before.st_ino, before.st_size, 1)
        ):
            raise ValueError
        return payload
    except (OSError, ValueError):
        raise CoordinationError(
            "Verification requires bounded immutable regular observations"
        ) from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _observations(directory, receipt, command):
    settings = receipt["settings"]
    manifest = {
        "command": list(command),
        "cwd": receipt["cwd"],
        "timeout_seconds": settings["supervision_timeout_seconds"],
        "stop_grace_seconds": settings["stop_grace_seconds"],
        "shutdown_seconds": settings["shutdown_seconds"],
        "max_log_bytes": settings["max_log_bytes"],
    }
    recorded = _parse_json(_read(directory / "command.json", 1024 * 1024))
    raw = _read(directory / "supervision.json", MAX_RECEIPT_BYTES)
    supervision = _validate_result(_parse_json(raw))
    if (
        recorded != manifest
        or supervision["command_sha256"] != digest(manifest)
        or supervision["status"] != "exited"
        or supervision["exit_code"] != 0
        or supervision["children_stopped"] is not True
    ):
        raise CoordinationError(
            "Verification requires receipt-matched clean supervised shutdown"
        )
    payload = None
    for stream in ("stdout", "stderr"):
        info = supervision[stream]
        captured = _read(
            directory / f"{stream}.log",
            settings["max_report_bytes"]
            if stream == "stdout"
            else settings["max_log_bytes"],
        )
        if (
            info["truncated"]
            or len(captured) != info["bytes_saved"]
            or hashlib.sha256(captured).hexdigest() != info["sha256"]
        ):
            raise CoordinationError("Verification capture changed or exceeds budget")
        if stream == "stdout":
            payload = captured
    return supervision, hashlib.sha256(raw).hexdigest(), _parse_json(payload)


def verify_prepared(ledger, prepared):
    """Invoke a fresh standalone verifier under exclusion; never reopen or advance."""
    try:
        receipt = recheck_execution(prepared, ledger)
        state = ledger.snapshot()
        run = ledger.writer_run(receipt["run_id"])
        if (
            state["readers"]
            or state["mode"] not in {"WRITING", "BLOCKED"}
            or state["active_run_id"] != receipt["run_id"]
            or state["next_hour"] != receipt["hour_start"]
            or run is None
        ):
            raise CoordinationError(
                "Verification requires the active committed writer under exclusion"
            )
        intent = json.loads(run["intent_json"])
        if run["intent_sha256"] != digest(intent) or intent.get("execution") != {
            "path": str(prepared.path),
            "sha256": prepared.sha256,
            "receipt": receipt,
        }:
            raise CoordinationError(
                "Verification execution does not match committed intent"
            )
        entity = receipt["settings"]["entity"]
        high_water = ledger.authority_high_water() if entity else None
        if entity and high_water is None:
            raise CoordinationError(
                "Entity completion requires explicitly initialized ledger high-water"
            )
        original = _observations(
            Path(receipt["observations_directory"]), receipt, prepared.command
        )
        if (
            original[2].get("run_id") != receipt["cli_run_id"]
            or original[2].get("coordinator_run_id") != receipt["run_id"]
        ):
            raise CoordinationError("Writer report run scope mismatch")
        request = {
            "version": 1,
            "arguments": list(prepared.command[5:]),
            "execution_sha256": prepared.sha256,
            "supervision_sha256": original[1],
            "intent_sha256": run["intent_sha256"],
            "authority_high_water": high_water,
        }
        directory = prepared.path.parent / "verifications" / uuid4().hex
        _mkdir(directory)
        request_path = directory / "request.json"
        _publish(request_path, request)
        command = [
            receipt["runtime"]["executable"],
            "-I",
            "-B",
            "-m",
            "raw_sensor_compact.completion",
            "--request",
            str(request_path),
            "--request-sha256",
            digest(request),
        ]
        settings = receipt["settings"]
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
        supervision, _, result = _observations(observations, receipt, command)
        expected_binding = {
            "raw_root": receipt["binding"]["roots"]["raw"],
            "compacted_root": receipt["binding"]["roots"]["compacted"],
            "entity_root": receipt["binding"]["roots"]["entity"] if entity else "",
            "metrics_root": receipt["binding"]["roots"]["metrics"] if entity else "",
            "namespace": entity["namespace"] if entity else "",
            "hosts": entity["hosts"] if entity else [],
            "identity_policy": entity["identity_policy"] if entity else "reject",
            "identity_max_ratio": entity["identity_max_ratio"] if entity else 0.05,
            "window_start": receipt["hour_start"],
            "window_end": receipt["hour_end"],
            "event_types": settings["event_types"],
        }
        expected = {
            "version": 1,
            "status": "verified",
            "run_id": receipt["run_id"],
            "cli_run_id": receipt["cli_run_id"],
            "hour_start": receipt["hour_start"],
            "hour_end": receipt["hour_end"],
            "execution_sha256": prepared.sha256,
            "supervision_sha256": original[1],
            "intent_sha256": run["intent_sha256"],
            "request_sha256": digest(request),
            "binding_sha256": digest(expected_binding),
            "authority_high_water": None,
            "checkpoint_sha256": None,
            "checkpoint_generation_id": None,
        }
        if entity:
            from .authority import validate_anchor

            validate_anchor(
                result.get("authority_high_water"), prepared.binding, high_water
            )
            for key in ("checkpoint_sha256", "checkpoint_generation_id"):
                if not isinstance(result.get(key), str) or not re.fullmatch(
                    r"[a-f0-9]{64}", result[key]
                ):
                    raise CoordinationError(
                        "Entity completion checkpoint identity missing"
                    )
                expected[key] = result[key]
            expected["authority_high_water"] = result["authority_high_water"]
        if (
            set(result)
            != set(expected)
            | {
                "archive_sha256",
                "archive_records",
                "raw_source_rows",
                "raw_output_rows",
            }
            or any(
                result[k] != v or type(result[k]) is not type(v)
                for k, v in expected.items()
            )
            or not isinstance(result["archive_sha256"], str)
            or not re.fullmatch(r"[a-f0-9]{64}", result["archive_sha256"])
        ):
            raise CoordinationError("Independent verification evidence scope mismatch")
        if (
            any(
                type(result[k]) is not int or result[k] < 0
                for k in ("archive_records", "raw_source_rows", "raw_output_rows")
            )
            or not 3 <= result["archive_records"] <= 10_000
            or result["raw_source_rows"] != result["raw_output_rows"]
        ):
            raise CoordinationError("Independent verification evidence count mismatch")
        if (
            recheck_execution(prepared, ledger) != receipt
            or _observations(
                Path(receipt["observations_directory"]), receipt, prepared.command
            )
            != original
            or _read(request_path, 1024 * 1024) != canonical(request).encode()
            or ledger.writer_run(receipt["run_id"]) != run
            or entity
            and ledger.authority_high_water() != high_water
        ):
            raise CoordinationError(
                "Execution or intent changed during independent verification"
            )
        result.update(verifier=supervision, verifier_observations=str(observations))
        if len(canonical(result).encode()) > MAX_COMPLETION_BYTES:
            raise CoordinationError(
                "Independent completion evidence exceeds byte budget"
            )
        return result
    except BaseException as error:
        state = ledger.snapshot()
        if (
            state["mode"] == "WRITING"
            and state["active_run_id"] == prepared.receipt["run_id"]
        ):
            ledger.block_writer(
                state["active_run_id"],
                _now(),
                "Independent verification uncertain; retain exclusion and original evidence",
            )
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        raise CoordinationError(
            "Independent completion verification failed; details suppressed"
        ) from None
