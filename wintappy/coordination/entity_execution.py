"""Distinct bounded entity-only invocations, never launch or completion authority.

Receipt preparation neither opens/upgrades the ledger nor mutates datasets. The
entity writer must establish exclusive ownership and independently verify inputs
before launch. Public one-hour compaction receipts deliberately reject this type.
"""

import hashlib
import json
import re
from pathlib import Path
from uuid import uuid4

from .execution import (
    PROJECT,
    PreparedExecution,
    _artifact_layout,
    _integer,
    _label,
    _local,
    _origin,
    _read_bytes,
    _regular,
    _seconds,
    _separate,
    resolve_storage,
    runtime_identity,
)
from .ledger import _mkdir, _state_directory, validate_writer_command
from .protocol import (
    CoordinationError,
    canonical,
    digest,
    identifier,
    timestamp,
    validate_binding,
)
from .supervisor import _parse_json, _publish

MAX_ENTITY_EXECUTION_BYTES = 1024 * 1024
MAX_ENTITY_HOURS = 168
COMMANDS = {"entities-init", "entities-replay"}
DEFAULTS = {
    "threads": 2,
    "memory_limit": "1GB",
    "max_temp_size": "1GB",
    "timeout": 300,
    "max_files": 10_000,
    "max_hours": 24,
    "max_entity_rows": 1_000_000,
    "max_entity_bytes": 256 * 1024 * 1024,
    "supervision_timeout_seconds": 3600,
    "stop_grace_seconds": 2,
    "shutdown_seconds": 30,
    "max_log_bytes": 8 * 1024 * 1024,
    "max_report_bytes": 8 * 1024 * 1024,
}
REQUIRED = {
    "executable",
    "project",
    "event_types",
    "temp_directory",
    "run_evidence_directory",
}
OPTIONAL = {"profile", "region", "endpoint_url", "extension_directory"}
SETTINGS = set(DEFAULTS) | {
    "event_types",
    "temp_directory",
    "run_evidence_directory",
    "entity",
    "storage",
    "archive_limits",
}
RECEIPT_FIELDS = {
    "version",
    "operation",
    "entity_command",
    "run_id",
    "cli_run_id",
    "hour_start",
    "hour_end",
    "created_at",
    "binding",
    "runtime",
    "settings",
    "cwd",
    "command",
    "observations_directory",
}
ARCHIVE_LIMITS = {
    "record_bytes": 64 * 1024**2,
    "run_bytes": 256 * 1024**2,
    "records": 10_000,
}


def _window(start, end):
    first, last = timestamp(start, hour=True), timestamp(end, hour=True)
    hours = int((last - first).total_seconds() // 3600)
    if not 1 <= hours <= MAX_ENTITY_HOURS:
        raise CoordinationError("Entity execution window exceeds fixed hour budget")
    return hours


def _storage(storage, binding):
    remote = any(r.startswith("s3://") for r in binding["roots"].values())
    if not remote:
        if storage is not None:
            raise CoordinationError("Inactive entity storage must be explicit")
        return
    if not isinstance(storage, dict) or set(storage) != {
        "profile",
        "region",
        "endpoint_url",
        "extension_directory",
        "credential_delivery",
        "addressing_style",
        "connect_timeout_seconds",
        "read_timeout_seconds",
        "total_max_attempts",
    }:
        raise CoordinationError("Incomplete resolved entity execution storage")
    _label(storage["profile"])
    if (
        _label(storage["region"]) is None
        or _origin(storage["endpoint_url"]) != storage["endpoint_url"]
        or storage["credential_delivery"] != "ephemeral_frozen"
        or storage["addressing_style"] != "path"
    ):
        raise CoordinationError("Entity storage scope/policy mismatch")
    for key, expected in (
        ("connect_timeout_seconds", 10),
        ("read_timeout_seconds", 30),
        ("total_max_attempts", 3),
    ):
        if type(storage[key]) is not int or storage[key] != expected:
            raise CoordinationError("Entity storage timeout/retry policy mismatch")
    extension = storage["extension_directory"]
    if extension is not None:
        if _local(extension) != extension:
            raise CoordinationError("Entity extension path must be canonical")
        _separate(extension, binding)


def _validate_settings(settings, binding, *, check_storage=True):
    if not isinstance(settings, dict) or set(settings) != SETTINGS:
        raise CoordinationError(
            "Entity execution settings must be complete/allowlisted"
        )
    for key, high in (
        ("threads", 64),
        ("max_files", 1_000_000),
        ("max_hours", MAX_ENTITY_HOURS),
        ("max_entity_rows", 1_000_000_000),
        ("max_entity_bytes", 64 * 1024**3),
        ("max_log_bytes", 64 * 1024**2),
        ("max_report_bytes", 64 * 1024**2),
    ):
        _integer(settings[key], 1, high)
    for key, high in (
        ("timeout", 86400),
        ("supervision_timeout_seconds", 7 * 86400),
        ("stop_grace_seconds", 300),
        ("shutdown_seconds", 600),
    ):
        _seconds(settings[key], high)
    if (
        settings["timeout"] >= settings["supervision_timeout_seconds"]
        or settings["stop_grace_seconds"] >= settings["shutdown_seconds"]
        or settings["max_report_bytes"] > settings["max_log_bytes"]
    ):
        raise CoordinationError("Inconsistent entity execution time/capture budgets")
    factors = {
        "B": 1,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
    }
    for key in ("memory_limit", "max_temp_size"):
        value = settings[key]
        match = (
            re.fullmatch(r"([1-9]\d*)(B|KB|MB|GB|KiB|MiB|GiB)", value)
            if isinstance(value, str)
            else None
        )
        ceiling = (32 if key == "memory_limit" else 64) * 1000**3
        if (
            match is None
            or len(match[1]) > 12
            or int(match[1]) * factors[match[2]] > ceiling
        ):
            raise CoordinationError(
                "Entity resource budget exceeds qualification envelope"
            )
    events = settings["event_types"]
    if (
        not isinstance(events, list)
        or not 1 <= len(events) <= 128
        or any(
            not isinstance(e, str)
            or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", e)
            for e in events
        )
        or events != sorted(set(events))
        or "raw_process" not in events
    ):
        raise CoordinationError(
            "Entity event allowlist must canonically include raw_process"
        )
    if settings["entity"] != binding["entity"] or settings["entity"] is None:
        raise CoordinationError(
            "Entity execution requires the active bound entity policy"
        )
    for key in ("temp_directory", "run_evidence_directory"):
        if _local(settings[key]) != settings[key]:
            raise CoordinationError("Entity execution paths must be canonical")
        _separate(settings[key], binding)
    if canonical(settings["archive_limits"]) != canonical(ARCHIVE_LIMITS):
        raise CoordinationError("Entity archive budgets mismatch")
    if check_storage:
        _storage(settings["storage"], binding)


def command_for(receipt):
    settings, roots = receipt["settings"], receipt["binding"]["roots"]
    entity = settings["entity"]
    command = [
        receipt["runtime"]["executable"],
        "-I",
        "-B",
        "-m",
        "raw_sensor_compact.cli",
        receipt["entity_command"],
    ]
    values = {
        "source-root": roots["raw"],
        "destination-root": roots["compacted"],
        "start-hour": receipt["hour_start"],
        "end-hour": receipt["hour_end"],
        "run-id": receipt["cli_run_id"],
        "coordinator-run-id": receipt["run_id"],
    }
    for key in (
        "threads",
        "memory_limit",
        "max_temp_size",
        "temp_directory",
        "timeout",
        "max_files",
        "max_hours",
        "run_evidence_directory",
        "max_entity_rows",
        "max_entity_bytes",
    ):
        values[key.replace("_", "-")] = settings[key]
    values.update(
        {
            "process-entity-root": roots["entity"],
            "processing-metrics-root": roots["metrics"],
            "entity-namespace": entity["namespace"],
            "unknown-identity-policy": entity["identity_policy"],
            "unknown-identity-max-ratio": entity["identity_max_ratio"],
        }
    )
    storage = settings["storage"]
    if storage:
        values.update(
            {"region": storage["region"], "endpoint-url": storage["endpoint_url"]}
        )
        if storage["extension_directory"]:
            values["extension-directory"] = storage["extension_directory"]
    for key, value in values.items():
        command.extend(["--" + key, str(value)])
    for event in settings["event_types"]:
        command.extend(["--event-type", event])
    for host in entity["hosts"]:
        command.extend(["--entity-host", host])
    return command


def validate_entity_receipt(receipt):
    if (
        not isinstance(receipt, dict)
        or set(receipt) != RECEIPT_FIELDS
        or type(receipt["version"]) is not int
        or receipt["version"] != 1
        or receipt["operation"] != "entity-only-execution"
        or not isinstance(receipt["entity_command"], str)
        or receipt["entity_command"] not in COMMANDS
    ):
        raise CoordinationError("Invalid entity execution receipt shape/version")
    validate_binding(receipt["binding"])
    identifier(receipt["run_id"])
    identifier(receipt["cli_run_id"])
    timestamp(receipt["created_at"])
    hours = _window(receipt["hour_start"], receipt["hour_end"])
    if receipt["run_id"] == receipt["cli_run_id"] or receipt["cwd"] != str(PROJECT):
        raise CoordinationError("Entity execution run/runtime identity mismatch")
    runtime = receipt["runtime"]
    if not isinstance(runtime, dict) or set(runtime) != {
        "executable",
        "executable_realpath",
        "executable_sha256",
        "python_version",
        "python_implementation",
        "duckdb_version",
        "duckdb_sha256",
        "module_directory",
        "source_sha256",
    }:
        raise CoordinationError("Incomplete entity execution runtime identity")
    if (
        any(not isinstance(v, str) or not v or len(v) > 4096 for v in runtime.values())
        or runtime["executable"] != str(PROJECT / ".venv/bin/python")
        or runtime["duckdb_version"] != "2.0.0"
        or runtime["module_directory"] != str(PROJECT / "src/raw_sensor_compact")
        or runtime["python_implementation"] != "cpython"
    ):
        raise CoordinationError(
            "Entity execution requires the qualified standalone runtime"
        )
    for key in ("executable_sha256", "duckdb_sha256", "source_sha256"):
        if not re.fullmatch(r"[a-f0-9]{64}", runtime[key]):
            raise CoordinationError("Entity runtime artifact identity missing")
    _validate_settings(receipt["settings"], receipt["binding"])
    if hours > receipt["settings"]["max_hours"]:
        raise CoordinationError(
            "Entity execution window exceeds configured hour budget"
        )
    path = _local(receipt["observations_directory"])
    _separate(path, receipt["binding"])
    if (
        path != receipt["observations_directory"]
        or Path(path).parts[-3:]
        != ("entity-executions", receipt["run_id"], "observations")
        or canonical(receipt["command"]) != canonical(command_for(receipt))
    ):
        raise CoordinationError("Entity execution command/observation path mismatch")
    validate_writer_command(receipt["command"])
    if len(canonical(receipt).encode()) > MAX_ENTITY_EXECUTION_BYTES:
        raise CoordinationError("Entity execution receipt exceeds byte budget")
    return receipt


def load_entity_execution(path, sha256):
    if not isinstance(sha256, str) or not re.fullmatch(r"[a-f0-9]{64}", sha256):
        raise CoordinationError("Invalid entity execution receipt identity")
    path = Path(_local(str(path)))
    try:
        _regular(path.lstat())
    except OSError:
        raise CoordinationError(
            "Missing or unreadable entity execution receipt"
        ) from None
    payload = _read_bytes(path, MAX_ENTITY_EXECUTION_BYTES)
    if hashlib.sha256(payload).hexdigest() != sha256:
        raise CoordinationError("Entity execution receipt bytes changed")
    value = validate_entity_receipt(_parse_json(payload))
    if canonical(value).encode() != payload:
        raise CoordinationError("Entity receipt requires exact canonical bytes")
    return value


def prepare_entity_execution(directory, binding, start, end, now, config, *, operation):
    # Refuse unrelated flags before any runtime or credential resolution.
    if (
        not isinstance(config, dict)
        or not REQUIRED <= set(config)
        or set(config) - REQUIRED - set(DEFAULTS) - OPTIONAL
        or not isinstance(operation, str)
        or operation not in COMMANDS
    ):
        raise CoordinationError(
            "Entity execution configuration/command is not allowlisted"
        )
    hours = _window(start, end)
    timestamp(now)
    validate_binding(binding)
    if binding["entity"] is None:
        raise CoordinationError("Entity execution requires all four active input roots")
    directory = _state_directory(directory, binding)
    project = _local(config["project"])
    settings = {
        **DEFAULTS,
        **{k: v for k, v in config.items() if k in set(DEFAULTS) | REQUIRED},
    }
    settings.pop("project")
    settings.pop("executable")
    for key in ("temp_directory", "run_evidence_directory"):
        settings[key] = _local(settings[key])
    settings.update(
        entity=binding["entity"], archive_limits=dict(ARCHIVE_LIMITS), storage=None
    )
    _validate_settings(settings, binding, check_storage=False)
    if hours > settings["max_hours"]:
        raise CoordinationError(
            "Entity execution window exceeds configured hour budget"
        )
    _artifact_layout(settings, directory)
    storage_config = dict(config)
    if any(r.startswith("s3://") for r in binding["roots"].values()):
        storage_config["extension_directory"] = _local(
            config.get("extension_directory") or str(directory / "extensions")
        )
    settings["storage"], credentials = resolve_storage(binding, storage_config)
    _validate_settings(settings, binding)
    runtime = runtime_identity(config["executable"], project)
    run_id = uuid4().hex
    run_directory = directory / "entity-executions" / run_id
    receipt = {
        "version": 1,
        "operation": "entity-only-execution",
        "entity_command": operation,
        "run_id": run_id,
        "cli_run_id": uuid4().hex,
        "hour_start": start,
        "hour_end": end,
        "created_at": now,
        "binding": binding,
        "runtime": runtime,
        "settings": settings,
        "cwd": project,
        "observations_directory": str(run_directory / "observations"),
    }
    receipt["command"] = command_for(receipt)
    validate_entity_receipt(receipt)
    frozen = json.loads(canonical(receipt))
    _mkdir(run_directory)
    path = run_directory / "execution.json"
    _publish(path, frozen)
    return PreparedExecution(
        path,
        digest(frozen),
        frozen,
        tuple(frozen["command"]),
        json.loads(canonical(binding)),
        credentials,
    )


def recheck_entity_execution(prepared, ledger):
    receipt = load_entity_execution(prepared.path, prepared.sha256)
    _artifact_layout(receipt["settings"], ledger.directory)
    expected = (
        ledger.directory / "entity-executions" / receipt["run_id"] / "execution.json"
    )
    if (
        prepared.path != expected
        or canonical(receipt) != canonical(prepared.receipt)
        or canonical(prepared.binding) != ledger.binding
        or canonical(receipt["binding"]) != ledger.binding
        or list(prepared.command) != receipt["command"]
        or receipt["observations_directory"] != str(expected.parent / "observations")
        or runtime_identity(receipt["runtime"]["executable"], receipt["cwd"])
        != receipt["runtime"]
    ):
        raise CoordinationError(
            "Effective entity execution receipt/invocation mismatch"
        )
    return receipt
