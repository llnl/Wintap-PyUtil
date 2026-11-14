"""Allowlisted effective compactor invocations, not completion certificates.

Only the installed standalone CLI is supported. Credentials travel ephemerally,
never in the receipt, argv, diagnostics or hashes. Trusted local code and cooperative
exclusion remain prerequisites; these checks are not hostile-process attestation.
"""

import hashlib
import json
import math
import os
import re
import stat
import subprocess
from dataclasses import dataclass, field, replace
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import boto3
from botocore.config import Config

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

MAX_EXECUTION_BYTES = 1024 * 1024
PROJECT = Path(__file__).resolve().parents[2] / "extras/raw-sensor-compact"
DEFAULTS = {
    "threads": 2,
    "memory_limit": "1GB",
    "max_temp_size": "1GB",
    "timeout": 300,
    "max_files": 10_000,
    "max_hours": 1,
    "grace_minutes": 10,
    "delete_source": True,
    "delete_confirm_attempts": 5,
    "delete_confirm_initial_delay_seconds": 1.0,
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
    "target_size_bytes",
    "temp_directory",
    "run_evidence_directory",
}
OPTIONAL = {"profile", "region", "endpoint_url", "extension_directory"}
SETTINGS = set(DEFAULTS) | {
    "event_types",
    "target_size_bytes",
    "temp_directory",
    "run_evidence_directory",
    "entity",
    "storage",
    "archive_limits",
}
RECEIPT_FIELDS = {
    "version",
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

# Execute with -I: no inherited PYTHONPATH, cwd import, or user site injection.
# Hash only public executable/code artifacts, never environment/credential values.
PROBE = r"""
import hashlib,json,pathlib,sys,duckdb,_duckdb,raw_sensor_compact
def hash_file(path):
    path=pathlib.Path(path)
    if path.stat().st_size > 512*1024*1024:
        raise ValueError("Artifact budget")
    result=hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024*1024),b""):
            result.update(chunk)
    return result.hexdigest()
source=pathlib.Path(raw_sensor_compact.__file__).resolve().parent
files=sorted(source.rglob("*.py"))
if len(files)>256 or sum(p.stat().st_size for p in files)>16*1024*1024:
    raise ValueError("Source budget")
code={p.relative_to(source).as_posix():hash_file(p) for p in files}
print(json.dumps({"executable":sys.executable,
    "executable_realpath":str(pathlib.Path(sys.executable).resolve()),
    "executable_sha256":hash_file(sys.executable),
    "python_version":sys.version,"python_implementation":sys.implementation.name,
    "duckdb_version":duckdb.__version__,"duckdb_sha256":hash_file(_duckdb.__file__),
    "module_directory":str(source),"source_sha256":hashlib.sha256(
        json.dumps(code,sort_keys=True,separators=(",",":")).encode()).hexdigest()}))
"""


@dataclass(frozen=True)
class PreparedExecution:
    path: Path
    sha256: str
    receipt: dict
    command: tuple
    binding: dict
    credentials: dict = field(repr=False)

    def _replace(self, **changes):
        return replace(self, **changes)


def _integer(value, low, high):
    if type(value) is not int or not low <= value <= high:
        raise CoordinationError("Invalid execution integer budget")


def _seconds(value, high):
    if (
        type(value) not in {int, float}
        or not math.isfinite(value)
        or not 0 < value <= high
    ):
        raise CoordinationError("Invalid execution time budget")


def _local(value):
    if not isinstance(value, str) or not value or len(value) > 4096:
        raise CoordinationError("Execution requires an explicit local path")
    path = Path(value).expanduser().absolute()
    if (
        "://" in value
        or any(ord(c) < 32 for c in value)
        or any(part.is_symlink() for part in (path, *path.parents))
    ):
        raise CoordinationError("Execution requires a nonsymlink local path")
    return str(path.resolve())


def _separate(path, binding):
    for root in binding["roots"].values():
        if not root.startswith("s3://"):
            a, b = path.rstrip("/") + "/", root.rstrip("/") + "/"
            if a.startswith(b) or b.startswith(a):
                raise CoordinationError(
                    "Execution artifacts must not overlap data roots"
                )


def _origin(value):
    if not isinstance(value, str) or not value or len(value) > 2048:
        raise CoordinationError("Invalid non-secret S3 origin")
    try:
        uri = urlsplit(value)
        if (
            uri.scheme not in {"http", "https"}
            or not uri.hostname
            or uri.username
            or uri.password
            or uri.path not in {"", "/"}
            or uri.query
            or uri.fragment
            or any(c.isspace() for c in value)
        ):
            raise ValueError()
        _ = uri.port
    except ValueError:
        raise CoordinationError("Invalid non-secret S3 origin") from None
    return value.rstrip("/")


def _label(value):
    if value is not None and (
        not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", value)
    ):
        raise CoordinationError("Invalid non-secret storage selector")
    return value


def resolve_storage(binding, config, *, session_factory=None):
    """Resolve SDK/environment/profile selection once; never list or write data.

    Tests inject a session factory. Active remote execution may resolve credentials,
    but the returned frozen credentials are excluded from all persisted evidence.
    """
    if not any(r.startswith("s3://") for r in binding["roots"].values()):
        return None, {}
    try:
        profile = _label(
            config.get("profile")
            or os.environ.get("AWS_PROFILE")
            or os.environ.get("AWS_DEFAULT_PROFILE")
        )
        endpoint = (
            config.get("endpoint_url")
            or os.environ.get("AWS_ENDPOINT_URL_S3")
            or os.environ.get("AWS_ENDPOINT_URL")
        )
        if endpoint:
            endpoint = _origin(endpoint)
        session = (session_factory or boto3.Session)(profile_name=profile)
        credentials = session.get_credentials()
        if credentials is None:
            raise CoordinationError("No execution credentials resolved")
        frozen = credentials.get_frozen_credentials()
        region = _label(
            config.get("region")
            or os.environ.get("AWS_DEFAULT_REGION")
            or os.environ.get("AWS_REGION")
            or session.region_name
            or "us-east-1"
        )
        client = session.client(
            "s3",
            endpoint_url=endpoint,
            region_name=region,
            aws_access_key_id=frozen.access_key,
            aws_secret_access_key=frozen.secret_key,
            aws_session_token=frozen.token,
            config=Config(
                s3={"addressing_style": "path"},
                connect_timeout=10,
                read_timeout=30,
                retries={"mode": "standard", "total_max_attempts": 3},
            ),
        )
        settings = {
            "profile": profile,
            "region": region,
            "endpoint_url": _origin(client.meta.endpoint_url),
            "extension_directory": config.get("extension_directory"),
            "credential_delivery": "ephemeral_frozen",
            "addressing_style": "path",
            "connect_timeout_seconds": 10,
            "read_timeout_seconds": 30,
            "total_max_attempts": 3,
        }
        environment = {
            "AWS_ACCESS_KEY_ID": frozen.access_key,
            "AWS_SECRET_ACCESS_KEY": frozen.secret_key,
        }
        if frozen.token:
            environment["AWS_SESSION_TOKEN"] = frozen.token
        if any(
            not isinstance(v, str) or not v or "\0" in v for v in environment.values()
        ):
            raise CoordinationError("Invalid ephemeral execution credentials")
        return settings, environment
    except CoordinationError:
        raise
    except Exception:  # noqa: BLE001 - SDK/provider errors may contain credentials.
        raise CoordinationError(
            "Execution storage resolution failed; details suppressed"
        ) from None


def runtime_identity(executable, project):
    if project != str(PROJECT) or executable != str(PROJECT / ".venv/bin/python"):
        raise CoordinationError("Execution requires the qualified standalone runtime")
    try:
        result = subprocess.run(
            [executable, "-I", "-B", "-c", PROBE],
            cwd=project,
            env={},
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=20,
            check=True,
        )
        if len(result.stdout) > 16384:
            raise ValueError()
        identity = _parse_json(result.stdout)
        if (
            identity["duckdb_version"] != "2.0.0"
            or identity["executable"] != executable
            or identity["module_directory"] != str(PROJECT / "src/raw_sensor_compact")
        ):
            raise ValueError()
        return identity
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        raise CoordinationError(
            "Qualified standalone execution runtime unavailable"
        ) from None


def _validate_settings(settings, binding):
    if not isinstance(settings, dict) or set(settings) != SETTINGS:
        raise CoordinationError("Execution settings must be allowlisted and complete")
    for key, low, high in (
        ("threads", 1, 64),
        ("max_files", 1, 1_000_000),
        ("max_hours", 1, 1),
        ("target_size_bytes", 1, 64 * 1024**3),
        ("grace_minutes", 5, 1440),
        ("delete_confirm_attempts", 1, 20),
        ("max_entity_rows", 1, 1_000_000_000),
        ("max_entity_bytes", 1, 64 * 1024**3),
        ("max_log_bytes", 1, 64 * 1024**2),
        ("max_report_bytes", 1, 64 * 1024**2),
    ):
        _integer(settings[key], low, high)
    for key, high in (
        ("timeout", 86400),
        ("supervision_timeout_seconds", 7 * 86400),
        ("stop_grace_seconds", 300),
        ("shutdown_seconds", 600),
        ("delete_confirm_initial_delay_seconds", 30),
    ):
        _seconds(settings[key], high)
    if (
        settings["timeout"] >= settings["supervision_timeout_seconds"]
        or settings["stop_grace_seconds"] >= settings["shutdown_seconds"]
        or settings["max_report_bytes"] > settings["max_log_bytes"]
    ):
        raise CoordinationError("Inconsistent execution time/capture budgets")
    for key in ("memory_limit", "max_temp_size"):
        match = re.fullmatch(r"([1-9]\d*)(B|KB|MB|GB|KiB|MiB|GiB)", str(settings[key]))
        if not isinstance(settings[key], str) or match is None:
            raise CoordinationError("Execution storage budgets must be explicit")
        factors = {
            "B": 1,
            "KB": 1000,
            "MB": 1000**2,
            "GB": 1000**3,
            "KiB": 1024,
            "MiB": 1024**2,
            "GiB": 1024**3,
        }
        ceiling = (32 if key == "memory_limit" else 64) * 1000**3
        if len(match[1]) > 12 or int(match[1]) * factors[match[2]] > ceiling:
            raise CoordinationError(
                "Execution storage budget exceeds qualification envelope"
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
    ):
        raise CoordinationError(
            "Execution event allowlist must be nonempty and canonical"
        )
    if (
        type(settings["delete_source"]) is not bool
        or settings["entity"] != binding["entity"]
    ):
        raise CoordinationError("Execution entity/deletion policy mismatch")
    for key in ("temp_directory", "run_evidence_directory"):
        if _local(settings[key]) != settings[key]:
            raise CoordinationError("Execution paths must be canonical")
        _separate(settings[key], binding)
    if settings["temp_directory"] == settings["run_evidence_directory"]:
        raise CoordinationError(
            "Spill and retained evidence directories must be separate"
        )
    if settings["archive_limits"] != {
        "record_bytes": 64 * 1024**2,
        "run_bytes": 256 * 1024**2,
        "records": 10_000,
    }:
        raise CoordinationError("Execution archive budgets mismatch")
    storage = settings["storage"]
    remote = any(r.startswith("s3://") for r in binding["roots"].values())
    if not remote:
        if storage is not None:
            raise CoordinationError("Inactive storage must be explicit")
    else:
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
            raise CoordinationError("Incomplete resolved execution storage")
        _label(storage["profile"])
        if (
            _label(storage["region"]) is None
            or _origin(storage["endpoint_url"]) != storage["endpoint_url"]
        ):
            raise CoordinationError("Execution storage must be canonical")
        if (
            storage["credential_delivery"] != "ephemeral_frozen"
            or storage["addressing_style"] != "path"
            or storage["connect_timeout_seconds"] != 10
            or storage["read_timeout_seconds"] != 30
            or storage["total_max_attempts"] != 3
        ):
            raise CoordinationError("Execution storage policy mismatch")
        extension = storage["extension_directory"]
        if extension is not None:
            if _local(extension) != extension:
                raise CoordinationError("Execution extension path must be canonical")
            _separate(extension, binding)


def command_for(receipt):
    settings, roots = receipt["settings"], receipt["binding"]["roots"]
    command = [
        receipt["runtime"]["executable"],
        "-I",
        "-B",
        "-m",
        "raw_sensor_compact.cli",
        "compact",
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
        "target_size_bytes",
        "threads",
        "memory_limit",
        "max_temp_size",
        "temp_directory",
        "timeout",
        "max_files",
        "max_hours",
        "run_evidence_directory",
        "grace_minutes",
        "delete_confirm_attempts",
        "delete_confirm_initial_delay_seconds",
    ):
        values[key.replace("_", "-")] = settings[key]
    entity = settings["entity"]
    values["unknown-identity-policy"] = (
        entity["identity_policy"] if entity else "reject"
    )
    values["unknown-identity-max-ratio"] = (
        entity["identity_max_ratio"] if entity else 0.05
    )
    for key in ("max_entity_rows", "max_entity_bytes"):
        values[key.replace("_", "-")] = settings[key]
    if entity:
        values.update(
            {
                "process-entity-root": roots["entity"],
                "processing-metrics-root": roots["metrics"],
                "entity-namespace": entity["namespace"],
            }
        )
    storage = settings["storage"]
    if storage:
        # Profile is a resolution input, not a second credential lookup in the child.
        values.update(
            {"region": storage["region"], "endpoint-url": storage["endpoint_url"]}
        )
        if storage["extension_directory"]:
            values["extension-directory"] = storage["extension_directory"]
    for key, value in values.items():
        command.extend(["--" + key, str(value)])
    for event in settings["event_types"]:
        command.extend(["--event-type", event])
    for host in entity["hosts"] if entity else []:
        command.extend(["--entity-host", host])
    if settings["delete_source"]:
        command.append("--delete-source")
    return command


def validate_receipt(receipt):
    if (
        not isinstance(receipt, dict)
        or set(receipt) != RECEIPT_FIELDS
        or type(receipt["version"]) is not int
        or receipt["version"] != 1
    ):
        raise CoordinationError("Invalid execution receipt shape/version")
    validate_binding(receipt["binding"])
    identifier(receipt["run_id"])
    identifier(receipt["cli_run_id"])
    first = timestamp(receipt["hour_start"], hour=True)
    timestamp(receipt["created_at"])
    if (
        receipt["run_id"] == receipt["cli_run_id"]
        or receipt["hour_end"]
        != (first + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        or receipt["cwd"] != str(PROJECT)
    ):
        raise CoordinationError("Execution hour/runtime identity mismatch")
    runtime = receipt["runtime"]
    if (
        not isinstance(runtime, dict)
        or set(runtime)
        != {
            "executable",
            "executable_realpath",
            "executable_sha256",
            "python_version",
            "python_implementation",
            "duckdb_version",
            "duckdb_sha256",
            "module_directory",
            "source_sha256",
        }
        or runtime.get("executable") != str(PROJECT / ".venv/bin/python")
        or runtime.get("duckdb_version") != "2.0.0"
        or runtime.get("module_directory") != str(PROJECT / "src/raw_sensor_compact")
    ):
        raise CoordinationError("Execution requires the qualified standalone runtime")
    for key in ("executable_sha256", "duckdb_sha256", "source_sha256"):
        if not isinstance(runtime[key], str) or not re.fullmatch(
            r"[a-f0-9]{64}", runtime[key]
        ):
            raise CoordinationError("Execution runtime artifact identity missing")
    _validate_settings(receipt["settings"], receipt["binding"])
    path = _local(receipt["observations_directory"])
    _separate(path, receipt["binding"])
    if path != receipt["observations_directory"] or receipt["command"] != command_for(
        receipt
    ):
        raise CoordinationError("Execution command/observation path mismatch")
    validate_writer_command(receipt["command"])
    if len(canonical(receipt).encode()) > MAX_EXECUTION_BYTES:
        raise CoordinationError("Execution receipt exceeds byte budget")
    return receipt


def _regular(info):
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        raise CoordinationError("Execution receipt must be one regular file")


def _read_bytes(path, budget):
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as source:
            _regular(os.fstat(source.fileno()))
            payload = source.read(budget + 1)
    except OSError:
        raise CoordinationError("Missing or unreadable execution receipt") from None
    if len(payload) > budget:
        raise CoordinationError("Execution receipt exceeds byte budget")
    return payload


def load_execution(path, sha256):
    if not isinstance(sha256, str) or not re.fullmatch(r"[a-f0-9]{64}", sha256):
        raise CoordinationError("Invalid execution receipt identity")
    path = Path(_local(str(path)))
    try:
        _regular(path.lstat())
    except OSError:
        raise CoordinationError("Missing or unreadable execution receipt") from None
    payload = _read_bytes(path, MAX_EXECUTION_BYTES)
    if hashlib.sha256(payload).hexdigest() != sha256:
        raise CoordinationError("Execution receipt bytes changed")
    value = validate_receipt(_parse_json(payload))
    if canonical(value).encode() != payload:
        raise CoordinationError("Execution receipt must contain exact canonical bytes")
    return value


def _artifact_layout(settings, directory):
    paths = [
        settings["temp_directory"],
        settings["run_evidence_directory"],
        str(directory),
        str(PROJECT),
    ]
    for i, first in enumerate(paths[:2]):
        for second in paths[i + 1 :]:
            a, b = first.rstrip("/") + "/", second.rstrip("/") + "/"
            if a.startswith(b) or b.startswith(a):
                raise CoordinationError(
                    "Spill, phase archive, ledger and runtime must be separate"
                )


def prepare_execution(directory, binding, hour, now, config):
    validate_binding(binding)
    directory = _state_directory(directory, binding)
    first = timestamp(hour, hour=True)
    timestamp(now)
    if (
        not isinstance(config, dict)
        or not REQUIRED <= set(config)
        or set(config) - REQUIRED - set(DEFAULTS) - OPTIONAL
    ):
        raise CoordinationError(
            "Execution configuration must be allowlisted and complete"
        )
    project = _local(config["project"])
    executable = config["executable"]
    settings = {
        **DEFAULTS,
        **{k: v for k, v in config.items() if k in set(DEFAULTS) | REQUIRED},
    }
    settings.pop("project")
    settings.pop("executable")
    settings["temp_directory"] = _local(settings["temp_directory"])
    settings["run_evidence_directory"] = _local(settings["run_evidence_directory"])
    settings["entity"] = binding["entity"]
    settings["archive_limits"] = {
        "record_bytes": 64 * 1024**2,
        "run_bytes": 256 * 1024**2,
        "records": 10_000,
    }
    storage_config = dict(config)
    if any(root.startswith("s3://") for root in binding["roots"].values()):
        storage_config["extension_directory"] = _local(
            config.get("extension_directory") or str(directory / "extensions")
        )
    settings["storage"], credentials = resolve_storage(binding, storage_config)
    _validate_settings(settings, binding)
    _artifact_layout(settings, directory)
    runtime = runtime_identity(executable, project)
    run_id = uuid4().hex
    run_directory = directory / "executions" / run_id
    receipt = {
        "version": 1,
        "run_id": run_id,
        "cli_run_id": uuid4().hex,
        "hour_start": hour,
        "hour_end": (first + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "created_at": now,
        "binding": binding,
        "runtime": runtime,
        "settings": settings,
        "cwd": project,
        "observations_directory": str(run_directory / "observations"),
    }
    receipt["command"] = command_for(receipt)
    validate_receipt(receipt)
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


def recheck_execution(prepared, ledger):
    receipt = load_execution(prepared.path, prepared.sha256)
    _artifact_layout(receipt["settings"], ledger.directory)
    expected_path = (
        ledger.directory / "executions" / receipt["run_id"] / "execution.json"
    )
    if (
        prepared.path != expected_path
        or receipt != prepared.receipt
        or canonical(prepared.binding) != ledger.binding
        or canonical(receipt["binding"]) != ledger.binding
        or list(prepared.command) != receipt["command"]
        or receipt["observations_directory"]
        != str(expected_path.parent / "observations")
        or runtime_identity(receipt["runtime"]["executable"], receipt["cwd"])
        != receipt["runtime"]
    ):
        raise CoordinationError("Effective execution receipt/invocation mismatch")
    return receipt


def execution_environment(prepared):
    """No inherited scope, profile, Python injection or credential-file fallback."""
    storage = prepared.receipt["settings"]["storage"]
    credentials = prepared.credentials
    allowed = {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"}
    if (
        not isinstance(credentials, dict)
        or set(credentials) - allowed
        or storage is None
        and credentials
        or storage is not None
        and not {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"} <= set(credentials)
        or any(
            not isinstance(v, str) or not v or "\0" in v for v in credentials.values()
        )
    ):
        raise CoordinationError("Invalid ephemeral execution credential delivery")
    return {
        "PATH": str(PROJECT / ".venv/bin") + ":/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "TMPDIR": prepared.receipt["settings"]["temp_directory"],
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_CONFIG_FILE": "/dev/null",
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null",
        **credentials,
    }


def validate_intent_execution(execution, run):
    """Validate stored evidence without reopening lost external receipt files."""
    if not isinstance(execution, dict) or set(execution) != {
        "path",
        "sha256",
        "receipt",
    }:
        raise CoordinationError("Invalid committed execution binding")
    receipt = validate_receipt(execution["receipt"])
    if (
        digest(receipt) != execution["sha256"]
        or receipt["command"] != run["command"]
        or receipt["binding"] != run["binding"]
        or any(
            receipt[k] != run[k]
            for k in ("run_id", "cli_run_id", "hour_start", "hour_end")
        )
        or str(Path(execution["path"]).parent / "observations")
        != receipt["observations_directory"]
    ):
        raise CoordinationError("Committed execution receipt/intent mismatch")
