"""Offline local bypass and durable Linux/native-Mac shared-reader lifetime.

An existing launch marker never authorizes resuming work after wrapper death.
Terminal retries only publish immutable messages; they cannot launch a command.
"""

import argparse
import fcntl
import math
import os
import shlex
import signal
import stat
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from .inputs import effective_input_roots
from .protocol import (
    CoordinationError,
    canonical,
    canonical_root,
    dataset_binding,
    decode_message,
    digest,
    identifier,
    make_cancellation,
    make_release,
    make_request,
    separate_transport_root,
    timestamp,
    validate_message,
)
from .supervisor import (
    MAX_RECEIPT_BYTES,
    _config,
    _manifest,
    _parse_json,
    _publish,
    _validate_result,
    run_reader_supervised,
)


def coordinated_inputs(env):
    return any(
        canonical_root(root).startswith("s3://") for root in effective_input_roots(env).values()
    )


def direct_dbt_requested(env):
    """Return whether the caller explicitly requested an uncoordinated DBT run."""
    mode = env.get("WINTAP_DBT_COORDINATION_MODE", "").strip().lower()
    if mode not in {"", "direct"}:
        raise CoordinationError("WINTAP_DBT_COORDINATION_MODE must be unset or direct")
    if mode != "direct":
        return False
    if env.get("WINTAP_DBT_PROCESS_ENTITY_ROOT", "").strip():
        raise CoordinationError(
            "Direct DBT mode does not support process-entity enrichment; use the coordinated reader"
        )
    if env.get("WINTAP_DBT_INCLUDE_COMPACTED", "true").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }:
        raise CoordinationError("Direct DBT mode requires WINTAP_DBT_INCLUDE_COMPACTED=false")
    return True


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def binding_from_environment(env):
    roots = effective_input_roots(env)
    entity = None
    if "entity" in roots:
        entity = {
            "namespace": env.get("WINTAP_DBT_PROCESS_ENTITY_NAMESPACE", ""),
            "hosts": env.get("WINTAP_DBT_PROCESS_ENTITY_HOSTS", "").split(","),
            "identity_policy": env.get("WINTAP_DBT_PROCESS_UNKNOWN_IDENTITY_POLICY", "reject"),
            "identity_max_ratio": float(
                env.get("WINTAP_DBT_PROCESS_UNKNOWN_IDENTITY_MAX_RATIO", "0.05")
            ),
        }
    return dataset_binding(env.get("WINTAP_COORDINATION_DATASET_ID", ""), roots, entity)


@contextmanager
def _job(directory, job_id, binding=None):
    # Importing ledger primitives is unnecessary on the entirely local DBT path.
    from .ledger import _mkdir, _state_directory

    identifier(job_id)
    root = Path(directory).expanduser().absolute()
    if binding is not None:
        root = _state_directory(root, binding)
    elif str(directory).startswith("s3://"):
        raise CoordinationError("Client retry state must be local")
    path = _state_directory(root / job_id, binding) if binding is not None else root / job_id
    _mkdir(path)
    descriptor = os.open(
        path / "client.lock",
        os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,
        0o600,
    )
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise CoordinationError("Client lock must be one regular persistent inode")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise CoordinationError("Client job busy; do not replace its identity") from None
        os.fsync(descriptor)
        from .ledger import _fsync_directory

        _fsync_directory(path)
        yield path
    finally:
        os.close(descriptor)


def _message(path):
    from .verification import _read

    return decode_message(_read(path, 1024 * 1024))


def _read_json(path, budget):
    from .verification import _read

    payload = _read(path, budget)
    value = _parse_json(payload)
    if canonical(value).encode() != payload:
        raise CoordinationError("Client records require exact canonical bytes")
    return value


def _retain_record(path, value):
    if path.exists():
        if _read_json(path, 1024 * 1024) != value:
            raise CoordinationError("Immutable client invocation conflict")
    else:
        _publish(path, value)


def _invocation(path, request):
    value = _read_json(path / "invocation.json", 1024 * 1024)
    if (
        set(value) != {"version", "request_sha256", "manifest"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or value["request_sha256"] != request["request_sha256"]
    ):
        raise CoordinationError("Client invocation/request mismatch")
    manifest = value["manifest"]
    if not isinstance(manifest, dict) or set(manifest) != {
        "command",
        "cwd",
        "timeout_seconds",
        "stop_grace_seconds",
        "shutdown_seconds",
        "max_log_bytes",
    }:
        raise CoordinationError("Malformed client command manifest")
    config, _ = _config(
        manifest["command"],
        path / "observations",
        manifest["timeout_seconds"],
        manifest["stop_grace_seconds"],
        manifest["shutdown_seconds"],
        manifest["max_log_bytes"],
        manifest["cwd"],
        None,
        allow_macos_reader=True,
    )
    if _manifest(config) != manifest:
        raise CoordinationError("Client invocation is not canonical")
    return manifest


def _shutdown(path, request, grant):
    manifest = _invocation(path, request)
    launch = _read_json(path / "launch.json", 4096)
    if (
        set(launch) != {"version", "request_sha256", "grant_id", "started_at", "manifest_sha256"}
        or type(launch["version"]) is not int
        or launch["version"] != 1
        or launch["request_sha256"] != request["request_sha256"]
        or launch["grant_id"] != grant["grant_id"]
        or launch["manifest_sha256"] != digest(manifest)
    ):
        raise CoordinationError("Reader launch/command/grant mismatch")
    timestamp(launch["started_at"])
    observations = path / "observations"
    result = _validate_result(_read_json(observations / "supervision.json", MAX_RECEIPT_BYTES))
    if (
        _read_json(observations / "command.json", 1024 * 1024) != manifest
        or result["command_sha256"] != digest(manifest)
        or result["children_stopped"] is not True
        or sys.platform == "darwin"
        and result.get("shutdown_scope") != "process_group"
        or sys.platform == "linux"
        and "shutdown_scope" in result
    ):
        raise CoordinationError(
            "Reader shutdown uncertain or command mismatched; retain reservation"
        )
    return result, manifest, launch


def _exit_code(result, signal_code):
    if signal_code is not None and (type(signal_code) is not int or signal_code not in {130, 143}):
        raise CoordinationError("Invalid retained reader signal outcome")
    code = (
        signal_code
        if signal_code is not None
        else 130 if result["status"] == "interrupted" else result.get("exit_code")
    )
    if type(code) is not int or not -255 <= code <= 255:
        code = 1
    if (
        result["status"] in {"timeout", "output_limit", "supervisor_failed", "launch_failed"}
        and code == 0
    ):
        code = 1
    return code


def _retain_message(path, value):
    if path.exists():
        if _message(path) != value:
            raise CoordinationError("Immutable client identity/message conflict")
    else:
        _publish(path, validate_message(value))


def _matching_grant(grant, request):
    validate_message(grant)
    if grant["kind"] != "grant" or any(
        grant[key] != request[key] for key in ("job_id", "request_sha256", "binding")
    ):
        raise CoordinationError("Grant does not match exact reader request/binding")
    return grant


def _transport(path, request, store, *, retain=False):
    from .execution import _label, _origin

    separate_transport_root(store.root, request["binding"])
    scope = getattr(store, "transport_scope", None)
    if (
        not isinstance(scope, dict)
        or set(scope) != {"endpoint_url", "region", "addressing_style"}
        or _origin(scope["endpoint_url"]) != scope["endpoint_url"]
        or _label(scope["region"]) is None
        or scope["addressing_style"] != "path"
    ):
        raise CoordinationError("Reader requires resolved non-secret transport scope")
    expected = {
        "version": 2,
        "root": store.root,
        "binding_sha256": digest(request["binding"]),
        "scope": scope,
    }
    target = path / "transport.json"
    if retain and not target.exists() and not (path / "request.json").exists():
        _publish(target, expected)
    if _read_json(target, 8192) != expected:
        raise CoordinationError("Client transport differs from the original durable job")


def _terminal(path, request, store):
    _transport(path, request, store)
    message = _message(path / "terminal.json")
    if message["kind"] not in {"release", "cancellation"} or any(
        message[key] != request[key] for key in ("job_id", "request_sha256", "binding")
    ):
        raise CoordinationError("Pending terminal identity conflict")
    if message["kind"] == "release":
        grant = _matching_grant(_message(path / "grant.json"), request)
        result, _, launch = _shutdown(path, request, grant)
        outcome = _read_json(path / "outcome.json", 4096)
        if (
            set(outcome) != {"terminal_sha256", "supervision_sha256", "signal_code"}
            or outcome["terminal_sha256"] != digest(message)
            or outcome["supervision_sha256"] != digest(result)
            or message["exit_code"] != _exit_code(result, outcome["signal_code"])
            or message["grant_id"] != grant["grant_id"]
            or timestamp(message["completed_at"]) < timestamp(launch["started_at"])
        ):
            raise CoordinationError("Pending release does not match recorded launch/grant")
    code = message.get("exit_code", 130)
    if message["kind"] == "cancellation":
        _invocation(path, request)
        if (path / "launch.json").exists():
            raise CoordinationError("Pre-grant cancellation cannot retire a launched reader")
        outcome = _read_json(path / "cancellation-outcome.json", 4096)
        if (
            set(outcome) != {"exit_code", "terminal_sha256"}
            or type(outcome["exit_code"]) is not int
            or outcome["exit_code"] not in {130, 143}
            or outcome["terminal_sha256"] != digest(message)
        ):
            raise CoordinationError("Cancellation retry outcome mismatch")
        code = outcome["exit_code"]
    separate_transport_root(store.root, request["binding"])
    pending = False
    try:
        store.publish(message)
    except CoordinationError:
        pending = True
    return {"job_id": request["job_id"], "exit_code": code, "terminal_pending": pending}


def retry_terminal(directory, job_id, store):
    identifier(job_id)
    existing = Path(canonical_root(str(directory))) / job_id
    request = _message(existing / "request.json")
    if request["job_id"] != job_id:
        raise CoordinationError("Retry path/request identity conflict")
    with _job(directory, job_id, request["binding"]) as path:
        request = _message(path / "request.json")
        return _terminal(path, request, store)


def _interrupted(signum, frame):
    raise SystemExit(128 + signum)


def _signal_code(error):
    if isinstance(error, KeyboardInterrupt):
        return 130
    if type(error.code) is int and error.code in {130, 143}:
        return error.code
    raise error


def run_reader(
    directory,
    request,
    store,
    command,
    *,
    clock=_now,
    sleep=time.sleep,
    poll_seconds=10,
    timeout_seconds=3600,
    stop_grace_seconds=2,
    shutdown_seconds=30,
    max_log_bytes=8 * 1024 * 1024,
    cwd=None,
    env=None,
):
    validate_message(request)
    if request["kind"] != "request" or sys.platform not in {"linux", "darwin"}:
        raise CoordinationError(
            "Coordinated readers support only the Linux/macOS supervision paths"
        )
    if (
        type(poll_seconds) not in (int, float)
        or not math.isfinite(poll_seconds)
        or not 0.1 <= poll_seconds <= 3600
    ):
        raise CoordinationError("Reader polling interval must be 0.1 to 3600 seconds")
    separate_transport_root(store.root, request["binding"])
    with _job(directory, request["job_id"], request["binding"]) as path:
        config, _ = _config(
            command,
            path / "observations",
            timeout_seconds,
            stop_grace_seconds,
            shutdown_seconds,
            max_log_bytes,
            cwd,
            env,
            allow_macos_reader=True,
        )
        manifest = _manifest(config)
        invocation = {
            "version": 1,
            "request_sha256": request["request_sha256"],
            "manifest": manifest,
        }
        if not (path / "invocation.json").exists() and (path / "request.json").exists():
            raise CoordinationError(
                "Original client invocation is missing; retain job for recovery"
            )
        _retain_record(path / "invocation.json", invocation)
        _transport(path, request, store, retain=True)
        _retain_message(path / "request.json", request)
        if (path / "terminal.json").exists():
            return _terminal(path, request, store)
        if (path / "launch.json").exists():
            raise CoordinationError(
                "Reader launch is unresolved; retain reservation for audited recovery"
            )
        # Persist identity before PUT. Request failure may already have reserved us.
        handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
        try:
            for sig in handlers:
                signal.signal(sig, _interrupted)
            try:
                store.publish(request)
                while True:
                    grant = store.read("grant", request["job_id"])
                    if grant is not None:
                        _matching_grant(grant, request)
                        break
                    sleep(poll_seconds)
            except (KeyboardInterrupt, SystemExit) as error:
                code = _signal_code(error)
                message = make_cancellation(request, clock())
                _publish(
                    path / "cancellation-outcome.json",
                    {"exit_code": code, "terminal_sha256": digest(message)},
                )
                _retain_message(path / "terminal.json", message)
                return _terminal(path, request, store)
            _retain_message(path / "grant.json", grant)
            # Durable before launch: restarting cannot run a cached permission again.
            _publish(
                path / "launch.json",
                {
                    "version": 1,
                    "request_sha256": request["request_sha256"],
                    "grant_id": grant["grant_id"],
                    "started_at": clock(),
                    "manifest_sha256": digest(manifest),
                },
            )
            interrupted_code = None
            observations = path / "observations"
            try:
                run_reader_supervised(
                    manifest["command"],
                    observations,
                    timeout_seconds=timeout_seconds,
                    stop_grace_seconds=stop_grace_seconds,
                    shutdown_seconds=shutdown_seconds,
                    max_log_bytes=max_log_bytes,
                    cwd=cwd,
                    env=env,
                )
            except (KeyboardInterrupt, SystemExit) as error:
                interrupted_code = _signal_code(error)
            # Ordinary completion and interrupted completion share the exact durable
            # command/shutdown check. A returned dictionary alone is never evidence.
            result, _, _ = _shutdown(path, request, grant)
            exit_code = _exit_code(result, interrupted_code)
            message = make_release(grant, clock(), exit_code)
            _retain_record(
                path / "outcome.json",
                {
                    "terminal_sha256": digest(message),
                    "supervision_sha256": digest(result),
                    "signal_code": interrupted_code,
                },
            )
            _retain_message(path / "terminal.json", message)
            return _terminal(path, request, store)
        finally:
            for sig, handler in handlers.items():
                signal.signal(sig, handler)


def _dbt_commands(action, executable, project):
    common = ["--project-dir", project, "--profiles-dir", project]
    preflight = executable + ["run-operation", "validate_raw_sensor_source"] + common
    actions = {
        "dbt-build": ["build"],
        "dbt-source": ["run-operation", "validate_raw_sensor_source"],
        "dbt-test": ["test"],
        "dbt-docs": ["docs", "generate"],
        "dbt-debug": ["debug"],
    }
    commands = [executable + actions[action] + common]
    return [preflight, *commands] if action == "dbt-build" else commands


def _dbt_run(action, executable, project):
    for command in _dbt_commands(action, executable, project):
        code = subprocess.call(command)
        if code:
            return code
    return 0


DBT_BATCH = "import json,sys; from wintappy.coordination.client import _dbt_run; raise SystemExit(_dbt_run(sys.argv[1],json.loads(sys.argv[2]),sys.argv[3]))"


def _window(env):
    bounds = []
    for edge, default_hour in (("START", 0), ("END", 23)):
        day = env.get("WINTAP_DBT_" + edge + "_DAY", "")
        if len(day) != 8 or not day.isdecimal():
            raise CoordinationError("Reader requires explicit canonical DBT dates")
        value = datetime.strptime(day, "%Y%m%d").replace(
            hour=int(env.get("WINTAP_DBT_" + edge + "_HOUR", "") or default_hour)
        )
        bounds.append(value)
    return bounds[0].strftime("%Y-%m-%dT%H:%M:%SZ"), (bounds[1] + timedelta(hours=1)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=[
            "dbt-build",
            "dbt-source",
            "dbt-test",
            "dbt-docs",
            "dbt-debug",
            "shared-command",
            "retry-terminal",
        ],
    )
    parser.add_argument("--dbt-command", default="uv run --dev --project . dbt")
    parser.add_argument("--dbt-project", default="wintap_dbt")
    parser.add_argument("--job-id")
    args, extra = parser.parse_known_args(argv)
    if extra and args.action != "shared-command":
        parser.error("Unexpected command arguments")
    outstanding_job_id = None
    try:
        if args.action not in {"retry-terminal", "shared-command"} and (
            direct_dbt_requested(os.environ) or not coordinated_inputs(os.environ)
        ):
            # Direct mode is an explicit operator choice for a read-only DBT run
            # before coordination/compaction has been initialized. The normal
            # remote route remains coordinated unless this switch is present.
            return _dbt_run(args.action, shlex.split(args.dbt_command), args.dbt_project)
        from .service import open_message_store

        root = os.environ.get("WINTAP_COORDINATION_ROOT", "")
        directory = os.environ.get("WINTAP_COORDINATION_CLIENT_STATE", "")
        if not directory:
            raise CoordinationError("Explicit local client retry-state directory required")
        binding = binding_from_environment(os.environ)
        separate_transport_root(root, binding)
        if not root.startswith("s3://"):
            raise CoordinationError("S3 coordination root required")
        if sys.platform not in {"linux", "darwin"}:
            raise CoordinationError(
                "Coordinated readers support only Linux and native macOS; no fallback"
            )
        store = open_message_store(root, {})
        if args.action == "retry-terminal":
            outstanding_job_id = identifier(args.job_id)
            result = retry_terminal(directory, args.job_id, store)
        else:
            job_id = args.job_id or uuid4().hex
            outstanding_job_id = identifier(job_id)
            path = Path(canonical_root(directory)) / identifier(job_id)
            start, end = _window(os.environ)
            if (path / "request.json").exists():
                request = _message(path / "request.json")
                if (
                    request["binding"] != binding
                    or request["action"]
                    != ("shared-command" if args.action == "dbt-debug" else args.action)
                    or request["start"] != start
                    or request["end"] != end
                ):
                    raise CoordinationError("Existing job scope mismatch")
            else:
                request = make_request(
                    binding,
                    job_id,
                    "shared-command" if args.action == "dbt-debug" else args.action,
                    start,
                    end,
                    _now(),
                )
            if args.action == "shared-command":
                command = extra[1:] if extra[:1] == ["--"] else extra
            else:
                command = [
                    sys.executable,
                    "-B",
                    "-c",
                    DBT_BATCH,
                    args.action,
                    canonical(shlex.split(args.dbt_command)),
                    args.dbt_project,
                ]
            result = run_reader(
                directory,
                request,
                store,
                command,
                poll_seconds=float(os.environ.get("WINTAP_COORDINATION_POLL_SECONDS", "10")),
                cwd=os.getcwd(),
                env=dict(os.environ),
            )
        print(canonical(result))
        return (
            2
            if result["terminal_pending"]
            else result["exit_code"] if result["exit_code"] >= 0 else 128 - result["exit_code"]
        )
    except (CoordinationError, OSError, ValueError):
        identity = " job_id=" + outstanding_job_id if outstanding_job_id else ""
        print(
            "Reader refused/uncertain; retain job state and reservation for retry/recovery"
            + identity,
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
