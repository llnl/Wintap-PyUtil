"""Linux-only, isolated subreaper supervision; observations are not receipts.

One fresh exec owns exactly one command tree. Orphaned/double-forked/setsid
descendants are adopted and reaped by that exec, not the ledger process. ECHILD
after root exit proves shutdown of this tree; a process group alone cannot do so.
This is cooperative supervision, not hostile-workload or external-service fencing.
Uncertain shutdown, worker death or missing durable observations must fail closed.
"""

import ctypes
import fcntl
import hashlib
import json
import math
import os
import re
import selectors
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from .protocol import CoordinationError, canonical, digest

MAX_CONTROL_BYTES = 1024 * 1024
MAX_LOG_BYTES = 64 * 1024 * 1024
MAX_RECEIPT_BYTES = 16384
SIGNALS = {signal.SIGINT, signal.SIGTERM}


def _budget(value, ceiling, *, integer=False):
    if (
        type(value) not in ({int} if integer else {int, float})
        or not 0 < value <= ceiling
        or not math.isfinite(value)
    ):
        raise CoordinationError("Invalid supervision budget")


def _config(command, directory, timeout, grace, shutdown, limit, cwd, env):
    if (
        sys.platform != "linux"
        or threading.current_thread() is not threading.main_thread()
    ):
        raise CoordinationError("Supervision requires Linux and the main thread")
    if (
        not isinstance(command, list)
        or not 1 <= len(command) <= 256
        or any(
            not isinstance(arg, str) or not arg or len(arg) > 4096 or "\0" in arg
            for arg in command
        )
    ):
        raise CoordinationError("Writer command must be a bounded argument list")
    _budget(timeout, 7 * 86400)
    _budget(grace, 300)
    _budget(shutdown, 600)
    _budget(limit, MAX_LOG_BYTES, integer=True)
    if grace >= shutdown:
        raise CoordinationError("Shutdown budget must exceed stop grace")
    directory = str(directory)
    if "://" in directory:
        raise CoordinationError("Supervision observations require a local directory")
    root = Path(directory).expanduser().resolve()
    if cwd is not None and not Path(cwd).is_dir():
        raise CoordinationError("Command working directory is missing")
    if env is not None and (
        not isinstance(env, dict)
        or any(
            not isinstance(k, str)
            or not k
            or "=" in k
            or "\0" in k
            or not isinstance(v, str)
            or "\0" in v
            for k, v in env.items()
        )
    ):
        raise CoordinationError("Command environment must contain string entries")
    config = {
        "command": command,
        "directory": str(root),
        "timeout_seconds": timeout,
        "stop_grace_seconds": grace,
        "shutdown_seconds": shutdown,
        "max_log_bytes": limit,
        "cwd": None if cwd is None else str(Path(cwd).resolve()),
        "env": env,
        "caller_pid": os.getpid(),
    }
    payload = canonical(config).encode()
    if len(payload) > MAX_CONTROL_BYTES:
        raise CoordinationError("Supervision control exceeds byte budget")
    return json.loads(payload), payload


def _fsync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _publish(path, value):
    # Exclusive creation, never repair/overwrite an original run observation.
    with path.open("xb") as output:
        os.chmod(path, 0o600)
        output.write(canonical(value).encode())
        output.flush()
        os.fsync(output.fileno())
    _fsync_directory(path.parent)


def _terminate(signum, frame):
    raise SystemExit(128 + signum)


def run_supervised(
    command,
    directory,
    *,
    timeout_seconds=3600,
    stop_grace_seconds=2,
    shutdown_seconds=30,
    max_log_bytes=8 * 1024 * 1024,
    cwd=None,
    env=None,
):
    """Create one immutable local run directory and wait through tree shutdown.

    Environment overrides apply only to the workload, not the supervising exec;
    environment values are not persisted. Log limits apply independently to each
    stream. Overflow is explicit failure; draining uses bounded 64 KiB chunks.
    Caller interruption requests tree shutdown before propagation. An interrupted
    or killed caller never authorizes reader release or writer completion.
    """
    config, payload = _config(
        command,
        directory,
        timeout_seconds,
        stop_grace_seconds,
        shutdown_seconds,
        max_log_bytes,
        cwd,
        env,
    )
    root = Path(config["directory"])
    try:
        root.mkdir(mode=0o700)
    except FileExistsError:
        raise CoordinationError("Supervision run directory already exists") from None
    _fsync_directory(root.parent)
    manifest = _manifest(config)
    _publish(root / "command.json", manifest)
    worker = None
    control = None
    handlers = {s: signal.getsignal(s) for s in SIGNALS}
    try:
        for sig in SIGNALS:
            signal.signal(sig, _terminate)
        # A sealed, bounded memory file cannot block waiting for a worker to read
        # control bytes. Do not persist environment values or put them in argv.
        control = os.memfd_create(
            "wintap-supervisor-control", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING
        )
        with os.fdopen(os.dup(control), "wb") as source:
            source.write(payload)
            source.flush()
        os.lseek(control, 0, os.SEEK_SET)
        fcntl.fcntl(
            control,
            fcntl.F_ADD_SEALS,
            fcntl.F_SEAL_WRITE
            | fcntl.F_SEAL_GROW
            | fcntl.F_SEAL_SHRINK
            | fcntl.F_SEAL_SEAL,
        )
        mask = signal.pthread_sigmask(signal.SIG_BLOCK, SIGNALS)
        try:
            worker = subprocess.Popen(
                [
                    sys.executable,
                    "-B",
                    "-m",
                    "wintappy.coordination.supervisor",
                    "--worker",
                    str(control),
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
                pass_fds=(control,),
                start_new_session=True,
            )
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, mask)
        worker.wait(timeout=timeout_seconds + shutdown_seconds + 10)
        if worker.returncode != 0:
            raise CoordinationError("Supervisor failed; child shutdown is uncertain")
        result = _validate_result(
            _read_json(root / "supervision.json", MAX_RECEIPT_BYTES)
        )
        if result.get("command_sha256") != digest(manifest):
            raise CoordinationError("Supervision command observation mismatch")
        if result.get("children_stopped") is not True:
            raise CoordinationError("Supervisor could not establish child shutdown")
        return result
    except BaseException:
        # Do not let a second Ctrl-C abort cleanup and imply the tree stopped.
        for sig in SIGNALS:
            signal.signal(sig, signal.SIG_IGN)
        if worker is not None and worker.poll() is None:
            worker.send_signal(signal.SIGTERM)
            try:
                worker.wait(timeout=shutdown_seconds + 5)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait(timeout=5)
                raise CoordinationError(
                    "Supervisor unresponsive; tree shutdown is uncertain"
                ) from None
        raise
    finally:
        if control is not None:
            os.close(control)
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise CoordinationError("Duplicate report field")
        result[key] = value
    return result


def _invalid_constant(value):
    raise CoordinationError("Nonfinite report value")


def _read_json(path, budget):
    return _parse_json(_read_bytes(path, budget))


def _read_bytes(path, budget):
    try:
        with path.open("rb") as source:
            payload = source.read(budget + 1)
    except OSError:
        raise CoordinationError("Missing or unreadable captured report") from None
    if len(payload) > budget:
        raise CoordinationError("Report exceeds byte budget")
    return payload


def _parse_json(payload):
    try:
        value = json.loads(
            payload, object_pairs_hook=_pairs, parse_constant=_invalid_constant
        )
    except (ValueError, UnicodeError, RecursionError):
        raise CoordinationError("Malformed structured report") from None
    if not isinstance(value, dict):
        raise CoordinationError("Structured report must be an object")
    canonical(value)  # Reject overflow such as 1e999 as well as literal NaN.
    return value


def read_report(directory, *, stream="stdout", max_bytes=1024 * 1024):
    """Read one exact, bounded, checksum-verified worker report; never a receipt."""
    _budget(max_bytes, MAX_LOG_BYTES, integer=True)
    if stream not in {"stdout", "stderr"}:
        raise CoordinationError("Unknown report stream")
    root = Path(directory)
    result = _validate_result(_read_json(root / "supervision.json", MAX_RECEIPT_BYTES))
    if result.get("status") != "exited" or result.get("children_stopped") is not True:
        raise CoordinationError("Incomplete supervision cannot supply a report")
    info = result[stream]
    if info["truncated"]:
        raise CoordinationError("Truncated report cannot be accepted")
    path = root / f"{stream}.log"
    payload = _read_bytes(path, max_bytes)
    if (
        len(payload) != info["bytes_saved"]
        or hashlib.sha256(payload).hexdigest() != info["sha256"]
    ):
        raise CoordinationError("Captured report changed")
    # Parse exactly the immutable bytes just checked, not a second path read.
    return _parse_json(payload)


def _validate_result(result):
    if set(result) != {
        "status",
        "exit_code",
        "children_stopped",
        "command_sha256",
        "stdout",
        "stderr",
    }:
        raise CoordinationError("Malformed supervision observation")
    if (
        not isinstance(result["status"], str)
        or result["status"]
        not in {
            "exited",
            "launch_failed",
            "timeout",
            "interrupted",
            "descendants_terminated",
            "output_limit",
            "shutdown_uncertain",
            "supervisor_failed",
        }
        or type(result["children_stopped"]) is not bool
        or result["exit_code"] is not None
        and (
            type(result["exit_code"]) is not int
            or not -255 <= result["exit_code"] <= 255
        )
        or not isinstance(result["command_sha256"], str)
        or not re.fullmatch("[a-f0-9]{64}", result["command_sha256"])
    ):
        raise CoordinationError("Malformed supervision observation")
    if result["status"] in {"exited", "output_limit", "descendants_terminated"} and (
        result["exit_code"] is None or not result["children_stopped"]
    ):
        raise CoordinationError(
            "Completed supervision requires root exit and full shutdown"
        )
    if result["status"] == "launch_failed" and (
        result["exit_code"] is not None or not result["children_stopped"]
    ):
        raise CoordinationError("Invalid failed-launch observation")
    if result["status"] == "shutdown_uncertain" and result["children_stopped"]:
        raise CoordinationError("Uncertain shutdown cannot assert stopped children")
    for name in ("stdout", "stderr"):
        info = result[name]
        if (
            not isinstance(info, dict)
            or set(info) != {"bytes_saved", "bytes_seen", "sha256", "truncated"}
            or type(info["bytes_saved"]) is not int
            or type(info["bytes_seen"]) is not int
            or not 0 <= info["bytes_saved"] <= min(MAX_LOG_BYTES, info["bytes_seen"])
            or type(info["truncated"]) is not bool
            or info["truncated"] != (info["bytes_seen"] > info["bytes_saved"])
            or not isinstance(info["sha256"], str)
            or not re.fullmatch("[a-f0-9]{64}", info["sha256"])
        ):
            raise CoordinationError("Malformed capture observation")
    truncated = any(result[n]["truncated"] for n in ("stdout", "stderr"))
    if (
        result["status"] == "exited"
        and truncated
        or result["status"] == "output_limit"
        and not truncated
    ):
        raise CoordinationError("Capture overflow/status mismatch")
    return result


class _Capture:
    def __init__(self, root, name, limit):
        self.output = (root / f"{name}.log").open("xb")
        os.chmod(root / f"{name}.log", 0o600)
        self.limit = limit
        self.seen = self.saved = 0
        self.hash = hashlib.sha256()

    def add(self, chunk):
        self.seen += len(chunk)
        retained = chunk[: max(0, self.limit - self.saved)]
        self.output.write(retained)
        self.hash.update(retained)
        self.saved += len(retained)

    def close(self):
        self.output.flush()
        os.fsync(self.output.fileno())
        self.output.close()
        return {
            "bytes_seen": self.seen,
            "bytes_saved": self.saved,
            "sha256": self.hash.hexdigest(),
            "truncated": self.seen > self.saved,
        }


def _prctl(option, value):
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(ctypes.c_int(option), ctypes.c_ulong(value), 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "Linux supervision prctl failed")


def _reap(root_pid, exit_code):
    # Return to deadline/signal handling even if a workload churns zombies.
    for _ in range(256):
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
        except ChildProcessError:
            return exit_code, True
        if pid == 0:
            return exit_code, False
        if pid == root_pid:
            exit_code = os.waitstatus_to_exitcode(status)
    return exit_code, False


def _signal_children(sig):
    # Only direct, unreaped children of this single-threaded isolated subreaper.
    # Their PIDs cannot be recycled until our waitpid; no global /proc/PID sweep.
    with open(f"/proc/self/task/{os.getpid()}/children", encoding="ascii") as source:
        children = source.read(MAX_CONTROL_BYTES + 1)
    if len(children) > MAX_CONTROL_BYTES:
        raise CoordinationError("Child enumeration exceeds supervision budget")
    for item in children.split():
        try:
            os.kill(int(item), sig)
        except ProcessLookupError:
            pass


def _stop_tree(root_pid, exit_code, seconds):
    deadline = time.monotonic() + seconds
    while True:
        exit_code, stopped = _reap(root_pid, exit_code)
        if stopped:
            return exit_code, True
        _signal_children(signal.SIGKILL)
        if time.monotonic() >= deadline:
            return exit_code, False
        time.sleep(0.01)


def _manifest(config):
    return {
        k: v for k, v in config.items() if k not in {"env", "directory", "caller_pid"}
    }


def _worker(config):
    root = Path(config["directory"])
    manifest = _manifest(config)
    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    for sig in SIGNALS:
        signal.signal(sig, stop)
    # Signals blocked across the caller's Popen are inherited through exec.
    signal.pthread_sigmask(signal.SIG_UNBLOCK, SIGNALS)
    parent = config["caller_pid"]
    _prctl(36, 1)  # PR_SET_CHILD_SUBREAPER, before launching any workload.
    _prctl(1, signal.SIGTERM)  # PR_SET_PDEATHSIG: request cleanup if caller dies.
    if os.getppid() != parent or parent == 1:
        stopping = True
    captures = {
        name: _Capture(root, name, config["max_log_bytes"])
        for name in ("stdout", "stderr")
    }
    result = {
        "status": "launch_failed",
        "exit_code": None,
        "children_stopped": True,
        "command_sha256": digest(manifest),
    }
    try:
        if stopping:
            result["status"] = "interrupted"
            return
        try:
            child = subprocess.Popen(
                config["command"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=config["cwd"],
                env=config["env"],
                close_fds=True,
                start_new_session=True,
            )
        except OSError:
            return
        result["children_stopped"] = False
        selector = None
        exit_code = None
        stopped = False
        try:
            selector = selectors.DefaultSelector()
            for name in captures:
                pipe = getattr(child, name)
                os.set_blocking(pipe.fileno(), False)
                selector.register(pipe, selectors.EVENT_READ, name)
            started = time.monotonic()
            stop_at = None
            status = "exited"
            while True:
                exit_code, stopped = _reap(child.pid, exit_code)
                observed = time.monotonic()
                if stop_at is None:
                    if stopping:
                        status, stop_at = "interrupted", observed
                    elif observed - started >= config["timeout_seconds"]:
                        status, stop_at = "timeout", observed
                    elif exit_code is not None and not stopped:
                        status, stop_at = "descendants_terminated", observed
                if stop_at is not None and not stopped:
                    _signal_children(
                        signal.SIGKILL
                        if observed - stop_at >= config["stop_grace_seconds"]
                        else signal.SIGTERM
                    )
                    if observed - stop_at >= config["shutdown_seconds"]:
                        status = "shutdown_uncertain"
                        break
                for key, _ in selector.select(0.02):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if chunk:
                        captures[key.data].add(chunk)
                    else:
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
                if stopped and not selector.get_map():
                    break
            result.update(status=status, exit_code=exit_code, children_stopped=stopped)
        except BaseException:
            # Logging, enumeration, selector and other failures must stop/reap
            # the tree too. Never retain the pre-launch 'no children' assertion.
            result.update(status="supervisor_failed", children_stopped=False)
            exit_code, stopped = _stop_tree(
                child.pid, exit_code, config["shutdown_seconds"]
            )
            result.update(exit_code=exit_code, children_stopped=stopped)
            raise
        finally:
            # Popen must not reap our child independently of the subreaper loop.
            child.returncode = exit_code
            child.stdout.close()
            child.stderr.close()
            if selector is not None:
                selector.close()
    finally:
        for name, capture in captures.items():
            result[name] = capture.close()
        if result["status"] == "exited" and any(
            result[n]["truncated"] for n in captures
        ):
            result["status"] = "output_limit"
        _publish(root / "supervision.json", result)


if __name__ == "__main__":
    if len(sys.argv) != 3 or sys.argv[1] != "--worker":
        raise SystemExit("Internal supervisor worker only")
    with os.fdopen(int(sys.argv[2]), "rb") as source:
        payload = source.read(MAX_CONTROL_BYTES + 1)
    if len(payload) > MAX_CONTROL_BYTES:
        raise SystemExit("Supervision control exceeds byte budget")
    _worker(json.loads(payload))
