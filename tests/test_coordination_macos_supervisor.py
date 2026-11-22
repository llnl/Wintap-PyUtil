"""Actual-Mac controls for the accepted best-effort reader process-group rule."""

import json
import os
import signal
import subprocess
import sys
import time

import pytest

pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="native macOS controls")


def command(code):
    return [sys.executable, "-B", "-c", code]


def supervise(tmp_path, code, **kwargs):
    from wintappy.coordination.supervisor import run_reader_supervised

    return run_reader_supervised(
        command(code),
        tmp_path / "run",
        timeout_seconds=5,
        stop_grace_seconds=0.1,
        shutdown_seconds=2,
        **kwargs,
    )


def wait_for(path, timeout=10):
    deadline = time.monotonic() + timeout
    while not path.exists():
        assert time.monotonic() < deadline, f"Timed out waiting for {path.name}"
        time.sleep(0.01)


def pid_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def process_state(pid):
    result = subprocess.run(
        ["/bin/ps", "-o", "stat=", "-p", str(pid)],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip().split(maxsplit=1)[0] if result.stdout.strip() else None


def cleanup_group(group_id):
    try:
        os.killpg(group_id, signal.SIGKILL)
    except ProcessLookupError:
        pass


def test_normal_exit_captures_streams_and_records_group_scope(tmp_path):
    result = supervise(
        tmp_path,
        "import sys; print('synthetic-out'); print('synthetic-err',file=sys.stderr); sys.exit(7)",
    )

    assert result["status"] == "exited" and result["exit_code"] == 7
    assert result["shutdown_scope"] == "process_group"
    assert result["children_stopped"] is True
    assert (tmp_path / "run/stdout.log").read_bytes() == b"synthetic-out\n"
    assert (tmp_path / "run/stderr.log").read_bytes() == b"synthetic-err\n"
    assert json.loads((tmp_path / "run/supervision.json").read_text()) == result


def test_same_group_child_is_stopped_before_normal_release(tmp_path):
    pidfile = tmp_path / "child.pid"
    workload = f"""
import os,subprocess,sys
from pathlib import Path
child=subprocess.Popen([sys.executable,'-B','-c','import time; time.sleep(60)'])
Path({str(pidfile)!r}).write_text(f'{{child.pid}} {{os.getpgid(child.pid)}}')
"""
    result = supervise(tmp_path, workload)

    child_pid, group_id = map(int, pidfile.read_text().split())
    assert result["status"] == "descendants_terminated"
    assert result["children_stopped"] is True
    state = process_state(child_pid)
    assert state is None or state.startswith("Z")
    leader_state = process_state(group_id)
    assert leader_state is None or leader_state.startswith("Z")


def test_timeout_terminates_managed_group(tmp_path):
    from wintappy.coordination.supervisor import run_reader_supervised

    pidfile = tmp_path / "root.pid"
    workload = f"""
import os,signal,time
from pathlib import Path
signal.signal(signal.SIGTERM,signal.SIG_IGN)
Path({str(pidfile)!r}).write_text(str(os.getpid()))
time.sleep(60)
"""
    result = run_reader_supervised(
        command(workload),
        tmp_path / "run",
        timeout_seconds=0.3,
        stop_grace_seconds=0.1,
        shutdown_seconds=2,
    )

    assert result["status"] == "timeout" and result["children_stopped"]
    assert result["shutdown_scope"] == "process_group"
    state = process_state(int(pidfile.read_text()))
    assert state is None or state.startswith("Z")


def test_missing_executable_and_output_limit_keep_group_scope(tmp_path):
    from wintappy.coordination.supervisor import run_reader_supervised

    missing = run_reader_supervised([str(tmp_path / "absent")], tmp_path / "missing")
    limited = run_reader_supervised(
        command("print('x'*10000)"),
        tmp_path / "limited",
        max_log_bytes=100,
    )

    assert missing["status"] == "launch_failed" and missing["children_stopped"]
    assert missing["shutdown_scope"] == "process_group"
    assert limited["status"] == "output_limit" and limited["children_stopped"]
    assert limited["shutdown_scope"] == "process_group"


def test_detached_child_demonstrates_the_accepted_residual_risk(tmp_path):
    pidfile = tmp_path / "escaped.pid"
    workload = f"""
import subprocess,sys
from pathlib import Path
child=subprocess.Popen([sys.executable,'-B','-c','import time; time.sleep(60)'],
                       start_new_session=True, stdin=subprocess.DEVNULL,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
Path({str(pidfile)!r}).write_text(str(child.pid))
"""
    try:
        result = supervise(tmp_path, workload)
        escaped_pid = int(pidfile.read_text())
        assert result["status"] == "exited" and result["children_stopped"]
        assert result["shutdown_scope"] == "process_group"
        assert pid_alive(escaped_pid)
    finally:
        if pidfile.exists():
            try:
                os.kill(int(pidfile.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_escaped_child_holding_capture_pipe_is_uncertain_and_not_released(tmp_path):
    from wintappy.coordination.protocol import CoordinationError
    from wintappy.coordination.supervisor import run_reader_supervised

    pidfile = tmp_path / "escaped.pid"
    workload = f"""
import subprocess,sys
from pathlib import Path
child=subprocess.Popen([sys.executable,'-B','-c','import time; time.sleep(60)'],
                       start_new_session=True)
Path({str(pidfile)!r}).write_text(str(child.pid))
"""
    try:
        with pytest.raises(CoordinationError, match="shutdown is uncertain"):
            run_reader_supervised(
                command(workload),
                tmp_path / "run",
                timeout_seconds=5,
                stop_grace_seconds=0.1,
                shutdown_seconds=0.3,
            )
        receipt = json.loads((tmp_path / "run/supervision.json").read_text())
        assert receipt["status"] == "shutdown_uncertain"
        assert receipt["children_stopped"] is False
        assert receipt["shutdown_scope"] == "process_group"
    finally:
        if pidfile.exists():
            try:
                os.kill(int(pidfile.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_caller_interruption_stops_group_and_records_shutdown(tmp_path):
    pidfile = tmp_path / "child.pid"
    caller_code = """
import json,sys
from wintappy.coordination.supervisor import run_reader_supervised
run_reader_supervised(json.loads(sys.argv[1]),sys.argv[2],timeout_seconds=30,
               stop_grace_seconds=0.1,shutdown_seconds=2)
"""
    workload = f"""
import os,signal,time
from pathlib import Path
signal.signal(signal.SIGTERM,signal.SIG_IGN)
Path({str(pidfile)!r}).write_text(str(os.getpid()))
time.sleep(60)
"""
    caller = subprocess.Popen(
        command(caller_code) + [json.dumps(command(workload)), str(tmp_path / "run")],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_for(pidfile)
        caller.send_signal(signal.SIGTERM)
        assert caller.wait(timeout=10) != 0
        wait_for(tmp_path / "run/supervision.json")
        result = json.loads((tmp_path / "run/supervision.json").read_text())
        assert result["status"] == "interrupted" and result["children_stopped"]
        assert result["shutdown_scope"] == "process_group"
    finally:
        if caller.poll() is None:
            caller.kill()
            caller.wait(timeout=5)
        if pidfile.exists():
            cleanup_group(int(pidfile.read_text()))


def test_sigkill_of_wrapper_is_detected_by_worker(tmp_path):
    pidfile = tmp_path / "root.pid"
    caller_code = """
import json,sys
from wintappy.coordination.supervisor import run_reader_supervised
run_reader_supervised(json.loads(sys.argv[1]),sys.argv[2],timeout_seconds=30,
               stop_grace_seconds=0.1,shutdown_seconds=2)
"""
    workload = f"""
import os,signal,time
from pathlib import Path
signal.signal(signal.SIGTERM,signal.SIG_IGN)
Path({str(pidfile)!r}).write_text(str(os.getpid()))
time.sleep(60)
"""
    caller = subprocess.Popen(
        command(caller_code) + [json.dumps(command(workload)), str(tmp_path / "run")],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_for(pidfile)
        caller.kill()
        caller.wait(timeout=5)
        wait_for(tmp_path / "run/supervision.json")
        result = json.loads((tmp_path / "run/supervision.json").read_text())
        assert result["status"] == "interrupted" and result["children_stopped"]
        assert result["shutdown_scope"] == "process_group"
    finally:
        if caller.poll() is None:
            caller.kill()
            caller.wait(timeout=5)
        if pidfile.exists():
            cleanup_group(int(pidfile.read_text()))


def test_supervisor_death_has_no_release_observation(tmp_path):
    worker_file = tmp_path / "worker.pid"
    root_file = tmp_path / "root.pid"
    caller_code = """
import sys
from pathlib import Path
from wintappy.coordination import supervisor as module
from wintappy.coordination.protocol import CoordinationError
original=module.subprocess.Popen
def observed(*args,**kwargs):
    process=original(*args,**kwargs)
    if args[0][-1] == '--mac-worker':
        Path(sys.argv[1]).write_text(str(process.pid))
    return process
module.subprocess.Popen=observed
try:
    module.run_reader_supervised(sys.argv[2:-1],sys.argv[-1],timeout_seconds=30,
                          stop_grace_seconds=0.1,shutdown_seconds=2)
except CoordinationError:
    raise SystemExit(9)
"""
    workload = f"""
import os,time
from pathlib import Path
Path({str(root_file)!r}).write_text(str(os.getpid()))
time.sleep(60)
"""
    caller = subprocess.Popen(
        command(caller_code) + [str(worker_file), *command(workload), str(tmp_path / "run")],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_for(worker_file)
        wait_for(root_file)
        os.kill(int(worker_file.read_text()), signal.SIGKILL)
        assert caller.wait(timeout=10) == 9
        assert not (tmp_path / "run/supervision.json").exists()
    finally:
        if caller.poll() is None:
            caller.kill()
            caller.wait(timeout=5)
        if root_file.exists():
            cleanup_group(int(root_file.read_text()))


def test_lost_worker_acknowledgment_retains_failed_call_even_with_receipt(tmp_path, monkeypatch):
    from wintappy.coordination import supervisor
    from wintappy.coordination.protocol import CoordinationError

    original = subprocess.Popen

    class LostAcknowledgment:
        def __init__(self, *args, **kwargs):
            self.process = original(*args, **kwargs)

        def __getattr__(self, name):
            return getattr(self.process, name)

        def communicate(self, *args, **kwargs):
            result = self.process.communicate(*args, **kwargs)
            self.process.returncode = -signal.SIGKILL
            return result

    monkeypatch.setattr(supervisor.subprocess, "Popen", LostAcknowledgment)
    with pytest.raises(CoordinationError, match="supervisor failed"):
        supervisor.run_reader_supervised(command("pass"), tmp_path / "run")

    receipt = json.loads((tmp_path / "run/supervision.json").read_text())
    assert receipt["children_stopped"] is True
    assert receipt["shutdown_scope"] == "process_group"


def test_coordinated_reader_runs_natively_and_releases_only_group_observation(
    tmp_path,
):
    from wintappy.coordination.client import run_reader
    from wintappy.coordination.protocol import make_grant, make_request

    from .test_coordination_execution import binding
    from .test_coordination_ledger import HOUR
    from .test_coordination_tick import DUE, transport

    _, store = transport()
    request = make_request(binding(tmp_path), "a" * 32, "shared-command", HOUR, DUE, DUE)
    store.publish(make_grant(request, "b" * 32, DUE))
    result = run_reader(
        tmp_path / "clients",
        request,
        store,
        command("print('native-reader')"),
        clock=lambda: DUE,
        timeout_seconds=5,
        stop_grace_seconds=0.1,
        shutdown_seconds=2,
    )

    assert result["exit_code"] == 0
    release = store.read("release", request["job_id"])
    assert release["exit_code"] == 0
    observations = next((tmp_path / "clients").glob("*/observations"))
    receipt = json.loads((observations / "supervision.json").read_text())
    assert receipt["shutdown_scope"] == "process_group"


def test_coordinated_reader_retains_reservation_when_supervisor_ack_is_lost(tmp_path, monkeypatch):
    from wintappy.coordination import client
    from wintappy.coordination.protocol import (
        CoordinationError,
        make_grant,
        make_request,
    )

    from .test_coordination_execution import binding
    from .test_coordination_ledger import HOUR
    from .test_coordination_tick import DUE, transport

    _, store = transport()
    request = make_request(binding(tmp_path), "a" * 32, "shared-command", HOUR, DUE, DUE)
    store.publish(make_grant(request, "b" * 32, DUE))
    original = client.run_reader_supervised
    reader_command = command("pass")

    def lose_acknowledgment(*args, **kwargs):
        original(*args, **kwargs)
        raise CoordinationError("synthetic supervisor acknowledgment loss")

    monkeypatch.setattr(client, "run_reader_supervised", lose_acknowledgment)
    with pytest.raises(CoordinationError, match="acknowledgment loss"):
        client.run_reader(
            tmp_path / "clients",
            request,
            store,
            reader_command,
            clock=lambda: DUE,
            timeout_seconds=5,
            stop_grace_seconds=0.1,
            shutdown_seconds=2,
        )

    assert store.read("release", request["job_id"]) is None
    job = tmp_path / "clients" / request["job_id"]
    assert (job / "launch.json").exists()
    assert not (job / "terminal.json").exists()
    with pytest.raises(CoordinationError, match="launch is unresolved"):
        client.run_reader(
            tmp_path / "clients",
            request,
            store,
            reader_command,
            clock=lambda: DUE,
            timeout_seconds=5,
            stop_grace_seconds=0.1,
            shutdown_seconds=2,
        )
    assert store.read("release", request["job_id"]) is None
