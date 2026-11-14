"""Real Linux trees and bounded local observations, not migration certification."""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="Linux subreaper contract"
)


def command(code):
    return [sys.executable, "-B", "-c", code]


def supervise(tmp_path, code, **kwargs):
    from wintappy.coordination.supervisor import run_supervised

    return run_supervised(
        command(code),
        tmp_path / "run",
        timeout_seconds=5,
        stop_grace_seconds=0.1,
        shutdown_seconds=2,
        **kwargs,
    )


def test_exit_code_and_both_streams_are_durably_captured(tmp_path):
    result = supervise(
        tmp_path,
        "import sys; print('synthetic-out'); print('synthetic-err',file=sys.stderr); sys.exit(7)",
    )
    assert result["status"] == "exited" and result["exit_code"] == 7
    assert result["children_stopped"] is True
    assert (tmp_path / "run/stdout.log").read_bytes() == b"synthetic-out\n"
    assert (tmp_path / "run/stderr.log").read_bytes() == b"synthetic-err\n"
    assert json.loads((tmp_path / "run/supervision.json").read_text()) == result


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_log_and_report_exhaustion_are_explicit_not_silent_truncation(tmp_path, stream):
    from wintappy.coordination.protocol import CoordinationError
    from wintappy.coordination.supervisor import read_report

    result = supervise(
        tmp_path, f"import sys; sys.{stream}.write('x'*200000)", max_log_bytes=1024
    )
    assert result["status"] == "output_limit" and result["children_stopped"]
    assert (tmp_path / f"run/{stream}.log").stat().st_size == 1024
    assert result[stream]["truncated"] and result[stream]["bytes_seen"] >= 200000
    with pytest.raises(CoordinationError):
        read_report(tmp_path / "run", stream=stream)


@pytest.mark.parametrize("detached", [False, True])
def test_root_exit_does_not_leave_sleeping_or_detached_descendants(tmp_path, detached):
    from wintappy.coordination.supervisor import run_supervised

    pidfile = tmp_path / "synthetic.pid"
    code = f"""
import subprocess,sys
from pathlib import Path
child=subprocess.Popen([sys.executable,'-B','-c','import time; time.sleep(60)'],
                       start_new_session={detached!r})
Path({str(pidfile)!r}).write_text(str(child.pid))
"""
    try:
        result = run_supervised(
            command(code),
            tmp_path / "run",
            timeout_seconds=5,
            stop_grace_seconds=0.1,
            shutdown_seconds=2,
        )
        assert result["status"] == "descendants_terminated"
        assert result["children_stopped"] and result["exit_code"] == 0
        assert not Path(f"/proc/{pidfile.read_text()}").exists()
    finally:
        if pidfile.exists():
            try:
                os.kill(int(pidfile.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_timeout_stops_sigterm_ignoring_tree_but_not_unrelated_child(tmp_path):
    from wintappy.coordination.supervisor import run_supervised

    unrelated = subprocess.Popen(command("import time; time.sleep(60)"))
    pidfile = tmp_path / "synthetic.pid"
    code = f"""
import os,signal,time
from pathlib import Path
signal.signal(signal.SIGTERM,signal.SIG_IGN)
Path({str(pidfile)!r}).write_text(str(os.getpid()))
time.sleep(60)
"""
    try:
        result = run_supervised(
            command(code),
            tmp_path / "run",
            timeout_seconds=0.5,
            stop_grace_seconds=0.1,
            shutdown_seconds=2,
        )
        assert result["status"] == "timeout" and result["children_stopped"]
        assert not Path(f"/proc/{pidfile.read_text()}").exists()
        assert unrelated.poll() is None
    finally:
        unrelated.kill()
        unrelated.wait(timeout=5)


def test_existing_run_directory_is_never_reused_or_overwritten(tmp_path):
    from wintappy.coordination.protocol import CoordinationError
    from wintappy.coordination.supervisor import run_supervised

    root = tmp_path / "run"
    root.mkdir()
    marker = root / "original"
    marker.write_bytes(b"retained evidence")
    with pytest.raises(CoordinationError, match="exists"):
        run_supervised(command("raise AssertionError('must not execute')"), root)
    assert marker.read_bytes() == b"retained evidence" and list(root.iterdir()) == [
        marker
    ]


@pytest.mark.parametrize("budget", [True, 0, -1, float("nan")])
def test_invalid_budget_is_rejected_before_directory_or_launch(tmp_path, budget):
    from wintappy.coordination.protocol import CoordinationError
    from wintappy.coordination.supervisor import run_supervised

    with pytest.raises(CoordinationError, match="budget"):
        run_supervised(
            command("raise AssertionError('must not execute')"),
            tmp_path / "run",
            timeout_seconds=budget,
        )
    assert not (tmp_path / "run").exists()


@pytest.mark.parametrize(
    "payload",
    ['{"status":"empty"}', '{"status":"empty","status":"completed"}', "NaN", "[]"],
)
def test_report_requires_bounded_unmodified_object_without_duplicate_keys(
    tmp_path, payload
):
    from wintappy.coordination.protocol import CoordinationError
    from wintappy.coordination.supervisor import read_report

    result = supervise(tmp_path, f"print({payload!r})")
    assert result["children_stopped"]
    if payload == '{"status":"empty"}':
        assert read_report(tmp_path / "run") == {"status": "empty"}
        with pytest.raises(CoordinationError, match="budget"):
            read_report(tmp_path / "run", max_bytes=4)
        (tmp_path / "run/stdout.log").write_bytes(b'{"status":"other"}\n')
    with pytest.raises(CoordinationError):
        read_report(tmp_path / "run")


def test_missing_executable_is_recorded_not_reported_as_empty_completion(tmp_path):
    from wintappy.coordination.supervisor import run_supervised

    result = run_supervised([str(tmp_path / "absent")], tmp_path / "run")
    assert result["status"] == "launch_failed" and result["exit_code"] is None
    assert result["children_stopped"] is True


def wait_for(path):
    deadline = time.monotonic() + 10
    while not path.exists():
        assert time.monotonic() < deadline, "Synthetic child failed to become ready"
        time.sleep(0.01)


def test_report_parses_the_same_bytes_that_passed_capture_checksum(
    tmp_path, monkeypatch
):
    from wintappy.coordination import supervisor as module
    from wintappy.coordination.protocol import CoordinationError

    supervise(tmp_path, 'print(\'{"status":"empty"}\')')
    original = module._read_json

    def replace_before_parse(path, budget):
        if path.name == "stdout.log":
            path.write_bytes(b'{"status":"fabricated"}\n')
        return original(path, budget)

    monkeypatch.setattr(module, "_read_json", replace_before_parse)
    try:
        report = module.read_report(tmp_path / "run")
    except CoordinationError:
        return
    assert report == {"status": "empty"}, (
        "Parsed bytes were not the checksum-verified bytes"
    )


def test_capture_failure_cleans_tree_and_never_records_false_shutdown(tmp_path):
    from wintappy.coordination.supervisor import _config

    root = tmp_path / "run"
    root.mkdir()
    pidfile = tmp_path / "synthetic.pid"
    workload = f"""
import os,time
from pathlib import Path
Path({str(pidfile)!r}).write_text(str(os.getpid()))
print('trigger capture failure',flush=True)
time.sleep(60)
"""
    _, payload = _config(command(workload), root, 5, 0.1, 2, 1024, None, None)
    worker = """
import json,sys
from wintappy.coordination import supervisor as s
def fail(self,chunk):
    raise OSError('injected capture failure')
s._Capture.add=fail
s._worker(json.loads(sys.argv[1]))
"""
    try:
        observed = subprocess.run(
            command(worker) + [payload.decode()],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert observed.returncode != 0
        result = json.loads((root / "supervision.json").read_text())
        assert result["status"] == "supervisor_failed" and result["children_stopped"]
        assert not Path(f"/proc/{pidfile.read_text()}").exists()
    finally:
        if pidfile.exists():
            try:
                os.kill(int(pidfile.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass


@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM, signal.SIGKILL])
def test_caller_interruption_or_death_requests_cleanup_of_detached_tree(tmp_path, sig):
    pidfile = tmp_path / "synthetic.pid"
    workload = f"""
import os,signal,time
from pathlib import Path
signal.signal(signal.SIGTERM,signal.SIG_IGN)
Path({str(pidfile)!r}).write_text(str(os.getpid()))
time.sleep(60)
"""
    caller_code = """
import json,sys
from wintappy.coordination.supervisor import run_supervised
run_supervised(json.loads(sys.argv[1]),sys.argv[2],timeout_seconds=30,
               stop_grace_seconds=0.1,shutdown_seconds=2)
"""
    caller = subprocess.Popen(
        command(caller_code) + [json.dumps(command(workload)), str(tmp_path / "run")],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        wait_for(pidfile)
        caller.send_signal(sig)
        assert caller.wait(timeout=10) != 0
        wait_for(tmp_path / "run/supervision.json")
        result = json.loads((tmp_path / "run/supervision.json").read_text())
        assert result["status"] == "interrupted" and result["children_stopped"]
        assert not Path(f"/proc/{pidfile.read_text()}").exists()
    finally:
        if caller.poll() is None:
            caller.kill()
            caller.wait(timeout=5)
        if pidfile.exists():
            try:
                os.kill(int(pidfile.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass


def test_nested_detached_reparenting_reaps_every_generation(tmp_path):
    pids = tmp_path / "synthetic-pids"
    grandchild = f"import os,signal,time; from pathlib import Path; signal.signal(signal.SIGTERM,signal.SIG_IGN); Path({str(pids)!r}).write_text(str(os.getpid())); time.sleep(60)"
    middle = f"import subprocess,sys,time; from pathlib import Path; child=subprocess.Popen({command(grandchild)!r},start_new_session=True); p=Path({str(pids)!r});\nwhile not p.exists(): time.sleep(0.01)\np.write_text(p.read_text()+' '+str(__import__('os').getpid())); time.sleep(60)"
    root_code = f"import subprocess,time; from pathlib import Path; child=subprocess.Popen({command(middle)!r},start_new_session=True); p=Path({str(pids)!r});\nwhile not p.exists() or len(p.read_text().split())<2: time.sleep(0.01)"
    try:
        result = supervise(tmp_path, root_code)
        assert (
            result["status"] == "descendants_terminated" and result["children_stopped"]
        )
        assert all(
            not Path(f"/proc/{pid}").exists() for pid in pids.read_text().split()
        )
    finally:
        if pids.exists():
            for pid in pids.read_text().split():
                try:
                    os.kill(int(pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass


def test_selector_setup_failure_also_stops_the_launched_root(tmp_path):
    from wintappy.coordination.supervisor import _config

    root = tmp_path / "run"
    root.mkdir()
    pidfile = tmp_path / "synthetic.pid"
    workload = f"import os,time; from pathlib import Path; Path({str(pidfile)!r}).write_text(str(os.getpid())); time.sleep(60)"
    _, payload = _config(command(workload), root, 5, 0.1, 2, 1024, None, None)
    worker = f"""
import json,sys,time
from pathlib import Path
from wintappy.coordination import supervisor as s
original=s.selectors.DefaultSelector
def fail():
    deadline=time.monotonic()+5
    while not Path({str(pidfile)!r}).exists():
        if time.monotonic()>deadline: raise AssertionError('not ready')
        time.sleep(0.01)
    raise OSError('injected selector setup failure')
s.selectors.DefaultSelector=fail
s._worker(json.loads(sys.argv[1]))
"""
    try:
        observed = subprocess.run(
            command(worker) + [payload.decode()],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        assert observed.returncode != 0
        result = json.loads((root / "supervision.json").read_text())
        assert result["status"] == "supervisor_failed" and result["children_stopped"]
        assert not Path(f"/proc/{pidfile.read_text()}").exists()
    finally:
        if pidfile.exists():
            try:
                os.kill(int(pidfile.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass


@pytest.mark.parametrize(
    "damaged", [{}, {"bytes_saved": True}, {"truncated": False, "bytes_saved": -1}]
)
def test_malformed_capture_observation_is_refused_as_coordination_error(
    tmp_path, damaged
):
    from wintappy.coordination.protocol import CoordinationError
    from wintappy.coordination.supervisor import read_report

    supervise(tmp_path, 'print(\'{"status":"empty"}\')')
    path = tmp_path / "run/supervision.json"
    value = json.loads(path.read_text())
    value["stdout"] = damaged
    path.write_text(json.dumps(value))
    with pytest.raises(CoordinationError):
        read_report(tmp_path / "run")


@pytest.mark.parametrize(
    "field,value",
    [("status", []), ("exit_code", True), ("exit_code", None), ("children_stopped", 1)],
)
def test_invalid_scalar_observations_fail_with_coordination_error(
    tmp_path, field, value
):
    from wintappy.coordination.protocol import CoordinationError
    from wintappy.coordination.supervisor import read_report

    supervise(tmp_path, 'print(\'{"status":"empty"}\')')
    path = tmp_path / "run/supervision.json"
    result = json.loads(path.read_text())
    result[field] = value
    path.write_text(json.dumps(result))
    with pytest.raises(CoordinationError):
        read_report(tmp_path / "run")


def test_enormous_integer_budget_fails_cleanly_before_access(tmp_path):
    from wintappy.coordination.protocol import CoordinationError
    from wintappy.coordination.supervisor import run_supervised

    with pytest.raises(CoordinationError, match="budget"):
        run_supervised(
            command("raise AssertionError('must not execute')"),
            tmp_path / "run",
            timeout_seconds=10**1000,
        )
    assert not (tmp_path / "run").exists()


def test_unavailable_subreaper_refuses_before_workload_launch(tmp_path):
    from wintappy.coordination import supervisor as module

    config, _ = module._config(
        command("raise AssertionError('must not execute')"),
        tmp_path / "run",
        5,
        0.1,
        2,
        1024,
        None,
        None,
    )
    # Run this capability probe in a separate exec: _worker changes signal state.
    code = """
import json,sys
from wintappy.coordination import supervisor as s
def fail(option,value): raise OSError('injected unsupported subreaper')
s._prctl=fail
s._worker(json.loads(sys.argv[1]))
"""
    observed = subprocess.run(
        command(code) + [json.dumps(config)],
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )
    assert observed.returncode != 0 and not (tmp_path / "run").exists()


def test_command_environment_is_not_persisted_or_used_for_supervising_exec(tmp_path):
    from wintappy.coordination.supervisor import run_supervised

    marker = "synthetic-private-config-value"
    argv = command("import os; assert os.environ['SYNTHETIC_CONTROL']")
    result = run_supervised(
        argv,
        tmp_path / "run",
        timeout_seconds=5,
        env={
            **os.environ,
            "SYNTHETIC_CONTROL": marker,
            "PYTHONPATH": str(tmp_path / "missing-import-root"),
        },
    )
    assert result["status"] == "exited" and result["exit_code"] == 0
    manifest = json.loads((tmp_path / "run/command.json").read_text())
    assert "env" not in manifest
    assert all(marker not in path.read_text() for path in (tmp_path / "run").iterdir())


def test_nonreading_worker_failure_is_bounded_and_refuses_missing_observation(
    tmp_path, monkeypatch
):
    from wintappy.coordination import supervisor as module
    from wintappy.coordination.protocol import CoordinationError

    original = subprocess.Popen

    def unresponsive_worker(argv, **kwargs):
        return original(command("import time; time.sleep(0.1)"), **kwargs)

    monkeypatch.setattr(module.subprocess, "Popen", unresponsive_worker)
    started = time.monotonic()
    with pytest.raises(CoordinationError):
        module.run_supervised(
            command("raise AssertionError('must not execute')"),
            tmp_path / "run",
            timeout_seconds=0.1,
            stop_grace_seconds=0.1,
            shutdown_seconds=0.2,
            env={"SYNTHETIC_LARGE_CONTROL": "x" * 300000},
        )
    assert time.monotonic() - started < 2
    assert not (tmp_path / "run/supervision.json").exists()
