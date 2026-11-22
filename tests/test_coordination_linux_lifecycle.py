"""Real Linux clients and writer, using isolated filesystem-backed test transport.

This fake transport is not an actual S3 endpoint qualification.
"""

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from wintappy.coordination.protocol import (
    CoordinationError,
    canonical,
    decode_message,
    make_request,
)

from .test_coordination_completion import NOW, prepared_empty
from .test_coordination_execution import binding
from .test_coordination_ledger import HOUR
from .test_coordination_service import settings

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="Real Linux reader/writer lifecycle"
)


class FileMessages:
    root = "s3://synthetic-bucket/coordination"

    def __init__(self, directory):
        self.transport_scope = {
            "endpoint_url": "https://synthetic.invalid",
            "region": "synthetic-region",
            "addressing_style": "path",
        }
        self.directory = Path(directory)
        self.directory.mkdir(exist_ok=True)

    def publish(self, message):
        path = self.directory / (message["kind"] + "-" + message["job_id"] + ".json")
        encoded = canonical(message).encode()
        if path.exists():
            if path.read_bytes() != encoded:
                raise CoordinationError("Synthetic immutable message conflict")
            return
        temporary = path.with_suffix(".pending")
        temporary.write_bytes(encoded)
        os.replace(temporary, path)

    def read(self, kind, job_id):
        path = self.directory / (kind + "-" + job_id + ".json")
        return decode_message(path.read_bytes()) if path.exists() else None

    def messages(self, kind):
        return [
            decode_message(path.read_bytes())
            for path in sorted(self.directory.glob(kind + "-*.json"))
        ]


WORKER = """
import json,pathlib,sys,time
from tests.test_coordination_linux_lifecycle import FileMessages
from wintappy.coordination.client import run_reader
root=pathlib.Path(sys.argv[1]); job=sys.argv[2]
request=json.loads((root/(job+'.request')).read_text())
command=[sys.executable,'-c', '''import pathlib,sys,time
root=pathlib.Path(sys.argv[1]); job=sys.argv[2]
(root/(job+'.started')).write_text(str(time.monotonic()))
while not (root/(job+'.stop')).exists(): time.sleep(.02)
(root/(job+'.ended')).write_text(str(time.monotonic()))
''',str(root),job]
result=run_reader(root/'clients',request,FileMessages(root/'messages'),command,
                 poll_seconds=.05+ .05,timeout_seconds=10,stop_grace_seconds=.1,
                 shutdown_seconds=2,clock=lambda:'2026-01-02T00:10:00Z')
(root/(job+'.result')).write_text(json.dumps(result))
raise SystemExit(result['exit_code'])
"""


def start_reader(tmp_path, identity):
    request = make_request(
        binding(tmp_path), identity * 32, "shared-command", HOUR, NOW, NOW
    )
    (tmp_path / (identity + ".request")).write_text(canonical(request))
    return subprocess.Popen(
        [sys.executable, "-B", "-c", WORKER, str(tmp_path), identity],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def until(predicate, seconds=10, *, processes=()):
    deadline = time.monotonic() + seconds
    while not predicate():
        if any(process.poll() is not None for process in processes):
            raise AssertionError(
                "Synthetic lifecycle child exited before its expected message"
            )
        if time.monotonic() >= deadline:
            raise AssertionError("Bounded synthetic lifecycle wait timed out")
        time.sleep(0.02)


def cleanup(processes):
    diagnostics = []
    for index, process in enumerate(processes):
        before_cleanup = process.poll()
        if before_cleanup is None:
            process.terminate()
        try:
            stdout, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate(timeout=5)
        diagnostics.append(
            {
                "child": index,
                "exit_before_cleanup": before_cleanup,
                "exit": process.returncode,
                "stdout": stdout[:65536].decode(errors="replace"),
                "stderr": stderr[:65536].decode(errors="replace"),
            }
        )
    error = sys.exception()
    if error is not None:
        error.add_note(
            "Synthetic reader subprocess diagnostics: " + canonical(diagnostics)
        )
    return diagnostics


def test_real_overlapping_readers_due_writer_and_queued_reader(tmp_path, monkeypatch):
    from wintappy.coordination import service

    prepared_empty(tmp_path)
    store = FileMessages(tmp_path / "messages")
    processes = [start_reader(tmp_path, identity) for identity in ("a", "b")]
    writer_intervals = []
    original = service.execute_and_complete_prepared

    def traced(*args, **kwargs):
        start = time.monotonic()
        value = original(*args, **kwargs)
        writer_intervals.append((start, time.monotonic()))
        return value

    monkeypatch.setattr(service, "execute_and_complete_prepared", traced)
    try:
        until(lambda: len(store.messages("request")) == 2, processes=processes)
        state = service.service_tick(
            settings(tmp_path), store, clock=lambda: "2026-01-02T00:09:59Z"
        )
        assert state["readers"] == 2
        until(
            lambda: all(
                (tmp_path / (identity + ".started")).exists() for identity in ("a", "b")
            )
        )
        processes.append(start_reader(tmp_path, "c"))
        until(lambda: len(store.messages("request")) == 3, processes=processes)
        state = service.service_tick(settings(tmp_path), store, clock=lambda: NOW)
        assert state["waiting"] == 1 and not state["writer_invoked"]
        assert not (tmp_path / "c.started").exists()
        (tmp_path / "a.stop").touch()
        until(lambda: store.read("release", "a" * 32) is not None)
        assert not service.service_tick(settings(tmp_path), store, clock=lambda: NOW)[
            "writer_invoked"
        ]
        (tmp_path / "b.stop").touch()
        until(lambda: store.read("release", "b" * 32) is not None)
        state = service.service_tick(settings(tmp_path), store, clock=lambda: NOW)
        assert state["writer_invoked"] and state["readers"] == 1
        until(lambda: (tmp_path / "c.started").exists())
        writer_start, writer_end = writer_intervals[0]
        assert (
            max(
                float((tmp_path / (identity + ".ended")).read_text())
                for identity in ("a", "b")
            )
            <= writer_start
        )
        assert float((tmp_path / "c.started").read_text()) >= writer_end
        (tmp_path / "c.stop").touch()
        until(lambda: store.read("release", "c" * 32) is not None)
        assert (
            service.service_tick(settings(tmp_path), store, clock=lambda: NOW)[
                "readers"
            ]
            == 0
        )
        for process in processes:
            stdout, stderr = process.communicate(timeout=5)
            assert process.returncode == 0, (stdout, stderr)
    finally:
        cleanup(processes)


@pytest.mark.parametrize("sig,code", [(signal.SIGINT, 130), (signal.SIGTERM, 143)])
def test_real_interruption_releases_only_after_proven_tree_shutdown(
    tmp_path, sig, code
):
    from wintappy.coordination import service

    prepared_empty(tmp_path)
    store = FileMessages(tmp_path / "messages")
    process = start_reader(tmp_path, "a")
    try:
        until(lambda: len(store.messages("request")) == 1)
        service.service_tick(
            settings(tmp_path), store, clock=lambda: "2026-01-02T00:09:59Z"
        )
        until(lambda: (tmp_path / "a.started").exists())
        process.send_signal(sig)
        stdout, stderr = process.communicate(timeout=10)
        release = store.read("release", "a" * 32)
        assert (
            release
            and release["exit_code"] == code
            and release["children_stopped"] is True
        )
        assert process.returncode == code, (stdout, stderr)
    finally:
        cleanup([process])


def test_failed_lifecycle_retains_child_exit_and_output():
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import sys; print('synthetic stdout'); print('synthetic stderr',file=sys.stderr); sys.exit(7)",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    child.wait(timeout=5)
    try:
        raise AssertionError("synthetic startup failure")
    except AssertionError as error:
        diagnostics = cleanup([child])
        assert diagnostics[0]["exit_before_cleanup"] == 7
        assert "synthetic stdout" in diagnostics[0]["stdout"]
        assert "synthetic stderr" in diagnostics[0]["stderr"]
        assert "synthetic stdout" in error.__notes__[0]


def test_abrupt_wrapper_death_keeps_reservation_and_no_restart_launch(tmp_path):
    from wintappy.coordination import service
    from wintappy.coordination.ledger import Ledger

    prepared_empty(tmp_path)
    store = FileMessages(tmp_path / "messages")
    process = start_reader(tmp_path, "a")
    try:
        until(lambda: len(store.messages("request")) == 1)
        service.service_tick(
            settings(tmp_path), store, clock=lambda: "2026-01-02T00:09:59Z"
        )
        until(lambda: (tmp_path / "a.started").exists())
        process.kill()
        process.communicate(timeout=5)
        state = service.service_tick(settings(tmp_path), store, clock=lambda: NOW)
        assert state["readers"] == 1 and not state["writer_invoked"]
        assert store.read("release", "a" * 32) is None
        restarted = start_reader(tmp_path, "a")
        _stdout, stderr = restarted.communicate(timeout=5)
        assert restarted.returncode != 0 and b"recovery" in stderr
        with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
            assert ledger.snapshot()["readers"] == 1
    finally:
        cleanup([process])
