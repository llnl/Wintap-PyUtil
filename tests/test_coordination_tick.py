"""Controlled-clock admission/draining slice; no unverified writer can reopen."""

import subprocess
import sys

import pytest

from .test_coordination_ledger import HOUR, NOW, initialize
from .test_coordination_protocol import FakeS3, binding, request

DUE = "2026-01-02T00:10:00Z"


def transport():
    from wintappy.coordination.protocol import MessageStore

    client = FakeS3()
    store = MessageStore("s3://synthetic-bucket/coordination", client)
    store.transport_scope = {
        "endpoint_url": "https://synthetic.invalid",
        "region": "synthetic-region",
        "addressing_style": "path",
    }
    return client, store


def tick(tmp_path, store, now=NOW, writer=None):
    from wintappy.coordination.coordinator import tick

    return tick(
        tmp_path / "state",
        binding(tmp_path),
        store,
        now,
        writer=writer,
        writer_command=["fake-compact"] if writer else None,
    )


@pytest.mark.parametrize(
    "now,expected",
    [
        ("2026-01-01T23:59:59Z", False),
        ("2026-01-02T00:00:00Z", False),
        ("2026-01-02T00:09:59Z", False),
        (DUE, True),
        ("2026-01-02T00:10:01Z", True),
        ("2026-01-04T10:00:00Z", True),
    ],
)
def test_midnight_grace_and_missed_ticks_use_the_persisted_hour(now, expected):
    from wintappy.coordination.coordinator import writer_due

    assert writer_due(HOUR, now) is expected


@pytest.mark.parametrize("grace", [True, 4, 1441, "10"])
def test_invalid_grace_is_rejected_before_ledger_access(tmp_path, grace):
    from wintappy.coordination.coordinator import tick
    from wintappy.coordination.protocol import CoordinationError

    _, store = transport()
    with pytest.raises(CoordinationError, match="grace"):
        tick(tmp_path / "state", binding(tmp_path), store, NOW, grace_minutes=grace)
    assert not (tmp_path / "state").exists()


def test_normal_tick_admits_overlapping_readers_and_terminal_first_cancellation(
    tmp_path,
):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import make_cancellation

    initialize(tmp_path)
    client, store = transport()
    for job in ["a" * 32, "b" * 32]:
        store.publish(request(tmp_path, job))
    result = tick(tmp_path, store)
    assert result["mode"] == "OPEN" and result["readers"] == 2
    assert not result["writer_invoked"]
    assert len(store.messages("grant")) == 2
    third = request(tmp_path, "c" * 32)
    store.publish(third)
    store.publish(make_cancellation(third, NOW))
    tick(tmp_path, store)
    assert len(store.messages("grant")) == 2
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.job(third["job_id"])["state"] == "CANCELLED"
        assert ledger.snapshot()["readers"] == 2
    assert len(client.puts) == 6  # Three requests, two grants, one cancellation.


def test_due_writer_waits_for_both_readers_and_queues_a_third(tmp_path):
    from wintappy.coordination.protocol import make_release

    initialize(tmp_path)
    _, store = transport()
    for job in ["a" * 32, "b" * 32]:
        store.publish(request(tmp_path, job))
    tick(tmp_path, store)
    grants = store.messages("grant")
    store.publish(request(tmp_path, "c" * 32))
    invoked = []
    writer = lambda run: invoked.append(run) or {"status": "completed", "exit_code": 0}
    result = tick(tmp_path, store, DUE, writer)
    assert result["mode"] == "DRAINING" and result["readers"] == 2
    assert result["waiting"] == 1 and not invoked
    store.publish(make_release(grants[0], DUE, 0))
    assert tick(tmp_path, store, DUE, writer)["readers"] == 1 and not invoked
    store.publish(make_release(grants[1], DUE, 1))
    result = tick(tmp_path, store, DUE, writer)
    assert len(invoked) == 1 and invoked[0]["hour_start"] == HOUR
    # No qualified completion adapter exists yet: even exit zero remains blocked.
    assert result["mode"] == "BLOCKED" and result["next_hour"] == HOUR
    assert result["waiting"] == 1 and len(store.messages("grant")) == 2
    assert not tick(tmp_path, store, "2026-01-04T10:00:00Z", writer)["writer_invoked"]
    assert len(invoked) == 1


def test_missing_writer_adapter_drains_without_launch_or_grant(tmp_path):
    initialize(tmp_path)
    _, store = transport()
    store.publish(request(tmp_path))
    result = tick(tmp_path, store, DUE)
    assert result["mode"] == "DRAINING" and result["readers"] == 0
    assert result["waiting"] == 1 and result["next_hour"] == HOUR
    assert not result["writer_invoked"] and not store.messages("grant")


def test_duplicate_tick_is_busy_during_committed_writer_callback(tmp_path):
    from wintappy.coordination.protocol import CoordinationError

    initialize(tmp_path)
    _, store = transport()

    def writer(run):
        assert run["run_id"] != run["cli_run_id"]
        with pytest.raises(CoordinationError, match="busy"):
            tick(tmp_path, store, DUE)
        return {"status": "empty", "exit_code": 0}

    assert tick(tmp_path, store, DUE, writer)["mode"] == "BLOCKED"


def test_callback_failure_retains_intent_and_cannot_launch_a_replacement(tmp_path):
    initialize(tmp_path)
    _, store = transport()
    invoked = []

    def writer(run):
        invoked.append(run)
        raise OSError("injected fake launch/child failure")

    result = tick(tmp_path, store, DUE, writer)
    assert result["mode"] == "BLOCKED" and result["writer_invoked"]
    assert result["next_hour"] == HOUR
    assert not tick(tmp_path, store, DUE, writer)["writer_invoked"]
    assert len(invoked) == 1


def test_surviving_fake_child_never_reopens_or_permits_a_new_writer(tmp_path):
    initialize(tmp_path)
    _, store = transport()
    children = []

    def writer(run):
        child = subprocess.Popen(
            [sys.executable, "-B", "-c", "import time; time.sleep(30)"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        children.append(child)
        return {"status": "completed", "exit_code": 0}

    try:
        result = tick(tmp_path, store, DUE, writer)
        assert children[0].poll() is None
        assert result["mode"] == "BLOCKED"
        assert not tick(tmp_path, store, DUE, writer)["writer_invoked"]
        assert len(children) == 1 and not store.messages("grant")
    finally:
        for child in children:
            child.terminate()
            child.wait(timeout=10)


@pytest.mark.parametrize("failure", ["list", "get"])
def test_api_errors_never_admit_or_turn_a_reader_reservation_into_empty_state(
    tmp_path, failure
):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError

    initialize(tmp_path)
    client, store = transport()
    store.publish(request(tmp_path))
    tick(tmp_path, store)
    store.publish(request(tmp_path, "b" * 32))
    client.failure = failure
    with pytest.raises(CoordinationError):
        tick(tmp_path, store, DUE)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.snapshot()["readers"] == 1
        assert ledger.snapshot()["next_hour"] == HOUR


def test_undelivered_grant_excludes_due_writer_until_explicit_cancellation(tmp_path):
    from wintappy.coordination.protocol import CoordinationError, make_cancellation

    initialize(tmp_path)
    client, store = transport()
    req = request(tmp_path)
    store.publish(req)
    client.failure = "ack"
    with pytest.raises(CoordinationError):
        tick(tmp_path, store)
    client.failure = ""
    result = tick(tmp_path, store, DUE)
    assert result["mode"] == "DRAINING" and result["readers"] == 1
    store.publish(make_cancellation(req, DUE))
    result = tick(tmp_path, store, DUE)
    assert result["readers"] == 0 and result["mode"] == "DRAINING"
    assert result["next_hour"] == HOUR


def test_ordinary_tick_refuses_missing_ledger(tmp_path):
    from wintappy.coordination.protocol import CoordinationError

    _, store = transport()
    with pytest.raises(CoordinationError, match="missing"):
        tick(tmp_path, store)
    assert not (tmp_path / "state/coordinator.duckdb").exists()


@pytest.mark.parametrize("conflict", ["terminal", "request", "list_failure"])
def test_later_tick_input_failure_preserves_all_prior_reservations(tmp_path, conflict):
    import copy

    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import (
        CoordinationError,
        make_cancellation,
        make_release,
        request_digest,
    )

    initialize(tmp_path)
    _, store = transport()
    req = request(tmp_path)
    store.publish(req)
    tick(tmp_path, store)
    grant = store.read("grant", req["job_id"])
    store.publish(make_release(grant, DUE, 0))
    if conflict == "terminal":
        store.publish(make_cancellation(req, DUE))
    elif conflict == "request":
        altered = copy.deepcopy(req)
        altered["action"] = "dbt-test"
        altered["request_sha256"] = request_digest(altered)
        # Simulate conflicting owned-key contents, not a permitted retry.
        from wintappy.coordination.protocol import encode_message

        store.client.objects[store.key("request", req["job_id"])] = encode_message(
            altered
        )
    else:
        original = store.messages

        def failed(kind):
            if kind == "request":
                raise CoordinationError("injected later listing failure")
            return original(kind)

        store.messages = failed
    invoked = []
    with pytest.raises(CoordinationError):
        tick(tmp_path, store, DUE, lambda run: invoked.append(run))
    assert not invoked
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.snapshot()["readers"] == 1
        assert ledger.job(req["job_id"])["terminal_json"] is None
        assert ledger.snapshot()["next_hour"] == HOUR


def test_unverified_writer_result_is_durably_recorded_as_observation_not_completion(
    tmp_path,
):
    import json

    from wintappy.coordination.ledger import Ledger

    initialize(tmp_path)
    _, store = transport()
    report = {"status": "completed", "exit_code": 0, "children_stopped": False}
    result = tick(tmp_path, store, DUE, lambda run: report)
    assert result["mode"] == "BLOCKED" and result["next_hour"] == HOUR
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        (details,) = ledger._connection.execute(
            "SELECT details_json FROM events WHERE kind='writer_unresolved'"
        ).fetchone()
        observation = json.loads(details)["observation"]
        assert observation["kind"] == "result_summary"
        assert observation["status"] == "completed" and observation["exit_code"] == 0
        assert observation["children_stopped"] is False


def test_invalid_writer_configuration_is_refused_before_access_or_admission(tmp_path):
    from wintappy.coordination.coordinator import tick as coordinator_tick
    from wintappy.coordination.protocol import CoordinationError

    initialize(tmp_path)
    _, store = transport()
    store.publish(request(tmp_path))
    accesses = []
    original = store.messages
    store.messages = lambda kind: accesses.append(kind) or original(kind)
    with pytest.raises(CoordinationError, match="command"):
        coordinator_tick(
            tmp_path / "state",
            binding(tmp_path),
            store,
            NOW,
            writer=lambda run: None,
            writer_command=[],
        )
    assert not accesses and not store.messages("grant")


@pytest.mark.parametrize("signal", [KeyboardInterrupt, SystemExit])
def test_callback_interruption_blocks_before_propagating(tmp_path, signal):
    from wintappy.coordination.ledger import Ledger

    initialize(tmp_path)
    _, store = transport()

    def interrupted(run):
        raise signal()

    with pytest.raises(signal):
        tick(tmp_path, store, DUE, interrupted)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.snapshot()["next_hour"] == HOUR


def test_writer_cannot_mutate_committed_identity_or_binding_by_changing_callback_input(
    tmp_path,
):
    import json

    from wintappy.coordination.ledger import Ledger

    initialize(tmp_path)
    _, store = transport()
    originals = []

    def mutate(run):
        originals.append(run["run_id"])
        run["run_id"] = "f" * 32
        run["hour_start"] = "2026-01-04T09:00:00Z"
        run["binding"]["dataset_id"] = "altered-dataset"

    result = tick(tmp_path, store, DUE, mutate)
    assert result["mode"] == "BLOCKED" and result["active_run_id"] == originals[0]
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        (payload,) = ledger._connection.execute(
            "SELECT intent_json FROM compaction_runs"
        ).fetchone()
        run = json.loads(payload)
        assert run["run_id"] == originals[0] and run["hour_start"] == HOUR
        assert run["binding"] == binding(tmp_path)


@pytest.mark.parametrize(
    "launch_child",
    [
        False,
        pytest.param(
            True,
            marks=pytest.mark.skipif(
                sys.platform != "linux", reason="Linux procfs surviving-child control"
            ),
        ),
    ],
)
def test_abrupt_tick_death_never_launches_replacement_even_when_child_survives(
    tmp_path, launch_child
):
    import os
    import signal
    from pathlib import Path

    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import canonical

    initialize(tmp_path)
    _, store = transport()
    pid_path = tmp_path / "synthetic-child.pid"
    code = """
import json,os,subprocess,sys
from pathlib import Path
from wintappy.coordination.coordinator import tick
from wintappy.coordination.protocol import MessageStore
from tests.test_coordination_protocol import FakeS3
def writer(run):
    if sys.argv[3]=='True':
        child=subprocess.Popen([sys.executable,'-B','-c','import time; time.sleep(120)'],
                               stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL,start_new_session=True)
        Path(sys.argv[4]).write_text(str(child.pid))
    os._exit(37)
tick(sys.argv[1],json.loads(sys.argv[2]),
     MessageStore('s3://synthetic-bucket/coordination',FakeS3()),sys.argv[5],
     writer=writer,writer_command=['fake-compact'])
raise AssertionError('writer callback did not run')
"""
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                code,
                str(tmp_path / "state"),
                canonical(binding(tmp_path)),
                str(launch_child),
                str(pid_path),
                DUE,
            ],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        assert result.returncode == 37, result.stdout + result.stderr
        if launch_child:
            pid = int(pid_path.read_text())
            os.kill(pid, 0)
            assert (
                Path(f"/proc/{pid}/stat").read_text().split(")", 1)[1].split()[0] != "Z"
            )
        with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
            assert ledger.snapshot()["mode"] == "BLOCKED"
            assert ledger.snapshot()["next_hour"] == HOUR
        invoked = []
        assert not tick(tmp_path, store, DUE, lambda run: invoked.append(run))[
            "writer_invoked"
        ]
        assert not invoked and not store.messages("grant")
    finally:
        if pid_path.exists():
            try:
                os.killpg(int(pid_path.read_text()), signal.SIGTERM)
            except ProcessLookupError:
                pass


@pytest.mark.parametrize(
    "report",
    [
        None,
        [],
        {"status": "empty", "exit_code": 0},
        {"status": "x" * 10000, "exit_code": True},
        {"status": "completed", "children_stopped": True},
    ],
)
def test_any_callback_report_is_bounded_diagnostics_and_never_permission(
    tmp_path, report
):
    from wintappy.coordination.ledger import Ledger

    initialize(tmp_path)
    _, store = transport()
    result = tick(tmp_path, store, DUE, lambda run: report)
    assert result["mode"] == "BLOCKED" and result["next_hour"] == HOUR
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        (details,) = ledger._connection.execute(
            "SELECT details_json FROM events WHERE kind='writer_unresolved'"
        ).fetchone()
        assert len(details.encode()) < 4096


def test_duplicate_tick_is_busy_before_transport_access(tmp_path):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError

    initialize(tmp_path)
    _, store = transport()

    def forbidden(kind):
        pytest.fail("Busy tick touched transport before acquiring the ledger lock")

    store.messages = forbidden
    with (
        Ledger.open(tmp_path / "state", binding(tmp_path)),
        pytest.raises(CoordinationError, match="busy"),
    ):
        tick(tmp_path, store)


def test_long_listing_crossing_grace_refreshes_clock_before_admission(tmp_path):
    from wintappy.coordination.coordinator import tick as coordinator_tick

    initialize(tmp_path)
    _, store = transport()
    store.publish(request(tmp_path))
    observed = [NOW]
    original = store.messages

    def delayed(kind):
        if kind == "request":
            observed[0] = DUE
        return original(kind)

    store.messages = delayed
    result = coordinator_tick(
        tmp_path / "state", binding(tmp_path), store, NOW, clock=lambda: observed[0]
    )
    assert result["mode"] == "DRAINING" and result["waiting"] == 1
    assert result["readers"] == 0 and not store.messages("grant")


@pytest.mark.parametrize("crosses_grace", [False, True])
@pytest.mark.parametrize("existing_reader", [False, True])
def test_ingestion_clock_is_refreshed_before_new_reader_admission(
    tmp_path, monkeypatch, crosses_grace, existing_reader
):
    from wintappy.coordination.coordinator import tick as coordinator_tick
    from wintappy.coordination.ledger import Ledger

    initialize(tmp_path)
    _, store = transport()
    if existing_reader:
        store.publish(request(tmp_path))
        tick(tmp_path, store)
    queued = request(tmp_path, "b" * 32)
    store.publish(queued)
    before = "2026-01-02T00:09:59Z"
    observed = [before]
    ingest = Ledger.ingest_many

    def delayed(self, messages, now=None):
        result = ingest(self, messages, now)
        if crosses_grace:
            observed[0] = DUE
        return result

    monkeypatch.setattr(Ledger, "ingest_many", delayed)
    result = coordinator_tick(
        tmp_path / "state",
        binding(tmp_path),
        store,
        before,
        clock=lambda: observed[0],
    )
    assert not result["writer_invoked"]
    assert result["mode"] == ("DRAINING" if crosses_grace else "OPEN")
    expected_readers = int(existing_reader) + int(not crosses_grace)
    assert result["readers"] == expected_readers
    assert result["waiting"] == int(crosses_grace)
    assert len(store.messages("grant")) == expected_readers
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.job(queued["job_id"])["state"] == (
            "WAITING" if crosses_grace else "GRANTED"
        )
        if existing_reader:
            assert ledger.job("a" * 32)["state"] == "GRANTED"


@pytest.mark.parametrize("fault", ["rollback", "unreadable"])
def test_clock_failure_after_ingestion_cannot_admit_or_revoke_readers(
    tmp_path, monkeypatch, fault
):
    from wintappy.coordination.coordinator import tick as coordinator_tick
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError

    initialize(tmp_path)
    _, store = transport()
    store.publish(request(tmp_path))
    tick(tmp_path, store)
    queued = request(tmp_path, "b" * 32)
    store.publish(queued)
    ingested = []
    ingest = Ledger.ingest_many

    def delayed(self, messages, now=None):
        result = ingest(self, messages, now)
        ingested.append(True)
        return result

    def clock():
        if not ingested:
            return "2026-01-02T00:09:59Z"
        if fault == "rollback":
            return NOW
        raise OSError("synthetic post-ingestion clock failure")

    monkeypatch.setattr(Ledger, "ingest_many", delayed)
    with pytest.raises(CoordinationError, match="clock"):
        coordinator_tick(tmp_path / "state", binding(tmp_path), store, NOW, clock=clock)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.snapshot()["readers"] == 1
        assert ledger.job(queued["job_id"])["state"] == "WAITING"
    assert len(store.messages("grant")) == 1


def test_clock_failure_or_rollback_before_ingestion_preserves_reservation(tmp_path):
    from wintappy.coordination.coordinator import tick as coordinator_tick
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError, make_release

    initialize(tmp_path)
    _, store = transport()
    store.publish(request(tmp_path))
    tick(tmp_path, store)
    store.publish(make_release(store.messages("grant")[0], DUE, 0))
    with pytest.raises(CoordinationError, match="clock"):
        coordinator_tick(
            tmp_path / "state", binding(tmp_path), store, DUE, clock=lambda: NOW
        )
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.snapshot()["readers"] == 1 and ledger.snapshot()["mode"] == "OPEN"


def test_clock_refresh_after_grant_publication_enters_draining_without_revoking_reader(
    tmp_path,
):
    from wintappy.coordination.coordinator import tick as coordinator_tick

    initialize(tmp_path)
    _, store = transport()
    store.publish(request(tmp_path))
    observed = [NOW]
    original = store.publish

    def delayed(message):
        original(message)
        observed[0] = DUE

    store.publish = delayed
    result = coordinator_tick(
        tmp_path / "state", binding(tmp_path), store, NOW, clock=lambda: observed[0]
    )
    assert result["mode"] == "DRAINING" and result["readers"] == 1
    assert len(store.messages("grant")) == 1 and not result["writer_invoked"]


def test_writer_disposition_records_completion_observation_time_not_tick_start(
    tmp_path,
):
    from wintappy.coordination.coordinator import tick as coordinator_tick
    from wintappy.coordination.ledger import Ledger

    initialize(tmp_path)
    _, store = transport()
    observed = [DUE]
    end = "2026-01-02T00:15:00Z"

    def writer(run):
        observed[0] = end
        return {"status": "empty", "exit_code": 0}

    result = coordinator_tick(
        tmp_path / "state",
        binding(tmp_path),
        store,
        DUE,
        clock=lambda: observed[0],
        writer=writer,
        writer_command=["fake-compact"],
    )
    assert result["mode"] == "BLOCKED" and result["next_hour"] == HOUR
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger._connection.execute(
            "SELECT observed_at FROM events WHERE kind='writer_unresolved'"
        ).fetchone() == (end,)


@pytest.mark.parametrize("fault", ["rollback", "unreadable"])
def test_post_writer_clock_failure_still_blocks_committed_intent(tmp_path, fault):
    from wintappy.coordination.coordinator import tick as coordinator_tick
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError

    initialize(tmp_path)
    _, store = transport()
    finished = []

    def clock():
        if not finished:
            return DUE
        if fault == "rollback":
            return NOW
        raise OSError("injected clock failure")

    with pytest.raises(CoordinationError, match="clock"):
        coordinator_tick(
            tmp_path / "state",
            binding(tmp_path),
            store,
            DUE,
            clock=clock,
            writer=lambda run: finished.append(run),
            writer_command=["fake-compact"],
        )
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert (
            ledger.snapshot()["mode"] == "BLOCKED"
            and ledger.snapshot()["next_hour"] == HOUR
        )


@pytest.mark.skipif(sys.platform != "linux", reason="Linux subreaper contract")
def test_real_supervised_callback_holds_ledger_lock_and_still_cannot_certify_empty_hour(
    tmp_path,
):
    import json

    from wintappy.coordination.coordinator import tick as coordinator_tick
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import canonical
    from wintappy.coordination.supervisor import read_report, run_supervised

    initialize(tmp_path)
    _, store = transport()
    code = """
import json,sys
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError
try:
    with Ledger.open(sys.argv[1],json.loads(sys.argv[2])):
        raise AssertionError('supervised callback lost its ledger lock')
except CoordinationError as error:
    assert 'busy' in str(error)
print(json.dumps({'status':'empty','exit_code':0}))
"""
    argv = [
        sys.executable,
        "-B",
        "-c",
        code,
        str(tmp_path / "state"),
        canonical(binding(tmp_path)),
    ]
    reports = []

    def writer(run):
        result = run_supervised(
            argv,
            tmp_path / "observations",
            timeout_seconds=5,
            stop_grace_seconds=0.1,
            shutdown_seconds=2,
        )
        reports.append(read_report(tmp_path / "observations"))
        return result

    result = coordinator_tick(
        tmp_path / "state",
        binding(tmp_path),
        store,
        DUE,
        writer=writer,
        writer_command=argv,
    )
    assert result["mode"] == "BLOCKED" and result["next_hour"] == HOUR
    assert reports == [{"status": "empty", "exit_code": 0}]
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        intent = json.loads(
            ledger._connection.execute(
                "SELECT intent_json FROM compaction_runs"
            ).fetchone()[0]
        )
        assert intent["command"] == argv
    assert not tick(tmp_path, store, DUE)["writer_invoked"]


@pytest.mark.skipif(sys.platform != "linux", reason="Linux subreaper contract")
def test_supervisor_death_with_surviving_workload_remains_blocked(tmp_path):
    import os
    import signal
    from pathlib import Path

    from wintappy.coordination.coordinator import tick as coordinator_tick
    from wintappy.coordination.supervisor import run_supervised

    initialize(tmp_path)
    _, store = transport()
    pidfile = tmp_path / "synthetic.pid"
    code = f"""
import os,signal,time
from pathlib import Path
Path({str(pidfile)!r}).write_text(str(os.getpid()))
os.kill(os.getppid(),signal.SIGKILL)
time.sleep(60)
"""
    argv = [sys.executable, "-B", "-c", code]
    try:
        result = coordinator_tick(
            tmp_path / "state",
            binding(tmp_path),
            store,
            DUE,
            writer=lambda run: run_supervised(
                argv, tmp_path / "observations", timeout_seconds=5, shutdown_seconds=3
            ),
            writer_command=argv,
        )
        assert result["mode"] == "BLOCKED" and result["next_hour"] == HOUR
        pid = int(pidfile.read_text())
        assert Path(f"/proc/{pid}/stat").read_text().split(")", 1)[1].split()[0] != "Z"
        assert not (tmp_path / "observations/supervision.json").exists()
        assert not tick(tmp_path, store, DUE)["writer_invoked"]
    finally:
        if pidfile.exists():
            try:
                os.kill(int(pidfile.read_text()), signal.SIGKILL)
            except ProcessLookupError:
                pass


@pytest.mark.parametrize("platform", ["darwin", "win32"])
def test_unsupported_writer_supervision_refuses_launch_and_tick_stays_blocked(
    tmp_path, monkeypatch, platform
):
    from wintappy.coordination import supervisor
    from wintappy.coordination.protocol import CoordinationError

    native_platform = supervisor.sys.platform
    initialize(tmp_path)
    _, store = transport()
    store.publish(request(tmp_path))
    monkeypatch.setattr(supervisor.sys, "platform", platform)
    monkeypatch.setattr(
        supervisor.subprocess,
        "Popen",
        lambda *a, **kw: pytest.fail("unsupported launch"),
    )
    command = [sys.executable, "-B", "-c", "print('synthetic')"]
    directory = tmp_path / "observations"
    with pytest.raises(CoordinationError, match="requires Linux"):
        supervisor.run_supervised(command, directory)
    assert not directory.exists()

    monkeypatch.setattr(supervisor.sys, "platform", native_platform)

    def unsupported_writer(run):
        monkeypatch.setattr(supervisor.sys, "platform", platform)
        try:
            return supervisor.run_supervised(command, directory)
        finally:
            monkeypatch.setattr(supervisor.sys, "platform", native_platform)

    result = tick(
        tmp_path, store, DUE, unsupported_writer
    )
    assert result["mode"] == "BLOCKED" and result["next_hour"] == HOUR
    assert result["waiting"] == 1 and not store.messages("grant")
    assert not directory.exists()


def test_macos_supervision_dispatch_is_explicitly_reader_scoped(tmp_path, monkeypatch):
    from wintappy.coordination import supervisor
    from wintappy.coordination.protocol import CoordinationError

    monkeypatch.setattr(supervisor.sys, "platform", "darwin")
    calls = []

    def observe(config, payload, root, manifest):
        calls.append((config, payload, root, manifest))
        return {"shutdown_scope": "process_group"}

    monkeypatch.setattr(supervisor, "_run_macos_supervised", observe)
    reader_root = tmp_path / "reader"
    assert supervisor.run_reader_supervised([sys.executable, "-B", "-c", "pass"], reader_root) == {
        "shutdown_scope": "process_group"
    }
    assert len(calls) == 1
    assert calls[0][2] == reader_root.resolve()

    writer_root = tmp_path / "writer"
    with pytest.raises(CoordinationError, match="requires Linux"):
        supervisor.run_supervised([sys.executable, "-B", "-c", "pass"], writer_root)
    assert not writer_root.exists()
