"""Real DuckDB reservations survive failures before any external effect."""

import copy

import pytest

from .test_coordination_protocol import FakeS3, binding, request

NOW = "2026-01-02T00:01:00Z"
HOUR = "2026-01-01T23:00:00Z"


def initialize(tmp_path):
    from wintappy.coordination.ledger import Ledger

    Ledger.initialize(tmp_path / "state", binding(tmp_path), HOUR)


def test_missing_ledger_refuses_to_create_empty_authority(tmp_path):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError

    with (
        pytest.raises(CoordinationError, match="missing"),
        Ledger.open(tmp_path / "state", binding(tmp_path)),
    ):
        pass
    assert not (tmp_path / "state/coordinator.duckdb").exists()


def test_init_is_explicit_binding_is_immutable_and_every_open_takes_lock(tmp_path):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError

    initialize(tmp_path)
    with pytest.raises(CoordinationError, match="exists"):
        initialize(tmp_path)
    changed = {**binding(tmp_path), "dataset_id": "different-dataset"}
    with (
        pytest.raises(CoordinationError, match="binding"),
        Ledger.open(tmp_path / "state", changed),
    ):
        pass
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        with (
            pytest.raises(CoordinationError, match="busy"),
            Ledger.open(tmp_path / "state", binding(tmp_path)),
        ):
            pass
        assert ledger.snapshot()["mode"] == "OPEN"


@pytest.mark.parametrize("damage", ["missing_reason", "different_reason"])
def test_corrupt_blocked_diagnostic_refuses_open_without_changing_authority(
    tmp_path, damage
):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError

    initialize(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        run = ledger.begin_writer(HOUR, NOW, ["fake-compact"])
        ledger.block_writer(run["run_id"], NOW, "synthetic unresolved outcome")
        if damage == "missing_reason":
            ledger._connection.execute("UPDATE dataset_state SET block_reason=''")
            ledger._connection.execute("UPDATE compaction_runs SET block_reason=''")
        else:
            ledger._connection.execute(
                "UPDATE compaction_runs SET block_reason='different diagnosis'"
            )
    path = tmp_path / "state/coordinator.duckdb"
    before = path.read_bytes()
    with (
        pytest.raises(CoordinationError),
        Ledger.open(tmp_path / "state", binding(tmp_path)),
    ):
        pass
    assert path.read_bytes() == before


@pytest.mark.parametrize("phase", ["before_commit", "after_commit"])
def test_atomic_tick_ingestion_crash_commits_every_terminal_or_none(tmp_path, phase):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import make_cancellation, make_release

    initialize(tmp_path)
    req = request(tmp_path)
    cancelled = request(tmp_path, "b" * 32)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.ingest(req)
        grant = ledger.admit(NOW)[0]
        messages = [
            make_release(grant, NOW, 0),
            make_cancellation(cancelled, NOW),
            cancelled,
        ]

        def crash(observed):
            if observed == phase:
                raise RuntimeError("injected batch commit failure")

        ledger.failure_hook = crash
        with pytest.raises(RuntimeError):
            ledger.ingest_many(messages, NOW)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.snapshot()["readers"] == int(phase == "before_commit")
        assert bool(ledger.job(cancelled["job_id"])) == (phase == "after_commit")
        ledger.ingest_many(messages, NOW)
        assert ledger.snapshot()["readers"] == 0 and ledger.admit(NOW) == []


@pytest.mark.parametrize("budget", ["count", "bytes"])
def test_atomic_tick_ingestion_budget_failure_rolls_back_entire_batch(
    tmp_path, monkeypatch, budget
):
    from wintappy.coordination import ledger as module
    from wintappy.coordination.protocol import CoordinationError

    initialize(tmp_path)
    with module.Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        if budget == "count":
            monkeypatch.setattr(module, "MAX_MESSAGES", 1)
        else:
            monkeypatch.setattr(module, "MAX_LIST_BYTES", 1)
        with pytest.raises(CoordinationError, match="budget"):
            ledger.ingest_many([request(tmp_path)] * 4, NOW)
        assert ledger.snapshot()["waiting"] == 0
        assert ledger.job("a" * 32) is None


def test_atomic_tick_ingestion_iterator_failure_rolls_back_earlier_request(tmp_path):
    from wintappy.coordination.ledger import Ledger

    initialize(tmp_path)

    def incomplete():
        yield request(tmp_path)
        raise OSError("injected incomplete tick input")

    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        with pytest.raises(OSError):
            ledger.ingest_many(incomplete(), NOW)
        assert ledger.snapshot()["waiting"] == 0


@pytest.mark.parametrize("phase", ["before_commit", "after_commit"])
def test_blocking_intent_transaction_failure_never_advances_hour(tmp_path, phase):
    from wintappy.coordination.ledger import Ledger

    initialize(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        run = ledger.begin_writer(HOUR, NOW, ["fake-compact"])

        def crash(observed):
            if observed == phase:
                raise RuntimeError("injected blocked disposition commit failure")

        ledger.failure_hook = crash
        with pytest.raises(RuntimeError):
            ledger.block_writer(run["run_id"], NOW, "synthetic unresolved outcome")
        assert ledger.snapshot()["mode"] == (
            "WRITING" if phase == "before_commit" else "BLOCKED"
        )
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.snapshot()["next_hour"] == HOUR


def test_wrong_writer_id_or_oversized_observation_cannot_change_intent(tmp_path):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError

    initialize(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        run = ledger.begin_writer(HOUR, NOW, ["fake-compact"])
        with pytest.raises(CoordinationError, match="active"):
            ledger.block_writer("f" * 32, NOW, "wrong writer")
        with pytest.raises(CoordinationError, match="observation"):
            ledger.block_writer(
                run["run_id"],
                NOW,
                "oversized result",
                observation={"untrusted": "x" * 4096},
            )
        assert ledger.snapshot()["mode"] == "WRITING"
        assert ledger.snapshot()["next_hour"] == HOUR


@pytest.mark.parametrize("phase", ["before_commit", "after_commit"])
def test_grant_transaction_failure_never_loses_committed_reservation(tmp_path, phase):
    from wintappy.coordination.ledger import Ledger

    initialize(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.ingest(request(tmp_path))

        def crash(observed):
            if observed == phase:
                raise RuntimeError("injected transaction crash")

        ledger.failure_hook = crash
        with pytest.raises(RuntimeError):
            ledger.admit(NOW)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        expected = int(phase == "after_commit")
        assert ledger.snapshot()["readers"] == expected
        assert len(ledger.pending_grants()) == expected
        ledger.admit(NOW)
        assert ledger.snapshot()["readers"] == 1
        assert len(ledger.pending_grants()) == 1


def test_lost_grant_ack_and_reordered_requests_preserve_one_reservation(tmp_path):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError, MessageStore

    initialize(tmp_path)
    client = FakeS3()
    store = MessageStore("s3://synthetic-bucket/coordination", client)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        req = request(tmp_path)
        ledger.ingest(req)
        ledger.admit(NOW)
        original = ledger.pending_grants()
        client.on_put = lambda: (
            ledger.snapshot()["readers"] == 1 or pytest.fail("PUT before reservation")
        )
        client.failure = "ack"
        with pytest.raises(CoordinationError):
            ledger.flush_grants(store)
        assert ledger.snapshot()["readers"] == 1
        assert ledger.pending_grants() == original
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.ingest(req)
        ledger.admit(NOW)
        assert ledger.pending_grants() == original
        client.failure = ""
        ledger.flush_grants(store)
        assert ledger.snapshot()["readers"] == 1 and not ledger.pending_grants()


@pytest.mark.parametrize("before_request", [True, False])
def test_cancellation_tombstone_suppresses_grants_and_late_requests(
    tmp_path, before_request
):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import make_cancellation

    initialize(tmp_path)
    req = request(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        if not before_request:
            ledger.ingest(req)
            ledger.admit(NOW)
        ledger.ingest(make_cancellation(req, NOW))
        ledger.ingest(req)
        ledger.admit(NOW)
        assert ledger.snapshot()["readers"] == 0 and not ledger.pending_grants()
        assert ledger.job(req["job_id"])["state"] == "CANCELLED"


def test_release_requires_recorded_grant_but_valid_early_terminal_is_retained(tmp_path):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import (
        CoordinationError,
        make_grant,
        make_release,
    )

    initialize(tmp_path)
    req = request(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        fake = make_grant(req, "b" * 32, NOW)
        with pytest.raises(CoordinationError):
            ledger.ingest(make_release(fake, NOW, 0))
        ledger.ingest(req)
        ledger.admit(NOW)
        grant = ledger.pending_grants()[0]
        wrong = {**make_release(grant, NOW, 1), "grant_id": "c" * 32}
        with pytest.raises(CoordinationError):
            ledger.ingest(wrong)
        assert ledger.snapshot()["readers"] == 1
        release = make_release(grant, NOW, 1)
        ledger.ingest(release)
        ledger.ingest(release)
        ledger.ingest(req)
        ledger.admit(NOW)
        assert ledger.snapshot()["readers"] == 0
        assert ledger.job(req["job_id"])["state"] == "RELEASED"


def test_conflicting_duplicate_never_retires_reservation(tmp_path):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import (
        CoordinationError,
        make_cancellation,
        request_digest,
    )

    initialize(tmp_path)
    req = request(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.ingest(req)
        ledger.admit(NOW)
        changed = copy.deepcopy(req)
        changed["action"] = "dbt-test"
        changed["request_sha256"] = request_digest(changed)
        for value in [changed, make_cancellation(changed, NOW)]:
            with pytest.raises(CoordinationError, match="conflict"):
                ledger.ingest(value)
        assert ledger.snapshot()["readers"] == 1


def test_due_writer_drains_all_reservations_and_intent_restart_is_blocked(tmp_path):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError, make_release

    initialize(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        for job in ["a" * 32, "b" * 32]:
            ledger.ingest(request(tmp_path, job))
        ledger.admit(NOW)
        grants = ledger.pending_grants()
        ledger.drain(NOW)
        ledger.ingest(request(tmp_path, "c" * 32))
        assert ledger.admit(NOW) == []
        for grant in grants:
            with pytest.raises(CoordinationError, match="reader"):
                ledger.begin_writer(HOUR, NOW, ["fake-compact"])
            ledger.ingest(make_release(grant, NOW, 0))
        run = ledger.begin_writer(HOUR, NOW, ["fake-compact"])
        assert run["run_id"] != run["cli_run_id"]
        assert ledger.snapshot()["mode"] == "WRITING"
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.snapshot()["next_hour"] == HOUR
        assert ledger.admit(NOW) == []
        with pytest.raises(CoordinationError):
            ledger.begin_writer(HOUR, NOW, ["fake-compact"])


@pytest.mark.parametrize("phase", ["before_publish", "after_publish"])
def test_publication_crash_retains_the_exact_outbox_and_reader(tmp_path, phase):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import MessageStore

    initialize(tmp_path)
    client = FakeS3()
    store = MessageStore("s3://synthetic-bucket/coordination", client)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.ingest(request(tmp_path))
        ledger.admit(NOW)
        original = ledger.pending_grants()

        def crash(observed):
            if observed == phase:
                raise RuntimeError("injected publication crash")

        ledger.failure_hook = crash
        with pytest.raises(RuntimeError):
            ledger.flush_grants(store)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.pending_grants() == original
        assert ledger.snapshot()["readers"] == 1
        ledger.flush_grants(store)
        assert len(client.puts) == 1
        assert not ledger.pending_grants() and ledger.snapshot()["readers"] == 1


@pytest.mark.parametrize("phase", ["before_commit", "after_commit"])
def test_writer_intent_crash_never_launches_or_reopens_committed_intent(
    tmp_path, phase
):
    from wintappy.coordination.ledger import Ledger

    initialize(tmp_path)
    launched = []
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)

        def crash(observed):
            if observed == phase:
                raise RuntimeError("injected writer intent crash")

        ledger.failure_hook = crash
        with pytest.raises(RuntimeError):
            run = ledger.begin_writer(HOUR, NOW, ["fake-compact"])
            launched.append(run)
    assert not launched
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.snapshot()["next_hour"] == HOUR
        assert ledger.snapshot()["mode"] == (
            "DRAINING" if phase == "before_commit" else "BLOCKED"
        )
        assert ledger.admit(NOW) == []


@pytest.mark.parametrize("kind", ["release", "cancellation"])
@pytest.mark.parametrize("phase", ["before_commit", "after_commit"])
def test_terminal_transaction_crash_preserves_or_retires_one_reservation(
    tmp_path, phase, kind
):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import make_cancellation, make_release

    initialize(tmp_path)
    req = request(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.ingest(req)
        ledger.admit(NOW)
        terminal = (
            make_release(ledger.pending_grants()[0], NOW, 0)
            if kind == "release"
            else make_cancellation(req, NOW)
        )

        def crash(observed):
            if observed == phase:
                raise RuntimeError("injected terminal commit crash")

        ledger.failure_hook = crash
        with pytest.raises(RuntimeError):
            ledger.ingest(terminal)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.snapshot()["readers"] == int(phase == "before_commit")
        ledger.ingest(terminal)
        ledger.ingest(req)
        ledger.admit(NOW)
        assert ledger.snapshot()["readers"] == 0


def test_real_other_process_is_busy_before_duckdb_open(tmp_path):
    import os
    import subprocess
    import sys

    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import canonical

    initialize(tmp_path)
    code = """
import json,sys
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError
try:
    with Ledger.open(sys.argv[1],json.loads(sys.argv[2])):
        raise AssertionError('second process opened the ledger')
except CoordinationError as error:
    assert 'busy' in str(error), error
"""
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                code,
                str(tmp_path / "state"),
                canonical(binding(tmp_path)),
            ],
            env=os.environ.copy(),
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert ledger.snapshot()["readers"] == 0


@pytest.mark.parametrize(
    "damage",
    ["orphan_writer", "reader_during_writer", "request_digest", "extra_column"],
)
def test_corrupt_ledger_refuses_open_without_recreating_or_changing_it(
    tmp_path, damage
):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError

    initialize(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        if damage in {"orphan_writer", "reader_during_writer"}:
            ledger.drain(NOW)
            ledger.begin_writer(HOUR, NOW, ["fake-compact"])
            ledger._connection.execute(
                "UPDATE dataset_state SET mode='OPEN',active_run_id=NULL"
            )
            if damage == "reader_during_writer":
                ledger.ingest(request(tmp_path))
                ledger.admit(NOW)
                ledger._connection.execute(
                    "UPDATE dataset_state SET mode='WRITING',"
                    "active_run_id=(SELECT run_id FROM compaction_runs)"
                )
        elif damage == "request_digest":
            ledger.ingest(request(tmp_path))
            ledger._connection.execute("UPDATE jobs SET request_sha256=?", ["f" * 64])
        else:
            ledger._connection.execute(
                "ALTER TABLE metadata ADD COLUMN unknown VARCHAR"
            )
    path = tmp_path / "state/coordinator.duckdb"
    before = path.read_bytes()
    with (
        pytest.raises(CoordinationError),
        Ledger.open(tmp_path / "state", binding(tmp_path)),
    ):
        pass
    assert path.read_bytes() == before


def test_s3_state_directory_is_refused_before_creating_local_paths(
    tmp_path, monkeypatch
):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError

    monkeypatch.chdir(tmp_path)
    with pytest.raises(CoordinationError, match="local"):
        Ledger.initialize("s3://synthetic-bucket/ledger", binding(tmp_path), HOUR)
    assert not (tmp_path / "s3:").exists()


def test_transport_root_cannot_publish_under_any_data_scan_root(tmp_path):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError, MessageStore

    initialize(tmp_path)
    client = FakeS3()
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.ingest(request(tmp_path))
        ledger.admit(NOW)
        with pytest.raises(CoordinationError, match="overlap"):
            ledger.flush_grants(
                MessageStore(
                    binding(tmp_path)["roots"]["raw"] + "/coordination", client
                )
            )
        assert ledger.snapshot()["readers"] == 1 and not client.puts


@pytest.mark.parametrize("operation", ["admit", "writer"])
@pytest.mark.parametrize("phase", ["before_commit", "after_commit"])
def test_abrupt_process_death_recovers_real_duckdb_wal(tmp_path, operation, phase):
    import subprocess
    import sys

    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import canonical

    initialize(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        if operation == "admit":
            ledger.ingest(request(tmp_path))
        else:
            ledger.drain(NOW)
    code = """
import json,os,sys
from wintappy.coordination.ledger import Ledger
with Ledger.open(sys.argv[1],json.loads(sys.argv[2])) as ledger:
    def crash(observed):
        if observed==sys.argv[4]:
            os._exit(37)
    ledger.failure_hook=crash
    if sys.argv[3]=='admit':
        ledger.admit(sys.argv[5])
    else:
        ledger.begin_writer(sys.argv[6],sys.argv[5],['fake-compact'])
raise AssertionError('failure point was not reached')
"""
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            code,
            str(tmp_path / "state"),
            canonical(binding(tmp_path)),
            operation,
            phase,
            NOW,
            HOUR,
        ],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 37, result.stdout + result.stderr
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        state = ledger.snapshot()
        assert state["next_hour"] == HOUR
        if operation == "admit":
            assert state["readers"] == int(phase == "after_commit")
            assert state["mode"] == "OPEN"
            ledger.admit(NOW)
            assert (
                ledger.snapshot()["readers"] == 1 and len(ledger.pending_grants()) == 1
            )
        else:
            assert state["mode"] == (
                "BLOCKED" if phase == "after_commit" else "DRAINING"
            )
            assert ledger.admit(NOW) == []


def test_old_reader_never_expires_and_conflicting_remote_grant_preserves_it(tmp_path):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError, MessageStore

    initialize(tmp_path)
    client = FakeS3()
    store = MessageStore("s3://synthetic-bucket/coordination", client)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.ingest(request(tmp_path))
        ledger.admit(NOW)
        client.objects[store.key("grant", "a" * 32)] = b"conflicting-existing-grant"
        with pytest.raises(CoordinationError, match="conflict"):
            ledger.flush_grants(store)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.admit("2030-01-01T00:00:00Z")
        ledger.drain("2030-01-01T00:00:00Z")
        with pytest.raises(CoordinationError, match="reader"):
            ledger.begin_writer(HOUR, "2030-01-01T00:00:00Z", ["fake-compact"])
        assert ledger.snapshot()["readers"] == 1 and len(ledger.pending_grants()) == 1
        assert not client.puts


def test_failed_initial_transaction_is_not_promoted_or_automatically_replaced(
    tmp_path, monkeypatch
):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError

    def crash(*args):
        raise RuntimeError("injected init transaction failure")

    monkeypatch.setattr(Ledger, "_event", crash)
    with pytest.raises(RuntimeError):
        initialize(tmp_path)
    with (
        pytest.raises(CoordinationError),
        Ledger.open(tmp_path / "state", binding(tmp_path)),
    ):
        pass
    with pytest.raises(CoordinationError, match="exists"):
        initialize(tmp_path)
