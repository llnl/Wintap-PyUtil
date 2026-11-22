"""Genuine failed originals, durable retry exclusion and real crash boundaries."""

import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical, make_request

from .test_coordination_completion import NOW
from .test_coordination_execution import binding
from .test_coordination_ledger import HOUR
from .test_coordination_retry import failed_merge

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux actual unprivileged merge failure and crash controls",
)


def retry(ledger, prepared, **kwargs):
    from wintappy.coordination.intentional_retry import retry_and_complete_prepared

    return retry_and_complete_prepared(
        ledger,
        prepared,
        NOW,
        clock=lambda: NOW,
        confirmed=True,
        reason="synthetic isolated retry",
        **kwargs,
    )


def test_retry_completes_only_fresh_run_and_preserves_failed_original(tmp_path):
    prepared, archive = failed_merge(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        original = ledger.writer_run(prepared.receipt["run_id"])
        request = make_request(
            binding(tmp_path),
            "b" * 32,
            "dbt-build",
            HOUR,
            prepared.receipt["hour_end"],
            NOW,
        )
        ledger.ingest(request, NOW)
        job = ledger.job(request["job_id"])
        result = retry(ledger, prepared)
        assert result["next_hour"] == prepared.receipt["hour_end"]
        assert result["mode"] == "OPEN"
        assert ledger.writer_run(original["run_id"]) == original
        assert ledger.completion_evidence(original["run_id"]) is None
        row = ledger._connection.execute(
            "SELECT original_run_id,retry_run_id FROM retry_dispositions"
        ).fetchone()
        assert row[0] == original["run_id"] and row[1] != row[0]
        new = ledger.writer_run(row[1])
        assert new["cli_run_id"] not in {
            original["cli_run_id"],
            original["run_id"],
            new["run_id"],
        }
        assert (
            ledger.completion_evidence(row[1])["writer_supervision"]["exit_code"] == 0
        )
        assert ledger.job(request["job_id"]) == job
        ledger._validate()
        assert len(ledger.admit(NOW)) == 1
        with pytest.raises(CoordinationError):
            retry(ledger, prepared)
    assert all(path.read_bytes() == payload for path, payload in archive.items())
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.writer_run(original["run_id"]) == original
        assert ledger.completion_evidence(original["run_id"]) is None


@pytest.mark.parametrize(
    "phase",
    [
        "before_retry_commit",
        "after_retry_commit",
        "before_retry_launch",
        "after_retry_launch",
    ],
)
def test_actual_retry_crashes_preserve_one_owner_and_never_relaunch(tmp_path, phase):
    prepared, archive = failed_merge(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        original = ledger.writer_run(prepared.receipt["run_id"])
    code = """
import json,os,sys
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.recovery import load_original_execution
from wintappy.coordination.intentional_retry import retry_and_complete_prepared
with Ledger.open(sys.argv[1], json.loads(sys.argv[2])) as ledger:
    prepared = load_original_execution(ledger,sys.argv[3],{})
    def crash(phase):
        if phase == sys.argv[4]: os._exit(73)
    ledger.failure_hook = crash
    retry_and_complete_prepared(ledger,prepared,sys.argv[5],clock=lambda:sys.argv[5],
        confirmed=True,reason="synthetic actual crash")
"""
    child = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            code,
            str(tmp_path / "state"),
            canonical(binding(tmp_path)),
            prepared.receipt["run_id"],
            phase,
            NOW,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert child.returncode == 73, child.stderr
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        state = ledger.snapshot()
        assert state["mode"] == "BLOCKED" and state["next_hour"] == HOUR
        assert ledger.writer_run(original["run_id"]) == original
        assert ledger.admit(NOW) == []
        rows = ledger._connection.execute(
            "SELECT retry_run_id FROM retry_dispositions"
        ).fetchall()
        assert len(rows) == (0 if phase == "before_retry_commit" else 1)
        if rows:
            new = ledger.writer_run(rows[0][0])
            assert new["state"] == "BLOCKED" and state["active_run_id"] == new["run_id"]
            observations = Path(
                json.loads(new["intent_json"])["execution"]["receipt"][
                    "observations_directory"
                ]
            )
            assert observations.exists() is (phase == "after_retry_launch")
            with pytest.raises(CoordinationError):
                retry(ledger, prepared)
        assert ledger.completion_evidence(original["run_id"]) is None
    assert all(path.read_bytes() == payload for path, payload in archive.items())


def test_lost_retry_commit_ack_blocks_new_owner_without_launch(tmp_path):
    prepared, _ = failed_merge(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:

        def lost(phase):
            if phase == "after_retry_commit":
                raise OSError("synthetic lost acknowledgment")

        ledger.failure_hook = lost
        with pytest.raises(CoordinationError):
            retry(ledger, prepared)
        state = ledger.snapshot()
        assert (
            state["mode"] == "BLOCKED"
            and state["active_run_id"] != prepared.receipt["run_id"]
        )
        new = ledger.writer_run(state["active_run_id"])
        assert not Path(
            json.loads(new["intent_json"])["execution"]["receipt"][
                "observations_directory"
            ]
        ).exists()
        ledger._validate()


@pytest.mark.parametrize("damage", ["input", "output", "archive", "receipt"])
def test_retry_requires_fresh_original_disposition_before_commit(tmp_path, damage):
    prepared, archive = failed_merge(tmp_path)
    if damage == "input":
        next((tmp_path / "raw_sensor").rglob("*.parquet")).unlink()
    elif damage == "output":
        (tmp_path / "compacted/raw_file/dayPK=20260101/hourPK=23").mkdir(parents=True)
    elif damage == "archive":
        next(iter(archive)).unlink()
    else:
        prepared.path.unlink()
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        state = ledger.snapshot()
        with pytest.raises(CoordinationError):
            retry(ledger, prepared)
        assert ledger.snapshot() == state
        assert (
            ledger._connection.execute(
                "SELECT count(*) FROM retry_dispositions"
            ).fetchone()[0]
            == 0
        )


def test_v3_upgrade_preserves_failed_original_and_all_existing_rows(tmp_path):
    from wintappy.coordination.ledger import SCHEMA_V3

    prepared, _ = failed_merge(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        retained = {
            table: ledger._connection.execute(f"SELECT * FROM {table}").fetchall()
            for table in (
                "dataset_state",
                "jobs",
                "outbox",
                "compaction_runs",
                "authority_checks",
                "completion_evidence",
            )
        }
        metadata = ledger._connection.execute("SELECT * FROM metadata").fetchone()
        events = ledger._connection.execute(
            "SELECT * FROM events WHERE kind<>'retry_history_initialized'"
        ).fetchall()
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    with duckdb.connect(str(legacy / "coordinator.duckdb")) as connection:
        connection.execute(SCHEMA_V3)
        connection.execute(
            "INSERT INTO metadata VALUES (1,3,?,?,?)", list(metadata[2:])
        )
        for table, rows in {**retained, "events": events}.items():
            if rows:
                connection.executemany(
                    f"INSERT INTO {table} VALUES ({','.join('?' for _ in rows[0])})",
                    rows,
                )
    with pytest.raises(CoordinationError), Ledger.open(legacy, binding(tmp_path)):
        pass
    Ledger.upgrade(
        legacy,
        binding(tmp_path),
        NOW,
        confirmed=True,
        reason="synthetic compatible retry upgrade",
    )
    with Ledger.open(legacy, binding(tmp_path)) as ledger:
        assert ledger.ledger_id == metadata[2]
        for table, rows in retained.items():
            assert (
                ledger._connection.execute(f"SELECT * FROM {table}").fetchall() == rows
            )
        assert ledger.snapshot()["active_run_id"] == prepared.receipt["run_id"]
        assert (
            ledger._connection.execute(
                "SELECT count(*) FROM retry_dispositions"
            ).fetchone()[0]
            == 0
        )


def test_post_commit_input_change_refuses_before_actual_writer_launch(tmp_path):
    prepared, _ = failed_merge(tmp_path)
    source = next((tmp_path / "raw_sensor").rglob("*.parquet"))
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:

        def changed(phase):
            if phase == "before_retry_launch":
                (source.parent / "extra.parquet").write_bytes(source.read_bytes())

        ledger.failure_hook = changed
        with pytest.raises(CoordinationError):
            retry(ledger, prepared)
        state = ledger.snapshot()
        assert state["mode"] == "BLOCKED" and state["next_hour"] == HOUR
        new = ledger.writer_run(state["active_run_id"])
        assert new["run_id"] != prepared.receipt["run_id"]
        assert not Path(
            json.loads(new["intent_json"])["execution"]["receipt"][
                "observations_directory"
            ]
        ).exists()


def test_changed_input_at_launch_cannot_complete_or_resolve_as_original(
    tmp_path, monkeypatch
):
    from wintappy.coordination import intentional_retry
    from wintappy.coordination.recovery import load_original_execution, resolve_hour

    prepared, _ = failed_merge(tmp_path)
    source = next((tmp_path / "raw_sensor").rglob("*.parquet"))
    launch = intentional_retry._launch_prepared
    launches = []

    def changed(ledger, new, environment):
        # Same-schema/same-count but different actual Parquet data; a count-only
        # comparison must not mask the original source-identity defect.
        seeded = subprocess.run(
            [
                new.receipt["runtime"]["executable"],
                "-I",
                "-B",
                "-c",
                'import duckdb,sys; duckdb.connect().execute("COPY (SELECT 2 AS value) TO ? (FORMAT PARQUET)",[sys.argv[1]])',
                str(source),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert seeded.returncode == 0, seeded.stderr
        result = launch(ledger, new, environment)
        launches.append(1)
        return result

    monkeypatch.setattr(intentional_retry, "_launch_prepared", changed)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        with pytest.raises(CoordinationError):
            retry(ledger, prepared)
        assert launches == [1]
        state = ledger.snapshot()
        assert state["mode"] == "BLOCKED" and state["next_hour"] == HOUR
        new = load_original_execution(ledger, state["active_run_id"], {})
        with pytest.raises(CoordinationError):
            resolve_hour(
                ledger,
                new,
                NOW,
                clock=lambda: NOW,
                confirmed=True,
                reason="synthetic changed-source refusal",
            )
        assert ledger.completion_evidence(new.receipt["run_id"]) is None
