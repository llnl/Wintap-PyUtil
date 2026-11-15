"""Opt-in compatibility and fail-closed schema staging, not entity launch tests."""

import json
import os
import subprocess
import sys

import duckdb
import pytest

from wintappy.coordination.ledger import SCHEMA_V1, SCHEMA_V2, SCHEMA_V3, Ledger
from wintappy.coordination.protocol import (
    CoordinationError,
    canonical,
    make_cancellation,
    make_release,
)

from .test_coordination_ledger import HOUR, NOW
from .test_coordination_protocol import binding, request

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux explicit upgrade, real reader reservations and process boundaries",
)


def rows(connection):
    return {
        table: connection.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
        for (table,) in connection.execute("SHOW TABLES").fetchall()
    }


def legacy_scope(root, version, *, writer=False):
    bound, state = binding(root), root / "state"
    if version == 4:
        Ledger.initialize(state, bound, HOUR)
    else:
        state.mkdir()
    with duckdb.connect(str(state / "coordinator.duckdb")) as connection:
        if version < 4:
            connection.execute({1: SCHEMA_V1, 2: SCHEMA_V2, 3: SCHEMA_V3}[version])
            values = [version, "a" * 32, canonical(bound)]
            if version == 3:
                values.append(HOUR)
            connection.execute(
                "INSERT INTO metadata VALUES (1," + ",".join("?" for _ in values) + ")",
                values,
            )
            connection.execute(
                "INSERT INTO dataset_state VALUES (1,'OPEN',?,NULL,'')", [HOUR]
            )
        ledger = Ledger(connection, state, bound)
        if version == 3:
            ledger._event(
                "completion_history_initialized",
                "dataset",
                NOW,
                {"first_hour": HOUR, "source_schema": 0},
            )
        ledger.ingest(request(root), NOW)
        grant = ledger.admit(NOW)[0]
        ledger.ingest(make_cancellation(request(root, "b" * 32), NOW), NOW)
        if writer:
            ledger.ingest(make_release(grant, NOW, 0), NOW)
            ledger.drain(NOW)
            ledger.begin_writer(HOUR, NOW, ["synthetic-retained-writer"])
        ledger._validate(version=version)
        before = rows(connection)
    return bound, before


def upgrade(state, bound, *, now=NOW):
    Ledger.upgrade(
        state,
        bound,
        now,
        target_version=5,
        confirmed=True,
        reason="synthetic explicit entity schema selection",
    )


@pytest.mark.parametrize("version", [1, 2, 3])
def test_ordinary_open_never_migrates_and_default_upgrade_still_targets_v4(
    tmp_path, version
):
    bound, before = legacy_scope(tmp_path, version)
    state = tmp_path / "state"
    with pytest.raises(CoordinationError), Ledger.open(state, bound):
        pass
    with duckdb.connect(str(state / "coordinator.duckdb")) as connection:
        assert rows(connection) == before
    Ledger.upgrade(
        state, bound, NOW, confirmed=True, reason="synthetic unchanged default upgrade"
    )
    with Ledger.open(state, bound) as ledger:
        assert ledger._connection.execute(
            "SELECT schema_version FROM metadata"
        ).fetchone() == (4,)
        assert "entity_runs" not in rows(ledger._connection)
        assert ledger.snapshot()["readers"] == 1
        with pytest.raises(CoordinationError, match="opt-in v5"):
            ledger.entity_run("c" * 32)
    upgrade(state, bound)
    with Ledger.open(state, bound) as ledger:
        assert ledger.snapshot()["readers"] == 1
        assert ledger.snapshot()["next_hour"] == HOUR


@pytest.mark.parametrize("version", [1, 2, 3, 4])
@pytest.mark.parametrize("writer", [False, True])
def test_direct_opt_in_preserves_every_original_row_and_writer_disposition(
    tmp_path, version, writer
):
    bound, before = legacy_scope(tmp_path, version, writer=writer)
    state = tmp_path / "state"
    upgrade(state, bound)
    # Read actual durable rows before ordinary open blocks a restarted INTENT.
    with duckdb.connect(str(state / "coordinator.duckdb")) as connection:
        after = rows(connection)
    metadata = before["metadata"][0]
    assert after["metadata"] == [(1, 5, *metadata[2:4], HOUR)]
    for table, original in before.items():
        if table not in {"metadata", "events"}:
            assert after[table] == original
    assert set(before["events"]) <= set(after["events"])
    assert after["entity_runs"] == after["entity_evidence"] == []
    with Ledger.open(state, bound) as ledger:
        assert ledger.snapshot()["next_hour"] == HOUR
        assert ledger.snapshot()["readers"] == (0 if writer else 1)
        if writer:
            assert ledger.snapshot()["mode"] == "BLOCKED"
            run_id = ledger.snapshot()["active_run_id"]
            assert ledger.writer_run(run_id)["state"] == "BLOCKED"
            assert ledger.completion_evidence(run_id) is None


@pytest.mark.parametrize("version", [1, 2, 3])
@pytest.mark.parametrize("phase", ["before_upgrade_commit", "after_upgrade_commit"])
def test_legacy_opt_in_has_one_actual_atomic_upgrade_boundary(tmp_path, version, phase):
    bound, before = legacy_scope(tmp_path, version)
    state = tmp_path / "state"
    script = """
import json,os,sys
from wintappy.coordination.ledger import Ledger
original=Ledger.__init__
def intercepted(self,*args,**kwargs):
    original(self,*args,**kwargs)
    def crash(observed):
        if observed==sys.argv[3]: os._exit(86)
    self.failure_hook=crash
Ledger.__init__=intercepted
Ledger.upgrade(sys.argv[1],json.loads(sys.argv[2]),sys.argv[4],target_version=5,
    confirmed=True,reason='synthetic atomic legacy opt-in upgrade')
raise AssertionError('actual opt-in upgrade boundary not reached')
"""
    child = subprocess.run(
        [sys.executable, "-B", "-c", script, str(state), canonical(bound), phase, NOW],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert child.returncode == 86, child.stdout + child.stderr
    with duckdb.connect(str(state / "coordinator.duckdb")) as connection:
        after = rows(connection)
    if phase == "before_upgrade_commit":
        assert after == before
        with pytest.raises(CoordinationError), Ledger.open(state, bound):
            pass
    else:
        assert after["metadata"][0][1] == 5
        assert after["jobs"] == before["jobs"]
        assert after["outbox"] == before["outbox"]
        with Ledger.open(state, bound) as ledger:
            assert ledger.snapshot()["readers"] == 1


@pytest.mark.parametrize(
    "damage",
    [
        "missing_origin",
        "duplicate_origin",
        "source_type",
        "changed_upgrade",
        "missing_reason",
        "clock",
        "oversized_origin",
        "missing_table",
        "extra_table",
        "entity_run",
        "entity_proof",
        "entity_audit",
    ],
)
def test_opt_in_schema_corruption_or_unimplemented_entity_history_refuses_open(
    tmp_path, damage
):
    bound, _ = legacy_scope(tmp_path, 4)
    state = tmp_path / "state"
    upgrade(state, bound)
    with duckdb.connect(str(state / "coordinator.duckdb")) as connection:
        if damage == "missing_origin":
            connection.execute(
                "DELETE FROM events WHERE kind='entity_history_initialized'"
            )
        elif damage == "duplicate_origin":
            connection.execute(
                "INSERT INTO events SELECT ?,observed_at,kind,identity,details_json "
                "FROM events WHERE kind='entity_history_initialized'",
                ["e" * 32],
            )
        elif damage in {"source_type", "oversized_origin"}:
            value = {"source_schema": True}
            if damage == "oversized_origin":
                value["extra"] = "x" * (16 * 1024)
            connection.execute(
                "UPDATE events SET details_json=? WHERE kind='entity_history_initialized'",
                [canonical(value)],
            )
        elif damage in {"changed_upgrade", "missing_reason"}:
            value = {"from": 3, "to": 5, "reason": "synthetic changed source"}
            if damage == "missing_reason":
                value.update({"from": 4, "reason": None})
            connection.execute(
                "UPDATE events SET details_json=? WHERE kind='schema_upgraded'",
                [canonical(value)],
            )
        elif damage == "clock":
            connection.execute(
                "UPDATE events SET observed_at=? WHERE kind='entity_history_initialized'",
                [HOUR],
            )
        elif damage == "missing_table":
            connection.execute("DROP TABLE entity_runs")
        elif damage == "extra_table":
            connection.execute("CREATE TABLE unexpected_entity_history (value VARCHAR)")
        elif damage == "entity_run":
            connection.execute(
                "INSERT INTO entity_runs VALUES (?,?,'entities-init',?,?,'INTENT',?,?,?,'')",
                ["c" * 32, "d" * 32, HOUR, "2026-01-02T00:00:00Z", NOW, "{}", "f" * 64],
            )
        elif damage == "entity_proof":
            connection.execute(
                "INSERT INTO entity_evidence VALUES (1,?,?,?,'')",
                ["c" * 32, "{}", "f" * 64],
            )
        else:
            connection.execute(
                "INSERT INTO events VALUES (?,?,'entity_completed',?,'{}')",
                ["e" * 32, NOW, "c" * 32],
            )
        before = rows(connection)
    with pytest.raises(CoordinationError), Ledger.open(state, bound):
        pass
    with duckdb.connect(str(state / "coordinator.duckdb")) as connection:
        assert rows(connection) == before


def test_schema_selection_never_opens_historical_runtime_or_launches_data_work(
    tmp_path, monkeypatch
):
    from wintappy.coordination import execution, verification

    bound, _ = legacy_scope(tmp_path, 4)

    def forbidden(*args, **kwargs):
        pytest.fail(
            "Opt-in schema selection must not load runtime/data or launch children"
        )

    monkeypatch.setattr(execution, "_read_bytes", forbidden)
    monkeypatch.setattr(verification, "_read", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    upgrade(tmp_path / "state", bound)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.entity_run("c" * 32) is None
        assert ledger.entity_evidence("c" * 32) is None
        assert ledger.snapshot()["readers"] == 1
        origin = ledger._connection.execute(
            "SELECT details_json FROM events WHERE kind='entity_history_initialized'"
        ).fetchone()
        assert json.loads(origin[0]) == {"source_schema": 4}
