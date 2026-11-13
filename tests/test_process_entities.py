"""Real cross-runtime snapshots and DBT ancestry, using synthetic orphan breakpoints."""

import json
import subprocess
from pathlib import Path

import duckdb
import pytest

from .test_compacted_inputs import ROOT, build, fixture_queries, seed_fixture
from .test_dbt_source_validation import CONFIG, env, operation, parquet_file


COMPACTOR = ROOT / "extras/raw-sensor-compact"
RUNNER = ["uv", "run", "--project", str(COMPACTOR), "--locked", "python"]


def compact(tmp_path, command="entities-init", *extra):
    result = subprocess.run(
        [*RUNNER, "-m", "raw_sensor_compact.cli", command,
         "--source-root", str(tmp_path / "input/raw_sensor"),
         "--destination-root", str(tmp_path / "input/compacted"),
         "--start-hour", "2026-01-01T23:00:00Z", "--end-hour", "2026-01-02T01:00:00Z",
         "--process-entity-root", str(tmp_path / "input/process_entities"),
         "--entity-host", "test-host", "--entity-namespace", "test-pid-v2", *extra],
        cwd=COMPACTOR, capture_output=True, text=True, timeout=90,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


def setup(env, tmp_path):
    seed_fixture(tmp_path / "input/raw_sensor")
    (tmp_path / "input/compacted").mkdir()
    base = "select " + fixture_queries()["raw_process"]
    with duckdb.connect() as conn:
        for path in (tmp_path / "input/raw_sensor/raw_process").glob("**/*.parquet"):
            # Replace only our synthetic fixture, not a user dataset.
            conn.execute("create or replace temp table rewrite as select * replace('bridge' as ParentPidHash, 9::BIGINT as ParentPID) from read_parquet(?, hive_partitioning=false)", [str(path)])
            conn.execute("copy rewrite to ? (format parquet)", [str(path)])
        history = tmp_path / "input/raw_sensor/raw_process/dayPK=20260101/hourPK=23/history.parquet"
        current = tmp_path / "input/raw_sensor/raw_process/dayPK=20260102/hourPK=00/fanout.parquet"
        queries = []
        for key, pid, parent, parent_pid, activity in (
            ("root", 1, "root", 1, "START"),
            ("bridge", 9, "root", 1, "START"),
            ("bridge", 9, "root", 1, "STOP"),
            ("activity-only", 43, "bridge", 9, "START"),
        ):
            queries.append(f"select * replace ('{key}' as PidHash, {pid} as PID, '{key}' as ProcessName, "
                           f"'{parent}' as ParentPidHash, {parent_pid} as ParentPID, '{activity}' as ActivityType, "
                           f"{134117000000000000 + (100 if activity == 'STOP' else 0)}::BIGINT as EventTime) from ({base})")
        conn.execute("copy (" + " union all ".join(queries) + ") to ? (format parquet)", [str(history)])
        # One absent bridge would disconnect 33 selected lifecycle seeds.
        conn.execute("copy (select * replace ('fan-' || i as PidHash, 100+i as PID, 'bridge' as ParentPidHash, "
                     "9 as ParentPID) from (" + base + "), range(32) r(i)) to ? (format parquet)", [str(current)])
        file = tmp_path / "input/raw_sensor/raw_process_file/dayPK=20260102/hourPK=00/activity-only.parquet"
        conn.execute("copy (select * replace ('activity-only' as PidHash, 43 as PID) from (select " +
                     fixture_queries()["raw_process_file"] + ")) to ? (format parquet)", [str(file)])
    compact(tmp_path)
    env.update(WINTAP_DBT_START_DAY="20260102", WINTAP_DBT_END_DAY="20260102",
               WINTAP_DBT_START_HOUR="00", WINTAP_DBT_END_HOUR="00",
               WINTAP_DBT_PROCESS_ENTITY_ROOT=str(tmp_path / "input/process_entities"),
               WINTAP_DBT_PROCESS_ENTITY_HOSTS="TEST-HOST",
               WINTAP_DBT_PROCESS_ENTITY_NAMESPACE="test-pid-v2")


@pytest.mark.parametrize("state", ["raw", "mixed", "compacted"])
def test_durable_ancestors_and_unchanged_window_counts(env, tmp_path, state):
    setup(env, tmp_path)
    if state != "raw":
        compact(tmp_path, "compact", "--target-size-bytes", "134217728", "--delete-source")
        if state == "mixed":
            # Replay overlapping identical evidence into state but preserve raw fact overlap semantics.
            path = next((tmp_path / "input/compacted/raw_process/dayPK=20260102/hourPK=00").glob("*.parquet"))
            dest = tmp_path / "input/raw_sensor/raw_process/dayPK=20260102/hourPK=00/overlap.parquet"
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(path.read_bytes())
    assert not any(h["changed"] for h in compact(tmp_path, "entities-replay")["hosts"])
    build(env, tmp_path / "build")
    with duckdb.connect(env["WINTAP_DBT_DATABASE"], read_only=True) as conn:
        assert conn.execute("select count(*) from process").fetchone()[0] == 33
        assert conn.execute("select count(*) from process_summary").fetchone()[0] == 33
        assert conn.execute("select count(*) from process_path where level=2").fetchone()[0] == 33
        assert conn.execute("select count(*) from process_ancestry_quality where ancestry_status='root_reached'").fetchone()[0] == 33
        assert conn.execute("select ptree_list from process_path where pid_hash='process-one'").fetchone()[0] == ["process-one", "bridge", "root"]
        assert conn.execute("select count(*) from process_context where context_only").fetchone()[0] == 2
        assert conn.execute("select activity_only, window_lifecycle from process_context where pid_hash='activity-only'").fetchone() == (True, False)
        assert conn.execute("select count(*) from process where pid_hash in ('root', 'bridge', 'activity-only')").fetchone()[0] == 0
        # Raw duplicates are still counted by bronze, never injected from entity history.
        assert conn.execute("select sum(num_process_start) from process").fetchone()[0] == 33
        assert conn.execute("select sum(num_dups) from stg_raw_process").fetchone()[0] == (72 if state == "mixed" else 36)
        assert conn.execute("select seen_stop, stop_filetime is not null from stg_process_entities where pid_hash='bridge'").fetchone() == (True, True)
    compact(tmp_path, "entities-verify")


def test_missing_or_wrong_namespace_state_fails_preflight(env, tmp_path):
    setup(env, tmp_path)
    env["WINTAP_DBT_PROCESS_ENTITY_NAMESPACE"] = "wrong"
    result = operation(env, tmp_path)
    assert result.returncode != 0 and "metadata/namespace/host mismatch" in result.stdout + result.stderr
    env["WINTAP_DBT_PROCESS_ENTITY_NAMESPACE"] = "test-pid-v2"
    snapshot = next((tmp_path / "input/process_entities").glob("**/*.parquet"))
    snapshot.unlink()
    assert operation(env, tmp_path).returncode != 0


def test_missing_processing_metrics_fails_preflight(env, tmp_path):
    setup(env, tmp_path)
    for metric in (tmp_path / "input/processing_metrics").glob("**/*.parquet"):
        metric.unlink()
    result = operation(env, tmp_path)
    assert result.returncode != 0
    assert "Processing metrics" in result.stdout + result.stderr


def test_identity_threshold_mismatch_fails_preflight(env, tmp_path):
    setup(env, tmp_path)
    env["WINTAP_DBT_PROCESS_UNKNOWN_IDENTITY_MAX_RATIO"] = "0.10"
    result = operation(env, tmp_path)
    assert result.returncode != 0
    assert "Processing metrics" in result.stdout + result.stderr


def test_parent_conflict_terminates_instead_of_arbitrary_repair(env, tmp_path):
    setup(env, tmp_path)
    conflict = tmp_path / "input/raw_sensor/raw_process/dayPK=20260102/hourPK=00/conflict.parquet"
    with duckdb.connect() as conn:
        conn.execute("copy (select * replace('other-parent' as ParentPidHash, 99::BIGINT as ParentPID) from (select " +
                     fixture_queries()["raw_process"] + ")) to ? (format parquet)", [str(conflict)])
    # DBT sees current evidence even BEFORE hourly state catches up.
    build(env, tmp_path / "conflict-build")
    with duckdb.connect(env["WINTAP_DBT_DATABASE"], read_only=True) as conn:
        assert conn.execute("select parent_status, parent_pid_hash, parent_os_pid from process where pid_hash='process-one'").fetchone() == ("conflicted", None, None)
        assert conn.execute("select level from process_path where pid_hash='process-one'").fetchone()[0] == 0
        assert conn.execute("select context_status from process_context where pid_hash='process-one'").fetchone()[0] == "parent_conflict"


@pytest.mark.parametrize("mode,level,status", [("missing", 1, "missing_ancestor"), ("cycle", 2, "cycle"), ("self", 1, "cycle")])
def test_missing_ancestor_and_nonroot_cycle_are_not_complete(env, tmp_path, mode, level, status):
    setup(env, tmp_path)
    history = tmp_path / "input/raw_sensor/raw_process/dayPK=20260101/hourPK=23/history.parquet"
    with duckdb.connect() as conn:
        parent, parent_pid = ("bridge", 9) if mode == "self" else ("cycle", 8)
        conn.execute("create temp table modified as select * replace (case when pidhash='bridge' then ? else parentpidhash end as ParentPidHash, case when pidhash='bridge' then ? else parentpid end as ParentPID) from read_parquet(?, hive_partitioning=false)", [parent, parent_pid, str(history)])
        if mode == "cycle":
            conn.execute("insert into modified select * replace('cycle' as PidHash, 8 as PID, 'bridge' as ParentPidHash, 9 as ParentPID, 'cycle' as ProcessName) from modified where pidhash='root'")
        conn.execute("copy modified to ? (format parquet)", [str(history)])
    fresh = tmp_path / "input/rebuilt_entities"
    compact(tmp_path, "entities-init", "--process-entity-root", str(fresh))
    env["WINTAP_DBT_PROCESS_ENTITY_ROOT"] = str(fresh)
    build(env, tmp_path / "edge-build")
    with duckdb.connect(env["WINTAP_DBT_DATABASE"], read_only=True) as conn:
        assert conn.execute("select level, ancestry_status from process_ancestry_quality where pid_hash='process-one'").fetchone() == (level, status)
        assert conn.execute("select count(*) from process_ancestry_quality where ancestry_status='root_reached'").fetchone()[0] == 0


@pytest.mark.parametrize("name,value", [
    ("WINTAP_DBT_PROCESS_ENTITY_ROOT", "raw_sensor"),
    ("WINTAP_DBT_PROCESS_ENTITY_NAMESPACE", ""),
    ("WINTAP_DBT_PROCESS_ENTITY_HOSTS", ""),
    ("WINTAP_DBT_PROCESS_UNKNOWN_IDENTITY_POLICY", "invent"),
    ("WINTAP_DBT_PROCESS_UNKNOWN_IDENTITY_MAX_RATIO", "1.1"),
    ("WINTAP_DBT_PROCESS_UNKNOWN_IDENTITY_MAX_RATIO", "not-a-number"),
])
def test_entity_config_guards(env, tmp_path, name, value):
    setup(env, tmp_path)
    env[name] = str(tmp_path / "input/raw_sensor") if value == "raw_sensor" else value
    with pytest.raises(ValueError):
        CONFIG.validate(env)
