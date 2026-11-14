"""Real two-runtime repair preserves original migration, authority and DBT coverage."""

import json
import subprocess

import duckdb

from .test_compacted_inputs import build
from .test_dbt_source_validation import env as env_fixture
from .test_dbt_source_validation import operation
from .test_entity_freshness import evolve
from .test_process_entities import COMPACTOR, RUNNER, compact, setup

env = env_fixture


def test_post_delete_checkpoint_repair_passes_real_dbt_without_entity_replay(
    env, tmp_path
):
    setup(env, tmp_path)
    archive_root = tmp_path / "run-evidence"
    run_id = "a" * 32
    code = """
from raw_sensor_compact.cli import main
from raw_sensor_compact.config import CompactionError
from raw_sensor_compact.processing_metrics import ProcessingMetricsStore
publish = ProcessingMetricsStore.publish
calls = 0
def fail_second(self, conn, hour, row):
    global calls
    calls += 1
    if calls == 2:
        raise CompactionError('injected second checkpoint PUT failure')
    publish(self, conn, hour, row)
ProcessingMetricsStore.publish = fail_second
raise SystemExit(main())
"""
    result = subprocess.run(
        [
            *RUNNER,
            "-c",
            code,
            "compact",
            "--source-root",
            str(tmp_path / "input/raw_sensor"),
            "--destination-root",
            str(tmp_path / "input/compacted"),
            "--process-entity-root",
            str(tmp_path / "input/process_entities"),
            "--entity-host",
            "test-host",
            "--entity-namespace",
            "test-pid-v2",
            "--start-hour",
            "2026-01-01T23:00:00Z",
            "--end-hour",
            "2026-01-02T01:00:00Z",
            "--target-size-bytes",
            "134217728",
            "--delete-source",
            "--run-id",
            run_id,
            "--coordinator-run-id",
            "b" * 32,
            "--run-evidence-directory",
            str(archive_root),
        ],
        cwd=COMPACTOR,
        check=False,
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert result.returncode == 2, result.stdout + result.stderr
    error = json.loads(result.stderr.splitlines()[-1])
    assert error["run_id"] == run_id and "checkpoint" in error["message"]
    assert not list((tmp_path / "input/raw_sensor").glob("**/*.parquet"))
    assert list((tmp_path / "input/compacted").glob("**/*.parquet"))
    original = {p.name: p.read_bytes() for p in (archive_root / run_id).glob("*.json")}
    phases = [json.loads(value)["phase"] for value in original.values()]
    assert phases.count("checkpoint_verified") == 1 and "blocked" in phases
    metrics = list((tmp_path / "input/processing_metrics").glob("**/*.parquet"))
    for path in metrics:
        path.unlink()
    assert operation(env, tmp_path).returncode != 0
    # A later valid receipt does not invalidate the original migration's coverage.
    evolve(tmp_path)
    state = {
        p.relative_to(tmp_path).as_posix(): p.read_bytes()
        for p in (tmp_path / "input/process_entities").glob("**/*.parquet")
    }
    report = compact(
        tmp_path,
        "checkpoints-repair",
        "--repair-run-id",
        run_id,
        "--run-id",
        "d" * 32,
        "--confirm-repair",
        "--run-evidence-directory",
        str(archive_root),
    )
    assert (
        report["status"] == "repaired"
        and report["processing_metrics"]["run_id"] == run_id
    )
    assert {
        p.name: p.read_bytes() for p in (archive_root / run_id).glob("*.json")
    } == original
    assert {
        p.relative_to(tmp_path).as_posix(): p.read_bytes()
        for p in (tmp_path / "input/process_entities").glob("**/*.parquet")
    } == state
    with duckdb.connect() as conn:
        rows = conn.execute(
            "select distinct run_id, raw_status from read_parquet(?) where run_id=?",
            [str(tmp_path / "input/processing_metrics/**/*.parquet"), run_id],
        ).fetchall()
        assert rows == [(run_id, "merged")]
    assert operation(env, tmp_path).returncode == 0
    assert build(env, tmp_path / "build-after-repair") > 0
