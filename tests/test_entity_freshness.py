"""Real DBT preflight must distinguish current authority from historical coverage."""

import duckdb
import pytest

from .test_dbt_source_validation import env as env_fixture
from .test_dbt_source_validation import operation
from .test_process_entities import compact, fixture_queries, setup

env = env_fixture


def evolve(tmp_path):
    snapshot = next(
        (tmp_path / "input/process_entities").glob("hostPK=*/entities.parquet")
    )
    before = snapshot.read_bytes()
    later = (
        tmp_path / "input/raw_sensor/raw_process/dayPK=20260102/hourPK=01/later.parquet"
    )
    later.parent.mkdir(parents=True)
    with duckdb.connect() as conn:
        conn.execute(
            "copy (select * replace ('next-generation' as PidHash, 999 as PID) "
            "from (select "
            + fixture_queries()["raw_process"]
            + ")) to ? (format parquet)",
            [str(later)],
        )
    compact(
        tmp_path,
        "entities-replay",
        "--start-hour",
        "2026-01-02T01:00:00Z",
        "--end-hour",
        "2026-01-02T02:00:00Z",
    )
    return snapshot, before


def test_historical_hashes_differ_but_forward_state_passes(env, tmp_path):
    setup(env, tmp_path)
    evolve(tmp_path)
    with duckdb.connect() as conn:
        assert (
            conn.execute(
                "select count(distinct entity_snapshot_hashes) from read_parquet(?)",
                [str(tmp_path / "input/processing_metrics/**/*.parquet")],
            ).fetchone()[0]
            == 2
        )
    result = operation(env, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "damage", ["rollback", "replacement", "bootstrap", "missing_authority"]
)
def test_untrusted_current_snapshot_fails_real_preflight(env, tmp_path, damage):
    setup(env, tmp_path)
    snapshot, before = evolve(tmp_path)
    if damage == "rollback":
        snapshot.write_bytes(before)
    elif damage == "missing_authority":
        (tmp_path / "input/process_entities/_authority/authority.parquet").unlink(
            missing_ok=True
        )
    else:
        with duckdb.connect() as conn:
            metadata = dict(
                conn.execute(
                    "select decode(key),decode(value) from parquet_kv_metadata(?)",
                    [str(snapshot)],
                ).fetchall()
            )
            if damage == "bootstrap":
                metadata["bootstrap_start"] = "2025-12-31T00:00:00Z"
            conn.execute(
                "create temp table changed as select * from "
                "read_parquet(?, hive_partitioning=false)"
                + (" limit 0" if damage == "replacement" else ""),
                [str(snapshot)],
            )

            def quote(s):
                return "'" + s.replace("'", "''") + "'"

            kv = ",".join(quote(k) + ":" + quote(v) for k, v in metadata.items())
            conn.execute(
                f"copy changed to ? (format parquet, kv_metadata {{{kv}}})",
                [str(snapshot)],
            )
    result = operation(env, tmp_path)
    assert result.returncode != 0, "Stale/untrusted snapshot passed real DBT preflight"
    assert "authority" in (result.stdout + result.stderr).lower()


@pytest.mark.parametrize(
    "field,value",
    [
        ("entity_snapshot_hashes", "'{}'"),
        ("entity_input_fingerprint", "'untrusted'"),
        ("expected_host_count", "99"),
        ("entity_bootstrap_start", "'2025-12-31T00:00:00Z'"),
        ("status", "null"),
        ("hour_end", "null"),
        ("window_start", "'2025-12-31T00:00:00Z'"),
        ("entity_generation_id", "'untrusted'"),
        ("metric_version", "'3'"),
    ],
)
def test_historical_checkpoint_must_match_its_receipt(env, tmp_path, field, value):
    setup(env, tmp_path)
    evolve(tmp_path)
    metric = (
        tmp_path / "input/processing_metrics/dayPK=20260102/hourPK=00/metrics.parquet"
    )
    with duckdb.connect() as conn:
        conn.execute(
            f"create temp table changed as select * replace ({value} as {field}) "
            "from read_parquet(?, hive_partitioning=false)",
            [str(metric)],
        )
        conn.execute("copy changed to ? (format parquet)", [str(metric)])
    result = operation(env, tmp_path)
    assert result.returncode != 0, (
        "Unbound historical checkpoint passed real DBT preflight"
    )


@pytest.mark.parametrize(
    "change",
    [
        "delete from changed where generation=1",
        "update changed set parent_id='broken' where generation=2",
        "update changed set input_fingerprint=repeat('a',64) where generation=1",
        "update changed set status='pending' where generation=2",
    ],
)
def test_receipt_chain_and_pending_state_fail_real_preflight(env, tmp_path, change):
    setup(env, tmp_path)
    evolve(tmp_path)
    authority = tmp_path / "input/process_entities/_authority/authority.parquet"
    with duckdb.connect() as conn:
        conn.execute(
            "create temp table changed as select * from "
            "read_parquet(?, hive_partitioning=false)",
            [str(authority)],
        )
        conn.execute(change)
        conn.execute("copy changed to ? (format parquet)", [str(authority)])
    result = operation(env, tmp_path)
    assert result.returncode != 0
    assert "authority" in (result.stdout + result.stderr).lower()


def test_chunked_hash_unicode_and_multiple_chunks_agree_across_runtimes(env, tmp_path):
    setup(env, tmp_path)
    current = (
        tmp_path
        / "input/raw_sensor/raw_process/dayPK=20260102/hourPK=00/chunked.parquet"
    )
    with duckdb.connect() as conn:
        conn.execute(
            "copy (select * replace ('chunk-' || i as PidHash, 10000+i as PID, "
            "'synthetic-é-雪' as ProcessName) from (select "
            + fixture_queries()["raw_process"]
            + "), range(2049) r(i)) to ? (format parquet)",
            [str(current)],
        )
    compact(tmp_path, "entities-replay")
    result = operation(env, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr


def test_explicit_legacy_rebuild_and_two_root_cutover_preserve_old_evidence(
    env, tmp_path
):
    setup(env, tmp_path)
    old_snapshot = next(
        (tmp_path / "input/process_entities").glob("hostPK=*/entities.parquet")
    )
    before = old_snapshot.read_bytes()
    old_metric = (
        tmp_path / "input/processing_metrics/dayPK=20260102/hourPK=00/metrics.parquet"
    )
    before_metric = old_metric.read_bytes()
    (tmp_path / "input/process_entities/_authority/authority.parquet").unlink()
    assert operation(env, tmp_path).returncode != 0
    fresh_entities = tmp_path / "input/rebuilt_entities"
    fresh_metrics = tmp_path / "input/rebuilt_metrics"
    compact(
        tmp_path,
        "entities-init",
        "--process-entity-root",
        str(fresh_entities),
        "--processing-metrics-root",
        str(fresh_metrics),
    )
    env.update(
        WINTAP_DBT_PROCESS_ENTITY_ROOT=str(fresh_entities),
        WINTAP_DBT_PROCESSING_METRICS_ROOT=str(fresh_metrics),
    )
    result = operation(env, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert (
        old_snapshot.read_bytes() == before and old_metric.read_bytes() == before_metric
    )
