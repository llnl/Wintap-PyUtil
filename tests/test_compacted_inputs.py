"""Real DBT dual-root checks and opt-in end-to-end compactor migration."""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from .test_dbt_source_validation import CONFIG, ROOT, env, operation, parquet_file
from .test_network_activity_sql import render


@pytest.mark.parametrize("state", ["raw", "mixed", "compacted"])
def test_preflight_both_roots(env, tmp_path, state):
    path = parquet_file(tmp_path)
    if state != "raw":
        dest = tmp_path / "input/compacted" / path.relative_to(tmp_path / "input/raw_sensor")
        dest.parent.mkdir(parents=True)
        path.rename(dest)
    if state == "mixed":
        parquet_file(tmp_path)
    elif state == "compacted":
        (tmp_path / "input/raw_sensor/pidstat/dayPK=20261031/hourPK=09").rmdir()
        (tmp_path / "input/raw_sensor/pidstat/dayPK=20261031").rmdir()
        (tmp_path / "input/raw_sensor/pidstat").rmdir()
        (tmp_path / "input/raw_sensor").rmdir()
    result = operation(env, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr


def test_compacted_override_and_local_date_discovery(env, tmp_path):
    path = parquet_file(tmp_path)
    compacted = tmp_path / "different/compacted"
    dest = compacted / path.relative_to(tmp_path / "input/raw_sensor")
    dest.parent.mkdir(parents=True)
    path.rename(dest)
    env["WINTAP_DBT_COMPACTED_ROOT"] = str(compacted)
    env.pop("WINTAP_DBT_START_DAY")
    env.pop("WINTAP_DBT_END_DAY")
    result = subprocess.run(
        ["make", "print-dbt-config"], cwd=ROOT, env=env, capture_output=True, text=True, timeout=90
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "WINTAP_DBT_START_DAY=20261031" in result.stdout
    assert f"WINTAP_DBT_COMPACTED_ROOT={compacted}" in result.stdout


def test_overlap_roots_rejected(env):
    env["WINTAP_DBT_COMPACTED_ROOT"] = env["WINTAP_DBT_RAW_SENSOR_DATASET"] + "/raw_sensor"
    with pytest.raises(ValueError, match="overlap"):
        CONFIG.validate(env)


def test_overlap_rows_are_not_suppressed(env, tmp_path):
    seed_fixture(tmp_path / "input/raw_sensor")
    path = tmp_path / "input/raw_sensor/pidstat/dayPK=20260101/hourPK=23/host0.parquet"
    dest = tmp_path / "input/compacted/pidstat/dayPK=20260101/hourPK=23/host0.parquet"
    dest.parent.mkdir(parents=True)
    dest.write_bytes(path.read_bytes())
    env.update(
        WINTAP_DBT_START_DAY="20260101",
        WINTAP_DBT_END_DAY="20260101",
        WINTAP_DBT_START_HOUR="23",
        WINTAP_DBT_END_HOUR="23",
    )
    build(env, tmp_path / "overlap-build", select=("stg_pidstat_metrics", "pidstat_metrics"))
    with duckdb.connect(env["WINTAP_DBT_DATABASE"], read_only=True) as conn:
        assert conn.execute("select count(*) from stg_pidstat_metrics").fetchone() == (6,)
        assert conn.execute(
            "select count(*) from pidstat_metrics where container_id is null"
        ).fetchone() == (6,)


def fixture_queries():
    """Known telemetry, genuine duplicates, and optional-column drift."""
    common = "'test-agent' AgentId, 'test-host' Hostname, 'process-one' PidHash, 42 PID, 'worker' ProcessName"
    process = common + ", 'START' ActivityType, 134117856000000000::BIGINT EventTime"
    process += ", " + ", ".join(
        f"NULL::VARCHAR {c}"
        for c in "ProcessArgs UserName ParentPidHash ProcessPath FileMD5 FileSHA2".split()
    )
    process += ", " + ", ".join(
        f"0::BIGINT {c}"
        for c in "ParentPID CpuCycleCount CpuUtilization CommitCharge CommitPeak ReadOperationCount WriteOperationCount ReadTransferKilobytes WriteTransferKilobytes HardFaultCount TokenElevationType ExitCode".split()
    )
    host = "'test-agent' AgentId, 'test-host' Hostname, 1767225600::BIGINT EventTime, 'test-os' OS, '1' OSVersion, 'arm64' Arch, 4 ProcessorCount, 2000 ProcessorSpeed, false HasBattery, '' \"Domain\", '' DomainRole, 0::BIGINT LastBoot, '7' WintapVersion, '7' ETLVersion"
    file = (
        common
        + ", '/case/File' file_path, 'file-one' file_hash, 'Read' ActivityType, 10::BIGINT BytesRequested, 2 EventCount, 134117856000000000::BIGINT FirstSeen, 134117856001000000::BIGINT LastSeen, 1767225600::BIGINT EventTime"
    )
    network = (
        common
        + ", '{proto}' Protocol, '{activity}' IpEvent, 134117856000000000::BIGINT FirstSeenMs, 134117856001000000::BIGINT LastSeenMs, '127.0.0.1' SourceIPAddressString, '127.0.0.2' DestinationIPAddressString, 2130706433::BIGINT LocalIpAddr, 2130706434::BIGINT RemoteIpAddr, '' LocalIpPrivateGateway, '' RemoteIpPrivateGateway, 1000 LocalPort, 2000 RemotePort, 2 EventCount, 10 PacketSize, 5 MinPacketSize, 5 MaxPacketSize, 50 PacketSizeSquared"
    )
    macip = "'test-agent' AgentId, 'test-host' Hostname, '' PrivateGateway, 2130706433::BIGINT IpAddr, '00:00:00:00:00:01' Mac, 1500 MTU, 1767225600::BIGINT EventTime"
    pidstat = "timestamp '2026-01-01 00:00:00' AS time, 1000 uid, 42 pid, 'worker' command, 'test-host' hostname, NULL::VARCHAR cgroup_path, 1::BIGINT pid_ns_inode, NULL::VARCHAR container_runtime, NULL::VARCHAR container_id"
    pidstat += ", " + ", ".join(
        f"1::FLOAT {c}"
        for c in "usr_percent system_percent guest_percent wait_percent cpu_percent minflt_per_sec majflt_per_sec mem_percent kb_read_per_sec kb_write_per_sec kb_cancelled_write_per_sec context_switch_per_sec nonvoluntary_context_switch_per_sec".split()
    )
    pidstat += ", 0 cpu_core, 100::BIGINT vsz_kb, 50::BIGINT rss_kb, 0 iodelay_ticks"
    return {
        "raw_host": host,
        "raw_process": process,
        "raw_process_file": file,
        "raw_macip": macip,
        "pidstat": pidstat,
        "raw_process_conn_incr/protoPK=tcp": network.format(proto="tcp", activity="TcpIpSend"),
        "raw_process_conn_incr/protoPK=udp": network.format(proto="udp", activity="UdpIpSend"),
    }


def seed_fixture(directory):
    with duckdb.connect() as conn:
        for day, hour in (("20260101", "23"), ("20260102", "00")):
            for event, fields in fixture_queries().items():
                family, _, proto = event.partition("/")
                leaf = directory / family / f"dayPK={day}" / f"hourPK={hour}"
                if proto:
                    leaf /= proto
                leaf.mkdir(parents=True)
                for i in range(2):
                    query = "select " + fields + " from range(2)"
                    if family == "pidstat" and i == 0:
                        query = "select * exclude(container_id) from (" + query + ")"
                    elif i == 1:
                        # Reorder fields without changing rows.
                        cols = [r[0] for r in conn.execute("describe " + query).fetchall()]
                        query = (
                            "select "
                            + ",".join('"' + c + '"' for c in reversed(cols))
                            + " from ("
                            + query
                            + ")"
                        )
                    conn.execute(
                        "copy (" + query + ") to ? (format parquet)",
                        [str(leaf / f"host{i}.parquet")],
                    )


def test_existing_process_model_depends_on_row_order():
    """Diagnostic: unordered ANY_VALUE is not a stable migration comparison."""
    query = "select " + fixture_queries()["raw_process"]
    model = render(ROOT / "wintap_dbt/models/silver/process.sql")
    inputs, outputs = [], []
    with duckdb.connect() as conn:
        conn.execute("set threads=1")
        conn.execute("create table stg_raw_process as " + query + " where false")
        for paths in (("/before", "/after"), ("/after", "/before")):
            conn.execute("delete from stg_raw_process")
            for path in paths:
                conn.execute(
                    "insert into stg_raw_process select * replace (? as ProcessPath) from ("
                    + query
                    + ")",
                    [path],
                )
            inputs.append(conn.execute("select * from stg_raw_process order by all").fetchall())
            outputs.append(
                conn.execute(
                    "select process_path, filename, file_id from (" + model + ")"
                ).fetchall()
            )
    assert inputs[0] == inputs[1]  # Same rows and multiplicities.
    assert outputs[0] != outputs[1]
    assert outputs[0][0][0] == "/before"
    assert outputs[1][0][0] == "/after"


def build(env, directory, select=(), timeout=240):
    CONFIG.validate(env)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "--log-path",
            str(directory / "logs"),
            "build",
            "--project-dir",
            str(ROOT / "wintap_dbt"),
            "--profiles-dir",
            str(ROOT / "wintap_dbt"),
            "--target-path",
            str(directory / "target"),
            "--no-partial-parse",
            *(["--select", *select] if select else []),
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    results = json.loads((directory / "target/run_results.json").read_text())
    assert all(r["status"] in ("success", "pass") for r in results["results"])
    return len(results["results"])


def snapshot(database, env=None):
    with duckdb.connect(database, read_only=True) as conn:
        if env and env["WINTAP_DBT_RAW_SENSOR_DATASET"].startswith("s3://"):
            # DBT's temporary secret is connection-local. Set up the fresh
            # verification connection too; no credentials are persisted.
            conn.load_extension("httpfs")
            quote = lambda value: "'" + value.replace("'", "''") + "'"
            endpoint = env["AWS_ENDPOINT_URL"].removeprefix("https://").removeprefix("http://")
            conn.execute(
                "create secret (type s3, provider credential_chain, chain 'config', "
                "profile "
                + quote(env["AWS_PROFILE"])
                + ", region "
                + quote(env["AWS_DEFAULT_REGION"])
                + ", endpoint "
                + quote(endpoint)
                + ", url_style 'path', use_ssl "
                + env["DUCKDB_USE_SSL"]
                + ")"
            )
        result = {}
        for schema, table in conn.execute(
            "select table_schema, table_name from information_schema.tables where table_schema = 'main'"
        ).fetchall():
            quoted = '"' + table + '"'
            description = conn.execute("describe " + quoted).fetchall()
            cols = [c[0] for c in description]
            # Only the derived source-object filename is allowed to change.
            if table in ("stg_pidstat_metrics", "pidstat_metrics"):
                cols.remove("filename")
            select = ",".join('"' + c + '"' for c in cols)
            # Sorted result hashing bounds memory for live pidstat. Sorting is a
            # test comparison only; neither compaction nor input scans sort rows.
            reader = conn.execute("select " + select + " from " + quoted + " order by all")
            digest = hashlib.sha256()
            count = 0
            while rows := reader.fetchmany(10000):
                for row in rows:
                    digest.update((repr(row) + "\n").encode())
                count += len(rows)
            result[table] = (
                [(c[0], c[1]) for c in description if c[0] in cols],
                count,
                digest.hexdigest(),
            )
        return result


@pytest.mark.skipif(
    os.environ.get("RAW_COMPACT_DBT_TEST_APPROVED") != "isolated-local",
    reason="Explicit isolated-local compactor/DBT migration opt-in required",
)
def test_full_local_migration(env, tmp_path):
    seed_fixture(tmp_path / "input/raw_sensor")
    env.update(
        WINTAP_DBT_START_DAY="20260101",
        WINTAP_DBT_START_HOUR="23",
        WINTAP_DBT_END_DAY="20260102",
        WINTAP_DBT_END_HOUR="00",
    )
    baseline = None
    counts = []
    for state, start, end in (
        ("raw", None, None),
        ("mixed", "2026-01-01T23:00:00Z", "2026-01-02T00:00:00Z"),
        ("compacted", "2026-01-02T00:00:00Z", "2026-01-02T01:00:00Z"),
    ):
        if start:
            result = subprocess.run(
                [
                    "uv",
                    "run",
                    "--project",
                    str(ROOT / "extras/raw-sensor-compact"),
                    "--locked",
                    "raw-sensor-compact",
                    "compact",
                    "--source-root",
                    str(tmp_path / "input/raw_sensor"),
                    "--destination-root",
                    str(tmp_path / "input/compacted"),
                    "--start-hour",
                    start,
                    "--end-hour",
                    end,
                    "--target-size-bytes",
                    "1000000",
                    "--delete-source",
                ],
                capture_output=True,
                text=True,
                timeout=90,
            )
            assert result.returncode == 0, result.stdout + result.stderr
            report = json.loads(result.stdout)
            assert all(p["row_delta"] == 0 for p in report["partitions"])
            assert sum(h["source_deleted_objects"] for h in report["hours"]) == 14
        directory = tmp_path / state
        env["WINTAP_DBT_DATABASE"] = str(directory / "wintap.duckdb")
        counts.append(build(env, directory))
        actual = snapshot(env["WINTAP_DBT_DATABASE"], env)
        if baseline is None:
            baseline = actual
        else:
            assert actual == baseline, [name for name in actual if actual[name] != baseline[name]]
    assert not list((tmp_path / "input/raw_sensor").rglob("*.parquet"))
    assert baseline["process_summary"][1]  # Nonempty gold, not just empty model equivalence.
    print(
        json.dumps(
            {
                "states": ["raw", "mixed", "compacted"],
                "build_gates": counts,
                "equivalent_models": len(baseline),
                "source_files_deleted": 28,
            }
        )
    )
