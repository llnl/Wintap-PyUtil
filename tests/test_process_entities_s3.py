"""Explicitly opted-in qualification on a fresh synthetic S3 dataset; no originals."""

import hashlib
import json
import os
import subprocess
from urllib.parse import urlsplit

import boto3
import duckdb
import pytest

from .test_compacted_inputs import build, fixture_queries
from .test_dbt_source_validation import env
from .test_process_entities import COMPACTOR, RUNNER, setup


def test_isolated_s3_process_entities(env, tmp_path):
    if os.environ.get("RAW_ENTITY_S3_TEST_APPROVED") != "isolated-write":
        pytest.skip("Synthetic S3 fixture writes require explicit opt-in")
    root = os.environ["RAW_ENTITY_S3_TEST_ROOT"].rstrip("/")
    uri = urlsplit(root)
    assert uri.scheme == "s3" and "/entity-tests/dpe-" in uri.path
    profile = os.environ["RAW_ENTITY_S3_TEST_PROFILE"]
    client = boto3.Session(profile_name=profile).client("s3")
    prefix = uri.path.strip("/") + "/"
    assert not client.list_objects_v2(Bucket=uri.netloc, Prefix=prefix, MaxKeys=1).get("Contents")
    setup(env, tmp_path)
    raw = tmp_path / "input/raw_sensor"
    source_hashes = {}
    for file in raw.glob("**/*.parquet"):
        key = prefix + "raw_sensor/" + file.relative_to(raw).as_posix()
        data = file.read_bytes()
        source_hashes[key] = hashlib.sha256(data).hexdigest()
        client.put_object(Bucket=uri.netloc, Key=key, Body=data)

    def invoke(command, *extra):
        result = subprocess.run(
            [*RUNNER, "-m", "raw_sensor_compact.cli", command,
             "--source-root", root + "/raw_sensor", "--destination-root", root + "/compacted",
             "--process-entity-root", root + "/process_entities", "--entity-host", "TEST-HOST",
             "--entity-namespace", "test-pid-v2", "--profile", profile,
             "--start-hour", "2026-01-01T23:00:00Z", "--end-hour", "2026-01-02T01:00:00Z",
             "--timeout", "300", *extra], cwd=COMPACTOR, capture_output=True, text=True, timeout=360,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        return json.loads(result.stdout)

    initial = invoke("entities-init")
    entity_key = prefix + "process_entities/hostPK=" + hashlib.sha256(b"TEST-HOST").hexdigest() + "/entities.parquet"
    def get(key):
        response = client.get_object(Bucket=uri.netloc, Key=key)
        with response["Body"] as stream:
            return stream.read()
    first = get(entity_key)
    assert not invoke("entities-replay")["hosts"][0]["changed"]
    assert get(entity_key) == first
    stop = tmp_path / "stop.parquet"
    with duckdb.connect() as conn:
        conn.execute("copy (select * replace('STOP' as ActivityType, 'bridge' as ParentPidHash, "
                     "9::BIGINT as ParentPID, 134117856000000100::BIGINT as EventTime) from (select " +
                     fixture_queries()["raw_process"] + ")) to ? (format parquet)", [str(stop)])
    key = prefix + "raw_sensor/raw_process/dayPK=20260102/hourPK=00/stop.parquet"
    data = stop.read_bytes()
    source_hashes[key] = hashlib.sha256(data).hexdigest()
    client.put_object(Bucket=uri.netloc, Key=key, Body=data)
    assert invoke("entities-replay")["hosts"][0]["changed"]
    assert get(entity_key) != first
    invoke("entities-verify")
    assert all(hashlib.sha256(get(key)).hexdigest() == value for key, value in source_hashes.items())
    env.update(WINTAP_DBT_RAW_SENSOR_DATASET=root,
               WINTAP_DBT_COMPACTED_ROOT=root + "/compacted",
               WINTAP_DBT_PROCESS_ENTITY_ROOT=root + "/process_entities",
               AWS_PROFILE=profile, AWS_ENDPOINT_URL=client.meta.endpoint_url,
               AWS_DEFAULT_REGION=client.meta.region_name,
               DUCKDB_USE_SSL="true" if client.meta.endpoint_url.startswith("https:") else "false")
    gates = build(env, tmp_path / "s3-build", timeout=360)
    with duckdb.connect(env["WINTAP_DBT_DATABASE"], read_only=True) as conn:
        assert conn.execute("select count(*) from process_summary").fetchone()[0] == 33
        assert conn.execute("select count(*) from process_ancestry_quality where ancestry_status='root_reached'").fetchone()[0] == 33
        assert conn.execute("select seen_stop from stg_process_entities where pid_hash='process-one'").fetchone() == (True,)
    migrated = invoke("compact", "--target-size-bytes", "134217728", "--delete-source")
    assert all(not h["changed"] for h in migrated["process_entities"]["hosts"])
    assert not invoke("entities-replay")["hosts"][0]["changed"]
    assert get(entity_key) != first
    build(env, tmp_path / "s3-compacted-build", timeout=360)
    with duckdb.connect(env["WINTAP_DBT_DATABASE"], read_only=True) as conn:
        assert conn.execute("select count(*) from process_summary").fetchone()[0] == 33
        assert conn.execute("select count(*) from process_ancestry_quality where ancestry_status='root_reached'").fetchone()[0] == 33
    report = {"root": root, "initialized_rows": initial["hosts"][0]["rows"],
              "fixture_source_hashes_unchanged_before_migration": len(source_hashes),
              "dbt_gates_per_build": gates,
              "source_objects_deleted": sum(h.get("source_deleted_objects", 0) for h in migrated["hours"]),
              "root_reached_seeds": 33,
              "cleanup": "Not performed; synthetic S3 artifacts retained"}
    (tmp_path / "s3-evidence.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report))
