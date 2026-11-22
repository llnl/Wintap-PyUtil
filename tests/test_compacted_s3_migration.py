"""Opt-in live telemetry copies: never mutate the original raw/compacted roots."""

import hashlib
import json
import os
import re
import subprocess
from urllib.parse import urlsplit

import boto3
import pytest

from .test_compacted_inputs import build, snapshot
from .test_dbt_source_validation import ROOT, env

DBT_EVENTS = (
    "pidstat",
    "raw_host",
    "raw_process",
    "raw_process_file",
    "raw_process_conn_incr",
    "raw_macip",
    "raw_macip_sensor",
    "raw_process_registry",
    "raw_registry",
    "raw_imageload",
    "raw_image_load",
    "raw_selinuxavc",
    "raw_selinuxinteraction",
    "raw_selinuxtransition",
)


def any_value_model_closure(manifest):
    """Models containing ANY_VALUE and their dependents; not a causal proof."""
    nodes = {
        key: node for key, node in manifest["nodes"].items() if node["resource_type"] == "model"
    }
    affected = {
        key
        for key, node in nodes.items()
        if re.search(
            r"\bany_value\s*\(",
            node.get("compiled_code") or node.get("raw_code", ""),
            re.IGNORECASE,
        )
    }
    while True:
        downstream = {
            key for key, node in nodes.items() if affected.intersection(node["depends_on"]["nodes"])
        }
        expanded = affected | downstream
        if expanded == affected:
            return {nodes[key]["name"] for key in affected}
        affected = expanded


def compare_snapshots(baseline, actual, diagnostic_models=()):
    """Never waive schemas/bronze fidelity; explicitly report accepted drift."""
    # Loaded evidence and fresh snapshots may use JSON lists vs Python tuples.
    baseline, actual = (json.loads(json.dumps(value)) for value in (baseline, actual))
    assert actual.keys() == baseline.keys(), "Model set changed"
    diagnostics = {}
    unexpected = []
    for name in baseline:
        assert actual[name][0] == baseline[name][0], f"Schema changed: {name}"
        if actual[name] != baseline[name]:
            if name.startswith("stg_") or name not in diagnostic_models:
                unexpected.append(name)
            else:
                diagnostics[name] = {
                    "baseline_rows": baseline[name][1],
                    "actual_rows": actual[name][1],
                    "baseline_sha256": baseline[name][2],
                    "actual_sha256": actual[name][2],
                }
    assert not unexpected, f"Unexpected model result changes: {unexpected}"
    return diagnostics


def test_any_value_closure_includes_dependents_only():
    nodes = {
        "model.bronze": {
            "resource_type": "model",
            "name": "stg_process",
            "raw_code": "select *",
            "depends_on": {"nodes": []},
        },
        "model.process": {
            "resource_type": "model",
            "name": "process",
            "compiled_code": "select ANY_VALUE (path)",
            "depends_on": {"nodes": ["model.bronze"]},
        },
        "model.summary": {
            "resource_type": "model",
            "name": "summary",
            "raw_code": "select *",
            "depends_on": {"nodes": ["model.process"]},
        },
        "model.stable": {
            "resource_type": "model",
            "name": "stable",
            "raw_code": "select count(*)",
            "depends_on": {"nodes": ["model.bronze"]},
        },
    }
    assert any_value_model_closure({"nodes": nodes}) == {"process", "summary"}


def test_accepted_drift_is_reported_not_normalized():
    baseline = {"process": ([("path", "VARCHAR")], 2, "old")}
    actual = {"process": ([("path", "VARCHAR")], 3, "new")}
    with pytest.raises(AssertionError, match="Unexpected"):
        compare_snapshots(baseline, actual)
    assert compare_snapshots(baseline, actual, {"process"})["process"]["actual_rows"] == 3


@pytest.mark.parametrize("kind", ["bronze", "schema", "unaffected", "model-set"])
def test_accepted_drift_does_not_waive_integrity(kind):
    name = "stg_process" if kind == "bronze" else "process"
    baseline = {name: ([("value", "BIGINT")], 2, "old")}
    actual = {name: ([("value", "VARCHAR" if kind == "schema" else "BIGINT")], 2, "new")}
    if kind == "model-set":
        actual = {}
    with pytest.raises(AssertionError):
        compare_snapshots(baseline, actual, set() if kind == "unaffected" else {name})


@pytest.mark.skipif(
    os.environ.get("RAW_COMPACT_DBT_TEST_APPROVED") != "isolated-s3-copy-delete",
    reason="Explicit copied S3 fixture writes/deletion required",
)
def test_copied_live_hour_migration(env, tmp_path):
    scope = os.environ.get("RAW_COMPACT_DBT_TEST_SCOPE", "host-pidstat")
    assert scope in ("host-pidstat", "all-models")
    full = scope == "all-models"
    comparison = os.environ.get("RAW_COMPACT_DBT_COMPARISON", "strict")
    assert comparison in ("strict", "any-value-diagnostics")
    assert comparison == "strict" or full, "Diagnostic comparison requires full model scope"
    # Destination is supplied explicitly; restrict test deletion to a new test subtree.
    uri = urlsplit(os.environ["RAW_COMPACT_DBT_TEST_ROOT"])
    prefix = uri.path.strip("/")
    assert uri.scheme == "s3" and uri.netloc == "lintap-alpha"
    assert prefix.startswith("compaction-tests/dbt-union-")
    assert not uri.query and not uri.fragment and ".." not in prefix.split("/")
    session = boto3.Session(profile_name="lintap-alpha")
    client = session.client("s3")
    assert not client.list_objects_v2(Bucket=uri.netloc, Prefix=prefix + "/", MaxKeys=1).get(
        "Contents"
    ), "Fixture root must initially be absent"
    source_keys = []
    inventory = {}
    for event in DBT_EVENTS if full else ("raw_host", "pidstat"):
        objects = []
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket="lintap-alpha", Prefix=f"lintap/raw_sensor/{event}/dayPK=20261111/hourPK=08/"
        ):
            objects.extend(
                item for item in page.get("Contents", []) if item["Key"].endswith(".parquet")
            )
        if objects:
            source_keys.extend(item["Key"] for item in objects)
            inventory[event] = {"files": len(objects), "bytes": sum(o["Size"] for o in objects)}
    if full:
        assert {
            "raw_host",
            "pidstat",
            "raw_process",
            "raw_process_file",
            "raw_process_conn_incr",
        } <= inventory.keys()
        assert len(source_keys) <= 1000, "Bounded live fixture file limit exceeded"
        assert (
            sum(v["bytes"] for v in inventory.values()) <= 256 * 1024 * 1024
        ), "Bounded live fixture byte limit exceeded"
    else:
        assert len(source_keys) == 29, "Bounded previously qualified hour changed"
    (tmp_path / "inventory.json").write_text(json.dumps(inventory, indent=2))
    print(json.dumps({"scope": scope, "inventory": inventory}), flush=True)

    def hash_keys(keys):
        result = {}
        for key in keys:
            digest = hashlib.sha256()
            with client.get_object(Bucket="lintap-alpha", Key=key)["Body"] as body:
                for chunk in iter(lambda: body.read(1024 * 1024), b""):
                    digest.update(chunk)
            result[key] = digest.hexdigest()
        return result

    before = hash_keys(source_keys)
    (tmp_path / "original-sha256.json").write_text(json.dumps(before, indent=2))
    for key in source_keys:
        client.copy_object(
            Bucket=uri.netloc,
            Key=prefix + "/raw_sensor/" + key.removeprefix("lintap/raw_sensor/"),
            CopySource={"Bucket": "lintap-alpha", "Key": key},
        )
    env.update(
        WINTAP_DBT_RAW_SENSOR_DATASET=f"s3://{uri.netloc}/{prefix}",
        WINTAP_DBT_START_DAY="20261111",
        WINTAP_DBT_END_DAY="20261111",
        WINTAP_DBT_START_HOUR="08",
        WINTAP_DBT_END_HOUR="08",
        AWS_PROFILE="lintap-alpha",
        AWS_DEFAULT_REGION=client.meta.region_name,
        AWS_ENDPOINT_URL=client.meta.endpoint_url,
        DUCKDB_USE_SSL="true" if client.meta.endpoint_url.startswith("https://") else "false",
    )
    baseline = None
    gates = []
    reports = []
    differences = {}
    diagnostic_models = set()
    for state, events in (
        ("raw", ()),
        ("mixed", ("raw_host",)),
        ("compacted", tuple(e for e in inventory if e != "raw_host")),
    ):
        print(json.dumps({"state": state, "migrate_events": events}), flush=True)
        if events:
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
                    f"s3://{uri.netloc}/{prefix}/raw_sensor",
                    "--destination-root",
                    f"s3://{uri.netloc}/{prefix}/compacted",
                    "--start-hour",
                    "2026-11-11T08:00:00Z",
                    "--end-hour",
                    "2026-11-11T09:00:00Z",
                    *[arg for event in events for arg in ("--event-type", event)],
                    "--target-size-bytes",
                    "134217728",
                    "--delete-source",
                    "--profile",
                    "lintap-alpha",
                    "--timeout",
                    "600" if full else "180",
                ],
                env=env,
                capture_output=True,
                text=True,
                timeout=660 if full else 240,
            )
            assert result.returncode == 0, result.stdout + result.stderr
            report = json.loads(result.stdout)
            assert all(p["row_delta"] == 0 for p in report["partitions"])
            reports.append(report)
            (tmp_path / f"{state}-merge.json").write_text(json.dumps(report, indent=2))
        directory = tmp_path / state
        env["WINTAP_DBT_DATABASE"] = str(directory / "wintap.duckdb")
        gates.append(
            build(
                env,
                directory,
                select=(
                    ()
                    if full
                    else ("stg_raw_host", "host", "stg_pidstat_metrics", "pidstat_metrics")
                ),
                timeout=600 if full else 240,
            )
        )
        actual = snapshot(env["WINTAP_DBT_DATABASE"], env)
        (directory / "snapshot.json").write_text(json.dumps(actual, indent=2))
        if baseline is None:
            baseline = actual
            assert baseline["stg_pidstat_metrics"][1] == 897390
            if full:
                assert len(baseline) == 54
                assert baseline["process_summary"][1] > 0
            if comparison == "any-value-diagnostics":
                diagnostic_models = any_value_model_closure(
                    json.loads((directory / "target/manifest.json").read_text())
                )
        else:
            differences[state] = compare_snapshots(baseline, actual, diagnostic_models)
            (directory / "comparison.json").write_text(json.dumps(differences[state], indent=2))
        if state == "mixed":
            assert not client.list_objects_v2(
                Bucket=uri.netloc, Prefix=prefix + "/raw_sensor/raw_host/", MaxKeys=1
            ).get("Contents")
            assert client.list_objects_v2(
                Bucket=uri.netloc, Prefix=prefix + "/raw_sensor/pidstat/", MaxKeys=1
            ).get("Contents")
    assert not client.list_objects_v2(
        Bucket=uri.netloc, Prefix=prefix + "/raw_sensor/", MaxKeys=1
    ).get("Contents")
    assert before == hash_keys(source_keys)
    assert sum(h["source_deleted_objects"] for r in reports for h in r["hours"]) == len(source_keys)
    summary = {
        "scope": scope,
        "comparison": comparison,
        "event_inventory": inventory,
        "fixture_root": f"s3://{uri.netloc}/{prefix}",
        "states": ["raw", "mixed", "compacted"],
        "build_gates": gates,
        "models_built": len(baseline),
        "exact_models_all_states": len(baseline)
        - len(set().union(*(set(d) for d in differences.values()))),
        "bronze_models_equal": sum(name.startswith("stg_") for name in baseline),
        "any_value_reachable_models": sorted(diagnostic_models),
        "downstream_differences": differences,
        "source_rows": sum(p["source_rows"] for r in reports for p in r["partitions"]),
        "leaf_rows": [p for r in reports for p in r["partitions"]],
        "copied_source_objects_deleted": len(source_keys),
        "original_sha256_unchanged": len(source_keys),
    }
    (tmp_path / "summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary), flush=True)
