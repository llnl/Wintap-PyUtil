"""Active review controls: bounded exclusions and authenticated historical counts."""

from copy import deepcopy
from pathlib import Path

import pytest
from test_completion_verifier import rewrite_archive
from test_entities import HOURS, observation, seed
from test_entities import stores as stores_fixture
from test_entity_operation_verifier import (
    completed,
    expected_binding,
    original_observation,
    scope,
    verify_entities,
    verify_inputs,
)

from raw_sensor_compact.cli import run
from raw_sensor_compact.config import CompactionError, Dataset
from raw_sensor_compact.entity_store import update_entities
from raw_sensor_compact.inventory import Store
from raw_sensor_compact.processing_metrics import ProcessingMetricsStore

stores = stores_fixture


def evolve(conn, stores, args):
    seed(conn, stores[0], observation(activitytype="STOP", eventtime=200), hour="01")
    update_entities(
        conn, *stores, HOURS, ["HOST"], metrics_root=args.processing_metrics_root
    )


@pytest.mark.parametrize("statistic", ["rows", "snapshot_bytes", "parent_conflicts"])
def test_rehashed_historical_publication_statistics_are_not_independent_evidence(
    conn, stores, tmp_path, statistic
):
    args, original = completed(conn, stores, tmp_path)
    evolve(conn, stores, args)
    metrics = ProcessingMetricsStore(Store(Dataset.parse(args.processing_metrics_root)))

    def corrupt(records):
        records[2]["data"]["hosts"][0][statistic] += 1
        for index, row in enumerate(records[3]["data"]["rows"]):
            if statistic != "snapshot_bytes":
                key = (
                    "entity_rows" if statistic == "rows" else "entity_parent_conflicts"
                )
                row[key] += 1
            records[4 + index]["data"]["row"] = deepcopy(row)
            metrics.publish(conn, HOURS[index], row)

    rewrite_archive(args, corrupt)
    with pytest.raises(CompactionError):
        verify_entities(args, original)


def test_forward_evolution_requires_retained_independent_publication_observation(
    conn, stores, tmp_path
):
    args, original = completed(conn, stores, tmp_path)
    publication = verify_entities(args, original)
    evolve(conn, stores, args)
    with pytest.raises(CompactionError):
        verify_entities(args, original)
    result = verify_entities(args, original, publication=publication)
    assert result["authority_high_water"]["generation"] == 2
    assert result["checkpoint_generation_id"] == publication["checkpoint_generation_id"]


@pytest.mark.parametrize("stage", ["inputs", "completion"])
def test_quarantine_fingerprints_stream_without_python_invalid_row_collection(
    conn, stores, tmp_path, monkeypatch, stage
):
    args, anchor = scope(conn, stores, tmp_path)
    args.unknown_identity_policy = "quarantine"
    args.unknown_identity_max_ratio = 1.0
    seed(
        conn,
        stores[0],
        observation(pidhash="UNKNOWN", pid=None),
        filename="bad.parquet",
    )
    original = original_observation(conn, stores, args, anchor)
    if stage == "completion":
        assert run(args)[1] == 0
    import raw_sensor_compact.entities as entities

    existing = entities.rows

    def no_invalid_collection(connection, query, params=()):
        if "SELECT * FROM entity_input WHERE" in query:
            raise AssertionError(
                "verifier attempted Python invalid-row materialization"
            )
        return existing(connection, query, params)

    monkeypatch.setattr(entities, "rows", no_invalid_collection)
    if stage == "inputs":
        assert verify_inputs(args, anchor, original=original) == original
    else:
        assert verify_entities(args, original)["status"] == "verified"


@pytest.mark.parametrize("damage", ["missing", "shape", "duplicate", "bool", "extra"])
def test_original_hour_statistics_have_exact_window_shape_and_types(
    conn, stores, tmp_path, damage
):
    args, anchor = scope(conn, stores, tmp_path)
    original = original_observation(conn, stores, args, anchor)
    hours = original["inputs"]["hours"]
    if damage == "missing":
        original["inputs"]["hours"] = []
    elif damage == "shape":
        original["inputs"]["hours"] = "synthetic-malformed"
    elif damage == "duplicate":
        hours.append(deepcopy(hours[0]))
    elif damage == "bool":
        hours[0]["raw"]["files"] = True
    else:
        hours[0]["raw"]["unrecorded"] = 1
    import raw_sensor_compact.entity_operation_verifier as verifier

    with pytest.raises(CompactionError):
        verifier._original(
            original, args, original["execution_sha256"], expected_binding(args)
        )


@pytest.mark.parametrize(
    "key,value",
    [
        ("memory_limit", "33GB"),
        ("max_temp_size", "65GB"),
        ("memory_limit", "-1"),
        ("timeout", True),
        ("timeout", float("nan")),
        ("unknown_identity_max_ratio", True),
        ("unknown_identity_max_ratio", float("nan")),
        ("entity_namespace", "bad\x00namespace"),
        ("entity_host", ["H" * 257]),
    ],
)
def test_invalid_resource_or_identity_scope_refuses_before_connection(
    conn, stores, tmp_path, monkeypatch, key, value
):
    args, anchor = scope(conn, stores, tmp_path)
    setattr(args, key, value)
    import raw_sensor_compact.entity_operation_verifier as verifier

    called = []

    def forbidden(*a, **kw):
        called.append(True)
        raise AssertionError("invalid verifier scope reached a data connection")

    monkeypatch.setattr(verifier, "connection", forbidden)
    with pytest.raises(CompactionError):
        verify_inputs(args, anchor)
    assert not called


@pytest.mark.parametrize("key", ["temp_directory", "extension_directory"])
def test_verifier_work_paths_cannot_overlap_input_roots(
    conn, stores, tmp_path, monkeypatch, key
):
    args, anchor = scope(conn, stores, tmp_path)
    setattr(args, key, args.source_root)
    import raw_sensor_compact.entity_operation_verifier as verifier

    called = []

    def forbidden(*a, **kw):
        called.append(True)
        raise AssertionError("overlapping work path reached a connection")

    monkeypatch.setattr(verifier, "connection", forbidden)
    with pytest.raises(CompactionError):
        verify_inputs(args, anchor)
    assert not called


def test_resolved_remote_storage_rejects_credential_bearing_origin_before_connection(
    conn, stores, tmp_path, monkeypatch
):
    args, anchor = scope(conn, stores, tmp_path)
    for key, name in (
        ("source_root", "raw"),
        ("destination_root", "compacted"),
        ("process_entity_root", "entity"),
        ("processing_metrics_root", "metrics"),
    ):
        setattr(args, key, "s3://synthetic-test-bucket/" + name)
    args.region = "synthetic-region"
    args.endpoint_url = "https://synthetic-account:synthetic-secret@storage.invalid"
    import raw_sensor_compact.entity_operation_verifier as verifier

    called = []

    def forbidden(*a, **kw):
        called.append(True)
        raise AssertionError("invalid storage origin reached a data connection")

    monkeypatch.setattr(verifier, "connection", forbidden)
    with pytest.raises(CompactionError) as caught:
        verify_inputs(args, anchor)
    assert not called
    assert "synthetic-secret" not in str(caught.value)
    assert Path(args.run_evidence_directory).exists() is False
