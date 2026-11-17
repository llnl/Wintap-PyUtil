"""Genuine entity-only inputs/archives; public compact validators stay one hour."""

import hashlib
import json
import os
from copy import copy, deepcopy
from pathlib import Path

import pytest
from test_completion_verifier import rewrite_archive
from test_entities import HOURS, observation, seed
from test_entities import stores as stores_fixture
from test_phase_evidence import invocation

from raw_sensor_compact.cli import _verify_entity_state, run
from raw_sensor_compact.completion import verify_authority, verify_completion
from raw_sensor_compact.config import CompactionError, Dataset, hour_window
from raw_sensor_compact.engine import count_rows, schema
from raw_sensor_compact.entity_store import update_entities
from raw_sensor_compact.inventory import Store, discover
from raw_sensor_compact.phase_evidence import canonical, identity, inventory_evidence
from raw_sensor_compact.processing_metrics import (
    ProcessingMetricsStore,
    inventory_hour_stats,
)

stores = stores_fixture
EXECUTION = "e" * 64
SUPERVISION = "f" * 64


def expected_binding(args):
    return {
        "raw_root": args.source_root,
        "compacted_root": args.destination_root,
        "entity_root": args.process_entity_root,
        "metrics_root": args.processing_metrics_root,
        "namespace": args.entity_namespace,
        "hosts": args.entity_host,
        "identity_policy": args.unknown_identity_policy,
        "identity_max_ratio": args.unknown_identity_max_ratio,
        "window_start": args.start_hour,
        "window_end": args.end_hour,
        "event_types": args.event_type,
    }


def snapshot(tmp_path):
    return {
        str(path.relative_to(tmp_path)): path.read_bytes()
        for path in tmp_path.rglob("*")
        if path.is_file()
    }


def scope(conn, stores, tmp_path, *, replay=False, multi=False):
    args = invocation(
        stores, tmp_path, "entities-replay" if replay else "entities-init"
    )
    args.processing_metrics_root = str(tmp_path / "processing_metrics")
    if multi:
        args.end_hour = "2026-01-01T02:00:00Z"
    seed(conn, stores[0])
    anchor = None
    if replay:
        update_entities(
            conn,
            *stores,
            HOURS,
            ["HOST"],
            initialize=True,
            metrics_root=args.processing_metrics_root,
        )
        authority_args = copy(args)
        authority_args.command = "compact"
        authority_args.end_hour = "2026-01-01T01:00:00Z"
        anchor = verify_authority(authority_args)
    return args, anchor


def original_observation(conn, stores, args, anchor):
    """Independently constructed pre-launch fixture, never inferred from success."""
    hours = hour_window(args.start_hour, args.end_hour)
    inventories = [discover(s, hours, tuple(args.event_type)) for s in stores[:2]]
    inputs = {}
    for name, store, inventory in zip(
        ("raw", "compacted"), stores[:2], inventories, strict=True
    ):
        evidence = inventory_evidence(store, inventory)
        for item in evidence:
            files = [item["file"]["uri"]]
            item.update(rows=count_rows(conn, files), schema=schema(conn, files))
        inputs[name] = {
            "inventory_sha256": inventory.fingerprint,
            "content_sha256": identity(evidence),
            "files": len(evidence),
            "bytes": sum(item["file"]["size"] for item in evidence),
            "rows": sum(item["rows"] for item in evidence),
        }
    process = [discover(s, hours, ("raw_process",)) for s in stores[:2]]
    inputs["process_input_fingerprint"] = hashlib.sha256(
        json.dumps([i.fingerprint for i in process], sort_keys=True).encode()
    ).hexdigest()
    inputs["source_fingerprint"], inputs["compacted_fingerprint"] = (
        i.fingerprint for i in process
    )
    inputs["hours"] = [
        {
            "hour_start": hour.strftime("%Y-%m-%dT%H:00:00Z"),
            "raw": inventory_hour_stats(process[0], hour),
            "compacted": inventory_hour_stats(process[1], hour),
        }
        for hour in hours
    ]
    metrics = ProcessingMetricsStore(Store(Dataset.parse(args.processing_metrics_root)))
    checked = (
        _verify_entity_state(conn, *stores, args, metrics.store.dataset)
        if anchor
        else None
    )
    return {
        "version": 1,
        "operation": "entity-only-inputs",
        "status": "verified",
        "command": args.command,
        "run_id": args.coordinator_run_id,
        "cli_run_id": args.run_id,
        "window_start": args.start_hour,
        "window_end": args.end_hour,
        "binding_sha256": identity(expected_binding(args)),
        "execution_sha256": EXECUTION,
        "inputs": inputs,
        "authority_high_water": anchor,
        "entity_state_sha256": identity(checked) if checked else None,
        "checkpoint_state_sha256": identity([metrics.read(conn, h) for h in hours]),
    }


def verify_inputs(args, anchor, *, original=None):
    from raw_sensor_compact.entity_operation_verifier import verify_entity_inputs

    return verify_entity_inputs(
        args,
        execution_sha256=EXECUTION,
        authority_high_water=anchor,
        original=original,
    )


def verify_entities(args, original, **overrides):
    from raw_sensor_compact.entity_operation_verifier import verify_entity_completion

    return verify_entity_completion(
        args,
        execution_sha256=EXECUTION,
        supervision_sha256=SUPERVISION,
        original=original,
        **overrides,
    )


def completed(conn, stores, tmp_path, *, replay=False, multi=False):
    args, anchor = scope(conn, stores, tmp_path, replay=replay, multi=multi)
    original = original_observation(conn, stores, args, anchor)
    report, code = run(args)
    assert code == 0 and report["status"] == "completed"
    assert report["phase_evidence"]["migration_complete"] is False
    assert not list(Path(args.destination_root).rglob("*.parquet"))
    return args, original


@pytest.mark.parametrize("replay", [False, True])
@pytest.mark.parametrize("multi", [False, True])
def test_genuine_input_observation_and_postcommit_recheck_are_read_only(
    conn, stores, tmp_path, replay, multi
):
    args, anchor = scope(conn, stores, tmp_path, replay=replay, multi=multi)
    expected = original_observation(conn, stores, args, anchor)
    before = snapshot(tmp_path)
    result = verify_inputs(args, anchor)
    assert result == expected
    assert verify_inputs(args, anchor, original=result) == result
    assert snapshot(tmp_path) == before


def test_entity_operation_discovers_host_not_in_seed_scope(conn, stores, tmp_path):
    args, anchor = scope(conn, stores, tmp_path)
    seed(
        conn,
        stores[0],
        observation(hostname="OTHER", eventtime=200),
        filename="other.parquet",
    )
    original = original_observation(conn, stores, args, anchor)
    assert verify_inputs(args, anchor) == original

    report, code = run(args)
    assert code == 0
    assert [host["hostname"] for host in report["hosts"]] == ["HOST", "OTHER"]
    completed = verify_entities(args, original)
    assert completed["status"] == "verified"


@pytest.mark.parametrize("replay", [False, True])
@pytest.mark.parametrize("multi", [False, True])
def test_genuine_entity_completion_has_separate_read_only_evidence(
    conn, stores, tmp_path, replay, multi
):
    args, original = completed(conn, stores, tmp_path, replay=replay, multi=multi)
    before = snapshot(tmp_path)
    result = verify_entities(args, original)
    assert result["operation"] == "entity-only-completion"
    assert result["status"] == "verified"
    assert result["inputs_sha256"] == identity(original["inputs"])
    assert result["original_sha256"] == identity(original)
    assert result["authority_high_water"]["generation"] == (2 if replay else 1)
    assert (
        result["checkpoint_generation_id"]
        == result["authority_high_water"]["generation_id"]
    )
    assert result["checkpoint_hours"] == (2 if multi else 1)
    assert result["execution_sha256"] == EXECUTION
    assert result["supervision_sha256"] == SUPERVISION
    assert "raw_output_rows" not in result and "migration_complete" not in result
    assert snapshot(tmp_path) == before
    with pytest.raises(CompactionError):
        verify_completion(
            args,
            execution_sha256=EXECUTION,
            supervision_sha256=SUPERVISION,
            authority_high_water=None,
        )
    with pytest.raises(CompactionError):
        verify_authority(args)


@pytest.mark.parametrize("side", [0, 1])
@pytest.mark.parametrize("damage", ["late", "content", "missing", "marker"])
def test_same_count_content_and_extra_prefix_objects_refuse_before_launch(
    conn, stores, tmp_path, side, damage
):
    args, anchor = scope(conn, stores, tmp_path)
    if side:
        seed(conn, stores[1], filename="output.parquet")
    original = original_observation(conn, stores, args, anchor)
    root = stores[side]
    path = next(Path(root.dataset.root).rglob("*.parquet"))
    if damage == "late":
        seed(conn, root, filename="late.parquet")
    elif damage == "content":
        path.unlink()
        seed(
            conn, root, observation(processname="same-count-change"), filename=path.name
        )
    elif damage == "missing":
        path.unlink()
    else:
        path.with_name("unrecorded-marker").write_bytes(b"synthetic marker")
    before = snapshot(tmp_path)
    with pytest.raises(CompactionError):
        verify_inputs(args, anchor, original=original)
    assert snapshot(tmp_path) == before


@pytest.mark.parametrize("damage", ["late", "content", "missing", "output"])
def test_original_entity_completion_refuses_raw_or_compacted_mutation(
    conn, stores, tmp_path, damage
):
    args, original = completed(conn, stores, tmp_path)
    source = next(Path(args.source_root).rglob("*.parquet"))
    if damage == "late":
        seed(conn, stores[0], filename="late.parquet")
    elif damage == "content":
        source.unlink()
        seed(conn, stores[0], observation(eventtime=101))
    elif damage == "missing":
        source.unlink()
    else:
        seed(conn, stores[1])
    with pytest.raises(CompactionError):
        verify_entities(args, original)


@pytest.mark.parametrize(
    "damage",
    [
        "raw_phase",
        "order",
        "terminal",
        "terminal_bool",
        "intent",
        "checkpoint",
        "checkpoint_bool",
        "generation",
        "fingerprint",
        "host",
        "host_rows",
        "extra",
    ],
)
def test_rehashed_entity_archive_cannot_fake_clean_original_scope(
    conn, stores, tmp_path, damage
):
    args, original = completed(conn, stores, tmp_path)

    def corrupt(records):
        entity = records[2]["data"]
        checkpoint = records[3]["data"]["rows"][0]
        if damage == "raw_phase":
            records.insert(-1, {**records[-1], "phase": "raw_complete"})
        elif damage == "order":
            records[1], records[2] = records[2], records[1]
        elif damage == "terminal":
            records[-1]["phase"] = "blocked"
        elif damage == "terminal_bool":
            records[-1]["data"]["exit_code"] = False
        elif damage == "intent":
            records[1]["data"]["operation"] = "compact"
        elif damage == "checkpoint":
            checkpoint["raw_status"] = "merged"
        elif damage == "checkpoint_bool":
            checkpoint["entity_mode"] = 1
        elif damage == "generation":
            entity["generation_id"] = "0" * 64
        elif damage == "fingerprint":
            entity["process_input_fingerprint"] = "0" * 64
        elif damage == "host":
            entity["hosts"][0]["hostname"] = "UNEXPECTED-SYNTHETIC-HOST"
        elif damage == "host_rows":
            entity["hosts"][0]["rows"] += 1
        else:
            entity["unrecorded"] = True

    rewrite_archive(args, corrupt)
    with pytest.raises(CompactionError):
        verify_entities(args, original)


@pytest.mark.parametrize("damage", ["rollback", "snapshot", "pending", "checkpoint"])
def test_independent_authority_and_checkpoint_uncertainty_stays_blocked(
    conn, stores, tmp_path, damage
):
    args, anchor = scope(conn, stores, tmp_path, replay=True)
    authority = Path(args.process_entity_root) / "_authority/authority.parquet"
    old = authority.read_bytes()
    original = original_observation(conn, stores, args, anchor)
    assert run(args)[1] == 0
    if damage == "rollback":
        authority.write_bytes(old)
    elif damage == "snapshot":
        path = next(Path(args.process_entity_root).glob("hostPK=*/*.parquet"))
        path.write_bytes(path.read_bytes() + b"changed")
    elif damage == "pending":
        conn.execute(
            "CREATE TEMP TABLE corrupt AS SELECT * FROM read_parquet(?)",
            [str(authority)],
        )
        conn.execute(
            "UPDATE corrupt SET status='pending' WHERE generation="
            "(SELECT max(generation) FROM corrupt)"
        )
        authority.unlink()
        conn.execute("COPY corrupt TO ? (FORMAT PARQUET)", [str(authority)])
    else:
        next(Path(args.processing_metrics_root).rglob("*.parquet")).unlink()
    with pytest.raises(CompactionError):
        verify_entities(args, original)


def test_forward_evolution_keeps_original_generation_checkpoint_not_current_hash(
    conn, stores, tmp_path
):
    args, original = completed(conn, stores, tmp_path)
    publication = verify_entities(args, original)
    seed(conn, stores[0], observation(activitytype="STOP", eventtime=200), hour="01")
    update_entities(
        conn, *stores, HOURS, ["HOST"], metrics_root=args.processing_metrics_root
    )
    result = verify_entities(args, original, publication=publication)
    assert result["authority_high_water"]["generation"] == 2
    assert (
        result["checkpoint_generation_id"]
        != result["authority_high_water"]["generation_id"]
    )


@pytest.mark.parametrize("kind", ["hardlink", "fifo"])
@pytest.mark.parametrize(
    "target", ["raw", "snapshot", "authority", "checkpoint", "archive"]
)
def test_nonregular_or_aliased_objects_refuse_without_blocking_read(
    conn, stores, tmp_path, monkeypatch, kind, target
):
    args, original = completed(conn, stores, tmp_path)
    paths = {
        "raw": next(Path(args.source_root).rglob("*.parquet")),
        "snapshot": next(Path(args.process_entity_root).glob("hostPK=*/*.parquet")),
        "authority": Path(args.process_entity_root) / "_authority/authority.parquet",
        "checkpoint": next(Path(args.processing_metrics_root).rglob("*.parquet")),
        "archive": next(
            (Path(args.run_evidence_directory) / args.run_id).glob("*complete.json")
        ),
    }
    path = paths[target]
    if kind == "hardlink":
        os.link(path, tmp_path / "alias")
    else:
        path.unlink()
        os.mkfifo(path)
    original_open = Path.open
    opened = []

    def guarded(self, *a, **kw):
        if self == path:
            opened.append(self)
            if kind == "fifo":
                raise OSError("synthetic guard prevents FIFO blocking")
        return original_open(self, *a, **kw)

    monkeypatch.setattr(Path, "open", guarded)
    with pytest.raises(CompactionError):
        verify_entities(args, original)
    assert not opened


@pytest.mark.parametrize(
    "damage", ["files", "rows", "bytes", "digest", "command", "version"]
)
def test_untrusted_original_observation_does_not_override_independent_inputs(
    conn, stores, tmp_path, damage
):
    args, original = completed(conn, stores, tmp_path)
    original = deepcopy(original)
    if damage in {"files", "rows", "bytes"}:
        original["inputs"]["raw"][damage] += 1
    elif damage == "digest":
        original["inputs"]["raw"]["content_sha256"] = "0" * 64
    elif damage == "command":
        original["command"] = "compact"
    else:
        original["version"] = True
    with pytest.raises(CompactionError):
        verify_entities(args, original)


@pytest.mark.parametrize("budget", ["max_files", "max_entity_rows", "max_entity_bytes"])
def test_limits_refuse_without_pruning_or_data_mutation(conn, stores, tmp_path, budget):
    args, original = completed(conn, stores, tmp_path)
    if budget == "max_files":
        seed(conn, stores[0], filename="extra.parquet")
    elif budget == "max_entity_rows":
        seed(conn, stores[0], observation(pidhash="second"), hour="01")
        update_entities(
            conn, *stores, HOURS, ["HOST"], metrics_root=args.processing_metrics_root
        )
    setattr(args, budget, 1)
    before = snapshot(tmp_path)
    with pytest.raises(CompactionError):
        verify_entities(args, original)
    assert snapshot(tmp_path) == before


@pytest.mark.parametrize("stage", ["inputs", "completion"])
def test_final_input_recheck_detects_late_content_arrival(
    conn, stores, tmp_path, monkeypatch, stage
):
    args, anchor = scope(conn, stores, tmp_path)
    original = original_observation(conn, stores, args, anchor)
    if stage == "completion":
        assert run(args)[1] == 0
    import raw_sensor_compact.entity_operation_verifier as verifier

    observe = verifier._inputs
    calls = []

    def late(*a, **kw):
        result = observe(*a, **kw)
        calls.append(True)
        if len(calls) == 1:
            seed(conn, stores[0], filename="during-verification.parquet")
        return result

    monkeypatch.setattr(verifier, "_inputs", late)
    with pytest.raises(CompactionError):
        if stage == "inputs":
            verify_inputs(args, anchor, original=original)
        else:
            verify_entities(args, original)


@pytest.mark.parametrize(
    "damage", ["checksum", "version", "shape", "help", "arguments", "budget"]
)
def test_separate_request_is_bounded_canonical_and_suppresses_sensitive_errors(
    conn, stores, tmp_path, monkeypatch, capsys, damage
):
    args, anchor = scope(conn, stores, tmp_path)
    import raw_sensor_compact.entity_operation_verifier as verifier

    sensitive = "synthetic-sensitive-token"
    value = {
        "version": 1,
        "operation": "entity-only-inputs",
        "arguments": [
            "entities-init",
            "--source-root",
            args.source_root,
            "--destination-root",
            args.destination_root,
        ],
        "execution_sha256": EXECUTION,
        "ledger_id": "a" * 32,
        "dataset_sha256": "b" * 64,
        "authority_high_water": anchor,
        "original": None,
    }
    if damage == "version":
        value["version"] = True
    elif damage == "shape":
        value["extra"] = sensitive
    elif damage == "help":
        value["arguments"] += ["--help"]
    elif damage == "arguments":
        value["arguments"] += ["--threads", sensitive]
    elif damage == "budget":
        monkeypatch.setattr(verifier, "MAX_REQUEST_BYTES", 1)
    path = tmp_path / "request.json"
    path.write_text(canonical(value))
    with pytest.raises(CompactionError) as caught:
        verifier._request(path, "0" * 64 if damage == "checksum" else identity(value))
    captured = capsys.readouterr()
    assert sensitive not in captured.out + captured.err + str(caught.value)
