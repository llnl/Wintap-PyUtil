"""Independent read-only disposition checks, including rehashed semantic damage."""

import json
from copy import copy
from pathlib import Path

import pytest
from test_completion_verifier import (
    EXECUTION,
    SUPERVISION,
    archived,
    baseline,
    rewrite_archive,
)
from test_deletion_recovery import partial, repair_args
from test_entities import HOURS, observation, seed
from test_entities import stores as stores_fixture
from test_phase_evidence import invocation

from raw_sensor_compact.cli import run
from raw_sensor_compact.completion import (
    _authority,
    verify_authority,
    verify_completion,
)
from raw_sensor_compact.config import CompactionError, Limits, hour_window
from raw_sensor_compact.deletion import DeletionError, delete_hour
from raw_sensor_compact.deletion_recovery import verify_disposition
from raw_sensor_compact.entity_store import update_entities
from raw_sensor_compact.inventory import Store
from raw_sensor_compact.phase_evidence import identity, load_archive
from raw_sensor_compact.processing_metrics import ProcessingMetricsStore

stores = stores_fixture


def repaired(conn, stores, tmp_path, monkeypatch, *, entity=True):
    args = partial(conn, stores, tmp_path, monkeypatch, entity=entity)
    anchor = baseline(args) if entity else None
    repair = repair_args(args)
    assert run(repair)[1] == 0
    return args, repair, anchor


def verify(args, repair, anchor):
    return verify_completion(
        args,
        execution_sha256=EXECUTION,
        supervision_sha256=SUPERVISION,
        authority_high_water=anchor,
        repair_run_id=repair.run_id,
        repair_coordinator_run_id=repair.coordinator_run_id,
        repair_replace_checkpoints=repair.confirm_replace_checkpoints,
        repair_operation="deletions-repair",
    )


@pytest.mark.parametrize("entity", [False, True])
def test_disposition_verifier_is_read_only(conn, stores, tmp_path, monkeypatch, entity):
    args, repair, anchor = repaired(conn, stores, tmp_path, monkeypatch, entity=entity)
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    with monkeypatch.context() as patch:
        patch.setattr(
            "raw_sensor_compact.deletion.delete_hour",
            lambda *a, **k: pytest.fail("deleted during verification"),
        )
        patch.setattr(
            "raw_sensor_compact.cli.update_entities",
            lambda *a, **k: pytest.fail("replayed during verification"),
        )
        patch.setattr(
            "raw_sensor_compact.processing_metrics.ProcessingMetricsStore.publish",
            lambda *a, **k: pytest.fail("published during verification"),
        )
        result = verify(args, repair, anchor)
    assert result["repair_operation"] == "deletions-repair"
    assert result["raw_source_rows"] == result["raw_output_rows"] == 2
    assert {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == before


def test_disposition_preserves_historical_checkpoint_through_forward_entity_evolution(
    conn, stores, tmp_path, monkeypatch
):
    args, repair, anchor = repaired(conn, stores, tmp_path, monkeypatch)
    before = verify(args, repair, anchor)
    seed(conn, stores[0], observation(activitytype="STOP", eventtime=200), hour="01")
    update_entities(conn, *stores, HOURS, ["HOST"])
    after = verify(args, repair, before["authority_high_water"])
    assert (
        after["authority_high_water"]["generation"]
        > before["authority_high_water"]["generation"]
    )
    assert after["checkpoint_sha256"] == before["checkpoint_sha256"]
    assert after["checkpoint_generation_id"] == before["checkpoint_generation_id"]


@pytest.mark.parametrize(
    "damage",
    [
        "terminal_boolean",
        "terminal_integer",
        "replacement_integer",
        "wrong_operation",
        "wrong_run",
        "wrong_coordinator",
        "missing_disposition",
        "duplicate_disposition",
        "survivor_identity",
        "deleted_count",
        "remaining_keys",
        "original_link",
        "original_terminal_integer",
        "original_candidate",
        "duplicate_checkpoint",
        "repair_row",
        "repair_generation",
        "missing_prior",
        "replacement_unapproved",
        "checkpoint",
        "source",
        "output",
    ],
)
def test_rehashed_disposition_damage_cannot_certify_completion(
    conn, stores, tmp_path, monkeypatch, damage
):
    args, repair, anchor = repaired(conn, stores, tmp_path, monkeypatch)
    verify(args, repair, anchor)

    def corrupt(values):
        intent = next(
            (r["data"] for r in values if r["phase"] == "repair_intent"), None
        )
        if damage == "terminal_boolean":
            values[-1]["data"]["exit_code"] = False
        elif damage == "terminal_integer":
            values[-1]["data"]["migration_complete"] = 1
        elif damage == "replacement_integer":
            intent["confirmed_replacement"] = 0
        elif damage == "wrong_operation":
            values[0]["data"]["command"] = "checkpoints-repair"
        elif damage in {"wrong_run", "wrong_coordinator"}:
            for value in values:
                value["run_id" if damage == "wrong_run" else "coordinator_run_id"] = (
                    "f" * 32
                )
        elif damage == "missing_disposition":
            values[:] = [r for r in values if r["phase"] != "deletion_repair_verified"]
        elif damage == "duplicate_disposition":
            values.insert(
                -1,
                copy(
                    next(r for r in values if r["phase"] == "deletion_repair_verified")
                ),
            )
        elif damage == "survivor_identity":
            next(r["data"] for r in values if r["phase"] == "deletion_repair_intent")[
                "survivors"
            ][0]["sha256"] = "0" * 64
        elif damage in {"deleted_count", "remaining_keys"}:
            after = next(
                r["data"] for r in values if r["phase"] == "deletion_repair_verified"
            )
            after[
                "deleted_objects" if damage == "deleted_count" else "remaining_keys"
            ] = 999 if damage == "deleted_count" else ["synthetic-survivor"]
        elif damage == "original_link":
            intent["original_last_sha256"] = "0" * 64
        elif damage == "original_terminal_integer":
            values[-1]["data"]["migration_complete"] = 0
        elif damage == "original_candidate":
            next(r["data"] for r in values if r["phase"] == "checkpoint_intent")[
                "rows"
            ][0]["raw_output_rows"] += 1
        elif damage == "duplicate_checkpoint":
            values.insert(
                -1, copy(next(r for r in values if r["phase"] == "checkpoint_verified"))
            )
        elif damage == "repair_row":
            intent["rows"][0]["entity_input_fingerprint"] = "0" * 64
        elif damage == "repair_generation":
            next(r["data"] for r in values if r["phase"] == "entities_verified")[
                "generation_id"
            ] = "0" * 64
        elif damage == "missing_prior":
            intent.pop("replaced_checkpoints")
        elif damage == "replacement_unapproved":
            old = copy(intent["rows"][0])
            old["run_id"] = "f" * 32
            intent.update(replaced_checkpoints=[old], confirmed_replacement=True)

    if damage == "checkpoint":
        next(
            Path(stores[0].dataset.sibling("processing_metrics").root).rglob(
                "*.parquet"
            )
        ).unlink()
    elif damage == "source":
        seed(conn, stores[0])
    elif damage == "output":
        output = next(Path(stores[1].dataset.root).rglob("*.parquet"))
        output.write_bytes(output.read_bytes() + b"changed")
    else:
        original_damage = damage in {
            "original_terminal_integer",
            "original_candidate",
            "duplicate_checkpoint",
        }
        rewrite_archive(args if original_damage else repair, corrupt)
        if original_damage:
            # Authenticate the changed original so rejection must be semantic.
            rewrite_archive(
                repair,
                lambda values: next(
                    r["data"] for r in values if r["phase"] == "repair_intent"
                ).update(original_last_sha256=archived(args)[1][-1]["sha256"]),
            )
    with pytest.raises(CompactionError):
        verify(args, repair, anchor)


@pytest.mark.parametrize("entity", [False, True])
def test_multi_hour_disposition_uses_actual_interleaved_original_order(
    conn, stores, tmp_path, monkeypatch, entity
):
    for hour in ("00", "01"):
        path = seed(conn, stores[0], hour=hour, filename="first.parquet")
        seed(conn, stores[0], hour=hour, filename="second.parquet")
        other = (
            Path(stores[0].dataset.root)
            / f"raw_file/dayPK=20260101/hourPK={hour}/part.parquet"
        )
        other.parent.mkdir(parents=True)
        other.write_bytes(path.read_bytes())
    hours = hour_window("2026-01-01T00:00:00Z", "2026-01-01T03:00:00Z")
    if entity:
        update_entities(conn, *stores, hours, ["HOST"], initialize=True)
    args = invocation(stores, tmp_path)
    args.end_hour, args.event_type = "2026-01-01T03:00:00Z", ["raw_file", "raw_process"]
    if not entity:
        args.process_entity_root, args.entity_host, args.entity_namespace = (
            None,
            [],
            None,
        )

    def fail_once(source, parent, **kwargs):
        if parent.event_type == "raw_process" and parent.hour == "00":
            next(
                Path(source.dataset.file_uri(parent.relative)).glob("*.parquet")
            ).unlink()
            raise DeletionError("synthetic partial deletion")
        return delete_hour(source, parent, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr("raw_sensor_compact.compaction.delete_hour", fail_once)
        assert run(args)[1] == 2
    repair = repair_args(args)
    repair.event_type = args.event_type
    originals = {p: p.read_bytes() for p in archived(args)[0].iterdir()}
    assert run(repair)[1] == 0
    assert not list(Path(stores[0].dataset.root).rglob("*.parquet"))
    assert all(p.read_bytes() == data for p, data in originals.items())
    # Public completion/authority verification intentionally accepts one hour only.
    with pytest.raises(CompactionError, match="Select 1–max-hours"):
        verify(args, repair, None)
    with pytest.raises(CompactionError, match="Select 1–max-hours"):
        verify_authority(args)
    original_records = load_archive(args.run_evidence_directory, args.run_id)
    repair_records = load_archive(args.run_evidence_directory, repair.run_id)
    metrics = None
    if entity:
        metrics = ProcessingMetricsStore(
            Store(stores[0].dataset.sibling("processing_metrics"))
        )
        # Populate independently validated current authority/receipt tables before
        # using the lower-level multi-hour disposition seam; no public scope bypass.
        _authority(conn, *stores, args, metrics)
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    raw, row = verify_disposition(
        conn,
        args,
        original_records,
        repair_records,
        original_records[0]["data"]["binding"],
        *stores[:2],
        metrics,
        hours,
        tuple(args.event_type),
        Limits(max_files=args.max_files),
        repair_run_id=repair.run_id,
        repair_coordinator_run_id=repair.coordinator_run_id,
        replacement_authorized=repair.confirm_replace_checkpoints,
    )
    assert len(raw["hours"]) == 6  # Two populated hours plus an empty hour, two events.
    assert sum(p["source_rows"] for p in raw["partitions"]) == 6
    assert (
        (row["raw_source_rows"] == row["raw_output_rows"] == 3)
        if entity
        else row is None
    )
    assert {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == before
    assert all(p.read_bytes() == data for p, data in originals.items())


@pytest.mark.parametrize("damage", [None, "operation", "version", "missing", "integer"])
def test_version_four_request_has_exact_operation_scope(tmp_path, damage):
    from raw_sensor_compact.completion import _request
    from raw_sensor_compact.phase_evidence import canonical

    value = {
        "version": 4,
        "arguments": [
            "compact",
            "--source-root",
            str(tmp_path / "raw"),
            "--destination-root",
            str(tmp_path / "compacted"),
            "--start-hour",
            "2026-01-01T00:00:00Z",
            "--end-hour",
            "2026-01-01T01:00:00Z",
            "--target-size-bytes",
            "32",
        ],
        "execution_sha256": EXECUTION,
        "supervision_sha256": SUPERVISION,
        "intent_sha256": "a" * 64,
        "authority_high_water": None,
        "repair_run_id": "c" * 32,
        "repair_coordinator_run_id": "d" * 32,
        "repair_replace_checkpoints": False,
        "repair_operation": "deletions-repair",
    }
    if damage == "operation":
        value["repair_operation"] = "checkpoints-repair"
    elif damage == "version":
        value["version"] = 3
    elif damage == "missing":
        value.pop("repair_operation")
    elif damage == "integer":
        value["repair_replace_checkpoints"] = 0
    path = tmp_path / "request.json"
    path.write_text(canonical(value))
    if damage is None:
        assert _request(path, identity(value))[0] == json.loads(path.read_text())
    else:
        with pytest.raises(CompactionError):
            _request(path, identity(value))
