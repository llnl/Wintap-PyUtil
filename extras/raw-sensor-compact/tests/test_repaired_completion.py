"""Read-only verification of actual failed checkpoint phases and exact repair."""

import json
from copy import copy
from pathlib import Path

import pytest
from test_completion_verifier import (
    EXECUTION,
    SUPERVISION,
    archived,
    args_for,
    baseline,
    rewrite_archive,
)
from test_entities import HOURS, observation, seed
from test_entities import stores as stores_fixture

from raw_sensor_compact.cli import parser, run
from raw_sensor_compact.completion import verify_completion
from raw_sensor_compact.config import CompactionError
from raw_sensor_compact.entity_store import update_entities
from raw_sensor_compact.phase_evidence import canonical, identity
from raw_sensor_compact.processing_metrics import ProcessingMetricsStore

stores = stores_fixture


def failed_and_repaired(conn, stores, tmp_path, monkeypatch, *, delete=True):
    args = args_for(stores, tmp_path, entities=True, delete=delete)
    seed(conn, stores[0])
    update_entities(conn, *stores, HOURS, ["HOST"], initialize=True)
    anchor = baseline(args)
    publish = ProcessingMetricsStore.publish

    def obstruct(*a, **kw):
        raise CompactionError("synthetic checkpoint publication obstruction")

    monkeypatch.setattr(ProcessingMetricsStore, "publish", obstruct)
    with pytest.raises(CompactionError):
        run(args)
    monkeypatch.setattr(ProcessingMetricsStore, "publish", publish)
    repair = parser().parse_args(
        [
            "checkpoints-repair",
            "--source-root",
            args.source_root,
            "--destination-root",
            args.destination_root,
            "--start-hour",
            args.start_hour,
            "--end-hour",
            args.end_hour,
            "--repair-run-id",
            args.run_id,
        ]
    )
    for name, value in vars(args).items():
        if hasattr(repair, name):
            setattr(repair, name, value)
    repair.command, repair.run_id, repair.coordinator_run_id = (
        "checkpoints-repair",
        "c" * 32,
        "d" * 32,
    )
    repair.repair_run_id, repair.confirm_repair = args.run_id, True
    report, code = run(repair)
    assert code == 0 and report["status"] == "repaired"
    return args, repair, anchor


def verify(args, repair, anchor):
    return verify_completion(
        args,
        execution_sha256=EXECUTION,
        supervision_sha256=SUPERVISION,
        authority_high_water=anchor,
        repair_run_id=repair.run_id,
        repair_coordinator_run_id=repair.coordinator_run_id,
    )


def test_repaired_completion_is_read_only_and_preserves_forward_history(
    conn, stores, tmp_path, monkeypatch
):
    args, repair, anchor = failed_and_repaired(conn, stores, tmp_path, monkeypatch)
    originals = {
        path: path.read_bytes()
        for path in Path(args.run_evidence_directory).rglob("*.json")
    }
    result = verify(args, repair, anchor)
    seed(conn, stores[0], observation(activitytype="STOP", eventtime=200), hour="01")
    update_entities(conn, *stores, HOURS, ["HOST"])
    forward = verify(args, repair, result["authority_high_water"])
    assert (
        forward["authority_high_water"]["generation"]
        > result["authority_high_water"]["generation"]
    )
    assert forward["checkpoint_sha256"] == result["checkpoint_sha256"]
    assert all(path.read_bytes() == payload for path, payload in originals.items())


def test_repaired_no_deletion_completion_keeps_deletion_not_requested(
    conn, stores, tmp_path, monkeypatch
):
    args, repair, anchor = failed_and_repaired(
        conn, stores, tmp_path, monkeypatch, delete=False
    )

    result = verify(args, repair, anchor)
    metric = next(
        Path(stores[0].dataset.sibling("processing_metrics").root).rglob("*.parquet")
    )
    row = conn.execute(
        "SELECT deletion_status FROM read_parquet(?)", [str(metric)]
    ).fetchone()

    assert result["status"] == "verified"
    assert row == ("not_requested",)


@pytest.mark.parametrize(
    "damage",
    [
        "original_terminal",
        "missing_migration",
        "original_candidate",
        "repair_scope",
        "repair_terminal",
        "repair_link",
        "repair_row",
        "replacement_integer",
        "replacement_unapproved",
        "missing_prior",
        "repair_generation",
        "missing_archive",
        "missing_checkpoint",
    ],
)
def test_rehashed_repair_or_original_uncertainty_never_certifies_completion(
    conn, stores, tmp_path, monkeypatch, damage
):
    args, repair, anchor = failed_and_repaired(conn, stores, tmp_path, monkeypatch)
    verify(args, repair, anchor)

    def corrupt(records):
        if damage == "original_terminal":
            records[-1]["phase"] = "complete"
        elif damage == "missing_migration":
            records[:] = [r for r in records if r["phase"] != "migration_verified"]
        elif damage == "original_candidate":
            next(r for r in records if r["phase"] == "checkpoint_intent")["data"][
                "rows"
            ][0]["raw_output_rows"] += 1
        elif damage == "repair_scope":
            records[0]["data"]["binding"]["window_end"] = "2026-01-03T00:00:00Z"
        elif damage == "repair_terminal":
            records[-1]["data"]["exit_code"] = False
        elif damage == "repair_link":
            records[2]["data"]["original_last_sha256"] = "0" * 64
        elif damage == "repair_row":
            records[3]["data"]["row"]["run_id"] = "e" * 32
        elif damage == "replacement_integer":
            records[2]["data"]["confirmed_replacement"] = 0
        elif damage == "replacement_unapproved":
            old = copy(records[2]["data"]["rows"][0])
            old["run_id"] = "e" * 32
            records[2]["data"].update(
                replaced_checkpoints=[old], confirmed_replacement=True
            )
        elif damage == "missing_prior":
            records[2]["data"].pop("replaced_checkpoints")
        else:
            records[1]["data"]["generation_id"] = "0" * 64

    if damage == "missing_archive":
        next(archived(repair)[0].glob("*repair_intent.json")).unlink()
    elif damage == "missing_checkpoint":
        next(
            Path(stores[0].dataset.sibling("processing_metrics").root).rglob(
                "*.parquet"
            )
        ).unlink()
    else:
        rewrite_archive(
            args
            if damage
            in {"original_terminal", "missing_migration", "original_candidate"}
            else repair,
            corrupt,
        )
    with pytest.raises(CompactionError):
        verify(args, repair, anchor)


@pytest.mark.parametrize(
    "damage",
    [
        None,
        "boolean_version",
        "missing_identity",
        "changed_identity",
        "integer_replacement",
        "extra",
        "legacy_version",
    ],
)
def test_version_three_request_has_exact_repair_scope(tmp_path, damage):
    from raw_sensor_compact.completion import _request

    value = {
        "version": 3,
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
        "intent_sha256": "e" * 64,
        "authority_high_water": None,
        "repair_run_id": "c" * 32,
        "repair_coordinator_run_id": "d" * 32,
        "repair_replace_checkpoints": False,
    }
    if damage == "boolean_version":
        value["version"] = True
    elif damage == "missing_identity":
        value.pop("repair_run_id")
    elif damage == "changed_identity":
        value["repair_coordinator_run_id"] = "synthetic-sensitive-token"
    elif damage == "integer_replacement":
        value["repair_replace_checkpoints"] = 0
    elif damage == "extra":
        value["extra"] = "synthetic-sensitive-token"
    elif damage == "legacy_version":
        value["version"] = 1
    path = tmp_path / "synthetic-request.json"
    path.write_text(canonical(value))
    if damage is None:
        assert _request(path, identity(value))[0] == json.loads(path.read_text())
    else:
        with pytest.raises(CompactionError) as caught:
            _request(path, identity(value))
        assert "synthetic-sensitive-token" not in str(caught.value)
