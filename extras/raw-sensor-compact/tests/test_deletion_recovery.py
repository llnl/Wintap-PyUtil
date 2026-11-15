"""Partial deletion uses retained original migrations, not survivor row equality."""

import json
from pathlib import Path

import pytest
from test_entities import HOURS, seed
from test_entities import stores as stores_fixture
from test_phase_evidence import invocation, records

from raw_sensor_compact.cli import parser, run
from raw_sensor_compact.config import CompactionError
from raw_sensor_compact.deletion import DeletionError
from raw_sensor_compact.entity_store import update_entities
from raw_sensor_compact.phase_evidence import canonical, identity

stores = stores_fixture


def partial(conn, stores, tmp_path, monkeypatch, *, entity=True):
    seed(conn, stores[0], filename="first.parquet")
    seed(conn, stores[0], filename="second.parquet")
    if entity:
        update_entities(conn, *stores, HOURS, ["HOST"], initialize=True)
    args = invocation(stores, tmp_path)
    if not entity:
        args.process_entity_root = None
        args.entity_host = []
        args.entity_namespace = None

    def fail_after_one(source, parent, **kwargs):
        next(Path(source.dataset.root).rglob("*.parquet")).unlink()
        raise DeletionError("synthetic failure after actual first deletion")

    with monkeypatch.context() as patch:
        patch.setattr("raw_sensor_compact.compaction.delete_hour", fail_after_one)
        report, code = run(args)
    assert code == 2 and report["status"] == "completed_with_deletion_errors"
    assert report["phase_evidence"]["migration_complete"] is False
    assert len(list(Path(stores[0].dataset.root).rglob("*.parquet"))) == 1
    return args


def repair_args(original, *, replace=False):
    values = [
        "deletions-repair",
        "--source-root",
        original.source_root,
        "--destination-root",
        original.destination_root,
        "--start-hour",
        original.start_hour,
        "--end-hour",
        original.end_hour,
        "--event-type",
        "raw_process",
        "--run-evidence-directory",
        original.run_evidence_directory,
        "--run-id",
        "d" * 32,
        "--coordinator-run-id",
        "e" * 32,
        "--repair-run-id",
        original.run_id,
        "--confirm-repair",
    ]
    if original.process_entity_root:
        values += [
            "--process-entity-root",
            original.process_entity_root,
            "--entity-namespace",
            original.entity_namespace,
            "--entity-host",
            "HOST",
        ]
    if replace:
        values += ["--confirm-replace-checkpoints"]
    return parser().parse_args(values)


def archive_bytes(tmp_path):
    return {
        p.name: p.read_bytes()
        for p in (tmp_path / "run-evidence" / ("a" * 32)).iterdir()
    }


def rehash(tmp_path, mutate):
    directory = tmp_path / "run-evidence" / ("a" * 32)
    paths = sorted(directory.glob("*.json"))
    values = [json.loads(p.read_text()) for p in paths]
    mutate(values)
    for path in paths:
        path.unlink()  # Only this test's synthetic archive.
    previous = ""
    for index, value in enumerate(values):
        value.pop("sha256", None)
        value.update(sequence=index, previous_sha256=previous)
        previous = identity(value)
        (directory / f"{index:06d}-{value['phase']}.json").write_text(
            canonical({**value, "sha256": previous}) + "\n"
        )


@pytest.mark.parametrize("entity", [False, True])
def test_partial_deletion_repair_keeps_original_failure_and_does_not_replay_entities(
    conn, stores, tmp_path, monkeypatch, entity
):
    original = partial(conn, stores, tmp_path, monkeypatch, entity=entity)
    before = archive_bytes(tmp_path)
    entity_bytes = {
        p: p.read_bytes() for p in Path(stores[2].store.dataset.root).rglob("*.parquet")
    }
    with monkeypatch.context() as patch:
        patch.setattr(
            "raw_sensor_compact.cli.update_entities",
            lambda *a, **k: pytest.fail("replayed entities"),
        )
        report, code = run(repair_args(original))
    assert code == 0 and report["status"] == "repaired"
    assert not list(Path(stores[0].dataset.root).rglob("*.parquet"))
    assert archive_bytes(tmp_path) == before
    assert all(p.read_bytes() == data for p, data in entity_bytes.items())
    assert records(tmp_path)[-1]["data"]["exit_code"] == 2
    assert report["phase_evidence"]["migration_complete"] is True


@pytest.mark.parametrize(
    "damage",
    [
        "source",
        "extra_source",
        "marker",
        "output",
        "extra_output",
        "missing_migration",
        "intent",
        "counts",
        "scope",
    ],
)
def test_partial_repair_refuses_changed_or_missing_original_evidence_before_deletion(
    conn, stores, tmp_path, monkeypatch, damage
):
    original = partial(conn, stores, tmp_path, monkeypatch)
    survivor = next(Path(stores[0].dataset.root).rglob("*.parquet"))
    if damage == "source":
        survivor.write_bytes(survivor.read_bytes() + b"changed")
    elif damage in {"extra_source", "marker"}:
        (
            survivor.parent
            / ("extra.parquet" if damage == "extra_source" else "marker.txt")
        ).write_bytes(b"extra")
    elif damage in {"output", "extra_output"}:
        output = next(Path(stores[1].dataset.root).rglob("*.parquet"))
        if damage == "output":
            output.write_bytes(output.read_bytes() + b"changed")
        else:
            (output.parent / "extra.parquet").write_bytes(output.read_bytes())
    else:

        def mutate(values):
            if damage == "missing_migration":
                values[:] = [v for v in values if v["phase"] != "migration_intent"]
            elif damage == "intent":
                next(v for v in values if v["phase"] == "migration_intent")["data"][
                    "source"
                ] = []
            elif damage == "counts":
                next(v for v in values if v["phase"] == "migration_verified")["data"][
                    "partitions"
                ][0]["source_rows"] = 999
            else:
                next(v for v in values if v["phase"] == "raw_complete")["data"][
                    "event_types"
                ] = ["raw_file"]

        rehash(tmp_path, mutate)
    calls = []
    monkeypatch.setattr(
        "raw_sensor_compact.deletion.delete_hour", lambda *a, **k: calls.append(a)
    )
    args = repair_args(original)
    with pytest.raises(CompactionError):
        run(args)
    assert not calls and survivor.exists()


def test_partial_repair_replacement_requires_explicit_prior_row_disposition(
    conn, stores, tmp_path, monkeypatch
):
    original = partial(conn, stores, tmp_path, monkeypatch)
    # A different valid historical row is not a failed-original checkpoint.
    run(invocation(stores, tmp_path, "entities-replay", "c" * 32))
    survivor = next(Path(stores[0].dataset.root).rglob("*.parquet"))
    with pytest.raises(CompactionError, match="confirm-replace-checkpoints"):
        run(repair_args(original))
    assert survivor.exists()
    args = repair_args(original, replace=True)
    args.run_id = "f" * 32
    report, code = run(args)
    assert code == 0 and report["status"] == "repaired"
    prior = next(
        v["data"] for v in records(tmp_path, "f" * 32) if v["phase"] == "repair_intent"
    )
    assert prior["replaced_checkpoints"][0]["run_id"] == "c" * 32
    assert prior["rows"][0]["entity_generation_id"] != report.get("generation_id")


def test_partial_repair_failure_retains_observations_and_new_retry_identity(
    conn, stores, tmp_path, monkeypatch
):
    original = partial(conn, stores, tmp_path, monkeypatch)
    before = archive_bytes(tmp_path)
    with monkeypatch.context() as patch:
        patch.setattr(
            "raw_sensor_compact.processing_metrics.ProcessingMetricsStore.publish",
            lambda *a: (_ for _ in ()).throw(
                CompactionError("synthetic repair publication failure")
            ),
        )
        with pytest.raises(CompactionError):
            run(repair_args(original))
    assert records(tmp_path, "d" * 32)[-1]["phase"] == "blocked"
    assert archive_bytes(tmp_path) == before
    args = repair_args(original)
    args.run_id = "f" * 32
    report, code = run(args)
    assert code == 0 and report["status"] == "repaired"
    assert archive_bytes(tmp_path) == before


def test_full_prefix_inventory_bounds_empty_directories(stores):
    from raw_sensor_compact.config import Leaf
    from raw_sensor_compact.deletion_recovery import prefix_keys

    leaf = Leaf("raw_process", "20260101", "00")
    directory = Path(stores[0].dataset.file_uri(leaf.relative))
    for number in range(4):
        (directory / f"synthetic-empty-{number}").mkdir(parents=True)
    with pytest.raises(CompactionError, match="budget"):
        prefix_keys(stores[0], leaf, 1)


@pytest.mark.parametrize("phase", ["repair_intent", "deletion_repair_intent"])
def test_changed_checkpoint_after_intent_prevents_any_survivor_deletion(
    conn, stores, tmp_path, monkeypatch, phase
):
    from raw_sensor_compact.config import Dataset
    from raw_sensor_compact.inventory import Store
    from raw_sensor_compact.phase_evidence import RunEvidence
    from raw_sensor_compact.processing_metrics import ProcessingMetricsStore

    original = partial(conn, stores, tmp_path, monkeypatch)
    survivor = next(Path(stores[0].dataset.root).rglob("*.parquet"))
    metrics = ProcessingMetricsStore(
        Store(Dataset.parse(stores[0].dataset.sibling("processing_metrics").root))
    )
    append = RunEvidence.append

    def interfere(archive, phase, data):
        append(archive, phase, data)
        if phase == target_phase:
            row = metrics.read(conn, HOURS[0])
            metrics.publish(conn, HOURS[0], {**row, "run_id": "f" * 32})

    target_phase = phase
    monkeypatch.setattr(RunEvidence, "append", interfere)
    with pytest.raises(CompactionError):
        run(repair_args(original))
    assert survivor.exists()


def test_full_prefix_budget_is_checked_before_parquet_discovery(
    conn, stores, tmp_path, monkeypatch
):
    original = partial(conn, stores, tmp_path, monkeypatch)
    directory = next(Path(stores[0].dataset.root).rglob("*.parquet")).parent
    for number in range(4):
        (directory / f"synthetic-empty-{number}").mkdir()
    args = repair_args(original)
    args.max_files = 2  # Original migration itself has exactly two source files.
    monkeypatch.setattr(
        "raw_sensor_compact.deletion_recovery.discover",
        lambda *a, **k: pytest.fail("Parquet traversal preceded prefix budget"),
    )
    with pytest.raises(CompactionError, match="budget"):
        run(args)
