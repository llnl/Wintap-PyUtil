"""Migration evidence survives coverage overwrite and publication failure."""

import io
import json
import os
from dataclasses import replace
from pathlib import Path

import pytest
from test_entities import HOURS, seed
from test_entities import stores as stores_fixture

from raw_sensor_compact.cli import main, parser, run
from raw_sensor_compact.config import CompactionError
from raw_sensor_compact.entity_store import update_entities
from raw_sensor_compact.phase_evidence import RunEvidence, load_archive

stores = stores_fixture
RUN_ID = "a" * 32
COORDINATOR_ID = "b" * 32


def invocation(stores, tmp_path, command="compact", run_id=RUN_ID):
    source, compacted, entities = stores
    args = parser().parse_args(
        [
            command,
            "--source-root",
            source.dataset.root,
            "--destination-root",
            compacted.dataset.root,
            "--process-entity-root",
            entities.store.dataset.root,
            "--entity-host",
            "HOST",
            "--entity-namespace",
            entities.namespace,
            "--start-hour",
            "2026-01-01T00:00:00Z",
            "--end-hour",
            "2026-01-01T01:00:00Z",
            "--event-type",
            "raw_process",
            "--run-id",
            run_id,
            "--coordinator-run-id",
            COORDINATOR_ID,
            "--run-evidence-directory",
            str(tmp_path / "run-evidence"),
            *(
                ["--target-size-bytes", "134217728", "--delete-source"]
                if command == "compact"
                else []
            ),
            *(
                ["--repair-run-id", RUN_ID, "--confirm-repair"]
                if command == "checkpoints-repair"
                else []
            ),
            *(["--confirm-delete"] if command == "reconcile-delete" else []),
        ]
    )
    return args


def records(tmp_path, run_id=RUN_ID):
    return [
        json.loads(p.read_text())
        for p in sorted((tmp_path / "run-evidence" / run_id).glob("*.json"))
    ]


def initialize(conn, stores):
    seed(conn, stores[0])
    update_entities(conn, *stores, HOURS, ["HOST"], initialize=True)


def test_checkpoint_failure_after_deletion_retains_original_phase_evidence(
    conn, stores, tmp_path, monkeypatch
):
    initialize(conn, stores)

    def fail(*args):
        raise CompactionError("injected checkpoint publication failure")

    monkeypatch.setattr("raw_sensor_compact.cli.ProcessingMetricsStore.publish", fail)
    with pytest.raises(CompactionError, match="checkpoint"):
        run(invocation(stores, tmp_path))
    assert not list(Path(stores[0].dataset.root).glob("**/*.parquet"))
    assert list(Path(stores[1].dataset.root).glob("**/*.parquet"))
    evidence = records(tmp_path)
    phases = {r["phase"] for r in evidence}
    assert {
        "run_intent",
        "entities_verified",
        "migration_verified",
        "deletion_intent",
        "deletion_verified",
        "checkpoint_intent",
        "blocked",
    } <= phases
    assert all(
        r["run_id"] == RUN_ID and r["coordinator_run_id"] == COORDINATOR_ID
        for r in evidence
    )


def test_success_exposes_run_mapping_and_replay_preserves_migration(
    conn, stores, tmp_path
):
    initialize(conn, stores)
    report, code = run(invocation(stores, tmp_path))
    assert code == 0
    assert report.get("run_id") == RUN_ID
    assert report.get("coordinator_run_id") == COORDINATOR_ID
    assert report["processing_metrics"]["run_id"] == RUN_ID
    archive = tmp_path / "run-evidence" / RUN_ID
    before = {p.name: p.read_bytes() for p in archive.glob("*.json")}
    assert before and any(r["phase"] == "complete" for r in records(tmp_path))
    replay, code = run(invocation(stores, tmp_path, "entities-replay", "c" * 32))
    assert code == 0
    assert replay["processing_metrics"]["run_id"] != RUN_ID
    assert {p.name: p.read_bytes() for p in archive.glob("*.json")} == before


def test_exclusive_checkpoint_repair_uses_original_migration_and_generation(
    conn, stores, tmp_path, monkeypatch
):
    initialize(conn, stores)
    publish = __import__(
        "raw_sensor_compact.cli", fromlist=["ProcessingMetricsStore"]
    ).ProcessingMetricsStore.publish

    def fail(*args):
        raise CompactionError("injected checkpoint publication failure")

    monkeypatch.setattr("raw_sensor_compact.cli.ProcessingMetricsStore.publish", fail)
    with pytest.raises(CompactionError):
        run(invocation(stores, tmp_path))
    monkeypatch.setattr(
        "raw_sensor_compact.cli.ProcessingMetricsStore.publish", publish
    )
    # Forward entity evolution may precede repair. The historical checkpoint must
    # still name its original receipt, not a new replay generation.
    replay, _ = run(invocation(stores, tmp_path, "entities-replay", "c" * 32))
    metric = Path(stores[0].dataset.sibling("processing_metrics").root) / (
        "dayPK=20260101/hourPK=00/metrics.parquet"
    )
    metric.unlink()
    original = records(tmp_path)
    report, code = run(invocation(stores, tmp_path, "checkpoints-repair", "d" * 32))
    assert code == 0 and report["status"] == "repaired"
    row = conn.execute(
        "select run_id, raw_status, entity_generation_id from read_parquet(?)",
        [str(metric)],
    ).fetchone()
    assert row[0:2] == (RUN_ID, "merged")
    assert row[2] != replay["generation_id"]
    assert records(tmp_path) == original


def test_skip_without_migration_evidence_cannot_certify_or_repair(
    conn, stores, tmp_path
):
    initialize(conn, stores)
    run(invocation(stores, tmp_path))
    skipped, _ = run(invocation(stores, tmp_path, run_id="e" * 32))
    assert skipped.get("phase_evidence", {}).get("migration_complete") is False
    args = invocation(stores, tmp_path, "checkpoints-repair", "d" * 32)
    args.repair_run_id = "e" * 32
    with pytest.raises(CompactionError, match="migration"):
        run(args)


def test_run_identity_reuse_does_not_overwrite_original_evidence(
    conn, stores, tmp_path
):
    initialize(conn, stores)
    run(invocation(stores, tmp_path))
    before = records(tmp_path)
    with pytest.raises(CompactionError, match="evidence.*exists"):
        run(invocation(stores, tmp_path))
    assert records(tmp_path) == before


def failed_checkpoint(conn, stores, tmp_path, monkeypatch):
    from raw_sensor_compact.processing_metrics import ProcessingMetricsStore

    initialize(conn, stores)
    publish = ProcessingMetricsStore.publish

    def fail(*args):
        raise CompactionError("injected checkpoint publication failure")

    monkeypatch.setattr(ProcessingMetricsStore, "publish", fail)
    with pytest.raises(CompactionError):
        run(invocation(stores, tmp_path))
    monkeypatch.setattr(ProcessingMetricsStore, "publish", publish)
    return publish


@pytest.mark.parametrize("mutation", ["output", "source", "entity"])
def test_repair_rechecks_surviving_state_after_checkpoint_put(
    conn, stores, tmp_path, monkeypatch, mutation
):
    from raw_sensor_compact.processing_metrics import ProcessingMetricsStore

    publish = failed_checkpoint(conn, stores, tmp_path, monkeypatch)

    def changed(self, conn, hour, row):
        publish(self, conn, hour, row)
        if mutation == "output":
            path = next(Path(stores[1].dataset.root).glob("**/*.parquet"))
            path.write_bytes(path.read_bytes() + b"changed")
        elif mutation == "source":
            path = (
                Path(stores[0].dataset.root)
                / "raw_process/dayPK=20260101/hourPK=00/new.parquet"
            )
            path.parent.mkdir(parents=True)
            path.write_bytes(b"unexpected producer file")
        else:
            path = next(Path(stores[2].store.dataset.root).glob("**/entities.parquet"))
            path.write_bytes(path.read_bytes() + b"changed")

    monkeypatch.setattr(ProcessingMetricsStore, "publish", changed)
    with pytest.raises(CompactionError):
        run(invocation(stores, tmp_path, "checkpoints-repair", "d" * 32))
    assert records(tmp_path, "d" * 32)[-1]["phase"] == "blocked"


def test_source_change_during_deletion_intent_fsync_prevents_deletion(
    conn, stores, tmp_path, monkeypatch
):
    from raw_sensor_compact.phase_evidence import RunEvidence

    initialize(conn, stores)
    append = RunEvidence.append

    def changed(self, phase, data):
        append(self, phase, data)
        if phase == "deletion_intent":
            path = next(Path(stores[0].dataset.root).glob("**/*.parquet"))
            path.write_bytes(path.read_bytes() + b"changed")

    monkeypatch.setattr(RunEvidence, "append", changed)
    with pytest.raises(CompactionError):
        run(invocation(stores, tmp_path))
    assert list(Path(stores[0].dataset.root).glob("**/*.parquet"))


@pytest.mark.parametrize(
    "corruption", ["checksum", "missing", "output", "source", "receipt"]
)
def test_repair_rejects_changed_or_insufficient_evidence_before_publication(
    conn, stores, tmp_path, monkeypatch, corruption
):
    from raw_sensor_compact.processing_metrics import ProcessingMetricsStore

    failed_checkpoint(conn, stores, tmp_path, monkeypatch)
    archive = tmp_path / "run-evidence" / RUN_ID
    if corruption == "checksum":
        path = next(archive.glob("*migration_verified.json"))
        path.write_text(path.read_text().replace('"merged_rows":2', '"merged_rows":99'))
        # Change a byte independently of fixture row counts.
        path.write_text(
            path.read_text().replace(
                '"phase":"migration_verified"', '"phase":"migration_changed"'
            )
        )
    elif corruption == "missing":
        next(archive.glob("*migration_verified.json")).unlink()
    elif corruption == "output":
        path = next(Path(stores[1].dataset.root).glob("**/*.parquet"))
        path.write_bytes(path.read_bytes() + b"changed")
    elif corruption == "source":
        path = (
            Path(stores[0].dataset.root)
            / "raw_process/dayPK=20260101/hourPK=00/new.parquet"
        )
        path.parent.mkdir(parents=True)
        path.write_bytes(b"producer file")
    else:
        path = Path(stores[2].store.dataset.root) / "_authority/authority.parquet"
        path.write_bytes(path.read_bytes() + b"changed")
    calls = []
    monkeypatch.setattr(
        ProcessingMetricsStore, "publish", lambda *args: calls.append(args)
    )
    with pytest.raises(CompactionError):
        run(invocation(stores, tmp_path, "checkpoints-repair", "d" * 32))
    assert not calls


def test_repair_requires_explicit_disposition_of_newer_coverage(
    conn, stores, tmp_path, monkeypatch
):
    failed_checkpoint(conn, stores, tmp_path, monkeypatch)
    run(invocation(stores, tmp_path, "entities-replay", "c" * 32))
    metric = next(
        Path(stores[0].dataset.sibling("processing_metrics").root).glob("**/*.parquet")
    )
    before = metric.read_bytes()
    with pytest.raises(CompactionError, match="confirm-replace-checkpoints"):
        run(invocation(stores, tmp_path, "checkpoints-repair", "d" * 32))
    assert metric.read_bytes() == before
    args = invocation(stores, tmp_path, "checkpoints-repair", "e" * 32)
    args.confirm_replace_checkpoints = True
    report, code = run(args)
    assert code == 0 and report["status"] == "repaired"
    replaced = next(
        r["data"] for r in records(tmp_path, "e" * 32) if r["phase"] == "repair_intent"
    )
    assert replaced["replaced_checkpoints"][0]["run_id"] == "c" * 32
    assert replaced["rows"][0]["run_id"] == RUN_ID


@pytest.mark.parametrize(
    "phase",
    [
        "run_intent",
        "migration_intent",
        "migration_verified",
        "deletion_intent",
        "checkpoint_intent",
    ],
)
def test_archive_publication_failure_stops_next_destructive_phase(
    conn, stores, tmp_path, monkeypatch, phase
):
    from raw_sensor_compact.phase_evidence import RunEvidence

    initialize(conn, stores)
    before = Path(stores[2].store.dataset.root) / "_authority/authority.parquet"
    original_authority = before.read_bytes()
    append = RunEvidence.append

    def fail(self, name, data):
        if name == phase:
            raise CompactionError("injected evidence publication failure")
        return append(self, name, data)

    monkeypatch.setattr(RunEvidence, "append", fail)
    with pytest.raises(CompactionError, match="evidence"):
        run(invocation(stores, tmp_path))
    if phase != "checkpoint_intent":
        assert list(Path(stores[0].dataset.root).glob("**/*.parquet"))
    if phase == "run_intent":
        assert before.read_bytes() == original_authority
    if phase in {"run_intent", "migration_intent"}:
        assert not list(Path(stores[1].dataset.root).glob("**/*.parquet"))


def test_reconcile_deletion_archives_migration_and_publishes_repairable_coverage(
    conn, stores, tmp_path, monkeypatch
):
    initialize(conn, stores)
    args = invocation(stores, tmp_path)
    args.delete_source = False
    run(args)
    report, code = run(invocation(stores, tmp_path, "reconcile-delete", "e" * 32))
    assert code == 0 and report["phase_evidence"]["migration_complete"]
    metric = next(
        Path(stores[0].dataset.sibling("processing_metrics").root).glob("**/*.parquet")
    )
    metric.unlink()
    args = invocation(stores, tmp_path, "checkpoints-repair", "d" * 32)
    args.repair_run_id = "e" * 32
    report, code = run(args)
    assert code == 0 and report["processing_metrics"]["run_id"] == "e" * 32


def test_main_exposes_blocked_run_mapping(conn, stores, tmp_path, monkeypatch, capsys):
    from raw_sensor_compact.processing_metrics import ProcessingMetricsStore

    initialize(conn, stores)
    monkeypatch.setattr(
        ProcessingMetricsStore,
        "publish",
        lambda *args: (_ for _ in ()).throw(CompactionError("checkpoint failure")),
    )
    args = invocation(stores, tmp_path)
    monkeypatch.setattr("argparse.ArgumentParser.parse_args", lambda *a: args)
    assert main([]) == 2
    report = json.loads(capsys.readouterr().err)
    assert report["run_id"] == RUN_ID and report["coordinator_run_id"] == COORDINATOR_ID
    assert report["phase_evidence"]["records"] >= 5


@pytest.mark.parametrize("mutation", ["output", "entity"])
def test_compact_rechecks_surviving_state_before_certifying_completion(
    conn, stores, tmp_path, monkeypatch, mutation
):
    from raw_sensor_compact.processing_metrics import ProcessingMetricsStore

    initialize(conn, stores)
    publish = ProcessingMetricsStore.publish

    def changed(self, conn, hour, row):
        publish(self, conn, hour, row)
        root = (
            stores[1].dataset.root
            if mutation == "output"
            else stores[2].store.dataset.root
        )
        paths = (
            Path(root).glob("**/*.parquet")
            if mutation == "output"
            else Path(root).glob("**/entities.parquet")
        )
        path = next(paths)
        path.write_bytes(path.read_bytes() + b"changed")

    monkeypatch.setattr(ProcessingMetricsStore, "publish", changed)
    with pytest.raises(CompactionError):
        run(invocation(stores, tmp_path))
    assert records(tmp_path)[-1]["phase"] == "blocked"


def test_same_metadata_source_change_during_intent_still_prevents_deletion(
    conn, stores, tmp_path, monkeypatch
):
    initialize(conn, stores)
    append = RunEvidence.append

    def changed(self, phase, data):
        append(self, phase, data)
        if phase == "deletion_intent":
            path = next(Path(stores[0].dataset.root).glob("**/*.parquet"))
            stat = path.stat()
            original = path.read_bytes()
            path.write_bytes(b"X" + original[1:])
            os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns))

    monkeypatch.setattr(RunEvidence, "append", changed)
    with pytest.raises(CompactionError):
        run(invocation(stores, tmp_path))
    assert list(Path(stores[0].dataset.root).glob("**/*.parquet"))


def test_archive_loader_rejects_ambiguous_duplicate_json_fields(tmp_path):
    archive = RunEvidence(tmp_path / "evidence", RUN_ID, COORDINATOR_ID, {}, "compact")
    path = next(archive.directory.glob("*.json"))
    encoded = path.read_text()
    path.write_text('{"sequence":99,' + encoded[1:])
    with pytest.raises(CompactionError, match="identity/lineage"):
        load_archive(archive.root, RUN_ID)


@pytest.mark.parametrize(
    "budget", ["MAX_RECORD_BYTES", "MAX_ARCHIVE_BYTES", "MAX_RECORDS"]
)
def test_archive_budget_failure_preserves_history(tmp_path, monkeypatch, budget):
    archive = RunEvidence(tmp_path / "evidence", RUN_ID, COORDINATOR_ID, {}, "compact")
    before = {p.name: p.read_bytes() for p in archive.directory.glob("*.json")}
    monkeypatch.setattr("raw_sensor_compact.phase_evidence." + budget, 1)
    with pytest.raises(CompactionError, match="budget"):
        archive.append("complete", {})
    assert {p.name: p.read_bytes() for p in archive.directory.glob("*.json")} == before
    if budget == "MAX_RECORDS":
        assert len(load_archive(str(archive.root), RUN_ID)) == 1
    else:
        with pytest.raises(CompactionError, match="budget"):
            load_archive(str(archive.root), RUN_ID)


@pytest.mark.parametrize("bad_id", ["../other", "A" * 32, "a" * 31, "a" * 33, "g" * 32])
def test_invalid_run_identity_cannot_create_archive(tmp_path, bad_id):
    with pytest.raises(CompactionError, match="identity"):
        RunEvidence(tmp_path / "evidence", bad_id, "", {}, "compact")
    assert not (tmp_path / "evidence").exists()


def test_symlink_or_remote_archive_roots_fail_without_data_mutation(
    conn, stores, tmp_path
):
    initialize(conn, stores)
    link = tmp_path / "link"
    target = tmp_path / "real"
    target.mkdir()
    link.symlink_to(target, target_is_directory=True)
    for root in [
        str(link / "evidence"),
        "s3://synthetic-bucket/run-evidence",
        stores[0].dataset.root,
    ]:
        args = invocation(stores, tmp_path)
        args.run_evidence_directory = root
        with pytest.raises(CompactionError):
            run(args)
    assert list(Path(stores[0].dataset.root).glob("**/*.parquet"))
    assert not list(Path(stores[1].dataset.root).glob("**/*.parquet"))


@pytest.mark.parametrize("failure", ["link", "fsync"])
def test_record_link_or_directory_fsync_uncertainty_never_deletes_source(
    conn, stores, tmp_path, monkeypatch, failure
):
    initialize(conn, stores)
    append = RunEvidence.append

    def fail(self, phase, data):
        if phase == "migration_verified":
            name = "os.link" if failure == "link" else "fsync_directory"

            def uncertain(*args):
                raise OSError("injected publication uncertainty")

            monkeypatch.setattr("raw_sensor_compact.phase_evidence." + name, uncertain)
        return append(self, phase, data)

    monkeypatch.setattr(RunEvidence, "append", fail)
    with pytest.raises(CompactionError, match="uncertain"):
        run(invocation(stores, tmp_path))
    assert list(Path(stores[0].dataset.root).glob("**/*.parquet"))


def test_remote_object_evidence_streams_with_size_and_deadline_bounds(tmp_path):
    from raw_sensor_compact.config import Dataset
    from raw_sensor_compact.inventory import ObjectFile, Store
    from raw_sensor_compact.phase_evidence import object_digest

    calls = []

    class Body(io.BytesIO):
        def read(self, size=-1):
            calls.append(size)
            assert size == 1024 * 1024
            return super().read(size)

    class Client:
        def get_object(self, **kwargs):
            return {"Body": Body(b"synthetic")}

    store = Store(
        Dataset.parse("s3://synthetic-bucket/compacted", event_root=True), Client()
    )
    file = ObjectFile(
        "raw_sensor/raw_process/dayPK=20260101/hourPK=00/a.parquet", "", 9
    )
    assert len(object_digest(store, file)) == 64
    assert calls
    for size in [8, 10]:
        with pytest.raises(CompactionError, match="changed"):
            object_digest(store, replace(file, size=size))
    store.deadline = 0
    with pytest.raises(CompactionError, match="timeout"):
        object_digest(store, file)


def test_mixed_protocols_and_empty_hours_repair_original_counts(
    conn, stores, tmp_path, monkeypatch, parquet
):
    initialize(conn, stores)
    for protocol, rows in [("TCP", 3), ("UDP", 7)]:
        path = (
            Path(stores[0].dataset.root)
            / f"raw_network/dayPK=20260101/hourPK=00/protoPK={protocol}/a.parquet"
        )
        parquet(path, f"select * from range({rows})")
    args = invocation(stores, tmp_path)
    args.event_type.append("raw_network")
    args.end_hour = "2026-01-01T02:00:00Z"
    report, code = run(args)
    assert code == 0 and report["phase_evidence"]["migration_complete"]
    metrics = list(
        Path(stores[0].dataset.sibling("processing_metrics").root).glob("**/*.parquet")
    )
    assert len(metrics) == 2
    for metric in metrics:
        metric.unlink()
    repair = invocation(stores, tmp_path, "checkpoints-repair", "d" * 32)
    repair.event_type = args.event_type
    repair.end_hour = args.end_hour
    report, code = run(repair)
    assert code == 0 and report["processing_metrics"]["hours"] == 2
    assert conn.execute(
        "select sum(raw_source_rows), sum(raw_output_rows) from read_parquet(?)",
        [[str(p) for p in metrics]],
    ).fetchone() == (11, 11)


@pytest.mark.parametrize("publication", ["authority", "checkpoint"])
def test_new_publication_directory_links_are_fsynced_before_success(
    conn, stores, tmp_path, monkeypatch, publication
):
    fsync = os.fsync
    directories = []

    def observed(descriptor):
        path = Path(os.readlink(f"/proc/self/fd/{descriptor}"))
        if path.is_dir():
            directories.append(path)
        return fsync(descriptor)

    monkeypatch.setattr(os, "fsync", observed)
    initialize(conn, stores)
    if publication == "authority":
        # The new _authority/ entry itself must persist before host replacement.
        assert Path(stores[2].store.dataset.root) in directories
    else:
        run(invocation(stores, tmp_path))
        root = Path(stores[0].dataset.sibling("processing_metrics").root)
        assert root in directories
        assert root / "dayPK=20260101" in directories
