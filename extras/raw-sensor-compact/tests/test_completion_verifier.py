"""Independent read-only completion checks; no ledger or report-flag authority."""

import json
import os
from pathlib import Path

import pytest
from test_entities import HOURS, observation, seed
from test_entities import stores as stores_fixture
from test_phase_evidence import invocation

from raw_sensor_compact.cli import run
from raw_sensor_compact.config import CompactionError
from raw_sensor_compact.entity_store import update_entities
from raw_sensor_compact.phase_evidence import canonical, identity

stores = stores_fixture
EXECUTION = "e" * 64
SUPERVISION = "f" * 64


def args_for(stores, tmp_path, *, entities=False, delete=True):
    args = invocation(stores, tmp_path)
    args.delete_source = delete
    args.delete_confirm_attempts = 1
    args.delete_confirm_initial_delay_seconds = 0.01
    if not entities:
        args.process_entity_root = None
        args.entity_host = []
        args.entity_namespace = None
    return args


def verify(args, high_water=None):
    from raw_sensor_compact.completion import verify_completion

    return verify_completion(
        args,
        execution_sha256=EXECUTION,
        supervision_sha256=SUPERVISION,
        authority_high_water=high_water,
    )


def baseline(args):
    from raw_sensor_compact.completion import verify_authority

    return verify_authority(args)


def archived(args):
    root = Path(args.run_evidence_directory) / args.run_id
    return root, [json.loads(p.read_text()) for p in sorted(root.glob("*.json"))]


def rewrite_archive(args, change):
    """Corrupt only qualification-owned records, recomputing the envelope chain."""
    root, records = archived(args)
    change(records)
    for path in root.glob("*.json"):
        path.unlink()
    previous = ""
    for number, record in enumerate(records):
        record.pop("sha256", None)
        record.update(sequence=number, previous_sha256=previous)
        record["sha256"] = previous = identity(record)
        (root / f"{number:06d}-{record['phase']}.json").write_text(
            canonical(record) + "\n"
        )


def snapshot(stores, args):
    roots = [Path(s.dataset.root) for s in stores[:2]]
    roots += [Path(stores[2].store.dataset.root), Path(args.run_evidence_directory)]
    return {
        str(p): p.read_bytes() for root in roots for p in root.rglob("*") if p.is_file()
    }


@pytest.mark.parametrize("delete", [False, True])
def test_real_raw_completion_is_read_only_and_binds_context(
    conn, stores, tmp_path, delete
):
    args = args_for(stores, tmp_path, delete=delete)
    seed(conn, stores[0])
    report, code = run(args)
    assert code == 0 and report["phase_evidence"]["migration_complete"]
    before = snapshot(stores, args)
    result = verify(args)
    assert result["version"] == 1 and result["status"] == "verified"
    assert result["run_id"] == args.coordinator_run_id
    assert result["cli_run_id"] == args.run_id
    assert result["hour_start"] == args.start_hour
    assert result["hour_end"] == args.end_hour
    assert result["execution_sha256"] == EXECUTION
    assert result["supervision_sha256"] == SUPERVISION
    assert result["authority_high_water"] is None
    assert result["raw_output_rows"] == 1
    assert snapshot(stores, args) == before


def test_empty_hour_requires_all_selected_events_and_bounded_discovery(
    stores, tmp_path
):
    args = args_for(stores, tmp_path)
    args.event_type = ["raw_file", "raw_network", "raw_process"]
    run(args)
    assert verify(args)["raw_output_rows"] == 0
    args.max_files = 1
    path = Path(stores[0].dataset.root) / "raw_file/dayPK=20260101/hourPK=00"
    path.mkdir(parents=True)
    for name in ("a.parquet", "b.parquet"):
        (path / name).write_bytes(b"late producer")
    with pytest.raises(CompactionError):
        verify(args)


@pytest.mark.parametrize(
    "field,value",
    [
        ("run_id", "c" * 32),
        ("coordinator_run_id", "d" * 32),
        ("start_hour", "2026-01-01T01:00:00Z"),
        ("event_type", ["raw_file"]),
        ("delete_source", False),
        ("source_root", "other-source"),
        ("destination_root", "other-destination"),
    ],
)
def test_completed_flags_do_not_override_exact_scope(
    conn, stores, tmp_path, field, value
):
    args = args_for(stores, tmp_path)
    seed(conn, stores[0])
    run(args)
    verify(args)
    setattr(
        args,
        field,
        str(tmp_path / value)
        if field in {"source_root", "destination_root"}
        else value,
    )
    with pytest.raises(CompactionError):
        verify(args)


@pytest.mark.parametrize(
    "corruption",
    [
        "missing_intent",
        "intent_source",
        "intent_rows",
        "missing_delete",
        "unknown_phase",
        "phase_order",
        "duplicate_complete",
        "blocked",
        "forged_rows",
    ],
)
def test_rehashed_archive_semantics_are_independently_checked(
    conn, stores, tmp_path, corruption
):
    args = args_for(stores, tmp_path)
    seed(conn, stores[0])
    run(args)
    verify(args)

    def corrupt(records):
        intent = next(r for r in records if r["phase"] == "migration_intent")
        migration = next(r for r in records if r["phase"] == "migration_verified")
        if corruption == "missing_intent":
            records.remove(intent)
        elif corruption == "intent_source":
            intent["data"]["source"][0]["sha256"] = "0" * 64
        elif corruption == "intent_rows":
            intent["data"]["partitions"][0]["source_rows"] = 999
        elif corruption == "missing_delete":
            records[:] = [r for r in records if r["phase"] != "deletion_verified"]
        elif corruption == "unknown_phase":
            records.insert(-1, {**records[-1], "phase": "unknown_phase"})
        elif corruption == "phase_order":
            a, b = records.index(intent), records.index(migration)
            records[a], records[b] = records[b], records[a]
        elif corruption == "duplicate_complete":
            records.append(dict(records[-1]))
        elif corruption == "blocked":
            records[-1]["phase"] = "blocked"
        else:
            migration["data"]["partitions"][0].update(source_rows=999, merged_rows=999)
            raw = next(r for r in records if r["phase"] == "raw_complete")
            raw["data"]["partitions"][0].update(source_rows=999, merged_rows=999)

    rewrite_archive(args, corrupt)
    with pytest.raises(CompactionError):
        verify(args)


@pytest.mark.parametrize(
    "corruption", ["output", "late_source", "missing_archive", "checksum"]
)
def test_independent_verifier_reopens_surviving_bytes(
    conn, stores, tmp_path, corruption
):
    args = args_for(stores, tmp_path)
    seed(conn, stores[0])
    run(args)
    verify(args)
    root, _ = archived(args)
    if corruption == "output":
        path = next(Path(stores[1].dataset.root).rglob("*.parquet"))
        path.write_bytes(path.read_bytes() + b"changed")
    elif corruption == "late_source":
        seed(conn, stores[0], filename="late.parquet")
    elif corruption == "missing_archive":
        next(root.glob("*migration_verified.json")).unlink()
    else:
        path = next(root.glob("*complete.json"))
        path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(CompactionError):
        verify(args)


def test_skipped_existing_hour_cannot_complete_even_with_zero_exit(
    conn, stores, tmp_path
):
    args = args_for(stores, tmp_path)
    seed(conn, stores[0])
    run(args)
    args.run_id = "c" * 32
    report, code = run(args)
    assert code == 0 and not report["phase_evidence"]["migration_complete"]
    with pytest.raises(CompactionError):
        verify(args)


@pytest.mark.parametrize("field", ["execution_sha256", "supervision_sha256"])
def test_verifier_context_requires_nonmissing_digest(stores, tmp_path, field):
    from raw_sensor_compact.completion import verify_completion

    args = args_for(stores, tmp_path)
    run(args)
    context = dict(
        execution_sha256=EXECUTION,
        supervision_sha256=SUPERVISION,
        authority_high_water=None,
    )
    context[field] = ""
    with pytest.raises(CompactionError):
        verify_completion(args, **context)


def test_historical_checkpoint_and_forward_lineage_do_not_compare_current_hashes(
    conn, stores, tmp_path
):
    args = args_for(stores, tmp_path, entities=True)
    seed(conn, stores[0])
    update_entities(conn, *stores, HOURS, ["HOST"], initialize=True)
    anchor = baseline(args)
    run(args)
    original = verify(args, anchor)
    seed(conn, stores[0], observation(activitytype="STOP", eventtime=200), hour="01")
    update_entities(conn, *stores, HOURS, ["HOST"])
    forward = verify(args, original["authority_high_water"])
    assert (
        forward["authority_high_water"]["generation"]
        > original["authority_high_water"]["generation"]
    )
    assert forward["checkpoint_generation_id"] == original["checkpoint_generation_id"]


@pytest.mark.parametrize(
    "corruption",
    ["no_baseline", "rollback", "fork", "checkpoint", "pending", "snapshot"],
)
def test_entity_completion_refuses_freshness_or_checkpoint_uncertainty(
    conn, stores, tmp_path, corruption
):
    args = args_for(stores, tmp_path, entities=True)
    seed(conn, stores[0])
    update_entities(conn, *stores, HOURS, ["HOST"], initialize=True)
    authority = Path(stores[2].store.dataset.root) / "_authority/authority.parquet"
    initial_authority = authority.read_bytes()
    target = Path(stores[2].store.dataset.root) / stores[2].relative("HOST")
    initial_snapshot = target.read_bytes()
    anchor = baseline(args)
    run(args)
    current = verify(args, anchor)["authority_high_water"]
    if corruption == "no_baseline":
        current = None
    elif corruption == "rollback":
        authority.write_bytes(initial_authority)
        target.write_bytes(initial_snapshot)
    elif corruption == "fork":
        current = {**current, "generation_id": "0" * 64}
    elif corruption == "checkpoint":
        metric = next(
            Path(stores[0].dataset.sibling("processing_metrics").root).rglob(
                "*.parquet"
            )
        )
        metric.unlink()
    elif corruption == "pending":
        conn.execute(
            "CREATE OR REPLACE TEMP TABLE pending AS SELECT * FROM read_parquet(?)",
            [str(authority)],
        )
        conn.execute(
            "UPDATE pending SET status='pending' WHERE generation="
            "(SELECT max(generation) FROM pending)"
        )
        conn.execute("COPY pending TO ? (FORMAT PARQUET)", [str(authority)])
    else:
        target.write_bytes(target.read_bytes() + b"changed")
    with pytest.raises(CompactionError):
        verify(args, current)


@pytest.mark.parametrize("kind", ["fifo", "hardlink"])
def test_completion_archive_refuses_nonregular_or_aliased_records_before_read(
    conn, stores, tmp_path, monkeypatch, kind
):
    args = args_for(stores, tmp_path)
    seed(conn, stores[0])
    run(args)
    verify(args)
    root, _ = archived(args)
    path = next(root.glob("*migration_verified.json"))
    if kind == "fifo":
        path.unlink()
        os.mkfifo(path)
    else:
        os.link(path, tmp_path / "alias")
    opened = []
    original = Path.open

    def observed(self, *args, **kwargs):
        if self == path:
            opened.append(str(self))
            if kind == "fifo":
                raise OSError("prevent blocking FIFO read in negative control")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "open", observed)
    with pytest.raises(CompactionError):
        verify(args)
    assert not opened


@pytest.mark.parametrize("kind", ["terminal_boolean", "passed_integer", "rows_boolean"])
def test_rehashed_boolean_integer_substitutions_cannot_certify_data(
    conn, stores, tmp_path, kind
):
    args = args_for(stores, tmp_path)
    seed(conn, stores[0])
    run(args)
    verify(args)

    def corrupt(records):
        if kind == "terminal_boolean":
            records[-1]["data"]["exit_code"] = False
        else:
            for record in records:
                for partition in record["data"].get("partitions", []):
                    if kind == "passed_integer" and "passed" in partition:
                        partition["passed"] = 1
                    elif kind == "rows_boolean":
                        partition["source_rows"] = True
                        if "merged_rows" in partition:
                            partition["merged_rows"] = True

    rewrite_archive(args, corrupt)
    with pytest.raises(CompactionError):
        verify(args)


def test_authority_initialization_candidate_rechecks_current_snapshots(
    conn, stores, tmp_path, monkeypatch
):
    from raw_sensor_compact.entity_authority import RELATIVE

    args = args_for(stores, tmp_path, entities=True)
    seed(conn, stores[0])
    update_entities(conn, *stores, HOURS, ["HOST"], initialize=True)
    baseline(args)
    original = type(stores[2]).read_object
    reads = []
    target = Path(stores[2].store.dataset.root) / stores[2].relative("HOST")

    def changed(self, relative, path):
        result = original(self, relative, path)
        if relative == RELATIVE:
            reads.append(relative)
            if len(reads) == 2:
                target.write_bytes(target.read_bytes() + b"changed")
        return result

    monkeypatch.setattr(type(stores[2]), "read_object", changed)
    with pytest.raises(CompactionError):
        baseline(args)


@pytest.mark.parametrize("corruption", ["checksum", "shape", "arguments", "budget"])
def test_separate_request_reader_refuses_unbound_or_oversized_input(
    tmp_path, monkeypatch, corruption
):
    import raw_sensor_compact.completion as completion

    value = {
        "version": 1,
        "arguments": ["compact"],
        "execution_sha256": EXECUTION,
        "supervision_sha256": SUPERVISION,
        "intent_sha256": "a" * 64,
        "authority_high_water": None,
    }
    if corruption == "shape":
        value["version"] = True
    elif corruption == "arguments":
        value["arguments"] = ["compact", "--unknown-synthetic-argument"]
    elif corruption == "budget":
        monkeypatch.setattr(completion, "MAX_REQUEST_BYTES", 1)
    path = tmp_path / "request.json"
    path.write_text(canonical(value))
    with pytest.raises(CompactionError):
        completion._request(
            path, "0" * 64 if corruption == "checksum" else identity(value)
        )


def test_request_argument_errors_do_not_echo_synthetic_sensitive_values(
    tmp_path, capsys
):
    from raw_sensor_compact.completion import _request

    sensitive = "synthetic-sensitive-token"
    value = {
        "version": 1,
        "arguments": ["compact", "--threads", sensitive],
        "execution_sha256": EXECUTION,
        "supervision_sha256": SUPERVISION,
        "intent_sha256": "a" * 64,
        "authority_high_water": None,
    }
    path = tmp_path / "request.json"
    path.write_text(canonical(value))
    with pytest.raises(CompactionError) as caught:
        _request(path, identity(value))
    captured = capsys.readouterr()
    assert sensitive not in captured.out + captured.err + str(caught.value)


@pytest.mark.parametrize(
    "damage", ["checksum", "version", "ledger", "shape", "arguments", "help", "budget"]
)
def test_authority_request_refuses_unbound_or_sensitive_inputs(
    tmp_path, capsys, monkeypatch, damage
):
    import raw_sensor_compact.completion as completion

    sensitive = "synthetic-sensitive-token"
    value = {
        "version": 2,
        "operation": "authority",
        "arguments": ["compact"],
        "execution_sha256": EXECUTION,
        "ledger_id": "a" * 32,
        "dataset_sha256": "b" * 64,
        "authority_high_water": None,
    }
    if damage == "version":
        value["version"] = True
    elif damage == "ledger":
        value["ledger_id"] = sensitive
    elif damage == "shape":
        value["extra"] = sensitive
    elif damage == "arguments":
        value["arguments"] = ["compact", "--threads", sensitive]
    elif damage == "help":
        value["arguments"] = ["compact", "--help"]
    elif damage == "budget":
        monkeypatch.setattr(completion, "MAX_REQUEST_BYTES", 1)
    path = tmp_path / "authority-request.json"
    path.write_text(canonical(value))
    with pytest.raises(CompactionError) as caught:
        completion._authority_request(
            path, "0" * 64 if damage == "checksum" else identity(value)
        )
    captured = capsys.readouterr()
    assert sensitive not in captured.out + captured.err + str(caught.value)


def test_separate_authority_main_returns_bound_candidate_and_rejects_request_change(
    conn, stores, tmp_path, monkeypatch, capsys
):
    import raw_sensor_compact.completion as completion

    args = args_for(stores, tmp_path, entities=True)
    seed(conn, stores[0])
    update_entities(conn, *stores, HOURS, ["HOST"], initialize=True)
    anchor = baseline(args)
    value = {
        "version": 2,
        "operation": "authority",
        "arguments": [
            "compact",
            "--source-root",
            args.source_root,
            "--destination-root",
            args.destination_root,
            "--process-entity-root",
            args.process_entity_root,
            "--entity-host",
            "HOST",
            "--entity-namespace",
            args.entity_namespace,
            "--start-hour",
            args.start_hour,
            "--end-hour",
            args.end_hour,
            "--event-type",
            "raw_process",
            "--target-size-bytes",
            str(args.target_size_bytes),
        ],
        "execution_sha256": EXECUTION,
        "ledger_id": "a" * 32,
        "dataset_sha256": "b" * 64,
        "authority_high_water": anchor,
    }
    path = tmp_path / "authority-request.json"
    path.write_text(canonical(value))
    command = [
        "--authority-only",
        "--request",
        str(path),
        "--request-sha256",
        identity(value),
    ]
    assert completion.main(command) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["authority_high_water"] == anchor
    assert result["ledger_id"] == value["ledger_id"]
    assert result["execution_sha256"] == EXECUTION
    assert result["request_sha256"] == identity(value)
    verifier = completion.verify_authority

    def changed(*a, **kw):
        result = verifier(*a, **kw)
        path.write_text(canonical({**value, "ledger_id": "c" * 32}))
        return result

    monkeypatch.setattr(completion, "verify_authority", changed)
    assert completion.main(command) == 2
    captured = capsys.readouterr()
    assert not captured.out and json.loads(captured.err)["status"] == "error"
