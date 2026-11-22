"""Retry evidence is read-only and original-bound, not a completion certificate."""

import hashlib
import json
import os
from pathlib import Path

import pytest
from test_entities import seed
from test_entities import stores as stores_fixture
from test_phase_evidence import invocation

from raw_sensor_compact.cli import run
from raw_sensor_compact.config import CompactionError
from raw_sensor_compact.phase_evidence import canonical, identity, load_archive

stores = stores_fixture


def failed_merge(conn, stores, tmp_path, monkeypatch):
    seed(conn, stores[0])
    args = invocation(stores, tmp_path)
    args.process_entity_root = None
    args.entity_namespace = None
    args.entity_host = []
    args.delete_source = False

    def fail(*args, **kwargs):
        raise CompactionError("synthetic COPY obstruction before output")

    with monkeypatch.context() as patch:
        patch.setattr("raw_sensor_compact.compaction.copy_parquet", fail)
        with pytest.raises(CompactionError) as error:
            run(args)
    assert error.value.phase == "merge"
    archive = load_archive(args.run_evidence_directory, args.run_id)
    assert [r["phase"] for r in archive] == [
        "run_intent",
        "migration_intent",
        "blocked",
    ]
    # External disposition removes only this fixture's empty failed hour directory.
    failed = Path(stores[1].dataset.root) / "raw_process/dayPK=20260101/hourPK=00"
    failed.rmdir()
    return args


def verify(args):
    from raw_sensor_compact.retry_verifier import verify_retry

    return verify_retry(args)


def rehash(args, mutate):
    directory = Path(args.run_evidence_directory) / args.run_id
    paths = sorted(directory.glob("*.json"))
    values = [json.loads(p.read_text()) for p in paths]
    mutate(values)
    for p in paths:
        p.unlink()  # Only this test's synthetic archive.
    previous = ""
    for i, value in enumerate(values):
        value.pop("sha256", None)
        value.update(sequence=i, previous_sha256=previous)
        previous = identity(value)
        (directory / f"{i:06d}-{value['phase']}.json").write_text(
            canonical({**value, "sha256": previous}) + "\n"
        )


def test_retry_verifier_keeps_actual_failed_archive_and_inputs_read_only(
    conn, stores, tmp_path, monkeypatch
):
    args = failed_merge(conn, stores, tmp_path, monkeypatch)
    original = {p: p.read_bytes() for p in tmp_path.rglob("*.json")}
    data = {p: p.read_bytes() for p in tmp_path.rglob("*.parquet")}
    result = verify(args)
    assert result["operation"] == "retry-preflight"
    assert result["disposition"] == "unchanged-inputs-absent-output"
    assert result["archive_records"] == 3 and result["source_rows"] == 1
    assert result["output_absent"] is True
    assert all(p.read_bytes() == value for p, value in original.items())
    assert all(p.read_bytes() == value for p, value in data.items())
    assert (
        load_archive(args.run_evidence_directory, args.run_id)[-1]["phase"] == "blocked"
    )


@pytest.mark.parametrize(
    "damage",
    [
        "changed_input",
        "missing_input",
        "extra_input",
        "marker",
        "output",
        "empty_output",
        "scope",
        "missing_intent",
        "count",
        "schema",
        "duplicate",
        "terminal",
        "run_id",
        "entity",
        "multi_hour",
        "multiple_events",
        "deletion",
    ],
)
def test_retry_preflight_refuses_unsupported_or_changed_original_state(
    conn, stores, tmp_path, monkeypatch, damage
):
    args = failed_merge(conn, stores, tmp_path, monkeypatch)
    source = next(Path(stores[0].dataset.root).rglob("*.parquet"))
    if damage == "changed_input":
        source.write_bytes(source.read_bytes() + b"changed")
    elif damage == "missing_input":
        source.unlink()
    elif damage in {"extra_input", "marker"}:
        (
            source.parent / ("extra.parquet" if damage == "extra_input" else "marker")
        ).write_bytes(source.read_bytes())
    elif damage in {"output", "empty_output"}:
        output = Path(stores[1].dataset.root) / "raw_process/dayPK=20260101/hourPK=00"
        output.mkdir()
        if damage == "output":
            (output / "part.parquet").write_bytes(source.read_bytes())
    elif damage == "entity":
        args.process_entity_root = stores[2].store.dataset.root
    elif damage == "multi_hour":
        args.end_hour = "2026-01-01T02:00:00Z"
        args.max_hours = 2
    elif damage == "multiple_events":
        args.event_type = ["raw_file", "raw_process"]
    elif damage == "deletion":
        args.delete_source = True
    else:

        def mutate(values):
            if damage == "scope":
                values[0]["data"]["binding"]["raw_root"] += "-other"
            elif damage == "missing_intent":
                values.pop(1)
            elif damage == "duplicate":
                values.insert(2, dict(values[1]))
            elif damage == "count":
                values[1]["data"]["partitions"][0]["source_rows"] = True
            elif damage == "schema":
                values[1]["data"]["partitions"][0]["schema"] = {"wrong": "VARCHAR"}
            elif damage == "terminal":
                values[-1]["data"] = {"status": "completed", "exit_code": 0}
            else:
                values[1]["coordinator_run_id"] = "c" * 32

        rehash(args, mutate)
    before = {p: p.read_bytes() for p in tmp_path.rglob("*.parquet")}
    with pytest.raises(CompactionError):
        verify(args)
    assert all(p.read_bytes() == value for p, value in before.items())


def test_retry_prefix_budget_precedes_parquet_discovery(
    conn, stores, tmp_path, monkeypatch
):
    args = failed_merge(conn, stores, tmp_path, monkeypatch)
    source = next(Path(stores[0].dataset.root).rglob("*.parquet"))
    for i in range(5):
        (source.parent / str(i)).mkdir()
    args.max_files = 2
    from raw_sensor_compact import retry_verifier

    monkeypatch.setattr(
        retry_verifier, "discover", lambda *a, **k: pytest.fail("unbounded discovery")
    )
    with pytest.raises(CompactionError, match="budget"):
        verify(args)


@pytest.mark.parametrize("damage", ["marker", "output"])
def test_retry_rechecks_prefixes_after_final_data_observation(
    conn, stores, tmp_path, monkeypatch, damage
):
    args = failed_merge(conn, stores, tmp_path, monkeypatch)
    from raw_sensor_compact import retry_verifier

    original = retry_verifier.recheck
    calls = []

    def late_change(*a, **k):
        result = original(*a, **k)
        calls.append(1)
        if len(calls) == 2:
            if damage == "marker":
                source = next(Path(stores[0].dataset.root).rglob("*.parquet"))
                (source.parent / "late-marker").write_bytes(b"synthetic")
            else:
                (
                    Path(stores[1].dataset.root)
                    / "raw_process/dayPK=20260101/hourPK=00"
                ).mkdir()
        return result

    monkeypatch.setattr(retry_verifier, "recheck", late_change)
    with pytest.raises(CompactionError):
        verify(args)
    assert len(calls) == 2


def request_value(args):
    return {
        "version": 1,
        "operation": "retry-preflight",
        "arguments": [
            "compact",
            "--source-root",
            args.source_root,
            "--destination-root",
            args.destination_root,
            "--start-hour",
            args.start_hour,
            "--end-hour",
            args.end_hour,
            "--event-type",
            "raw_process",
            "--run-id",
            args.run_id,
            "--coordinator-run-id",
            args.coordinator_run_id,
            "--run-evidence-directory",
            args.run_evidence_directory,
            "--target-size-bytes",
            "134217728",
        ],
        "execution_sha256": "1" * 64,
        "supervision_sha256": "2" * 64,
        "intent_sha256": "3" * 64,
        "ledger_id": "d" * 32,
        "dataset_sha256": "4" * 64,
        "archive_sha256": identity(
            load_archive(args.run_evidence_directory, args.run_id)[-1]
        ),
        "archive_records": 3,
    }


def test_retry_request_invocation_echoes_exact_context_not_permission(
    conn, stores, tmp_path, monkeypatch, capsys
):
    from raw_sensor_compact.retry_verifier import main

    args = failed_merge(conn, stores, tmp_path, monkeypatch)
    value = request_value(args)
    path = tmp_path / "request.json"
    path.write_text(canonical(value))
    assert main(["--request", str(path), "--request-sha256", identity(value)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["request_sha256"] == identity(value)
    assert result["archive_sha256"] == value["archive_sha256"]
    assert result["source_rows"] == 1 and result["output_absent"] is True
    assert not any(k in result for k in ("permission", "completed_at", "next_hour"))
    assert (
        load_archive(args.run_evidence_directory, args.run_id)[-1]["phase"] == "blocked"
    )


@pytest.mark.parametrize(
    "damage",
    [
        "version",
        "operation",
        "records",
        "unknown",
        "missing",
        "digest",
        "ledger",
        "arguments",
        "argument_type",
        "help",
        "command",
        "duplicate_json",
        "noncanonical",
        "checksum",
        "bytes",
        "hardlink",
        "symlink",
        "fifo",
    ],
)
def test_retry_request_refuses_malformed_context_before_data_reads(
    conn, stores, tmp_path, monkeypatch, capsys, damage
):
    from raw_sensor_compact import retry_verifier

    args = failed_merge(conn, stores, tmp_path, monkeypatch)
    value = request_value(args)
    if damage == "version":
        value["version"] = True
    elif damage == "operation":
        value["operation"] = "completion"
    elif damage == "records":
        value["archive_records"] = True
    elif damage == "unknown":
        value["permission"] = "retry"
    elif damage == "missing":
        value.pop("execution_sha256")
    elif damage == "digest":
        value["supervision_sha256"] = "x" * 64
    elif damage == "ledger":
        value["ledger_id"] = "not-an-identity"
    elif damage == "arguments":
        value["arguments"] = []
    elif damage == "argument_type":
        value["arguments"][1] = True
    elif damage == "help":
        value["arguments"].append("--help")
    elif damage == "command":
        value["arguments"][0] = "unsupported-operation"
    payload = canonical(value).encode()
    if damage == "duplicate_json":
        payload = b'{"version":1,' + payload[1:]
    elif damage == "noncanonical":
        payload += b"\n"
    elif damage == "bytes":
        payload = b" " * (retry_verifier.MAX_REQUEST_BYTES + 1)
    path = tmp_path / "request.json"
    path.write_bytes(payload)
    checksum = hashlib.sha256(payload).hexdigest()
    if damage == "checksum":
        checksum = "0" * 64
    elif damage == "hardlink":
        os.link(path, tmp_path / "request-alias.json")
    elif damage == "symlink":
        alias = tmp_path / "request-alias.json"
        alias.symlink_to(path)
        path = alias
    elif damage == "fifo":
        path.unlink()
        os.mkfifo(path)
    calls = []

    def unexpected(*a, **k):
        calls.append(1)
        raise AssertionError("request refusal must precede data reads")

    monkeypatch.setattr(retry_verifier, "verify_retry", unexpected)
    assert (
        retry_verifier.main(["--request", str(path), "--request-sha256", checksum]) == 2
    )
    assert calls == []
    assert not capsys.readouterr().out


@pytest.mark.parametrize("damage", ["archive", "request", "result_budget"])
def test_retry_invocation_rechecks_original_context_and_result_budget(
    conn, stores, tmp_path, monkeypatch, capsys, damage
):
    from raw_sensor_compact import retry_verifier

    args = failed_merge(conn, stores, tmp_path, monkeypatch)
    value = request_value(args)
    path = tmp_path / "request.json"
    if damage == "archive":
        value["archive_sha256"] = "0" * 64
    path.write_text(canonical(value))
    calls = []
    original = retry_verifier.verify_retry

    def late_change(*a, **k):
        result = original(*a, **k)
        calls.append(1)
        if damage == "request":
            path.write_text(canonical({**value, "intent_sha256": "0" * 64}))
        return result

    monkeypatch.setattr(retry_verifier, "verify_retry", late_change)
    if damage == "result_budget":
        monkeypatch.setattr(retry_verifier, "MAX_RESULT_BYTES", 1)
    assert (
        retry_verifier.main(
            ["--request", str(path), "--request-sha256", identity(value)]
        )
        == 2
    )
    assert len(calls) == (0 if damage == "result_budget" else 1)
    assert not capsys.readouterr().out


@pytest.mark.parametrize("damage", ["archive", "input"])
def test_retry_data_observation_rechecks_late_original_changes(
    conn, stores, tmp_path, monkeypatch, damage
):
    from raw_sensor_compact import retry_verifier

    args = failed_merge(conn, stores, tmp_path, monkeypatch)
    original, calls = retry_verifier.recheck, []

    def late_change(*a, **k):
        result = original(*a, **k)
        calls.append(1)
        if len(calls) == 2:
            if damage == "archive":
                rehash(
                    args, lambda values: values[-1]["data"].update(message="changed")
                )
            else:
                source = next(Path(stores[0].dataset.root).rglob("*.parquet"))
                payload = source.read_bytes()
                source.write_bytes(payload[:-1] + bytes([payload[-1] ^ 1]))
        return result

    monkeypatch.setattr(retry_verifier, "recheck", late_change)
    with pytest.raises(CompactionError):
        verify(args)
    assert len(calls) == 2
