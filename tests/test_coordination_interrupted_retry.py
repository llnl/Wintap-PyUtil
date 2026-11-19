"""Bounded evidence and CLI controls for interrupted pre-publication retry."""

import copy
from pathlib import Path

import pytest

from wintappy.coordination.protocol import CoordinationError, canonical, digest

from .test_coordination_authority import binding as entity_binding
from .test_coordination_completion import prepared_empty

EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def _record(path, value):
    path.write_bytes(canonical(value).encode())


def interrupted_receipt(tmp_path):
    prepared = prepared_empty(tmp_path)
    receipt = copy.deepcopy(prepared.receipt)
    receipt["binding"] = entity_binding(tmp_path)
    receipt["settings"]["entity"] = receipt["binding"]["entity"]
    receipt["settings"]["delete_source"] = True
    receipt["settings"]["run_evidence_directory"] = str(tmp_path / "hourly-evidence")
    receipt["observations_directory"] = str(tmp_path / "observations")
    return receipt


def write_interrupted_evidence(receipt):
    from wintappy.coordination.completion import _manifest
    from wintappy.coordination.intentional_retry import _interrupted_binding

    archive = (
        Path(receipt["settings"]["run_evidence_directory"]) / receipt["cli_run_id"]
    )
    archive.mkdir(parents=True)
    values = [
        {
            "coordinator_run_id": receipt["run_id"],
            "data": {"binding": _interrupted_binding(receipt), "command": "compact"},
            "evidence_version": "1",
            "phase": "run_intent",
            "previous_sha256": "",
            "run_id": receipt["cli_run_id"],
            "sequence": 0,
        },
        {
            "coordinator_run_id": receipt["run_id"],
            "data": {"operation": "compact"},
            "evidence_version": "1",
            "phase": "entities_intent",
            "previous_sha256": "",
            "run_id": receipt["cli_run_id"],
            "sequence": 1,
        },
    ]
    values[0]["sha256"] = digest(values[0])
    values[1]["previous_sha256"] = values[0]["sha256"]
    values[1]["sha256"] = digest(values[1])
    for value in values:
        _record(archive / f"{value['sequence']:06d}-{value['phase']}.json", value)

    observations = Path(receipt["observations_directory"])
    observations.mkdir()
    _record(observations / "command.json", _manifest(receipt, receipt["command"]))
    supervision = {
        "children_stopped": True,
        "command_sha256": digest(_manifest(receipt, receipt["command"])),
        "exit_code": -15,
        "status": "interrupted",
        "stderr": {
            "bytes_saved": 0,
            "bytes_seen": 0,
            "sha256": EMPTY_SHA256,
            "truncated": False,
        },
        "stdout": {
            "bytes_saved": 0,
            "bytes_seen": 0,
            "sha256": EMPTY_SHA256,
            "truncated": False,
        },
    }
    _record(observations / "supervision.json", supervision)


def test_interrupted_prepublication_evidence_accepts_only_empty_two_phase_archive(
    tmp_path,
):
    from wintappy.coordination.intentional_retry import (
        _interrupted_archive,
        _interrupted_supervision,
    )

    receipt = interrupted_receipt(tmp_path)
    write_interrupted_evidence(receipt)
    assert _interrupted_archive(receipt)["archive_records"] == 2
    assert _interrupted_supervision(receipt)[0]["exit_code"] == -15

    (tmp_path / "hourly-evidence" / receipt["cli_run_id"] / "extra.json").write_text(
        "{}"
    )
    with pytest.raises(CoordinationError):
        _interrupted_archive(receipt)


def test_interrupted_retry_cli_refuses_without_confirmation_before_loading_config(
    monkeypatch,
):
    from wintappy.coordination import service

    monkeypatch.setattr(service, "load_config", lambda *_: pytest.fail("config loaded"))
    assert (
        service.main(
            [
                "retry-interrupted-hour",
                "--config",
                "synthetic",
                "--run-id",
                "a" * 32,
            ]
        )
        == 1
    )
