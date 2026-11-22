"""Genuine failed merge and independent retry evidence; never automatic reopening."""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from wintappy.coordination.execution import prepare_execution
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical, digest
from wintappy.coordination.writer import execute_prepared

from .test_coordination_completion import NOW, prepared_empty
from .test_coordination_execution import PROJECT, binding, config
from .test_coordination_ledger import HOUR

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux actual unprivileged merge failure",
)


def failed_merge(tmp_path):
    prepared_empty(tmp_path)
    path = tmp_path / "raw_sensor/raw_file/dayPK=20260101/hourPK=23/part.parquet"
    path.parent.mkdir(parents=True)
    seed = subprocess.run(
        [
            str(PROJECT / ".venv/bin/python"),
            "-I",
            "-B",
            "-c",
            'import duckdb,sys; c=duckdb.connect(); c.execute("COPY (SELECT 1 AS value) TO ? (FORMAT PARQUET)",[sys.argv[1]])',
            str(path),
        ],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert seed.returncode == 0, seed.stderr
    parent = tmp_path / "compacted/raw_file"
    parent.mkdir()
    prepared = prepare_execution(
        tmp_path / "state",
        binding(tmp_path),
        HOUR,
        NOW,
        {**config(tmp_path), "event_types": ["raw_file"], "delete_source": False},
    )
    parent.chmod(0o555)
    try:
        with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
            ledger.drain(NOW)
            with pytest.raises(CoordinationError, match="Receipt-bound writer failed"):
                execute_prepared(ledger, prepared, NOW)
    finally:
        parent.chmod(0o755)
    observed = Path(prepared.receipt["observations_directory"])
    supervision = json.loads((observed / "supervision.json").read_text())
    assert supervision["exit_code"] == 2 and supervision["children_stopped"] is True
    report = json.loads((observed / "stderr.log").read_text())
    assert report["phase"] == "merge"
    assert report["phase_evidence"]["migration_complete"] is False
    archive = tmp_path / "phase-evidence" / prepared.receipt["cli_run_id"]
    assert len(list(archive.glob("*.json"))) == 3
    assert not (parent / "dayPK=20260101/hourPK=23").exists()
    return prepared, {p: p.read_bytes() for p in archive.iterdir()}


def verify(ledger, prepared):
    from wintappy.coordination.retry import verify_retry_prepared

    return verify_retry_prepared(ledger, prepared, NOW)


def test_real_retry_preflight_preserves_failed_original_cursor_and_exclusion(tmp_path):
    prepared, originals = failed_merge(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        before = ledger.snapshot()
        proof = verify(ledger, prepared)
        assert proof["verification"]["source_rows"] == 1
        assert proof["verification"]["source_files"] == 1
        assert (
            proof["verification"]["source_bytes"]
            == next((tmp_path / "raw_sensor").rglob("*.parquet")).stat().st_size
        )
        assert proof["writer_supervision"]["exit_code"] == 2
        assert proof["verification"]["output_absent"] is True
        assert ledger.snapshot() == before
        assert ledger.writer_run(prepared.receipt["run_id"])["state"] == "BLOCKED"
        assert ledger.completion_evidence(prepared.receipt["run_id"]) is None
        assert ledger.admit(NOW) == []
    assert all(p.read_bytes() == data for p, data in originals.items())
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.snapshot() == before
        assert ledger.completion_evidence(prepared.receipt["run_id"]) is None


@pytest.mark.parametrize(
    "damage",
    [
        "missing_input",
        "changed_input",
        "extra_input",
        "output",
        "capture",
        "supervision",
        "archive",
        "receipt",
    ],
)
def test_retry_preflight_never_relaxes_original_evidence_or_block(
    tmp_path, monkeypatch, damage
):
    from wintappy.coordination import retry

    prepared, _ = failed_merge(tmp_path)
    assert prepared.receipt["settings"]["delete_source"] is False
    original, launches = retry.run_supervised, []

    def observed_launch(*a, **k):
        launches.append(1)
        return original(*a, **k)

    monkeypatch.setattr(retry, "run_supervised", observed_launch)
    source = next((tmp_path / "raw_sensor").rglob("*.parquet"))
    if damage == "missing_input":
        source.unlink()
    elif damage == "changed_input":
        source.write_bytes(source.read_bytes() + b"changed")
    elif damage == "extra_input":
        (source.parent / "extra.parquet").write_bytes(source.read_bytes())
    elif damage == "output":
        output = tmp_path / "compacted/raw_file/dayPK=20260101/hourPK=23"
        output.mkdir(parents=True)
    elif damage in {"capture", "supervision"}:
        (
            Path(prepared.receipt["observations_directory"])
            / ("stderr.log" if damage == "capture" else "supervision.json")
        ).unlink()
    elif damage == "archive":
        next(
            iter(
                (tmp_path / "phase-evidence" / prepared.receipt["cli_run_id"]).glob(
                    "*migration_intent.json"
                )
            )
        ).unlink()
    else:
        prepared.path.unlink()
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        before = ledger.snapshot()
        with pytest.raises(CoordinationError):
            verify(ledger, prepared)
        assert ledger.snapshot() == before
        assert ledger.completion_evidence(prepared.receipt["run_id"]) is None
    assert len(launches) == (
        1
        if damage in {"missing_input", "changed_input", "extra_input", "output"}
        else 0
    )


@pytest.mark.parametrize(
    "damage",
    [
        "changed_capture",
        "hardlink",
        "symlink",
        "manifest",
        "shutdown",
        "rehash_archive",
    ],
)
def test_retry_original_corruption_refuses_before_verifier_launch(
    tmp_path, monkeypatch, damage
):
    from wintappy.coordination import retry

    prepared, _ = failed_merge(tmp_path)
    directory = Path(prepared.receipt["observations_directory"])
    if damage == "changed_capture":
        (directory / "stderr.log").write_bytes(
            (directory / "stderr.log").read_bytes() + b"changed"
        )
    elif damage == "hardlink":
        os.link(directory / "stderr.log", directory / "synthetic-alias.log")
    elif damage == "symlink":
        saved = directory / "synthetic-saved.log"
        (directory / "stderr.log").rename(saved)
        (directory / "stderr.log").symlink_to(saved)
    elif damage == "manifest":
        value = json.loads((directory / "command.json").read_text())
        value["cwd"] += "-other"
        (directory / "command.json").write_text(canonical(value))
    elif damage == "shutdown":
        value = json.loads((directory / "supervision.json").read_text())
        value["children_stopped"] = False
        (directory / "supervision.json").write_text(canonical(value))
    else:
        archive = tmp_path / "phase-evidence" / prepared.receipt["cli_run_id"]
        path = archive / "000002-blocked.json"
        value = json.loads(path.read_text())
        value.pop("sha256")
        value["data"]["message"] = "synthetic rehashed original"
        path.write_text(canonical({**value, "sha256": digest(value)}) + "\n")
    launches = []

    def unexpected(*a, **k):
        launches.append(1)
        raise AssertionError("original corruption must refuse before launch")

    monkeypatch.setattr(retry, "run_supervised", unexpected)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        before = ledger.snapshot()
        with pytest.raises(CoordinationError):
            verify(ledger, prepared)
        assert ledger.snapshot() == before and launches == []


@pytest.mark.parametrize(
    "damage",
    [
        "version",
        "rows_type",
        "files_type",
        "bytes_type",
        "rows_range",
        "files_zero",
        "rows_changed",
        "files_changed",
        "bytes_changed",
        "output_type",
        "context",
        "unknown",
        "missing",
        "capture",
        "shutdown",
        "request",
        "original",
        "proof_budget",
    ],
)
def test_retry_verifier_controls_reach_real_independent_launch(
    tmp_path, monkeypatch, damage
):
    from wintappy.coordination import retry

    prepared, originals = failed_merge(tmp_path)
    original, launches = retry.run_supervised, []
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        state = ledger.snapshot()
        tables = {
            table: ledger._connection.execute(f"SELECT * FROM {table}").fetchall()
            for table in ("events", "compaction_runs", "completion_evidence")
        }

        def observed_launch(command, observations, **kwargs):
            request_path = Path(observations).parent / "request.json"
            request = json.loads(request_path.read_text())
            assert request_path.read_bytes() == canonical(request).encode()
            assert command[-1] == digest(request)
            assert ledger.snapshot() == state
            assert request["execution_sha256"] == prepared.sha256
            assert request["operation"] == "retry-preflight"
            result = original(command, observations, **kwargs)
            assert result["exit_code"] == 0 and result["children_stopped"] is True
            launches.append(1)
            directory = Path(observations)
            payload_path = directory / "stdout.log"
            payload = json.loads(payload_path.read_text())
            if damage == "version":
                payload["version"] = True
            elif damage in {"rows_type", "files_type", "bytes_type"}:
                payload["source_" + damage.split("_")[0]] = True
            elif damage == "rows_range":
                payload["source_rows"] = 2**63
            elif damage == "files_zero":
                payload["source_files"] = 0
            elif damage in {"rows_changed", "files_changed", "bytes_changed"}:
                payload["source_" + damage.split("_")[0]] += 1
            elif damage == "output_type":
                payload["output_absent"] = 1
            elif damage == "context":
                payload["request_sha256"] = "0" * 64
            elif damage == "unknown":
                payload["permission"] = "retry"
            elif damage == "missing":
                payload.pop("source_files")
            if damage in {
                "version",
                "rows_type",
                "files_type",
                "bytes_type",
                "rows_range",
                "files_zero",
                "rows_changed",
                "files_changed",
                "bytes_changed",
                "output_type",
                "context",
                "unknown",
                "missing",
            }:
                # Keep shutdown/capture identities valid so the result guard is active.
                encoded = (canonical(payload) + "\n").encode()
                payload_path.write_bytes(encoded)
                value = json.loads((directory / "supervision.json").read_text())
                value["stdout"].update(
                    bytes_saved=len(encoded),
                    bytes_seen=len(encoded),
                    sha256=hashlib.sha256(encoded).hexdigest(),
                )
                (directory / "supervision.json").write_text(canonical(value))
            elif damage == "capture":
                payload_path.write_bytes(payload_path.read_bytes() + b"changed")
            elif damage == "shutdown":
                value = json.loads((directory / "supervision.json").read_text())
                value["children_stopped"] = False
                (directory / "supervision.json").write_text(canonical(value))
            elif damage == "request":
                request_path.write_text(
                    canonical({**request, "intent_sha256": "0" * 64})
                )
            elif damage == "original":
                original_capture = (
                    Path(prepared.receipt["observations_directory"]) / "stderr.log"
                )
                original_capture.write_bytes(original_capture.read_bytes() + b"changed")
            return result

        monkeypatch.setattr(retry, "run_supervised", observed_launch)
        if damage == "proof_budget":
            monkeypatch.setattr(retry, "MAX_PREFLIGHT_BYTES", 1)
        with pytest.raises(CoordinationError):
            verify(ledger, prepared)
        assert launches == [1]
        assert ledger.snapshot() == state
        assert all(
            ledger._connection.execute(f"SELECT * FROM {table}").fetchall() == rows
            for table, rows in tables.items()
        )
    assert all(p.read_bytes() == data for p, data in originals.items())
