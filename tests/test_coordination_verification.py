"""Separate qualified data verification, never a completion/cursor API."""

import json
import sys
from pathlib import Path

import pytest

from .test_coordination_execution import binding, config, initialize
from .test_coordination_ledger import HOUR, NOW

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="Qualified writer supervision is Linux-only"
)


def empty_run(tmp_path):
    from wintappy.coordination.execution import prepare_execution
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.writer import execute_prepared

    initialize(tmp_path)
    for name in ("raw_sensor", "compacted"):
        (tmp_path / name).mkdir()
    prepared = prepare_execution(
        tmp_path / "state", binding(tmp_path), HOUR, NOW, config(tmp_path)
    )
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        execute_prepared(ledger, prepared, NOW)
    return prepared


def verify(tmp_path, prepared):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.verification import verify_prepared

    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        before = ledger.snapshot()
        result = verify_prepared(ledger, prepared)
        assert ledger.snapshot() == before
        return result


def test_real_separate_standalone_empty_verifier_binds_intent_without_reopening(
    tmp_path,
):
    prepared = empty_run(tmp_path)
    result = verify(tmp_path, prepared)
    assert result["status"] == "verified"
    assert result["execution_sha256"] == prepared.sha256
    assert result["run_id"] == prepared.receipt["run_id"]
    assert result["cli_run_id"] == prepared.receipt["cli_run_id"]
    assert result["raw_output_rows"] == 0
    assert result["verifier"]["children_stopped"] is True


@pytest.mark.parametrize(
    "corruption",
    ["receipt", "command", "shutdown", "exit", "stdout", "archive", "intent"],
)
def test_verifier_refuses_missing_or_changed_execution_shutdown_phase_evidence(
    tmp_path, corruption
):
    from wintappy.coordination.protocol import CoordinationError

    prepared = empty_run(tmp_path)
    observations = Path(prepared.receipt["observations_directory"])
    if corruption == "receipt":
        prepared.path.unlink()
    elif corruption == "stdout":
        (observations / "stdout.log").write_bytes(b"forged complete report")
    elif corruption == "archive":
        root = (
            Path(prepared.receipt["settings"]["run_evidence_directory"])
            / prepared.receipt["cli_run_id"]
        )
        next(root.glob("*raw_complete.json")).unlink()
    elif corruption == "intent":
        # A valid-looking pre-intent receipt does not authorize verification.
        prepared = prepared._replace(sha256="0" * 64)
    else:
        path = observations / (
            "command.json" if corruption == "command" else "supervision.json"
        )
        value = json.loads(path.read_text())
        if corruption == "command":
            value["cwd"] = str(tmp_path)
        elif corruption == "shutdown":
            value["children_stopped"] = False
        else:
            value["exit_code"] = 2
        path.write_text(json.dumps(value, sort_keys=True, separators=(",", ":")))
    with pytest.raises(CoordinationError):
        verify(tmp_path, prepared)


def test_orphan_preintent_receipt_cannot_invoke_verifier(tmp_path):
    from wintappy.coordination.execution import prepare_execution
    from wintappy.coordination.protocol import CoordinationError

    initialize(tmp_path)
    prepared = prepare_execution(
        tmp_path / "state", binding(tmp_path), HOUR, NOW, config(tmp_path)
    )
    with pytest.raises(CoordinationError):
        verify(tmp_path, prepared)


def test_failed_verification_blocks_a_live_committed_intent(tmp_path):
    from wintappy.coordination.execution import prepare_execution
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError
    from wintappy.coordination.verification import verify_prepared

    initialize(tmp_path)
    prepared = prepare_execution(
        tmp_path / "state", binding(tmp_path), HOUR, NOW, config(tmp_path)
    )
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        ledger.begin_writer(
            HOUR,
            NOW,
            list(prepared.command),
            execution={
                "path": str(prepared.path),
                "sha256": prepared.sha256,
                "receipt": prepared.receipt,
            },
        )
        with pytest.raises(CoordinationError):
            verify_prepared(ledger, prepared)
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.snapshot()["next_hour"] == HOUR
