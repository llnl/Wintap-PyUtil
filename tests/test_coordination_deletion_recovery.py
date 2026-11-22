"""Actual failed deletion, receipt-bound maintenance and original-failure recovery."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from wintappy.coordination.execution import prepare_execution
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical, digest
from wintappy.coordination.recovery import resolve_hour
from wintappy.coordination.supervisor import read_report
from wintappy.coordination.verification import verify_authority_prepared
from wintappy.coordination.writer import execute_prepared

from .test_coordination_authority import prepared_entities
from .test_coordination_completion import NOW
from .test_coordination_execution import config

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux real unprivileged deletion failure",
)
REASON = "synthetic original and maintenance processes cannot resume"


def blocked_deletion_run(tmp_path):
    bound, old = prepared_entities(tmp_path)
    source = next((tmp_path / "raw").rglob("*.parquet"))
    other = tmp_path / "raw/raw_file/dayPK=20260101/hourPK=23/part.parquet"
    other.parent.mkdir(parents=True)
    other.write_bytes(source.read_bytes())
    prepared = prepare_execution(
        tmp_path / "state",
        bound,
        old.receipt["hour_start"],
        NOW,
        {**config(tmp_path), "event_types": ["raw_file", "raw_process"]},
    )
    with Ledger.open(tmp_path / "state", bound) as ledger:
        verify_authority_prepared(
            ledger, prepared, NOW, initialize=True, confirmed=True, reason=REASON
        )
        source.parent.chmod(0o555)
        try:
            ledger.drain(NOW)
            with pytest.raises(CoordinationError, match="Receipt-bound writer failed"):
                execute_prepared(ledger, prepared, NOW)
        finally:
            source.parent.chmod(0o755)
        report = read_report(
            prepared.receipt["observations_directory"],
            max_bytes=prepared.receipt["settings"]["max_report_bytes"],
        )
        assert report["status"] == "completed_with_deletion_errors"
        assert ledger.snapshot()["mode"] == "BLOCKED"
    assert source.exists() and not other.exists()
    observations = prepared.path.parent / "observations"
    supervision = json.loads((observations / "supervision.json").read_text())
    assert supervision["exit_code"] == 2
    assert supervision["status"] == "exited" and supervision["children_stopped"] is True
    assert report["phase_evidence"]["migration_complete"] is False
    directory = tmp_path / "phase-evidence" / prepared.receipt["cli_run_id"]
    return bound, prepared, {p: p.read_bytes() for p in directory.iterdir()}


def test_real_partial_deletion_repair_stays_blocked_until_verified_resolution(tmp_path):
    bound, prepared, original = blocked_deletion_run(tmp_path)
    from wintappy.coordination.recovery import repair_deletions

    entity = {p: p.read_bytes() for p in (tmp_path / "entity").rglob("*.parquet")}
    with Ledger.open(tmp_path / "state", bound) as ledger:
        before = ledger.snapshot()
        operation = repair_deletions(
            ledger, prepared, NOW, confirmed=True, reason=REASON
        )
        assert ledger.snapshot() == before
        assert all(p.read_bytes() == data for p, data in original.items())
        assert all(p.read_bytes() == data for p, data in entity.items())
        assert not list((tmp_path / "raw").rglob("*.parquet"))
        result = resolve_hour(
            ledger,
            prepared,
            NOW,
            clock=lambda: NOW,
            confirmed=True,
            reason=REASON,
            maintenance_id=operation,
        )
        assert (
            result["mode"] == "OPEN"
            and result["next_hour"] == prepared.receipt["hour_end"]
        )
        proof = ledger.completion_evidence(prepared.receipt["run_id"])
        assert proof["version"] == 3
        assert proof["writer_supervision"]["exit_code"] == 2
        assert proof["recovery"]["operation"] == "deletions-repair"
    assert all(p.read_bytes() == data for p, data in original.items())
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.snapshot()["mode"] == "OPEN"


@pytest.mark.parametrize(
    "damage", ["confirmation", "capture", "archive", "source", "output"]
)
def test_deletion_maintenance_refuses_scope_or_evidence_without_reopening(
    tmp_path, damage
):
    bound, prepared, _ = blocked_deletion_run(tmp_path)
    from wintappy.coordination.recovery import repair_deletions

    if damage == "capture":
        (prepared.path.parent / "observations/stderr.log").unlink()
    elif damage == "archive":
        next(
            (tmp_path / "phase-evidence" / prepared.receipt["cli_run_id"]).glob(
                "*migration_intent.json"
            )
        ).unlink()
    elif damage in {"source", "output"}:
        path = next(
            (tmp_path / ("raw" if damage == "source" else "compacted")).rglob(
                "*.parquet"
            )
        )
        path.write_bytes(path.read_bytes() + b"changed")
    with Ledger.open(tmp_path / "state", bound) as ledger:
        before = ledger.snapshot()
        with pytest.raises(CoordinationError):
            repair_deletions(
                ledger, prepared, NOW, confirmed=damage != "confirmation", reason=REASON
            )
        assert ledger.snapshot() == before


def test_crashed_deletion_maintenance_keeps_original_block_and_needs_abandonment(
    tmp_path,
):
    bound, prepared, originals = blocked_deletion_run(tmp_path)
    from wintappy.coordination.recovery import repair_deletions

    script = """
import json,os,sys
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.recovery import load_original_execution,repair_deletions
with Ledger.open(sys.argv[1],json.loads(sys.argv[2])) as ledger:
    prepared=load_original_execution(ledger,sys.argv[3],{})
    def crash(stage):
        if stage=='after_maintenance_commit': os._exit(91)
    ledger.failure_hook=crash
    repair_deletions(ledger,prepared,sys.argv[4],confirmed=True,reason='synthetic cannot resume')
raise AssertionError('failure hook not reached')
"""
    child = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            script,
            str(tmp_path / "state"),
            canonical(bound),
            prepared.receipt["run_id"],
            NOW,
        ],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert child.returncode == 91, child.stderr
    from wintappy.coordination.maintenance import abandon, history

    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.snapshot()["mode"] == "BLOCKED"
        with pytest.raises(CoordinationError, match="abandonment"):
            repair_deletions(ledger, prepared, NOW, confirmed=True, reason=REASON)
        operation = next(iter(history(ledger)))
        abandon(ledger, operation, NOW, confirmed=True, reason=REASON)
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert all(p.read_bytes() == data for p, data in originals.items())
        repair_deletions(ledger, prepared, NOW, confirmed=True, reason=REASON)


def repaired_deletion(tmp_path):
    from wintappy.coordination.recovery import repair_deletions

    bound, prepared, original = blocked_deletion_run(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        operation = repair_deletions(
            ledger, prepared, NOW, confirmed=True, reason=REASON
        )
    return bound, prepared, original, operation


def resolve(ledger, prepared, operation):
    return resolve_hour(
        ledger,
        prepared,
        NOW,
        clock=lambda: NOW,
        confirmed=True,
        reason=REASON,
        maintenance_id=operation,
    )


@pytest.mark.parametrize(
    "damage",
    [
        "receipt",
        "command",
        "supervision",
        "stdout",
        "stderr",
        "archive",
        "original_command",
        "original_supervision",
        "original_stderr",
    ],
)
def test_deletion_resolution_requires_all_original_and_repair_evidence(
    tmp_path, damage
):
    bound, prepared, originals, operation = repaired_deletion(tmp_path)
    directory = prepared.path.parent / "maintenance" / operation
    if damage == "receipt":
        target = directory / "receipt.json"
    elif damage == "archive":
        receipt = json.loads((directory / "receipt.json").read_text())
        target = next(
            (tmp_path / "phase-evidence" / receipt["cli_run_id"]).glob(
                "*deletion_repair_verified.json"
            )
        )
    elif damage.startswith("original_"):
        name = damage.removeprefix("original_")
        target = Path(prepared.receipt["observations_directory"]) / (
            name + (".log" if name == "stderr" else ".json")
        )
    else:
        target = (
            directory
            / "observations"
            / (damage + (".log" if damage in {"stdout", "stderr"} else ".json"))
        )
    target.unlink()
    with Ledger.open(tmp_path / "state", bound) as ledger:
        before = ledger.snapshot()
        with pytest.raises(CoordinationError):
            resolve(ledger, prepared, operation)
        assert ledger.snapshot() == before and ledger.admit(NOW) == []
        assert ledger.completion_evidence(prepared.receipt["run_id"]) is None
    assert all(p.read_bytes() == data for p, data in originals.items())


@pytest.mark.parametrize(
    "point",
    [
        "before_maintenance_commit",
        "after_maintenance_commit",
        "launch",
        "after_launch",
        "before_maintenance_finished_commit",
        "after_maintenance_finished_commit",
    ],
)
def test_deletion_maintenance_failures_keep_exact_block_and_audited_retry(
    tmp_path, monkeypatch, point
):
    from wintappy.coordination import maintenance
    from wintappy.coordination.recovery import repair_deletions

    bound, prepared, originals = blocked_deletion_run(tmp_path)
    supervised = maintenance.run_supervised
    launched = []

    def crash(stage):
        if stage == point:
            raise SystemExit(97)

    def launch(command, directory, **kwargs):
        assert (Path(directory).parent / "receipt.json").is_file()
        assert len(maintenance.history(ledger)) == 1
        launched.append(str(directory))
        if point == "launch":
            raise SystemExit(97)
        result = supervised(command, directory, **kwargs)
        if point == "after_launch":
            raise SystemExit(97)
        return result

    with Ledger.open(tmp_path / "state", bound) as ledger:
        before = ledger.snapshot()
        ledger.failure_hook = crash
        monkeypatch.setattr(maintenance, "run_supervised", launch)
        with pytest.raises(SystemExit):
            repair_deletions(ledger, prepared, NOW, confirmed=True, reason=REASON)
        ledger.failure_hook = lambda stage: None
        assert ledger.snapshot() == before
    monkeypatch.setattr(maintenance, "run_supervised", supervised)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        operations = maintenance.history(ledger)
        if point == "before_maintenance_commit":
            assert not operations and not launched
            return
        operation = next(iter(operations))
        if point == "after_maintenance_finished_commit":
            assert resolve(ledger, prepared, operation)["mode"] == "OPEN"
            return
        with pytest.raises(CoordinationError, match="abandonment"):
            repair_deletions(
                ledger,
                prepared,
                NOW,
                confirmed=True,
                reason=REASON,
                replace_checkpoints=True,
            )
        with pytest.raises(CoordinationError):
            resolve(ledger, prepared, operation)
        with pytest.raises(CoordinationError):
            maintenance.abandon(ledger, operation, NOW, confirmed=False, reason=REASON)
        assert (
            maintenance.abandon(ledger, operation, NOW, confirmed=True, reason=REASON)
            == before
        )
        with pytest.raises(CoordinationError):
            resolve(ledger, prepared, operation)
        new_id = repair_deletions(
            ledger,
            prepared,
            NOW,
            confirmed=True,
            reason=REASON,
            replace_checkpoints=True,
        )
        assert new_id != operation
        assert (
            resolve(ledger, prepared, new_id)["next_hour"]
            == prepared.receipt["hour_end"]
        )
    assert all(p.read_bytes() == data for p, data in originals.items())


@pytest.mark.parametrize(
    "damage", ["missing_intent", "changed_finished", "total_budget", "record_budget"]
)
def test_deletion_resolution_refuses_retained_history_damage(
    tmp_path, monkeypatch, damage
):
    from wintappy.coordination import maintenance

    bound, prepared, _, operation = repaired_deletion(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        if damage == "missing_intent":
            ledger._connection.execute(
                "DELETE FROM events WHERE kind='maintenance_intent'"
            )
        elif damage == "changed_finished":
            value = json.loads(
                ledger._connection.execute(
                    "SELECT details_json FROM events WHERE kind='maintenance_finished'"
                ).fetchone()[0]
            )
            value["receipt_sha256"] = "0" * 64
            ledger._connection.execute(
                "UPDATE events SET details_json=? WHERE kind='maintenance_finished'",
                [canonical(value)],
            )
        else:
            monkeypatch.setattr(
                maintenance,
                "MAX_MAINTENANCE_TOTAL_BYTES"
                if damage == "total_budget"
                else "MAX_MAINTENANCE_BYTES",
                1,
            )
        before = ledger.snapshot()
        with pytest.raises(CoordinationError):
            resolve(ledger, prepared, operation)
        assert ledger.snapshot() == before


@pytest.mark.parametrize(
    "point", ["before_completion_commit", "after_completion_commit"]
)
def test_abrupt_deletion_resolution_retains_all_or_none(tmp_path, point):
    bound, prepared, originals, operation = repaired_deletion(tmp_path)
    script = """
import json,os,sys
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.recovery import load_original_execution,resolve_hour
with Ledger.open(sys.argv[1],json.loads(sys.argv[2])) as ledger:
    prepared=load_original_execution(ledger,sys.argv[3],{})
    def crash(stage):
        if stage==sys.argv[5]: os._exit(97)
    ledger.failure_hook=crash
    resolve_hour(ledger,prepared,sys.argv[6],clock=lambda:sys.argv[6],confirmed=True,
                 reason='synthetic cannot resume',maintenance_id=sys.argv[4])
raise AssertionError('failure hook not reached')
"""
    child = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            script,
            str(tmp_path / "state"),
            canonical(bound),
            prepared.receipt["run_id"],
            operation,
            point,
            NOW,
        ],
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert child.returncode == 97, child.stderr.decode()
    with Ledger.open(tmp_path / "state", bound) as ledger:
        proof = ledger.completion_evidence(prepared.receipt["run_id"])
        if point == "after_completion_commit":
            assert ledger.snapshot()["mode"] == "OPEN" and proof["version"] == 3
        else:
            assert ledger.snapshot()["mode"] == "BLOCKED" and proof is None
            assert resolve(ledger, prepared, operation)["mode"] == "OPEN"
    assert all(p.read_bytes() == data for p, data in originals.items())


def test_service_deletion_repair_uses_original_receipt_and_separate_resolution(
    tmp_path, monkeypatch, capsys
):
    from wintappy.coordination import service

    from .test_coordination_service import settings

    bound, prepared, originals = blocked_deletion_run(tmp_path)
    value = settings(tmp_path)
    value["binding"] = bound
    path = tmp_path / "synthetic-config.json"
    path.write_text(canonical(value))
    monkeypatch.setattr(service, "_now", lambda: NOW)
    common = [
        "--config",
        str(path),
        "--run-id",
        prepared.receipt["run_id"],
        "--confirm",
        "--reason",
        REASON,
    ]
    assert service.main(["repair-deletions", *common]) == 0
    operation = json.loads(capsys.readouterr().out)["maintenance_id"]
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert (
            service.status_page(ledger, NOW)["maintenance"][0]["operation"]
            == "deletions-repair"
        )
    assert service.main(["resolve-hour", *common, "--maintenance-id", operation]) == 0
    assert json.loads(capsys.readouterr().out)["mode"] == "OPEN"
    assert all(p.read_bytes() == data for p, data in originals.items())


@pytest.mark.parametrize("kind", ["maintenance_intent", "maintenance_finished"])
def test_completed_deletion_proof_requires_retained_history_on_reopen(tmp_path, kind):
    bound, prepared, _, operation = repaired_deletion(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        resolve(ledger, prepared, operation)
        ledger._connection.execute("DELETE FROM events WHERE kind=?", [kind])
    with pytest.raises(CoordinationError), Ledger.open(tmp_path / "state", bound):
        pass


def test_completed_deletion_proof_does_not_reopen_historical_runtime_files(tmp_path):
    bound, prepared, _, operation = repaired_deletion(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        resolve(ledger, prepared, operation)
        retained = ledger.completion_evidence(prepared.receipt["run_id"])
    (prepared.path.parent / "maintenance" / operation / "receipt.json").unlink()
    (Path(prepared.receipt["observations_directory"]) / "supervision.json").unlink()
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.completion_evidence(prepared.receipt["run_id"]) == retained


def test_rehashed_repair_archive_still_requires_original_finished_capture(tmp_path):
    bound, prepared, _, operation = repaired_deletion(tmp_path)
    receipt = json.loads(
        (prepared.path.parent / "maintenance" / operation / "receipt.json").read_text()
    )
    directory = tmp_path / "phase-evidence" / receipt["cli_run_id"]
    previous = ""
    for path in sorted(directory.glob("*.json")):
        value = json.loads(path.read_text())
        value.pop("sha256")
        if value["phase"] == "deletion_repair_verified":
            value["data"]["details"]["synthetic_observation"] = "changed"
        value["previous_sha256"] = previous
        previous = digest(value)
        path.write_text(canonical({**value, "sha256": previous}) + "\n")
    with Ledger.open(tmp_path / "state", bound) as ledger:
        before = ledger.snapshot()
        with pytest.raises(CoordinationError):
            resolve(ledger, prepared, operation)
        assert ledger.snapshot() == before
