"""Exact blocked-run maintenance, retained failures and explicit abandonment."""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical
from wintappy.coordination.recovery import repair_checkpoints, resolve_hour

from .test_coordination_completion import NOW
from .test_coordination_recovery import blocked_checkpoint_run

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="Linux exclusive maintenance"
)
REASON = "synthetic original and maintenance processes cannot resume"


def repaired(tmp_path):
    bound, prepared, originals = blocked_checkpoint_run(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        operation_id = repair_checkpoints(
            ledger, prepared, NOW, confirmed=True, reason=REASON
        )
    return bound, prepared, originals, operation_id


def resolve(ledger, prepared, operation_id):
    return resolve_hour(
        ledger,
        prepared,
        NOW,
        clock=lambda: NOW,
        confirmed=True,
        reason=REASON,
        maintenance_id=operation_id,
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
def test_missing_or_changed_original_maintenance_evidence_cannot_resolve(
    tmp_path, damage
):
    bound, prepared, originals, operation_id = repaired(tmp_path)
    directory = prepared.path.parent / "maintenance" / operation_id
    if damage == "receipt":
        target = directory / "receipt.json"
    elif damage == "archive":
        receipt = json.loads((directory / "receipt.json").read_text())
        target = next(
            (tmp_path / "phase-evidence" / receipt["cli_run_id"]).glob(
                "*repair_intent.json"
            )
        )
    elif damage.startswith("original_"):
        target = Path(prepared.receipt["observations_directory"]) / (
            damage.removeprefix("original_")
            + (".log" if damage.endswith("stderr") else ".json")
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
            resolve(ledger, prepared, operation_id)
        assert ledger.snapshot() == before and ledger.admit(NOW) == []
        assert ledger.completion_evidence(prepared.receipt["run_id"]) is None
    assert all(path.read_bytes() == payload for path, payload in originals.items())


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
def test_crashed_maintenance_never_retries_or_reopens_without_abandonment(
    tmp_path, monkeypatch, point
):
    from wintappy.coordination import maintenance

    bound, prepared, originals = blocked_checkpoint_run(tmp_path)
    launched = []
    supervised = maintenance.run_supervised

    def crash(stage):
        if stage == point:
            raise SystemExit(97)

    def launch(command, directory, **kwargs):
        receipt_path = Path(directory).parent / "receipt.json"
        assert receipt_path.is_file()
        assert len(maintenance.history(ledger)) == 1  # Intent committed before launch.
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
            repair_checkpoints(ledger, prepared, NOW, confirmed=True, reason=REASON)
        ledger.failure_hook = lambda stage: None
        assert ledger.snapshot() == before
    monkeypatch.setattr(maintenance, "run_supervised", supervised)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        operations = maintenance.history(ledger)
        if point == "before_maintenance_commit":
            assert not operations and not launched
            return  # Uncommitted orphan receipt is not an operation or permission.
        operation_id = next(iter(operations))
        if point == "after_maintenance_finished_commit":
            assert operations[operation_id]["finished"] is not None
            assert resolve(ledger, prepared, operation_id)["mode"] == "OPEN"
            return
        with pytest.raises(CoordinationError):
            repair_checkpoints(ledger, prepared, NOW, confirmed=True, reason=REASON)
        with pytest.raises(CoordinationError):
            resolve(ledger, prepared, operation_id)
        with pytest.raises(CoordinationError):
            maintenance.abandon(
                ledger, operation_id, NOW, confirmed=False, reason=REASON
            )
        with pytest.raises(CoordinationError):
            maintenance.abandon(
                ledger,
                operation_id,
                "2026-01-01T23:59:59Z",
                confirmed=True,
                reason=REASON,
            )
        assert (
            maintenance.abandon(
                ledger, operation_id, NOW, confirmed=True, reason=REASON
            )
            == before
        )
        with pytest.raises(CoordinationError):
            resolve(ledger, prepared, operation_id)
        new_id = repair_checkpoints(
            ledger, prepared, NOW, confirmed=True, reason=REASON
        )
        assert new_id != operation_id
        assert (
            resolve(ledger, prepared, new_id)["next_hour"]
            == prepared.receipt["hour_end"]
        )
    assert all(path.read_bytes() == payload for path, payload in originals.items())


@pytest.mark.parametrize(
    "damage", ["missing_command", "missing_supervision", "changed_stderr"]
)
def test_repair_requires_original_failed_execution_before_launch(
    tmp_path, monkeypatch, damage
):
    from wintappy.coordination import maintenance

    bound, prepared, _ = blocked_checkpoint_run(tmp_path)
    directory = Path(prepared.receipt["observations_directory"])
    if damage == "changed_stderr":
        (directory / "stderr.log").write_text("synthetic changed failure capture")
    else:
        (directory / (damage.removeprefix("missing_") + ".json")).unlink()
    launched = []
    monkeypatch.setattr(
        maintenance, "run_supervised", lambda *a, **kw: launched.append(a)
    )
    with Ledger.open(tmp_path / "state", bound) as ledger:
        before = ledger.snapshot()
        with pytest.raises(CoordinationError):
            repair_checkpoints(ledger, prepared, NOW, confirmed=True, reason=REASON)
        assert not launched and not maintenance.history(ledger)
        assert ledger.snapshot() == before


@pytest.mark.parametrize(
    "damage",
    [
        "missing_intent",
        "missing_finished",
        "changed_finished",
        "future_finished",
        "malformed",
        "record_budget",
        "total_budget",
        "operation_budget",
    ],
)
def test_retained_maintenance_audits_refuse_damage_and_budget_exhaustion(
    tmp_path, monkeypatch, damage
):
    from wintappy.coordination import maintenance

    bound, prepared, _, operation_id = repaired(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        if damage.startswith("missing_"):
            ledger._connection.execute(
                "DELETE FROM events WHERE kind=?",
                ["maintenance_" + damage.removeprefix("missing_")],
            )
        elif damage == "future_finished":
            ledger._connection.execute(
                "UPDATE events SET observed_at='2026-01-02T01:10:00Z' WHERE kind='maintenance_finished'"
            )
        elif damage in {"changed_finished", "malformed"}:
            raw = ledger._connection.execute(
                "SELECT details_json FROM events WHERE kind='maintenance_finished'"
            ).fetchone()[0]
            value = json.loads(raw)
            value["receipt_sha256"] = "0" * 64
            ledger._connection.execute(
                "UPDATE events SET details_json=? WHERE kind='maintenance_finished'",
                ["[]" if damage == "malformed" else canonical(value)],
            )
        elif damage == "record_budget":
            monkeypatch.setattr(maintenance, "MAX_MAINTENANCE_BYTES", 1)
        elif damage == "total_budget":
            monkeypatch.setattr(maintenance, "MAX_MAINTENANCE_TOTAL_BYTES", 1)
        else:
            monkeypatch.setattr(maintenance, "MAX_MAINTENANCE_RUNS", 0)
        before = ledger.snapshot()
        with pytest.raises(CoordinationError):
            resolve(ledger, prepared, operation_id)
        assert ledger.snapshot() == before


@pytest.mark.parametrize(
    "point", ["before_completion_commit", "after_completion_commit"]
)
def test_abrupt_repaired_completion_retains_all_or_none(tmp_path, point):
    bound, prepared, originals, operation_id = repaired(tmp_path)
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
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            script,
            str(tmp_path / "state"),
            canonical(bound),
            prepared.receipt["run_id"],
            operation_id,
            point,
            NOW,
        ],
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 97, result.stderr.decode()
    with Ledger.open(tmp_path / "state", bound) as ledger:
        proof = ledger.completion_evidence(prepared.receipt["run_id"])
        if point == "after_completion_commit":
            assert ledger.snapshot()["mode"] == "OPEN" and proof["version"] == 2
        else:
            assert ledger.snapshot()["mode"] == "BLOCKED" and proof is None
            assert resolve(ledger, prepared, operation_id)["mode"] == "OPEN"
    assert all(path.read_bytes() == payload for path, payload in originals.items())


def test_service_checkpoint_repair_and_resolution_use_retained_original(
    tmp_path, monkeypatch, capsys
):
    from wintappy.coordination import service

    from .test_coordination_service import settings

    bound, prepared, originals = blocked_checkpoint_run(tmp_path)
    value = settings(tmp_path)
    value["binding"] = bound
    config_path = tmp_path / "synthetic-config.json"
    config_path.write_text(canonical(value))
    monkeypatch.setattr(service, "_now", lambda: NOW)
    common = [
        "--config",
        str(config_path),
        "--run-id",
        prepared.receipt["run_id"],
        "--confirm",
        "--reason",
        REASON,
    ]
    assert service.main(["repair-checkpoints", *common]) == 0
    operation_id = json.loads(capsys.readouterr().out)["maintenance_id"]
    with Ledger.open(tmp_path / "state", bound) as ledger:
        summary = service.status_page(ledger, NOW)
        assert summary["maintenance"] == [
            {
                "maintenance_id": operation_id,
                "run_id": prepared.receipt["run_id"],
                "operation": "checkpoints-repair",
                "created_at": NOW,
                "state": "FINISHED",
            }
        ]
        assert ledger.snapshot()["mode"] == "BLOCKED"
    assert (
        service.main(["resolve-hour", *common, "--maintenance-id", operation_id]) == 0
    )
    assert json.loads(capsys.readouterr().out)["mode"] == "OPEN"
    assert all(path.read_bytes() == payload for path, payload in originals.items())


@pytest.mark.parametrize("kind", ["maintenance_intent", "maintenance_finished"])
def test_completed_repair_requires_retained_maintenance_audits_on_reopen(
    tmp_path, kind
):
    bound, prepared, _, operation_id = repaired(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        resolve(ledger, prepared, operation_id)
        ledger._connection.execute("DELETE FROM events WHERE kind=?", [kind])
    with pytest.raises(CoordinationError), Ledger.open(tmp_path / "state", bound):
        pass


def test_conflicting_checkpoint_needs_abandonment_and_explicit_replacement(tmp_path):
    from wintappy.coordination import maintenance

    bound, prepared, originals = blocked_checkpoint_run(tmp_path)
    script = """
import json,sys,duckdb
from raw_sensor_compact.config import Dataset,hour_window
from raw_sensor_compact.inventory import Store
from raw_sensor_compact.phase_evidence import load_archive
from raw_sensor_compact.processing_metrics import ProcessingMetricsStore
records=load_archive(sys.argv[1],sys.argv[2])
row=next(r['data']['rows'][0] for r in records if r['phase']=='checkpoint_intent')
row['run_id']='f'*32
metrics=ProcessingMetricsStore(Store(Dataset.parse(sys.argv[3])))
metrics.publish(duckdb.connect(),hour_window(sys.argv[4],sys.argv[5])[0],row)
"""
    result = subprocess.run(
        [
            prepared.receipt["runtime"]["executable"],
            "-I",
            "-B",
            "-c",
            script,
            str(tmp_path / "phase-evidence"),
            prepared.receipt["cli_run_id"],
            str(tmp_path / "metrics"),
            prepared.receipt["hour_start"],
            prepared.receipt["hour_end"],
        ],
        capture_output=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, result.stderr.decode()
    metrics_path = next((tmp_path / "metrics").rglob("*.parquet"))
    old_bytes = metrics_path.read_bytes()
    with Ledger.open(tmp_path / "state", bound) as ledger:
        before = ledger.snapshot()
        with pytest.raises(CoordinationError):
            repair_checkpoints(ledger, prepared, NOW, confirmed=True, reason=REASON)
        assert ledger.snapshot() == before and metrics_path.read_bytes() == old_bytes
        pending_id = next(iter(maintenance.history(ledger)))
        with pytest.raises(CoordinationError):
            repair_checkpoints(
                ledger,
                prepared,
                NOW,
                confirmed=True,
                reason=REASON,
                replace_checkpoints=True,
            )
        maintenance.abandon(ledger, pending_id, NOW, confirmed=True, reason=REASON)
        new_id = repair_checkpoints(
            ledger,
            prepared,
            NOW,
            confirmed=True,
            reason=REASON,
            replace_checkpoints=True,
        )
        receipt = maintenance.history(ledger)[new_id]["receipt"]
        archive = tmp_path / "phase-evidence" / receipt["cli_run_id"]
        intent = json.loads(next(archive.glob("*repair_intent.json")).read_text())[
            "data"
        ]
        assert intent["confirmed_replacement"] is True
        assert intent["replaced_checkpoints"][0]["run_id"] == "f" * 32
        assert resolve(ledger, prepared, new_id)["mode"] == "OPEN"
    assert all(path.read_bytes() == payload for path, payload in originals.items())


def test_prior_repaired_history_allows_a_later_pending_operation_to_reopen_blocked(
    tmp_path,
):
    from wintappy.coordination import maintenance
    from wintappy.coordination.execution import prepare_execution
    from wintappy.coordination.verification import verify_authority_prepared
    from wintappy.coordination.writer import execute_prepared

    from .test_coordination_execution import config

    bound, prepared, _, operation_id = repaired(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        resolve(ledger, prepared, operation_id)
    later = "2026-01-02T01:10:00Z"
    value = config(tmp_path)
    next_execution = prepare_execution(
        tmp_path / "state", bound, prepared.receipt["hour_end"], later, value
    )
    obstruction = tmp_path / "metrics" / "dayPK=20260102"
    obstruction.write_text("synthetic next-hour publication obstruction")
    with Ledger.open(tmp_path / "state", bound) as ledger:
        verify_authority_prepared(ledger, next_execution, later)
        ledger.drain(later)
        with pytest.raises(CoordinationError):
            execute_prepared(ledger, next_execution, later)
        obstruction.unlink()

        def crash(stage):
            if stage == "after_maintenance_commit":
                raise SystemExit(97)

        ledger.failure_hook = crash
        with pytest.raises(SystemExit):
            repair_checkpoints(
                ledger, next_execution, later, confirmed=True, reason=REASON
            )
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.snapshot()["next_hour"] == prepared.receipt["hour_end"]
        assert len(maintenance.history(ledger)) == 2


def test_retained_repaired_proof_does_not_reopen_historical_runtime_files(tmp_path):
    bound, prepared, _, operation_id = repaired(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        resolve(ledger, prepared, operation_id)
        retained = ledger.completion_evidence(prepared.receipt["run_id"])
    (prepared.path.parent / "maintenance" / operation_id / "receipt.json").unlink()
    (Path(prepared.receipt["observations_directory"]) / "supervision.json").unlink()
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.completion_evidence(prepared.receipt["run_id"]) == retained
        assert ledger.snapshot()["next_hour"] == prepared.receipt["hour_end"]
