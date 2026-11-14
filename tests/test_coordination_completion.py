"""Only fresh same-lock verification can atomically finish a live clean writer."""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from wintappy.coordination.execution import prepare_execution
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import (
    CoordinationError,
    canonical,
    digest,
    make_request,
)

from .test_coordination_execution import binding, config, initialize
from .test_coordination_ledger import HOUR

NOW = "2026-01-02T00:10:00Z"
LATER = "2026-01-02T01:10:00Z"

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="Qualified writer/completion supervision is Linux-only",
)


def prepared_empty(tmp_path, hour=HOUR, now=NOW):
    if not (tmp_path / "state").exists():
        initialize(tmp_path)
        for name in ("raw_sensor", "compacted"):
            (tmp_path / name).mkdir()
    return prepare_execution(
        tmp_path / "state", binding(tmp_path), hour, now, config(tmp_path)
    )


def complete(ledger, prepared, now=NOW, *, clock=None):
    from wintappy.coordination.writer import execute_and_complete_prepared

    return execute_and_complete_prepared(
        ledger, prepared, now, clock=clock or (lambda: now)
    )


def test_real_empty_completion_retains_proof_and_advances_exactly_one_hour(tmp_path):
    prepared = prepared_empty(tmp_path)
    req = make_request(
        binding(tmp_path),
        "a" * 32,
        "dbt-build",
        HOUR,
        prepared.receipt["hour_end"],
        NOW,
    )
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.ingest(req, NOW)
        ledger.drain(NOW)
        state = complete(ledger, prepared)
        assert state["mode"] == "OPEN"
        assert state["next_hour"] == prepared.receipt["hour_end"]
        assert state["active_run_id"] is None and state["readers"] == 0
        assert ledger.job(req["job_id"])["state"] == "WAITING"
        assert ledger.writer_run(prepared.receipt["run_id"])["state"] == "COMPLETED"
        proof = ledger.completion_evidence(prepared.receipt["run_id"])
        assert proof["verification"]["raw_source_rows"] == 0
        assert proof["verification"]["raw_output_rows"] == 0
        assert proof["verification"]["execution_sha256"] == prepared.sha256
        assert proof["writer_supervision"]["children_stopped"] is True
        assert len(ledger.admit(NOW)) == 1
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.completion_evidence(prepared.receipt["run_id"]) == proof
        assert ledger.snapshot()["readers"] == 1


def test_sequential_hours_preserve_prior_history_and_backlog_priority(tmp_path):
    first = prepared_empty(tmp_path, now=LATER)
    second = prepared_empty(tmp_path, first.receipt["hour_end"], LATER)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(LATER)
        assert complete(ledger, first, LATER)["mode"] == "DRAINING"
        original = ledger.completion_evidence(first.receipt["run_id"])
        assert ledger.admit(LATER) == []
        assert complete(ledger, second, LATER)["mode"] == "OPEN"
        assert ledger.snapshot()["next_hour"] == second.receipt["hour_end"]
        assert ledger.completion_evidence(first.receipt["run_id"]) == original
        assert ledger.writer_run(second.receipt["run_id"])["state"] == "COMPLETED"
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.completion_evidence(first.receipt["run_id"]) == original


def test_verification_crossing_next_grace_keeps_queued_reader_waiting(
    tmp_path, monkeypatch
):
    from wintappy.coordination import verification

    prepared = prepared_empty(tmp_path)
    current = [NOW]
    original = verification.verify_prepared

    def crossed(*args, **kwargs):
        result = original(*args, **kwargs)
        current[0] = LATER
        return result

    monkeypatch.setattr(verification, "verify_prepared", crossed)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.ingest(
            make_request(
                binding(tmp_path),
                "a" * 32,
                "dbt-build",
                HOUR,
                prepared.receipt["hour_end"],
                NOW,
            ),
            NOW,
        )
        ledger.drain(NOW)
        state = complete(ledger, prepared, clock=lambda: current[0])
        assert state["mode"] == "DRAINING"
        assert ledger.admit(LATER) == []
        assert state["waiting"] == 1 and state["readers"] == 0


@pytest.mark.parametrize("restart", [False, True])
def test_previously_blocked_run_requires_audited_recovery_not_automatic_completion(
    tmp_path, restart
):
    from wintappy.coordination.writer import execute_prepared

    prepared = prepared_empty(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        if restart:
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
        else:
            execute_prepared(ledger, prepared, NOW)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        before = ledger.snapshot()
        with pytest.raises(CoordinationError):
            complete(ledger, prepared)
        assert ledger.snapshot() == before
        assert ledger.completion_evidence(prepared.receipt["run_id"]) is None


def completed_empty(tmp_path):
    prepared = prepared_empty(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        complete(ledger, prepared)
    return prepared


@pytest.mark.parametrize(
    "damage",
    [
        "missing_proof",
        "missing_run",
        "cursor",
        "predecessor",
        "rehash_count",
        "rehash_shutdown",
        "rehash_capture",
        "inactive_checkpoint",
        "erase_completion",
    ],
)
def test_retained_completion_corruption_refuses_open_without_replacement(
    tmp_path, damage
):
    prepared = completed_empty(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        conn = ledger._connection
        if damage in {"missing_proof", "erase_completion"}:
            conn.execute("DELETE FROM completion_evidence")
        if damage in {"missing_run", "erase_completion"}:
            conn.execute("DELETE FROM compaction_runs")
        if damage in {"cursor", "erase_completion"}:
            conn.execute("UPDATE dataset_state SET next_hour=?", [HOUR])
        elif damage == "predecessor":
            conn.execute("UPDATE completion_evidence SET previous_sha256=?", ["0" * 64])
        elif damage.startswith("rehash_") or damage == "inactive_checkpoint":
            proof = ledger.completion_evidence(prepared.receipt["run_id"])
            if damage == "rehash_count":
                proof["verification"]["raw_source_rows"] = True
            elif damage == "rehash_shutdown":
                proof["writer_supervision"]["children_stopped"] = False
            elif damage == "rehash_capture":
                proof["verification"]["raw_source_rows"] = 1
                proof["verification"]["raw_output_rows"] = 1
            else:
                proof["verification"]["checkpoint_sha256"] = "0" * 64
                data = {
                    k: v
                    for k, v in proof["verification"].items()
                    if k not in {"verifier", "verifier_observations"}
                }
                captured = (canonical(data) + "\n").encode()
                proof["verification"]["verifier"]["stdout"].update(
                    bytes_saved=len(captured),
                    bytes_seen=len(captured),
                    sha256=hashlib.sha256(captured).hexdigest(),
                )
            conn.execute(
                "UPDATE completion_evidence SET proof_json=?,proof_sha256=?",
                [canonical(proof), digest(proof)],
            )
    with (
        pytest.raises(CoordinationError),
        Ledger.open(tmp_path / "state", binding(tmp_path)),
    ):
        pass
    assert (tmp_path / "state/coordinator.duckdb").is_file()


def test_real_entity_completion_advances_high_water_atomically_and_preserves_originals(
    tmp_path,
):
    from wintappy.coordination.verification import verify_authority_prepared

    from .test_coordination_authority import prepared_entities

    bound, prepared = prepared_entities(tmp_path)
    originals = {
        p: p.read_bytes()
        for p in (tmp_path / "entity").rglob("*.parquet")
        if p.name != "entities.parquet" and "_authority" not in p.parts
    }
    with Ledger.open(tmp_path / "state", bound) as ledger:
        before = verify_authority_prepared(
            ledger,
            prepared,
            NOW,
            initialize=True,
            confirmed=True,
            reason="synthetic baseline",
        )
        ledger.drain(NOW)
        assert complete(ledger, prepared)["mode"] == "OPEN"
        proof = ledger.completion_evidence(prepared.receipt["run_id"])
        after = ledger.authority_high_water()
        assert proof["authority_before"] == before
        assert proof["verification"]["authority_high_water"] == after
        assert after["generation"] > before["generation"]
        assert proof["verification"]["raw_source_rows"] == 1
        assert (
            ledger._connection.execute(
                "SELECT count(*) FROM authority_checks"
            ).fetchone()[0]
            == 2
        )
        assert all(p.read_bytes() == value for p, value in originals.items())
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.authority_high_water() == after
        assert ledger.completion_evidence(prepared.receipt["run_id"]) == proof
        with pytest.raises(CoordinationError):
            ledger.admit(NOW)


def test_changed_fresh_authority_observations_cannot_complete(tmp_path, monkeypatch):
    from wintappy.coordination import verification

    from .test_coordination_authority import prepared_entities

    bound, prepared = prepared_entities(tmp_path)
    original = verification._verified_authority_observation
    with Ledger.open(tmp_path / "state", bound) as ledger:
        anchor = verification.verify_authority_prepared(
            ledger,
            prepared,
            NOW,
            initialize=True,
            confirmed=True,
            reason="synthetic baseline",
        )

        def changed(*args, **kwargs):
            proof = original(*args, **kwargs)
            (Path(proof["observations_directory"]) / "stdout.log").unlink()
            return proof

        monkeypatch.setattr(verification, "_verified_authority_observation", changed)
        ledger.drain(NOW)
        with pytest.raises(CoordinationError):
            complete(ledger, prepared)
        assert ledger.authority_high_water() == anchor
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.completion_evidence(prepared.receipt["run_id"]) is None


@pytest.mark.parametrize(
    "phase", ["external_success", "before_completion_commit", "after_completion_commit"]
)
@pytest.mark.parametrize("entity", [False, True])
def test_abrupt_completion_death_recovers_all_or_none_and_cannot_relaunch(
    tmp_path, phase, entity
):
    if entity:
        from wintappy.coordination.verification import verify_authority_prepared

        from .test_coordination_authority import prepared_entities

        bound, prepared = prepared_entities(tmp_path)
        with Ledger.open(tmp_path / "state", bound) as ledger:
            anchor = verify_authority_prepared(
                ledger,
                prepared,
                NOW,
                initialize=True,
                confirmed=True,
                reason="synthetic crash baseline",
            )
    else:
        prepared = prepared_empty(tmp_path)
        bound, anchor = binding(tmp_path), None
    script = """
import json,os,sys
from pathlib import Path
from wintappy.coordination.execution import load_execution,PreparedExecution
from wintappy.coordination.ledger import Ledger
from wintappy.coordination import verification
from wintappy.coordination.writer import execute_and_complete_prepared
receipt=load_execution(sys.argv[3],sys.argv[4])
prepared=PreparedExecution(Path(sys.argv[3]),sys.argv[4],receipt,tuple(receipt['command']),receipt['binding'],{})
original=verification.verify_prepared
def verified(*args,**kwargs):
    result=original(*args,**kwargs)
    if sys.argv[5]=='external_success':
        os._exit(37)
    return result
verification.verify_prepared=verified
with Ledger.open(sys.argv[1],json.loads(sys.argv[2])) as ledger:
    if receipt['settings']['entity']:
        verification.verify_authority_prepared(ledger,prepared,sys.argv[6])
    ledger.drain(sys.argv[6])
    def crash(phase):
        if phase==sys.argv[5]:
            os._exit(37)
    ledger.failure_hook=crash
    execute_and_complete_prepared(ledger,prepared,sys.argv[6],clock=lambda:sys.argv[6])
raise AssertionError('failure point not reached')
"""
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            script,
            str(tmp_path / "state"),
            canonical(bound),
            str(prepared.path),
            prepared.sha256,
            phase,
            NOW,
        ],
        check=False,
        capture_output=True,
        timeout=60,
    )
    assert result.returncode == 37, result.stderr.decode()
    with Ledger.open(tmp_path / "state", bound) as ledger:
        committed = phase == "after_completion_commit"
        assert ledger.snapshot()["mode"] == ("OPEN" if committed else "BLOCKED")
        assert bool(ledger.completion_evidence(prepared.receipt["run_id"])) == committed
        assert ledger.snapshot()["next_hour"] == (
            prepared.receipt["hour_end"] if committed else HOUR
        )
        if entity:
            if committed:
                assert (
                    ledger.authority_high_water()["generation"] > anchor["generation"]
                )
                proof = ledger.completion_evidence(prepared.receipt["run_id"])
                assert proof["authority_before"] == anchor
                assert (
                    proof["verification"]["authority_high_water"]
                    == ledger.authority_high_water()
                )
            else:
                assert ledger.authority_high_water() == anchor
            assert ledger._connection.execute(
                "SELECT count(*) FROM authority_checks"
            ).fetchone()[0] == (3 if committed else 2)
        with pytest.raises(CoordinationError):
            complete(ledger, prepared)


@pytest.mark.parametrize("budget", ["proof_bytes", "history_count"])
def test_completion_budget_refuses_without_pruning_or_advancing(
    tmp_path, monkeypatch, budget
):
    from wintappy.coordination import completion

    first = completed_empty(tmp_path)
    second = prepared_empty(tmp_path, first.receipt["hour_end"], LATER)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        retained = ledger.completion_evidence(first.receipt["run_id"])
        monkeypatch.setattr(
            completion,
            "MAX_HISTORY_BYTES" if budget == "proof_bytes" else "MAX_COMPLETED_RUNS",
            1,
        )
        ledger.drain(LATER)
        with pytest.raises(CoordinationError):
            complete(ledger, second, LATER)
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.snapshot()["next_hour"] == first.receipt["hour_end"]
        assert ledger.completion_evidence(first.receipt["run_id"]) == retained
        assert ledger.completion_evidence(second.receipt["run_id"]) is None


def test_retained_completion_audit_cannot_be_removed(tmp_path):
    completed_empty(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger._connection.execute("DELETE FROM events WHERE kind='writer_completed'")
    with (
        pytest.raises(CoordinationError),
        Ledger.open(tmp_path / "state", binding(tmp_path)),
    ):
        pass


def test_history_origin_and_cursor_cannot_be_rebased_together(tmp_path):
    prepared = prepared_empty(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger._connection.execute(
            "UPDATE metadata SET first_hour=?", [prepared.receipt["hour_end"]]
        )
        ledger._connection.execute(
            "UPDATE dataset_state SET next_hour=?", [prepared.receipt["hour_end"]]
        )
    with (
        pytest.raises(CoordinationError),
        Ledger.open(tmp_path / "state", binding(tmp_path)),
    ):
        pass


def test_same_session_history_corruption_cannot_commit_a_later_hour(tmp_path):
    first = completed_empty(tmp_path)
    second = prepared_empty(tmp_path, first.receipt["hour_end"], LATER)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger._connection.execute("UPDATE completion_evidence SET proof_json='{}'")
        ledger.drain(LATER)
        with pytest.raises(CoordinationError):
            complete(ledger, second, LATER)
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.snapshot()["next_hour"] == first.receipt["hour_end"]
        assert ledger.completion_evidence(second.receipt["run_id"]) is None


def test_original_archive_removed_after_verification_cannot_complete(
    tmp_path, monkeypatch
):
    from wintappy.coordination import verification

    prepared = prepared_empty(tmp_path)
    original = verification.verify_prepared

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        directory = (
            Path(prepared.receipt["settings"]["run_evidence_directory"])
            / prepared.receipt["cli_run_id"]
        )
        next(directory.glob("*raw_complete.json")).unlink()
        return result

    monkeypatch.setattr(verification, "verify_prepared", changed)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        with pytest.raises(CoordinationError):
            complete(ledger, prepared)
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.completion_evidence(prepared.receipt["run_id"]) is None


@pytest.mark.parametrize(
    "phase", ["before_completion_commit", "after_completion_commit"]
)
def test_completion_commit_uncertainty_retains_all_or_none_and_never_relaunches(
    tmp_path, phase
):
    prepared = prepared_empty(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)

        def fail(observed):
            if observed == phase:
                raise OSError("synthetic lost completion acknowledgment")

        ledger.failure_hook = fail
        with pytest.raises(CoordinationError):
            complete(ledger, prepared)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        committed = phase == "after_completion_commit"
        assert ledger.snapshot()["mode"] == ("OPEN" if committed else "BLOCKED")
        assert ledger.snapshot()["next_hour"] == (
            prepared.receipt["hour_end"] if committed else HOUR
        )
        assert bool(ledger.completion_evidence(prepared.receipt["run_id"])) == committed
        assert ledger.writer_run(prepared.receipt["run_id"])["state"] == (
            "COMPLETED" if committed else "BLOCKED"
        )
        with pytest.raises(CoordinationError):
            complete(ledger, prepared)


@pytest.mark.parametrize("damage", ["archive", "writer_shutdown", "clock"])
def test_external_success_with_uncertain_verification_cannot_complete(
    tmp_path, monkeypatch, damage
):
    from wintappy.coordination import verification

    prepared = prepared_empty(tmp_path)
    original = verification.verify_prepared
    current = [NOW]

    def damaged(ledger, selected):
        if damage == "archive":
            root = (
                Path(selected.receipt["settings"]["run_evidence_directory"])
                / selected.receipt["cli_run_id"]
            )
            next(root.glob("*raw_complete.json")).unlink()
        elif damage == "writer_shutdown":
            path = Path(selected.receipt["observations_directory"]) / "supervision.json"
            value = json.loads(path.read_text())
            value["children_stopped"] = False
            path.write_text(json.dumps(value))
        result = original(ledger, selected)
        if damage == "clock":
            current[0] = "2026-01-02T00:09:59Z"
        return result

    monkeypatch.setattr(verification, "verify_prepared", damaged)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        with pytest.raises(CoordinationError):
            complete(ledger, prepared, clock=lambda: current[0])
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.snapshot()["next_hour"] == HOUR
        assert ledger.completion_evidence(prepared.receipt["run_id"]) is None
