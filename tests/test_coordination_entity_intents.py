"""Explicit-v5 typed exclusion only: genuine observations, COMMIT and restart."""

import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from wintappy.coordination.entity_verification import verify_entity_inputs_prepared
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import (
    CoordinationError,
    canonical,
    digest,
    make_request,
)
from wintappy.coordination.verification import verify_authority_prepared

from .test_coordination_authority import HOUR, NOW
from .test_coordination_entity_observation_qualification import (
    count_launches,
    multihour,
)
from .test_coordination_entity_observations import scope
from .test_coordination_entity_operations import (
    END,
    data_bytes,
    entity_config,
    ledger_rows,
    prepare_entity,
    seed_scope,
)

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux genuine typed intent and unprivileged COMMIT/restart exclusion",
)


def begin(ledger, prepared, **overrides):
    from wintappy.coordination.entity_intent import begin_entity_prepared

    return begin_entity_prepared(
        ledger,
        prepared,
        NOW,
        **{
            "clock": lambda: NOW,
            "confirmed": True,
            "reason": "synthetic typed entity intent, no entity launch",
            **overrides,
        },
    )


def postcommit(ledger, prepared, intent, **overrides):
    return verify_entity_inputs_prepared(
        ledger,
        prepared,
        NOW,
        **{
            "entity_run_id": prepared.receipt["run_id"],
            "original": intent["input_observation"],
            **overrides,
        },
    )


@pytest.mark.parametrize("replay", [False, True])
def test_genuine_multihour_intent_is_typed_and_changes_only_exclusive_owner(
    tmp_path, replay
):
    bound, prepared = multihour(tmp_path, replay=replay)
    before = data_bytes(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        waiting = make_request(bound, "a" * 32, "dbt-build", HOUR, END, NOW)
        ledger.ingest(waiting, NOW)
        rows, anchor = ledger_rows(ledger), ledger.authority_high_water()
        intent = begin(ledger, prepared)
        run = ledger.entity_run(prepared.receipt["run_id"])
        assert intent["operation"] == "entity-only-intent"
        assert intent["ledger_id"] == ledger.ledger_id
        assert intent["raw_next_hour"] == HOUR
        assert intent["entity_command"] == prepared.receipt["entity_command"]
        assert intent["execution"]["receipt"] == prepared.receipt
        assert intent["execution"]["sha256"] == prepared.sha256
        assert intent["input_observation"]["request"]["original"] is None
        assert intent["input_observation"]["result"]["status"] == "verified"
        assert intent["authority_before"] == anchor
        assert run["intent_json"] == canonical(intent)
        assert run["intent_sha256"] == digest(intent)
        assert run["state"] == "INTENT"
        assert ledger.snapshot()["mode"] == "WRITING"
        assert ledger.snapshot()["next_hour"] == HOUR
        assert ledger.snapshot()["active_run_id"] == run["run_id"]
        assert ledger.authority_high_water() == anchor
        assert ledger.entity_evidence(run["run_id"]) is None
        assert ledger.completion_evidence(run["run_id"]) is None
        assert ledger.admit(NOW) == []
        after = ledger_rows(ledger)
        for name in rows.keys() - {"dataset_state", "events", "entity_runs"}:
            assert after[name] == rows[name]
        assert (
            Path(prepared.path.parent / "intent.json").read_bytes()
            == canonical(intent).encode()
        )
        ledger._validate()
    assert data_bytes(tmp_path) == before
    assert not Path(prepared.receipt["observations_directory"]).exists()


@pytest.mark.parametrize("replay", [False, True])
def test_postcommit_input_observation_requires_original_typed_live_owner(
    tmp_path, replay
):
    bound, prepared = scope(tmp_path, replay=replay)
    before = data_bytes(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        intent = begin(ledger, prepared)
        rows = ledger_rows(ledger)
        fresh = postcommit(ledger, prepared, intent)
        assert {
            key: value
            for key, value in fresh["result"].items()
            if key != "request_sha256"
        } == {
            key: value
            for key, value in intent["input_observation"]["result"].items()
            if key != "request_sha256"
        }
        assert fresh["request"]["original"] is not None
        assert (
            fresh["observations_directory"]
            != intent["input_observation"]["observations_directory"]
        )
        assert ledger_rows(ledger) == rows
        assert ledger.entity_run(prepared.receipt["run_id"])["state"] == "INTENT"
    assert data_bytes(tmp_path) == before
    assert not Path(prepared.receipt["observations_directory"]).exists()


@pytest.mark.parametrize(
    "problem", ["confirmation", "reason", "clock", "rollback", "receipt"]
)
def test_typed_intent_early_contract_refuses_before_verifier_or_owner(
    tmp_path, monkeypatch, problem
):
    bound, prepared = scope(tmp_path)
    before = data_bytes(tmp_path)
    choices = {
        "confirmation": {"confirmed": False},
        "reason": {"reason": None},
        "clock": {"clock": None},
        "rollback": {"clock": lambda: "2026-01-02T00:09:59Z"},
        "receipt": {},
    }[problem]
    if problem == "receipt":
        prepared.path.write_bytes(prepared.path.read_bytes() + b"\n")
    calls = count_launches(monkeypatch)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            begin(ledger, prepared, **choices)
        assert calls == []
        assert ledger_rows(ledger) == rows
    assert data_bytes(tmp_path) == before


def test_v4_never_implicitly_upgrades_for_typed_intent(tmp_path, monkeypatch):
    bound, _ = seed_scope(tmp_path)
    prepared = prepare_entity(tmp_path, bound, "entities-init")
    calls = count_launches(monkeypatch)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows = ledger_rows(ledger)
        with pytest.raises(CoordinationError, match="opt-in v5"):
            begin(ledger, prepared)
        assert ledger_rows(ledger) == rows
        assert calls == []


@pytest.mark.parametrize("owner", ["reader", "raw-intent", "restarted-raw-intent"])
def test_other_committed_ownership_refuses_typed_intent_before_verification(
    tmp_path, monkeypatch, owner
):
    bound, compact = seed_scope(tmp_path, initialized=True, anchored=True)
    Ledger.upgrade(
        tmp_path / "state",
        bound,
        NOW,
        target_version=5,
        confirmed=True,
        reason="synthetic explicit typed-exclusion schema",
    )
    prepared = prepare_entity(tmp_path, bound, "entities-replay")
    with Ledger.open(tmp_path / "state", bound) as ledger:
        verify_authority_prepared(ledger, compact, NOW)
        if owner == "reader":
            request = make_request(bound, "b" * 32, "dbt-build", HOUR, END, NOW)
            ledger.ingest(request, NOW)
            assert len(ledger.admit(NOW)) == 1
        else:
            ledger.drain(NOW)
            ledger.begin_writer(HOUR, NOW, ["synthetic-raw-intent"])
        if owner == "raw-intent":
            calls = count_launches(monkeypatch)
            rows = ledger_rows(ledger)
            with pytest.raises(CoordinationError):
                begin(ledger, prepared)
            assert calls == [] and ledger_rows(ledger) == rows
            return
    with Ledger.open(tmp_path / "state", bound) as ledger:
        calls = count_launches(monkeypatch)
        rows = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            begin(ledger, prepared)
        assert calls == [] and ledger_rows(ledger) == rows


@pytest.mark.parametrize("phase", ["before_entity_commit", "after_entity_commit"])
def test_actual_typed_intent_commit_crash_preserves_restart_exclusion(tmp_path, phase):
    bound, _ = scope(tmp_path)
    before = data_bytes(tmp_path)
    script = """
import json,os,sys
from wintappy.coordination.entity_execution import prepare_entity_execution
from wintappy.coordination.entity_intent import begin_entity_prepared
from wintappy.coordination.ledger import Ledger
root,bound,settings,phase,start,end,now=sys.argv[1:]
bound=json.loads(bound)
prepared=prepare_entity_execution(root+'/state',bound,start,end,now,json.loads(settings),operation='entities-init')
print(json.dumps({'run_id':prepared.receipt['run_id']}),flush=True)
with Ledger.open(root+'/state',bound) as ledger:
    def crash(observed):
        if observed==phase: os._exit(87)
    ledger.failure_hook=crash
    begin_entity_prepared(ledger,prepared,now,clock=lambda:now,confirmed=True,
        reason='synthetic actual typed intent crash, no entity launch')
raise AssertionError('typed intent crash boundary did not execute')
"""
    child = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            script,
            str(tmp_path),
            canonical(bound),
            canonical(entity_config(tmp_path)),
            phase,
            HOUR,
            END,
            NOW,
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=90,
    )
    assert child.returncode == 87, (child.stdout, child.stderr)
    run_id = json.loads(child.stdout)["run_id"]
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.snapshot()["next_hour"] == HOUR
        assert ledger.authority_high_water() is None
        assert ledger.completion_evidence(run_id) is None
        assert ledger.entity_evidence(run_id) is None
        if phase == "before_entity_commit":
            assert ledger.entity_run(run_id) is None
            assert ledger.snapshot()["active_run_id"] is None
        else:
            assert ledger.entity_run(run_id)["state"] == "BLOCKED"
            assert ledger.snapshot()["mode"] == "BLOCKED"
            assert ledger.snapshot()["active_run_id"] == run_id
            assert ledger.admit(NOW) == []
    assert data_bytes(tmp_path) == before
    assert not (tmp_path / "state/entity-executions" / run_id / "observations").exists()


def test_typed_intent_commit_ack_loss_blocks_without_relaunch_or_cursor(tmp_path):
    bound, prepared = scope(tmp_path)
    before = data_bytes(tmp_path)

    def lost_ack(phase):
        if phase == "after_entity_commit":
            raise OSError("synthetic lost acknowledgment after actual intent COMMIT")

    with Ledger.open(tmp_path / "state", bound) as ledger:
        ledger.failure_hook = lost_ack
        with pytest.raises(CoordinationError):
            begin(ledger, prepared)
        run = ledger.entity_run(prepared.receipt["run_id"])
        assert run["state"] == "BLOCKED"
        retained = run["intent_json"], run["intent_sha256"]
        assert ledger.snapshot()["next_hour"] == HOUR
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            begin(ledger, prepared)
        run = ledger.entity_run(prepared.receipt["run_id"])
        assert (run["intent_json"], run["intent_sha256"]) == retained
        assert ledger_rows(ledger) == rows
        assert ledger.admit(NOW) == []
    assert data_bytes(tmp_path) == before


@pytest.mark.parametrize(
    "damage", ["caller-proof", "other-execution", "actual-artifact", "late-input"]
)
def test_postcommit_evidence_never_substitutes_for_retained_original(
    tmp_path, monkeypatch, damage
):
    bound, prepared = scope(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        intent = begin(ledger, prepared)
        run_id = prepared.receipt["run_id"]
        original = json.loads(canonical(intent["input_observation"]))
        if damage == "caller-proof":
            original["result"]["inputs"]["process_input_fingerprint"] = "f" * 64
        elif damage == "other-execution":
            prepared = prepare_entity(tmp_path, bound, "entities-init")
        elif damage == "actual-artifact":
            path = Path(original["observations_directory"]) / "command.json"
            path.write_bytes(path.read_bytes() + b"\n")
        else:
            path = next((tmp_path / "raw").rglob("*.parquet"))
            path.with_name("late.parquet").write_bytes(path.read_bytes())
        calls = count_launches(monkeypatch)
        with pytest.raises(CoordinationError):
            verify_entity_inputs_prepared(
                ledger, prepared, NOW, original=original, entity_run_id=run_id
            )
        if damage != "late-input":
            assert calls == []
        else:
            assert len(calls) == 1  # Actual unchanged-input standalone refusal.
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.entity_run(run_id)["state"] == "BLOCKED"
        assert ledger.entity_run(run_id)["intent_json"] == canonical(intent)
        assert ledger.snapshot()["next_hour"] == HOUR
        assert ledger.authority_high_water() is None


def test_reopen_validates_retained_typed_json_not_runtime_and_never_resumes(
    tmp_path, monkeypatch
):
    from wintappy.coordination import entity_execution, verification

    bound, prepared = scope(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        intent = begin(ledger, prepared)

    def forbidden(*args, **kwargs):
        pytest.fail(
            "Ordinary typed history validation must not reopen runtime/artifacts"
        )

    with monkeypatch.context() as guarded:
        guarded.setattr(entity_execution, "recheck_entity_execution", forbidden)
        guarded.setattr(verification, "_read", forbidden)
        with Ledger.open(tmp_path / "state", bound) as ledger:
            assert ledger.entity_run(prepared.receipt["run_id"])[
                "intent_json"
            ] == canonical(intent)
            assert ledger.snapshot()["mode"] == "BLOCKED"
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            postcommit(ledger, prepared, intent)
        with pytest.raises(CoordinationError):
            ledger.begin_writer(HOUR, NOW, ["synthetic-raw-after-entity-restart"])
        assert ledger_rows(ledger) == rows
        assert ledger.admit(NOW) == []


@pytest.mark.parametrize(
    "damage",
    ["intent-hash", "audit-missing", "audit-changed", "owner", "completed", "proof"],
)
def test_genuine_typed_history_corruption_refuses_without_modifying_rows(
    tmp_path, damage
):
    bound, prepared = scope(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        begin(ledger, prepared)
    with Ledger.open(tmp_path / "state", bound):
        pass  # Retain the genuine restart BLOCKED event before damage.
    with duckdb.connect(str(tmp_path / "state/coordinator.duckdb")) as connection:
        if damage == "intent-hash":
            connection.execute("UPDATE entity_runs SET intent_sha256=?", ["f" * 64])
        elif damage == "audit-missing":
            connection.execute("DELETE FROM events WHERE kind='entity_intent'")
        elif damage == "audit-changed":
            connection.execute(
                "UPDATE events SET details_json='{}' WHERE kind='entity_intent'"
            )
        elif damage == "owner":
            connection.execute("UPDATE dataset_state SET active_run_id=?", ["c" * 32])
        elif damage == "completed":
            connection.execute(
                "UPDATE entity_runs SET state='COMPLETED',block_reason=''"
            )
            connection.execute(
                "UPDATE dataset_state SET mode='OPEN',active_run_id=NULL,block_reason=''"
            )
        else:
            connection.execute(
                "INSERT INTO entity_evidence VALUES (1,?,?,?,'')",
                [prepared.receipt["run_id"], "{}", "f" * 64],
            )
        fake = Ledger(connection, tmp_path / "state", bound)
        rows = ledger_rows(fake)
    with pytest.raises(CoordinationError), Ledger.open(tmp_path / "state", bound):
        pass
    with duckdb.connect(str(tmp_path / "state/coordinator.duckdb")) as connection:
        assert ledger_rows(Ledger(connection, tmp_path / "state", bound)) == rows


@pytest.mark.parametrize("budget", ["record", "count", "aggregate"])
@pytest.mark.parametrize("stage", ["write", "read"])
def test_typed_history_budgets_refuse_without_pruning_originals(
    tmp_path, monkeypatch, budget, stage
):
    from wintappy.coordination import entity_intent

    bound, prepared = scope(tmp_path)
    if stage == "read":
        with Ledger.open(tmp_path / "state", bound) as ledger:
            begin(ledger, prepared)
        with Ledger.open(tmp_path / "state", bound):
            pass
    name = {
        "record": "MAX_ENTITY_INTENT_BYTES",
        "count": "MAX_ENTITY_RUNS",
        "aggregate": "MAX_ENTITY_HISTORY_BYTES",
    }[budget]
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows = ledger_rows(ledger)
    with monkeypatch.context() as limited:
        limited.setattr(entity_intent, name, 0)
        if stage == "read":
            with (
                pytest.raises(CoordinationError),
                Ledger.open(tmp_path / "state", bound),
            ):
                pass
        else:
            with Ledger.open(tmp_path / "state", bound) as ledger:
                with pytest.raises(CoordinationError):
                    begin(ledger, prepared)
                assert ledger_rows(ledger) == rows
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger_rows(ledger) == rows


def test_actual_competitor_is_excluded_on_both_sides_of_entity_intent_commit(tmp_path):
    bound, prepared = scope(tmp_path)
    seen = []
    script = """
import json,sys
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError
try:
    with Ledger.open(sys.argv[1],json.loads(sys.argv[2])): pass
except CoordinationError as error:
    assert 'busy' in str(error).lower()
    sys.exit(88)
raise AssertionError('competing process acquired typed entity intent lock')
"""

    def compete(phase):
        if phase not in {"before_entity_commit", "after_entity_commit"}:
            return
        child = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                script,
                str(tmp_path / "state"),
                canonical(bound),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        assert child.returncode == 88, (child.stdout, child.stderr)
        seen.append(phase)

    with Ledger.open(tmp_path / "state", bound) as ledger:
        ledger.failure_hook = compete
        begin(ledger, prepared)
    assert seen == ["before_entity_commit", "after_entity_commit"]
