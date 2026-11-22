"""Durable original-generation evidence only; genuine CLI launches are test setup."""

import inspect
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from wintappy.coordination import entity_completion, entity_intent
from wintappy.coordination.entity_execution import prepare_entity_execution
from wintappy.coordination.entity_verification import _command
from wintappy.coordination.execution import execution_environment
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import (
    CoordinationError,
    canonical,
    digest,
    make_request,
)
from wintappy.coordination.supervisor import run_supervised
from wintappy.coordination.verification import _observations

from .test_coordination_authority import HOUR, NOW
from .test_coordination_entity_completion_observations import (
    OBSERVED,
    completed_fixture,
    launch_fixture,
    launches,
    observe,
)
from .test_coordination_entity_completion_qualification import archived_report, state
from .test_coordination_entity_intent_history import ConnectionGuard
from .test_coordination_entity_observations import scope
from .test_coordination_entity_operations import (
    END,
    data_bytes,
    entity_config,
    ledger_rows,
)

RECORDED = "2026-01-02T00:10:03Z"

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux genuine current completion, durable publication and COMMIT/restart",
)


def retain(ledger, prepared, intent, prelaunch_observation, **overrides):
    from wintappy.coordination.entity_publication import (
        retain_entity_publication_prepared,
    )

    return retain_entity_publication_prepared(
        ledger,
        prepared,
        overrides.pop("now", OBSERVED),
        **{
            "clock": lambda: RECORDED,
            "original": intent["input_observation"],
            "prelaunch": prelaunch_observation,
            **overrides,
        },
    )


def publication(ledger, run_id):
    from wintappy.coordination.entity_publication import get_retained_entity_publication

    return get_retained_entity_publication(ledger, run_id)


def originals(prepared):
    roots = [
        prepared.path.parent,
        Path(prepared.receipt["settings"]["run_evidence_directory"])
        / prepared.receipt["cli_run_id"],
    ]
    return {
        str(p): p.read_bytes() for root in roots for p in root.rglob("*") if p.is_file()
    }


def assert_originals(values):
    assert all(Path(name).read_bytes() == payload for name, payload in values.items())


@pytest.mark.parametrize("replay", [False, True])
def test_fresh_current_publication_retention_changes_only_evidence_and_its_audit(
    tmp_path, monkeypatch, replay
):
    with completed_fixture(tmp_path, replay=replay, multiple=True) as fixture:
        bound, prepared, ledger, intent, prelaunch = fixture
        old = observe(ledger, prepared, intent, prelaunch)
        waiting = make_request(bound, "d" * 32, "dbt-build", HOUR, END, NOW)
        ledger.ingest(waiting, NOW)
        before, files = state(tmp_path, ledger), originals(prepared)
        calls = launches(monkeypatch)
        retained = retain(ledger, prepared, intent, prelaunch)
        run_id = prepared.receipt["run_id"]
        assert len(calls) == 1  # A new independent verifier, never the entity CLI.
        assert calls[0][0][4] == "raw_sensor_compact.entity_operation_verifier"
        assert set(retained) == {
            "version",
            "operation",
            "ledger_id",
            "binding",
            "run_id",
            "cli_run_id",
            "intent_sha256",
            "recorded_at",
            "observation",
            "completion",
        }
        assert retained["version"] == 1 and type(retained["version"]) is int
        assert retained["operation"] == "entity-original-publication"
        assert retained["ledger_id"] == ledger.ledger_id
        assert retained["binding"] == bound
        assert retained["run_id"] == run_id
        assert retained["cli_run_id"] == prepared.receipt["cli_run_id"]
        assert retained["intent_sha256"] == digest(intent)
        assert retained["recorded_at"] == RECORDED
        fresh = retained["observation"]
        assert fresh["observations_directory"] != old["observations_directory"]
        assert fresh["observed_at"] >= OBSERVED
        assert fresh["observed_at"] <= retained["recorded_at"]
        assert fresh["request"]["publication"] is None
        core = entity_completion.validate_completion_observation(
            fresh, bound, ledger.ledger_id, intent=intent
        )
        assert retained["completion"] == core
        assert set(core) == entity_completion.COMPLETION_FIELDS
        assert core["publication_sha256"] is None
        assert core["checkpoint_hours"] == 4 and core["archive_records"] == 9
        assert (
            core["checkpoint_generation_id"]
            == core["authority_high_water"]["generation_id"]
        )
        assert core["authority_high_water"]["generation"] == (
            intent["authority_before"]["generation"] + 1 if replay else 1
        )
        assert publication(ledger, run_id) == core
        assert ledger.entity_evidence(run_id) == retained
        rows = ledger_rows(ledger)
        assert rows["entity_evidence"] == [
            (1, run_id, canonical(retained), digest(retained), "")
        ]
        assert ledger._connection.execute(
            "SELECT observed_at,details_json FROM events WHERE kind='entity_publication' AND identity=?",
            [run_id],
        ).fetchall() == [
            (
                RECORDED,
                canonical(
                    {
                        "sequence": 1,
                        "intent_sha256": digest(intent),
                        "proof_sha256": digest(retained),
                        "previous_sha256": "",
                    }
                ),
            )
        ]
        for name in before[0].keys() - {"events", "entity_evidence"}:
            assert rows[name] == before[0][name]
        assert state(tmp_path, ledger)[1:] == before[1:]
        assert ledger.admit(RECORDED) == [] and ledger.pending_grants() == []
        assert ledger.job(waiting["job_id"])["state"] == "WAITING"
        assert ledger.completion_evidence(run_id) is None
        assert_originals(files)
        ledger._validate()


@pytest.mark.parametrize(
    "problem", ["original", "prelaunch", "precommit", "clock", "ack", "other-execution"]
)
def test_publication_early_original_and_session_refusals_launch_nothing(
    tmp_path, monkeypatch, problem
):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        options = {}
        if problem == "original":
            options["original"] = None
        elif problem == "prelaunch":
            options["prelaunch"] = None
        elif problem == "precommit":
            options["prelaunch"] = intent["input_observation"]
        elif problem == "clock":
            options["clock"] = None
        elif problem == "ack":
            ledger._entity_live_intent = None
        else:
            prepared = prepare_entity_execution(
                tmp_path / "state",
                prepared.binding,
                HOUR,
                END,
                NOW,
                entity_config(tmp_path),
                operation="entities-init",
            )
        before, files = state(tmp_path, ledger), originals(prepared)
        calls = launches(monkeypatch)
        with pytest.raises(CoordinationError):
            retain(ledger, prepared, intent, prelaunch, **options)
        assert calls == [] and state(tmp_path, ledger) == before
        assert_originals(files)


@pytest.mark.parametrize(
    "artifact", ["receipt", "intent", "prelaunch", "cli", "archive"]
)
def test_retention_rechecks_actual_original_artifacts_before_fresh_verification(
    tmp_path, monkeypatch, artifact
):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        path = {
            "receipt": prepared.path,
            "intent": prepared.path.parent / "intent.json",
            "prelaunch": Path(prelaunch["observations_directory"]) / "stdout.log",
            "cli": Path(prepared.receipt["observations_directory"]) / "stdout.log",
            "archive": archived_report(prepared),
        }[artifact]
        path.write_bytes(path.read_bytes() + b"\n")
        before, files = state(tmp_path, ledger), originals(prepared)
        calls = launches(monkeypatch)
        with pytest.raises(CoordinationError):
            retain(ledger, prepared, intent, prelaunch)
        assert calls == [] and state(tmp_path, ledger) == before
        assert_originals(files)


@pytest.mark.parametrize("damage", ["entity", "metrics", "late-raw"])
def test_old_valid_observation_is_not_a_substitute_for_fresh_current_data(
    tmp_path, monkeypatch, damage
):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        old = observe(ledger, prepared, intent, prelaunch)
        if damage == "late-raw":
            source = next((tmp_path / "raw").rglob("*.parquet"))
            source.with_name("late.parquet").write_bytes(source.read_bytes())
        else:
            next((tmp_path / damage).rglob("*.parquet")).write_bytes(
                b"synthetic invalid parquet"
            )
        assert entity_completion.recheck_completion_observation(ledger, prepared, old)
        before, files = state(tmp_path, ledger), originals(prepared)
        calls = launches(monkeypatch)
        with pytest.raises(CoordinationError):
            retain(ledger, prepared, intent, prelaunch)
        assert len(calls) == 1 and state(tmp_path, ledger) == before
        assert_originals(files)
        assert ledger._connection.execute(
            "SELECT count(*) FROM entity_evidence"
        ).fetchone() == (0,)


def test_retention_clock_regression_after_fresh_verifier_cannot_commit(
    tmp_path, monkeypatch
):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        times = iter([OBSERVED, NOW])
        before = state(tmp_path, ledger)
        calls = launches(monkeypatch)
        with pytest.raises(CoordinationError):
            retain(ledger, prepared, intent, prelaunch, clock=lambda: next(times))
        assert len(calls) == 1 and state(tmp_path, ledger) == before


def evolve_fixture(root, bound, prepared):
    """A distinct later hour changes the actual snapshot; no original checkpoint overwrite."""
    source = next((root / "raw/raw_process").rglob("*.parquet"))
    target = root / "raw/raw_process/dayPK=20260102/hourPK=01/part.parquet"
    target.parent.mkdir(parents=True)
    script = """
import duckdb,sys
with duckdb.connect() as connection:
    connection.execute("COPY (SELECT * REPLACE ('STOP' AS activitytype,200::BIGINT AS eventtime) FROM read_parquet(?,hive_partitioning=false)) TO ? (FORMAT PARQUET)",[sys.argv[1],sys.argv[2]])
"""
    child = subprocess.run(
        [
            prepared.receipt["runtime"]["executable"],
            "-I",
            "-B",
            "-c",
            script,
            str(source),
            str(target),
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
        env={},
    )
    assert child.returncode == 0, (child.stdout, child.stderr)
    forward = prepare_entity_execution(
        root / "state",
        bound,
        "2026-01-02T01:00:00Z",
        "2026-01-02T02:00:00Z",
        NOW,
        entity_config(root),
        operation="entities-replay",
    )
    launch_fixture(forward)


def test_later_generation_cannot_manufacture_missing_original_publication(
    tmp_path, monkeypatch
):
    with completed_fixture(tmp_path, multiple=True) as fixture:
        bound, prepared, ledger, intent, prelaunch = fixture
        old = observe(ledger, prepared, intent, prelaunch)
        evolve_fixture(tmp_path, bound, prepared)
        before, files = state(tmp_path, ledger), originals(prepared)
        # Contract-only validation still passes the old report; it is not durable evidence.
        assert entity_completion.validate_completion_observation(
            old, bound, ledger.ledger_id, intent=intent
        )
        with pytest.raises(CoordinationError):
            publication(ledger, prepared.receipt["run_id"])
        calls = launches(monkeypatch)
        with pytest.raises(CoordinationError):
            retain(ledger, prepared, intent, prelaunch)
        assert len(calls) == 1 and state(tmp_path, ledger) == before
        assert_originals(files)
        assert ledger.entity_evidence(prepared.receipt["run_id"]) is None


def test_reopen_uses_retained_json_without_runtime_reconstruction_or_permission(
    tmp_path, monkeypatch
):
    with completed_fixture(tmp_path) as fixture:
        bound, prepared, ledger, intent, prelaunch = fixture
        retained = retain(ledger, prepared, intent, prelaunch)
        run_id = prepared.receipt["run_id"]
        anchor = ledger.authority_high_water()
    prepared.path.write_bytes(b"synthetic unavailable original receipt")
    archived_report(prepared).write_bytes(b"synthetic unavailable original archive")
    before = data_bytes(tmp_path)

    def forbidden(*args, **kwargs):
        pytest.fail(
            "Ordinary publication history must not reopen runtime/artifacts/data"
        )

    from wintappy.coordination import (
        entity_execution,
        entity_verification,
        verification,
    )

    with monkeypatch.context() as guarded:
        for module, name in (
            (entity_execution, "recheck_entity_execution"),
            (entity_verification, "recheck_input_observation"),
            (entity_completion, "recheck_completion_observation"),
            (entity_completion, "run_supervised"),
            (verification, "_read"),
        ):
            guarded.setattr(module, name, forbidden)
        with Ledger.open(tmp_path / "state", bound) as ledger:
            assert ledger.snapshot()["mode"] == "BLOCKED"
            assert ledger.entity_run(run_id)["state"] == "BLOCKED"
            assert ledger.entity_evidence(run_id) == retained
            assert publication(ledger, run_id) == retained["completion"]
            assert ledger.authority_high_water() == anchor
            assert ledger.snapshot()["next_hour"] == HOUR
            assert ledger._entity_live_intent is None and ledger.admit(RECORDED) == []
            rows = ledger_rows(ledger)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger_rows(ledger) == rows
        with pytest.raises(CoordinationError):
            retain(ledger, prepared, intent, prelaunch)
        assert ledger_rows(ledger) == rows
    assert data_bytes(tmp_path) == before


def test_retained_publication_is_immutable_and_accessor_returns_a_copy(
    tmp_path, monkeypatch
):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        retained = retain(ledger, prepared, intent, prelaunch)
        copied = publication(ledger, prepared.receipt["run_id"])
        copied["archive_sha256"] = "a" * 64
        assert publication(ledger, prepared.receipt["run_id"]) == retained["completion"]
        before, files = state(tmp_path, ledger), originals(prepared)
        calls = launches(monkeypatch)
        with pytest.raises(CoordinationError):
            retain(ledger, prepared, intent, prelaunch)
        assert calls == [] and state(tmp_path, ledger) == before
        assert_originals(files)


@pytest.mark.parametrize(
    "phase", ["before_entity_publication_commit", "after_entity_publication_commit"]
)
def test_actual_publication_commit_process_death_keeps_original_exclusion(
    tmp_path, phase
):
    bound, _ = scope(tmp_path)
    raw = data_bytes(tmp_path, ("raw", "compacted"))
    script = """
import json,os,sys
from pathlib import Path
from tests.test_coordination_entity_intents import begin
from tests.test_coordination_entity_completion_observations import PRELAUNCH,OBSERVED,launch_fixture
from tests.test_coordination_entity_operations import prepare_entity
from wintappy.coordination.entity_verification import verify_entity_inputs_prepared
from wintappy.coordination.ledger import Ledger
root,bound,phase=sys.argv[1:]; root=Path(root); bound=json.loads(bound)
prepared=prepare_entity(root,bound,'entities-init')
with Ledger.open(root/'state',bound) as ledger:
    intent=begin(ledger,prepared)
    prelaunch=verify_entity_inputs_prepared(ledger,prepared,PRELAUNCH,
        original=intent['input_observation'],entity_run_id=prepared.receipt['run_id'])
    launch_fixture(prepared)
    print(json.dumps({'run_id':prepared.receipt['run_id']}),flush=True)
    from wintappy.coordination.entity_publication import retain_entity_publication_prepared
    def crash(observed):
        if observed==phase: os._exit(87)
    ledger.failure_hook=crash
    retain_entity_publication_prepared(ledger,prepared,OBSERVED,
        clock=lambda:'2026-01-02T00:10:03Z',original=intent['input_observation'],prelaunch=prelaunch)
raise AssertionError('publication COMMIT boundary was not reached')
"""
    child = subprocess.run(
        [sys.executable, "-B", "-c", script, str(tmp_path), canonical(bound), phase],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    (tmp_path / "publication-child.json").write_text(
        canonical(
            {
                "returncode": child.returncode,
                "stdout": child.stdout,
                "stderr": child.stderr,
            }
        )
    )
    assert child.returncode == 87, (child.stdout, child.stderr)
    run_id = json.loads(child.stdout)["run_id"]
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.entity_run(run_id)["state"] == "BLOCKED"
        assert ledger.snapshot()["active_run_id"] == run_id
        assert ledger.snapshot()["next_hour"] == HOUR
        assert ledger.authority_high_water() is None and ledger.admit(RECORDED) == []
        assert ledger.completion_evidence(run_id) is None
        if phase.startswith("before"):
            assert ledger.entity_evidence(run_id) is None
            with pytest.raises(CoordinationError):
                publication(ledger, run_id)
        else:
            assert (
                publication(ledger, run_id)
                == ledger.entity_evidence(run_id)["completion"]
            )
        rows = ledger_rows(ledger)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger_rows(ledger) == rows
    assert data_bytes(tmp_path, ("raw", "compacted")) == raw


@pytest.mark.parametrize(
    "phase", ["before_entity_publication_commit", "after_entity_publication_commit"]
)
def test_publication_commit_exception_preserves_rollback_or_committed_lost_ack(
    tmp_path, phase
):
    with completed_fixture(tmp_path) as fixture:
        bound, prepared, ledger, intent, prelaunch = fixture
        before, files = state(tmp_path, ledger), originals(prepared)

        def fail(observed):
            if observed == phase:
                raise OSError("synthetic publication COMMIT acknowledgment failure")

        ledger.failure_hook = fail
        with pytest.raises(CoordinationError):
            retain(ledger, prepared, intent, prelaunch)
        ledger.failure_hook = lambda phase: None
        run_id = prepared.receipt["run_id"]
        if phase.startswith("before"):
            assert state(tmp_path, ledger) == before
            assert ledger.entity_evidence(run_id) is None
        else:
            assert ledger.snapshot()["mode"] == "BLOCKED"
            assert ledger.entity_run(run_id)["state"] == "BLOCKED"
            assert ledger._entity_live_intent is None
            assert (
                publication(ledger, run_id)
                == ledger.entity_evidence(run_id)["completion"]
            )
        assert ledger.snapshot()["next_hour"] == HOUR
        assert ledger.authority_high_water() == before[2]
        assert data_bytes(tmp_path) == before[1]
        assert_originals(files)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.entity_run(run_id)["state"] == "BLOCKED"
        assert ledger.admit(RECORDED) == []


class PublicationGuard(ConnectionGuard):
    def cursor(self):
        return PublicationGuard(
            self.connection.cursor(), independent=True, payloads=self.payloads
        )

    def execute(self, query, parameters=None):
        sql = " ".join(query.lower().split())
        if "from entity_evidence" in sql and sql.startswith("select"):
            if self.independent:
                pytest.fail(
                    "Publication history escaped its original transaction connection"
                )
            if not sql.startswith("select count(*)"):
                if not self.payloads:
                    pytest.fail("Publication payload fetched before SQL budget refusal")
                self.scans.append(
                    ("publications", self.transaction, list(parameters or []))
                )
        return super().execute(query, parameters)


def test_new_publication_and_audit_are_authenticated_inside_original_commit(tmp_path):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        connection, phases = ledger._connection, []
        guard = PublicationGuard(connection)
        ledger._connection = guard

        def staged(phase):
            if phase == "before_entity_publication_commit":
                ledger._validate()
                proof = ledger.entity_evidence(prepared.receipt["run_id"])
                assert (
                    publication(ledger, prepared.receipt["run_id"])
                    == proof["completion"]
                )
                assert guard.transaction
                phases.append(phase)

        try:
            ledger.failure_hook = staged
            retained = retain(ledger, prepared, intent, prelaunch)
            assert retained["operation"] == "entity-original-publication"
        finally:
            ledger.failure_hook = lambda phase: None
            ledger._connection = connection
        assert phases == ["before_entity_publication_commit"]
        assert any(
            kind == "publications" and transaction
            for kind, transaction, _ in guard.scans
        )


@pytest.mark.parametrize(
    "damage",
    [
        "hash",
        "copy",
        "intent",
        "audit-missing",
        "audit-hash",
        "sequence",
        "predecessor",
        "orphan",
    ],
)
def test_retained_publication_corruption_refuses_without_pruning_or_runtime_reads(
    tmp_path, damage
):
    with completed_fixture(tmp_path) as fixture:
        bound, prepared, ledger, intent, prelaunch = fixture
        retained = retain(ledger, prepared, intent, prelaunch)
    with Ledger.open(tmp_path / "state", bound):
        pass  # Genuine restart BLOCKED audit precedes deliberate retained-row corruption.
    from wintappy.coordination import ledger as ledger_module

    with ledger_module._connect(tmp_path / "state/coordinator.duckdb") as connection:
        if damage == "hash":
            connection.execute("UPDATE entity_evidence SET proof_sha256=?", ["f" * 64])
        elif damage in {"copy", "intent"}:
            bad = json.loads(canonical(retained))
            if damage == "copy":
                bad["completion"]["checkpoint_sha256"] = "a" * 64
            else:
                bad["intent_sha256"] = "a" * 64
            connection.execute(
                "UPDATE entity_evidence SET proof_json=?,proof_sha256=?",
                [canonical(bad), digest(bad)],
            )
            details = {
                "sequence": 1,
                "intent_sha256": bad["intent_sha256"],
                "proof_sha256": digest(bad),
                "previous_sha256": "",
            }
            connection.execute(
                "UPDATE events SET details_json=? WHERE kind='entity_publication'",
                [canonical(details)],
            )
        elif damage == "audit-missing":
            connection.execute("DELETE FROM events WHERE kind='entity_publication'")
        elif damage == "audit-hash":
            connection.execute(
                "UPDATE events SET details_json='{}' WHERE kind='entity_publication'"
            )
        elif damage == "sequence":
            connection.execute("UPDATE entity_evidence SET sequence=2")
        elif damage == "predecessor":
            connection.execute(
                "UPDATE entity_evidence SET previous_sha256=?", ["f" * 64]
            )
        else:
            connection.execute("UPDATE entity_evidence SET run_id=?", ["f" * 32])
        rows = ledger_rows(Ledger(connection, tmp_path / "state", bound))
    with pytest.raises(CoordinationError), Ledger.open(tmp_path / "state", bound):
        pass
    with ledger_module._connect(tmp_path / "state/coordinator.duckdb") as connection:
        assert ledger_rows(Ledger(connection, tmp_path / "state", bound)) == rows


@pytest.mark.parametrize("budget", ["record", "count", "aggregate", "audit"])
@pytest.mark.parametrize("stage", ["write", "read"])
def test_publication_budgets_refuse_without_pruning_original_evidence(
    tmp_path, monkeypatch, budget, stage
):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        from wintappy.coordination import entity_publication

        if stage == "read":
            retain(ledger, prepared, intent, prelaunch)
        before, files = state(tmp_path, ledger), originals(prepared)
        module, name = {
            "record": (entity_publication, "MAX_ENTITY_PUBLICATION_BYTES"),
            "count": (entity_publication, "MAX_ENTITY_PUBLICATIONS"),
            "aggregate": (entity_intent, "MAX_ENTITY_HISTORY_BYTES"),
            "audit": (entity_intent, "MAX_ENTITY_AUDIT_BYTES"),
        }[budget]
        calls = launches(monkeypatch)
        with monkeypatch.context() as limited:
            limited.setattr(module, name, 0)
            with pytest.raises(CoordinationError):
                if stage == "read":
                    publication(ledger, prepared.receipt["run_id"])
                else:
                    retain(ledger, prepared, intent, prelaunch)
        assert calls == [] and state(tmp_path, ledger) == before
        assert_originals(files)


@pytest.mark.parametrize("entry", ["validate", "entity-lookup", "publication-lookup"])
def test_publication_sql_budget_precedes_owner_identifier_or_json_fetch(
    tmp_path, monkeypatch, entry
):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        retain(ledger, prepared, intent, prelaunch)
        from wintappy.coordination import entity_publication

        before = state(tmp_path, ledger)
        connection = ledger._connection
        with monkeypatch.context() as limited:
            limited.setattr(entity_publication, "MAX_ENTITY_PUBLICATION_BYTES", 0)
            ledger._connection = PublicationGuard(connection, payloads=False)
            try:
                with pytest.raises(CoordinationError, match="budget"):
                    if entry == "validate":
                        ledger._validate()
                    elif entry == "entity-lookup":
                        ledger.entity_run(prepared.receipt["run_id"])
                    else:
                        publication(ledger, prepared.receipt["run_id"])
            finally:
                ledger._connection = connection
        assert state(tmp_path, ledger) == before


def test_historical_verifier_uses_authenticated_original_core_not_todays_snapshot(
    tmp_path,
):
    with completed_fixture(tmp_path, multiple=True) as fixture:
        bound, prepared, ledger, intent, prelaunch = fixture
        retained = retain(ledger, prepared, intent, prelaunch)
        old_entities = data_bytes(tmp_path, ("entity",))
        old_metrics = data_bytes(tmp_path, ("metrics",))
        evolve_fixture(tmp_path, bound, prepared)
        assert data_bytes(tmp_path, ("entity",)) != old_entities
        assert all(
            data_bytes(tmp_path, ("metrics",))[k] == v for k, v in old_metrics.items()
        )
        core = publication(ledger, prepared.receipt["run_id"])
        assert core == retained["completion"]
        assert core["publication_sha256"] is None
        before = state(tmp_path, ledger)
        request = json.loads(canonical(retained["observation"]["request"]))
        request["publication"] = core
        directory = prepared.path.parent / "synthetic-historical-verifier"
        directory.mkdir()
        (directory / "request.json").write_text(canonical(request))
        observations = directory / "observations"
        settings = prepared.receipt["settings"]
        run_supervised(
            _command(prepared.receipt, observations, request),
            observations,
            timeout_seconds=settings["supervision_timeout_seconds"],
            stop_grace_seconds=settings["stop_grace_seconds"],
            shutdown_seconds=settings["shutdown_seconds"],
            max_log_bytes=settings["max_log_bytes"],
            cwd=prepared.receipt["cwd"],
            env=execution_environment(prepared),
        )
        observed = _observations(
            observations,
            prepared.receipt,
            _command(prepared.receipt, observations, request),
        )
        assert observed[0]["children_stopped"] is True
        result = observed[2]
        assert result["status"] == "verified"
        assert result["publication_sha256"] == digest(core)
        assert result["checkpoint_generation_id"] == core["checkpoint_generation_id"]
        assert result["checkpoint_sha256"] == core["checkpoint_sha256"]
        assert (
            result["authority_high_water"]["generation"]
            == core["authority_high_water"]["generation"] + 1
        )
        assert result["entity_state_sha256"] != core["entity_state_sha256"]
        with pytest.raises(CoordinationError):
            observe(
                ledger, prepared, intent, prelaunch
            )  # Qualified adapter stays current-only.
        assert state(tmp_path, ledger) == before


def test_retention_api_does_not_accept_caller_completion_or_publication_dicts(tmp_path):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        old = observe(ledger, prepared, intent, prelaunch)
        from wintappy.coordination.entity_publication import (
            retain_entity_publication_prepared,
        )

        assert set(
            inspect.signature(retain_entity_publication_prepared).parameters
        ) == {
            "ledger",
            "prepared",
            "now",
            "clock",
            "original",
            "prelaunch",
        }
        before = state(tmp_path, ledger)
        with pytest.raises(TypeError):
            retain(ledger, prepared, intent, prelaunch, publication=old["result"])
        assert state(tmp_path, ledger) == before
