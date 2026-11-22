"""Read-only original-bound completion; test-only CLI launches confer no permission."""

import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path

import pytest

from wintappy.coordination.completion import _manifest
from wintappy.coordination.entity_execution import prepare_entity_execution
from wintappy.coordination.entity_verification import (
    _command,
    verify_entity_inputs_prepared,
)
from wintappy.coordination.execution import execution_environment
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical, digest
from wintappy.coordination.supervisor import run_supervised
from wintappy.coordination.verification import _observations

from .test_coordination_authority import NOW
from .test_coordination_entity_intents import begin
from .test_coordination_entity_observation_qualification import (
    multihour,
    rehash_capture,
)
from .test_coordination_entity_observations import scope
from .test_coordination_entity_operations import data_bytes, entity_config, ledger_rows

PRELAUNCH = "2026-01-02T00:10:01Z"
OBSERVED = "2026-01-02T00:10:02Z"

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux genuine original inputs, typed exclusion and supervised CLI shutdown",
)


def launch_fixture(prepared):
    """Isolated test setup only, not an implemented entity writer/service adapter."""
    receipt, settings = prepared.receipt, prepared.receipt["settings"]
    run_supervised(
        list(prepared.command),
        Path(receipt["observations_directory"]),
        timeout_seconds=settings["supervision_timeout_seconds"],
        stop_grace_seconds=settings["stop_grace_seconds"],
        shutdown_seconds=settings["shutdown_seconds"],
        max_log_bytes=settings["max_log_bytes"],
        cwd=receipt["cwd"],
        env=execution_environment(prepared),
    )
    observed = _observations(
        Path(receipt["observations_directory"]), receipt, prepared.command
    )
    assert observed[0]["children_stopped"] is True
    assert observed[2]["status"] == "completed"
    assert observed[2]["run_id"] == receipt["cli_run_id"]
    assert observed[2]["coordinator_run_id"] == receipt["run_id"]
    assert observed[2]["phase_evidence"]["migration_complete"] is False
    assert "raw_complete" not in observed[2]
    return observed


@contextmanager
def completed_fixture(root, *, replay=False, multiple=False):
    bound, prepared = (multihour if multiple else scope)(root, replay=replay)
    raw = data_bytes(root, ("raw", "compacted"))
    with Ledger.open(root / "state", bound) as ledger:
        intent = begin(ledger, prepared)
        prelaunch = verify_entity_inputs_prepared(
            ledger,
            prepared,
            PRELAUNCH,
            original=intent["input_observation"],
            entity_run_id=prepared.receipt["run_id"],
        )
        rows = ledger_rows(ledger)
        launch_fixture(prepared)
        assert ledger_rows(ledger) == rows
        assert data_bytes(root, ("raw", "compacted")) == raw
        assert ledger.entity_run(prepared.receipt["run_id"])["state"] == "INTENT"
        yield bound, prepared, ledger, intent, prelaunch


def observe(ledger, prepared, intent, prelaunch_observation, **overrides):
    from wintappy.coordination.entity_completion import (
        verify_entity_completion_prepared,
    )

    return verify_entity_completion_prepared(
        ledger,
        prepared,
        overrides.pop("now", OBSERVED),
        **{
            "original": intent["input_observation"],
            "prelaunch": prelaunch_observation,
            **overrides,
        },
    )


def launches(monkeypatch):
    from wintappy.coordination import entity_completion

    calls, actual = [], entity_completion.run_supervised

    def run(*args, **kwargs):
        calls.append(args)
        return actual(*args, **kwargs)

    monkeypatch.setattr(entity_completion, "run_supervised", run)
    return calls


@pytest.mark.parametrize("replay", [False, True])
def test_genuine_current_generation_completion_is_read_only_and_original_bound(
    tmp_path, replay
):
    with completed_fixture(tmp_path, replay=replay, multiple=True) as fixture:
        bound, prepared, ledger, intent, prelaunch = fixture
        before, rows = data_bytes(tmp_path), ledger_rows(ledger)
        anchor = ledger.authority_high_water()
        acknowledgment = ledger._entity_live_intent
        proof = observe(ledger, prepared, intent, prelaunch)
        result = proof["result"]
        assert proof["operation"] == "entity-completion-observation"
        assert proof["receipt"] == prepared.receipt
        assert proof["intent_sha256"] == digest(intent)
        assert proof["prelaunch"] == prelaunch
        assert proof["request"]["original"] == prelaunch["request"]["original"]
        assert proof["request"]["publication"] is None
        assert result["operation"] == "entity-only-completion"
        assert result["ledger_id"] == ledger.ledger_id
        assert result["dataset_sha256"] == digest(bound)
        assert result["intent_sha256"] == digest(intent)
        assert result["supervision_sha256"] == digest(proof["execution_supervision"])
        assert result["checkpoint_hours"] == 4
        assert result["archive_records"] == 9
        assert result["authority_high_water"]["generation"] == (
            anchor["generation"] + 1 if anchor else 1
        )
        assert (
            result["checkpoint_generation_id"]
            == result["authority_high_water"]["generation_id"]
        )
        assert result["publication_sha256"] is None
        assert proof["supervision"]["children_stopped"] is True
        assert ledger_rows(ledger) == rows
        assert data_bytes(tmp_path) == before
        assert ledger.authority_high_water() == anchor
        assert ledger._entity_live_intent == acknowledgment
        assert ledger.entity_evidence(prepared.receipt["run_id"]) is None
        assert ledger.completion_evidence(prepared.receipt["run_id"]) is None


@pytest.mark.parametrize(
    "damage", ["original", "prelaunch", "precommit", "receipt", "intent", "clock"]
)
def test_early_binding_refusals_launch_no_verifier_and_change_no_rows(
    tmp_path, monkeypatch, damage
):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        options = {}
        if damage == "original":
            options["original"] = None
        elif damage == "prelaunch":
            options["prelaunch"] = None
        elif damage == "precommit":
            options["prelaunch"] = intent["input_observation"]
        elif damage == "clock":
            options["now"] = NOW
        else:
            path = (
                prepared.path
                if damage == "receipt"
                else prepared.path.parent / "intent.json"
            )
            path.write_bytes(path.read_bytes() + b"\n")
        rows, before = ledger_rows(ledger), data_bytes(tmp_path)
        calls = launches(monkeypatch)
        with pytest.raises(CoordinationError):
            observe(ledger, prepared, intent, prelaunch, **options)
        assert calls == []
        assert ledger_rows(ledger) == rows
        assert data_bytes(tmp_path) == before


def test_restart_cannot_reuse_completion_as_live_acknowledgment(tmp_path, monkeypatch):
    with completed_fixture(tmp_path) as fixture:
        bound, prepared, _, intent, prelaunch = fixture
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows = ledger_rows(ledger)
        assert ledger.snapshot()["mode"] == "BLOCKED"
        calls = launches(monkeypatch)
        with pytest.raises(CoordinationError):
            observe(ledger, prepared, intent, prelaunch)
        assert calls == []
        assert ledger_rows(ledger) == rows


@pytest.mark.parametrize(
    "artifact", ["command.json", "supervision.json", "stdout.log", "stderr.log"]
)
@pytest.mark.parametrize("alias", ["bytes", "symlink", "hardlink"])
def test_actual_cli_artifacts_require_original_bytes_and_single_link_files(
    tmp_path, monkeypatch, artifact, alias
):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        path = Path(prepared.receipt["observations_directory"]) / artifact
        if alias == "bytes":
            path.write_bytes(path.read_bytes() + b"\n")
        else:
            moved = path.with_name(path.name + ".retained")
            path.rename(moved)
            if alias == "symlink":
                path.symlink_to(moved)
            else:
                os.link(moved, path)
        rows, before = ledger_rows(ledger), data_bytes(tmp_path)
        calls = launches(monkeypatch)
        with pytest.raises(CoordinationError):
            observe(ledger, prepared, intent, prelaunch)
        assert calls == []
        assert ledger_rows(ledger) == rows
        assert data_bytes(tmp_path) == before


@pytest.mark.parametrize(
    "damage",
    [
        "intent",
        "original",
        "inputs",
        "execution",
        "dataset",
        "run",
        "version",
        "records",
        "hours",
        "historical",
        "generation",
        "publication",
        "request-publication",
    ],
)
def test_rehashed_completion_contract_never_confers_current_or_historical_authority(
    tmp_path, damage
):
    with completed_fixture(tmp_path) as fixture:
        bound, prepared, ledger, intent, prelaunch = fixture
        proof = observe(ledger, prepared, intent, prelaunch)
        from wintappy.coordination.entity_completion import (
            validate_completion_observation,
        )

        bad = json.loads(canonical(proof))
        result = bad["result"]
        key = {
            "intent": "intent_sha256",
            "original": "original_sha256",
            "inputs": "inputs_sha256",
            "execution": "execution_sha256",
            "dataset": "dataset_sha256",
            "run": "run_id",
        }.get(damage)
        if key:
            result[key] = "a" * (32 if damage == "run" else 64)
        elif damage == "version":
            result["version"] = True
        elif damage == "records":
            result["archive_records"] += 1
        elif damage == "hours":
            result["checkpoint_hours"] = True
        elif damage == "historical":
            result["checkpoint_generation_id"] = "a" * 64
        elif damage == "generation":
            result["authority_high_water"]["generation"] += 1
        elif damage == "publication":
            result["publication_sha256"] = "a" * 64
        else:
            bad["request"]["publication"] = {
                "synthetic": "unauthenticated original generation"
            }
            result["request_sha256"] = digest(bad["request"])
            command = _command(
                prepared.receipt, Path(bad["observations_directory"]), bad["request"]
            )
            bad["supervision"]["command_sha256"] = digest(
                _manifest(prepared.receipt, command)
            )
        rehash_capture(bad)
        rows = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            validate_completion_observation(bad, bound, ledger.ledger_id, intent=intent)
        assert ledger_rows(ledger) == rows


@pytest.mark.parametrize(
    "artifact",
    ["request.json", "command.json", "supervision.json", "stdout.log", "stderr.log"],
)
def test_completed_verifier_observation_recheck_reads_actual_artifacts(
    tmp_path, artifact
):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        proof = observe(ledger, prepared, intent, prelaunch)
        from wintappy.coordination.entity_completion import (
            recheck_completion_observation,
        )

        directory = Path(proof["observations_directory"])
        path = (
            directory.parent / artifact
            if artifact == "request.json"
            else directory / artifact
        )
        path.write_bytes(path.read_bytes() + b"\n")
        rows, before = ledger_rows(ledger), data_bytes(tmp_path)
        with pytest.raises(CoordinationError):
            recheck_completion_observation(ledger, prepared, proof)
        assert ledger_rows(ledger) == rows
        assert data_bytes(tmp_path) == before


@pytest.mark.parametrize(
    "budget",
    [
        "MAX_ENTITY_REQUEST_BYTES",
        "MAX_ENTITY_RESULT_BYTES",
        "MAX_ENTITY_OBSERVATION_BYTES",
    ],
)
def test_completion_byte_budgets_refuse_without_pruning_or_permission_changes(
    tmp_path, monkeypatch, budget
):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        from wintappy.coordination import entity_completion

        originals = {
            str(p.relative_to(tmp_path)): p.read_bytes()
            for p in prepared.path.parent.rglob("*")
            if p.is_file()
        }
        rows, before = ledger_rows(ledger), data_bytes(tmp_path)
        monkeypatch.setattr(entity_completion, budget, 1)
        with pytest.raises(CoordinationError):
            observe(ledger, prepared, intent, prelaunch)
        assert all(
            (tmp_path / name).read_bytes() == value for name, value in originals.items()
        )
        assert ledger_rows(ledger) == rows
        assert data_bytes(tmp_path) == before


def test_actual_post_cli_input_arrival_is_not_certified_by_original_reports(tmp_path):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        source = tmp_path / "raw/raw_process/dayPK=20260101/hourPK=23/part.parquet"
        source.with_name("late.parquet").write_bytes(source.read_bytes())
        rows, before = ledger_rows(ledger), data_bytes(tmp_path)
        with pytest.raises(CoordinationError):
            observe(ledger, prepared, intent, prelaunch)
        assert ledger_rows(ledger) == rows
        assert data_bytes(tmp_path) == before


def test_actual_forward_generation_has_no_caller_publication_fallback(tmp_path):
    with completed_fixture(tmp_path) as fixture:
        bound, prepared, ledger, intent, prelaunch = fixture
        forward = prepare_entity_execution(
            tmp_path / "state",
            bound,
            "2026-01-02T00:00:00Z",
            "2026-01-02T01:00:00Z",
            NOW,
            entity_config(tmp_path),
            operation="entities-replay",
        )
        # Deliberate isolated out-of-band evolution; never an admission/launch API.
        launch_fixture(forward)
        rows, before = ledger_rows(ledger), data_bytes(tmp_path)
        with pytest.raises(CoordinationError):
            observe(ledger, prepared, intent, prelaunch)
        assert ledger_rows(ledger) == rows
        assert data_bytes(tmp_path) == before
        assert ledger.authority_high_water() is None
