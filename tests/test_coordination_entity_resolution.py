"""Fresh immutable acknowledgment resolutions; originals never become release tokens."""

import json
import os
import sys
from pathlib import Path

import pytest

from wintappy.coordination import entity_history, entity_intent, entity_writer
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical, digest
from wintappy.coordination.supervisor import _publish

from .test_coordination_entity_completion_observations import completed_fixture
from .test_coordination_entity_operations import (
    HOUR,
    NOW,
    data_bytes,
    execute_entity,
    ledger_rows,
    prepare_entity,
    writer_scope,
)
from .test_coordination_entity_publications import assert_originals, originals, retain
from .test_coordination_entity_writer_history import record, staged

BLOCK = "2026-01-02T00:10:05Z"
RESOLVED = "2026-01-02T00:10:06Z"

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux genuine original completion and independently supervised resolution",
)


def resolve(ledger, prepared, **overrides):
    return entity_writer.resolve_entity_prepared(
        ledger,
        prepared,
        overrides.pop("now", RESOLVED),
        **{
            "clock": lambda: RESOLVED,
            "confirmed": True,
            "reason": "synthetic immutable acknowledgment resolution; cannot resume",
            **overrides,
        },
    )


def blocked_completion(root, *, publication=False):
    bound, _ = writer_scope(root)
    prepared = prepare_entity(root, bound, "entities-init")
    with Ledger.open(root / "state", bound) as ledger:

        def fail(phase):
            if publication and phase == "after_entity_launch":
                intent = json.loads(
                    ledger.entity_run(prepared.receipt["run_id"])["intent_json"]
                )
                prelaunch = json.loads(
                    (prepared.path.parent / "prelaunch.json").read_text()
                )
                retain(ledger, prepared, intent, prelaunch, now=NOW, clock=lambda: NOW)
            if phase == "after_entity_completion_commit":
                entity_writer._block_uncertain(
                    ledger, prepared.receipt["run_id"], BLOCK
                )
                raise OSError("synthetic actual completion COMMIT acknowledgment loss")

        ledger.failure_hook = fail
        with pytest.raises(CoordinationError):
            execute_entity(ledger, prepared)
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.entity_run(prepared.receipt["run_id"])["state"] == "BLOCKED"
        assert record(ledger, prepared.receipt["run_id"])["proof"]["recovery"] is None
        assert ledger.authority_high_water()["generation"] == 1
    return bound, prepared


def resolution_rows(ledger):
    return ledger._connection.execute(
        "SELECT observed_at,details_json FROM events WHERE kind='entity_resolution_record' "
        "ORDER BY try_cast(json_extract_string(details_json,'$.sequence') AS BIGINT)"
    ).fetchall()


def verifier_calls(monkeypatch):
    calls, actual = [], entity_writer.run_supervised

    def run(*args, **kwargs):
        calls.append(args)
        return actual(*args, **kwargs)

    monkeypatch.setattr(entity_writer, "run_supervised", run)
    return calls


@pytest.mark.parametrize("committed", [False, True])
def test_recovery_clock_before_retained_block_refuses_before_verifier(
    tmp_path, monkeypatch, committed
):
    if committed:
        bound, prepared = blocked_completion(tmp_path)
    else:
        with completed_fixture(tmp_path) as fixture:
            bound, prepared, ledger, _, prelaunch = fixture
            _publish(prepared.path.parent / "prelaunch.json", prelaunch)
            entity_intent.block_entity(
                ledger,
                prepared.receipt["run_id"],
                BLOCK,
                "synthetic later observed block; original child cannot resume",
            )
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows, data, artifacts = (
            ledger_rows(ledger),
            data_bytes(tmp_path),
            originals(prepared),
        )
        calls = verifier_calls(monkeypatch)
        now = "2026-01-02T00:10:04Z"
        assert NOW < now < BLOCK
        with pytest.raises(
            CoordinationError, match="clock predates retained entity block"
        ):
            resolve(ledger, prepared, now=now, clock=lambda: now)
        assert calls == []
        assert ledger_rows(ledger) == rows
        assert data_bytes(tmp_path) == data
        assert_originals(artifacts)


@pytest.mark.parametrize("publication", [False, True])
def test_ack_resolution_retains_distinct_fresh_evidence_and_original_bytes(
    tmp_path, monkeypatch, publication
):
    bound, prepared = blocked_completion(tmp_path, publication=publication)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        before, data, artifacts = (
            ledger_rows(ledger),
            data_bytes(tmp_path),
            originals(prepared),
        )
        original = record(ledger, prepared.receipt["run_id"])
        anchor = ledger.authority_high_water()
        assert ledger.admit(RESOLVED) == []
        calls = verifier_calls(monkeypatch)
        result = resolve(ledger, prepared)
        assert result["mode"] == "DRAINING"
        assert result["next_hour"] == HOUR
        assert result["active_run_id"] is None
        assert len(calls) == 1
        observations = calls[0][1]
        request_path = observations.parent / "request.json"
        request_bytes = request_path.read_bytes()
        request = json.loads(request_bytes)
        assert request_bytes == canonical(request).encode()
        assert request["operation"] == "entity-only-completion"
        assert calls[0][0] == [
            prepared.receipt["runtime"]["executable"],
            "-I",
            "-B",
            "-m",
            "raw_sensor_compact.entity_operation_verifier",
            "--request",
            str(request_path),
            "--request-sha256",
            digest(request),
        ]
        assert (
            json.loads((observations / "command.json").read_text())["command"]
            == calls[0][0]
        )
        assert "entities-replay" not in calls[0][0]
        retained = json.loads(resolution_rows(ledger)[0][1])
        proof = retained["proof"]
        assert retained["sequence"] == 1
        assert retained["previous_sha256"] == ""
        assert retained["proof_sha256"] == digest(proof)
        assert proof["operation"] == "entity-completion-resolution"
        assert proof["completion_sha256"] == original["proof_sha256"]
        assert proof["intent_sha256"] == original["proof"]["intent_sha256"]
        assert proof["verification"] == original["proof"]["verification"]
        assert (
            proof["observation"]["observations_directory"]
            != original["proof"]["observation"]["observations_directory"]
        )
        assert proof["observation"]["request"]["publication"] is None
        assert record(ledger, prepared.receipt["run_id"]) == original
        assert ledger.entity_evidence(prepared.receipt["run_id"]) == original["proof"]
        assert ledger.authority_high_water() == anchor
        after = ledger_rows(ledger)
        for table in (
            "authority_checks",
            "entity_evidence",
            "completion_evidence",
            "compaction_runs",
            "jobs",
            "outbox",
        ):
            assert after[table] == before[table]
        assert ledger.entity_run(prepared.receipt["run_id"])["state"] == "COMPLETED"
        ledger._validate()
        frozen_rows = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            resolve(ledger, prepared)
        assert len(calls) == 1
        assert ledger_rows(ledger) == frozen_rows
    assert data_bytes(tmp_path) == data
    assert_originals(artifacts)
    # History/accessors must authenticate retained JSON, not reconstruct from artifacts.
    monkeypatch.setattr(
        entity_writer,
        "run_supervised",
        lambda *a, **k: pytest.fail("reopen launched a verifier"),
    )
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger_rows(ledger) == frozen_rows
        assert record(ledger, prepared.receipt["run_id"]) == original


@pytest.fixture(scope="module")
def resolved_history(tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic-immutable-resolution")
    bound, prepared = blocked_completion(root, publication=True)
    with Ledger.open(root / "state", bound) as ledger:
        resolve(ledger, prepared)
        yield root, bound, prepared, ledger


@pytest.mark.parametrize(
    "damage",
    [
        "completion",
        "intent",
        "verification",
        "observation",
        "block",
        "clock",
        "sequence",
        "predecessor",
        "audit",
        "duplicate",
        "orphan",
    ],
)
def test_rehashed_resolution_corruption_refuses_without_mutation(
    resolved_history, damage
):
    root, _, prepared, ledger = resolved_history
    rows, data = ledger_rows(ledger), data_bytes(root)
    with staged(ledger):
        retained = json.loads(resolution_rows(ledger)[0][1])
        proof = retained["proof"]
        if damage == "completion":
            proof["completion_sha256"] = "f" * 64
        elif damage == "intent":
            proof["intent_sha256"] = "f" * 64
        elif damage == "verification":
            proof["verification"]["checkpoint_hours"] += 1
        elif damage == "observation":
            proof["observation"]["prelaunch"]["result"]["inputs"]["raw"]["files"] += 1
        elif damage == "block":
            proof["blocked_by_sha256"] = "f" * 64
        elif damage == "clock":
            proof["resolved_at"] = HOUR
        elif damage == "sequence":
            retained["sequence"] = 0
        elif damage == "predecessor":
            retained["previous_sha256"] = "f" * 64
        elif damage == "audit":
            ledger._connection.execute(
                "UPDATE events SET details_json='{}' WHERE kind='entity_resolved'"
            )
        elif damage in {"duplicate", "orphan"}:
            ledger._event(
                "entity_resolution_record",
                prepared.receipt["run_id"] if damage == "duplicate" else "f" * 32,
                RESOLVED,
                retained,
            )
        retained["proof_sha256"] = digest(proof)
        ledger._connection.execute(
            "UPDATE events SET details_json=? WHERE kind='entity_resolution_record' AND identity=?",
            [canonical(retained), prepared.receipt["run_id"]],
        )
        injected = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            ledger._validate()
        assert ledger_rows(ledger) == injected
    assert ledger_rows(ledger) == rows
    assert data_bytes(root) == data


@pytest.mark.parametrize(
    "artifact", ["intent", "prelaunch", "cli", "archive", "original-verifier"]
)
def test_ack_resolution_rechecks_actual_originals_before_fresh_verification(
    tmp_path, monkeypatch, artifact
):
    bound, prepared = blocked_completion(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        original = record(ledger, prepared.receipt["run_id"])["proof"]
        path = {
            "intent": prepared.path.parent / "intent.json",
            "prelaunch": prepared.path.parent / "prelaunch.json",
            "cli": Path(prepared.receipt["observations_directory"]) / "stdout.log",
            "archive": Path(prepared.receipt["settings"]["run_evidence_directory"])
            / prepared.receipt["cli_run_id"]
            / "000002-entities_verified.json",
            "original-verifier": Path(original["observation"]["observations_directory"])
            / "stdout.log",
        }[artifact]
        path.write_bytes(path.read_bytes() + b"\n")
        rows, data = ledger_rows(ledger), data_bytes(tmp_path)
        calls = verifier_calls(monkeypatch)
        with pytest.raises(CoordinationError):
            resolve(ledger, prepared)
        assert calls == []
        assert ledger_rows(ledger) == rows
        assert data_bytes(tmp_path) == data


@pytest.mark.parametrize("budget", ["record", "aggregate", "count", "audit"])
def test_resolution_budgets_refuse_retained_history_without_pruning(
    resolved_history, monkeypatch, budget
):
    from wintappy.coordination import entity_resolution

    root, _, prepared, ledger = resolved_history
    rows, data = ledger_rows(ledger), data_bytes(root)
    module, key = {
        "record": (entity_resolution, "MAX_ENTITY_RESOLUTION_BYTES"),
        "count": (entity_resolution, "MAX_ENTITY_RESOLUTIONS"),
        "aggregate": (entity_intent, "MAX_ENTITY_HISTORY_BYTES"),
        "audit": (entity_intent, "MAX_ENTITY_AUDIT_BYTES"),
    }[budget]
    with monkeypatch.context() as limited:
        limited.setattr(module, key, 0 if budget == "count" else 1)
        with pytest.raises(CoordinationError, match="budget"):
            ledger.entity_evidence(prepared.receipt["run_id"])
    assert ledger_rows(ledger) == rows
    assert data_bytes(root) == data


def test_changed_data_is_rejected_by_a_genuine_fresh_ack_verifier(
    tmp_path, monkeypatch
):
    bound, prepared = blocked_completion(tmp_path)
    source = tmp_path / "raw/raw_process/dayPK=20260101/hourPK=23/part.parquet"
    source.with_name("late.parquet").write_bytes(source.read_bytes())
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows, data = ledger_rows(ledger), data_bytes(tmp_path)
        calls = verifier_calls(monkeypatch)
        with pytest.raises(CoordinationError):
            resolve(ledger, prepared)
        assert len(calls) == 1
        assert ledger_rows(ledger) == rows
        assert data_bytes(tmp_path) == data
        assert ledger.snapshot()["mode"] == "BLOCKED"


@pytest.mark.parametrize("committed", [False, True])
@pytest.mark.parametrize(
    "phase", ["before_entity_resolution_commit", "after_entity_resolution_commit"]
)
def test_uncertain_resolution_commit_preserves_evidence_and_requires_new_audit(
    tmp_path, committed, phase
):
    if committed:
        bound, prepared = blocked_completion(tmp_path)
    else:
        with completed_fixture(tmp_path) as fixture:
            bound, prepared, ledger, _, prelaunch = fixture
            _publish(prepared.path.parent / "prelaunch.json", prelaunch)
            entity_intent.block_entity(
                ledger,
                prepared.receipt["run_id"],
                BLOCK,
                "synthetic original clean blocked entity",
            )
    with Ledger.open(tmp_path / "state", bound) as ledger:
        original = entity_history.retained_record(ledger, prepared.receipt["run_id"])
        anchor, data, artifacts = (
            ledger.authority_high_water(),
            data_bytes(tmp_path),
            originals(prepared),
        )

        def fail(observed):
            target = (
                phase
                if committed
                else phase.replace("entity_resolution", "entity_completion")
            )
            if observed == target:
                raise OSError("synthetic resolution COMMIT acknowledgment failure")

        ledger.failure_hook = fail
        with pytest.raises(CoordinationError):
            resolve(ledger, prepared)
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.snapshot()["next_hour"] == HOUR
        assert ledger.entity_run(prepared.receipt["run_id"])["state"] == "BLOCKED"
        if not committed and phase.startswith("after"):
            assert ledger.authority_high_water()["generation"] == 1
        else:
            assert ledger.authority_high_water() == anchor
        if committed:
            assert record(ledger, prepared.receipt["run_id"]) == original
            assert len(resolution_rows(ledger)) == int(phase.startswith("after"))
        ledger._validate()
        retained_before_retry = resolution_rows(ledger)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert resolve(ledger, prepared)["mode"] == "DRAINING"
        assert (
            resolution_rows(ledger)[: len(retained_before_retry)]
            == retained_before_retry
        )
        if committed:
            assert len(resolution_rows(ledger)) == 1 + int(phase.startswith("after"))
            assert record(ledger, prepared.receipt["run_id"]) == original
        ledger._validate()
    assert data_bytes(tmp_path) == data
    assert_originals(artifacts)
