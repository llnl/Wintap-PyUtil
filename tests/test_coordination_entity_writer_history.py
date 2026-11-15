"""Genuine typed successes; original publication and raw history remain separate."""

import json
import os
import sys
from contextlib import contextmanager

import pytest

from wintappy.coordination import entity_intent
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical, digest

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
from .test_coordination_entity_publications import retain

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux genuine typed writer, current-only resolution and original history",
)


@contextmanager
def staged(ledger):
    ledger._connection.execute("BEGIN TRANSACTION")
    try:
        yield
    finally:
        ledger._connection.execute("ROLLBACK")


@pytest.fixture(scope="module")
def successful_history(tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic-entity-success-history")
    bound, _ = writer_scope(root)
    prepared = prepare_entity(root, bound, "entities-init")
    with Ledger.open(root / "state", bound) as ledger:
        execute_entity(ledger, prepared)
        yield root, bound, prepared, ledger


def record(ledger, run_id):
    return json.loads(
        ledger._connection.execute(
            "SELECT details_json FROM events "
            "WHERE kind='entity_completion_record' AND identity=?",
            [run_id],
        ).fetchone()[0]
    )


def store_record(ledger, run_id, retained):
    retained["proof_sha256"] = digest(retained["proof"])
    ledger._connection.execute(
        "UPDATE events SET details_json=? "
        "WHERE kind='entity_completion_record' AND identity=?",
        [canonical(retained), run_id],
    )


def test_genuine_completed_history_survives_replay_without_original_rewrites(tmp_path):
    bound, _ = writer_scope(tmp_path)
    prepared = prepare_entity(tmp_path, bound, "entities-init")
    before = data_bytes(tmp_path, ("raw", "compacted"))
    with Ledger.open(tmp_path / "state", bound) as ledger:
        execute_entity(ledger, prepared)
        original_intent = ledger.entity_run(prepared.receipt["run_id"])
        original_proof = ledger.entity_evidence(prepared.receipt["run_id"])
        original_anchor = ledger.authority_high_water()
    replay = prepare_entity(tmp_path, bound, "entities-replay")
    with Ledger.open(tmp_path / "state", bound) as ledger:
        execute_entity(ledger, replay)
        assert ledger.authority_high_water()["generation"] == 2
        assert ledger.entity_run(prepared.receipt["run_id"]) == original_intent
        assert ledger.entity_evidence(prepared.receipt["run_id"]) == original_proof
        assert original_proof["verification"]["authority_high_water"] == original_anchor
        assert ledger.snapshot()["next_hour"] == HOUR
        ledger._validate()
    assert data_bytes(tmp_path, ("raw", "compacted")) == before
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.entity_evidence(prepared.receipt["run_id"]) == original_proof
        assert ledger.entity_run(replay.receipt["run_id"])["state"] == "COMPLETED"


@pytest.mark.parametrize(
    "damage",
    [
        "intent",
        "receipt",
        "verification",
        "prelaunch",
        "authority",
        "cursor",
        "completed-clock",
        "sequence",
        "predecessor",
        "audit",
        "duplicate",
        "orphan",
    ],
)
def test_rehashed_success_history_corruption_refuses_without_mutation(
    successful_history, damage
):
    root, _, prepared, ledger = successful_history
    run_id = prepared.receipt["run_id"]
    before, data = ledger_rows(ledger), data_bytes(root)
    with staged(ledger):
        retained = record(ledger, run_id)
        proof = retained["proof"]
        if damage == "intent":
            proof["intent_sha256"] = "f" * 64
        elif damage == "receipt":
            proof["receipt"]["cli_run_id"] = "f" * 32
        elif damage == "verification":
            proof["verification"]["checkpoint_hours"] += 1
        elif damage == "prelaunch":
            proof["observation"]["prelaunch"]["result"]["inputs"]["raw"]["files"] += 1
        elif damage == "authority":
            proof["authority_before_sha256"] = "f" * 64
        elif damage == "cursor":
            proof["raw_cursor_before_sha256"] = "f" * 64
        elif damage == "completed-clock":
            proof["completed_at"] = HOUR
        elif damage == "sequence":
            retained["sequence"] += 1
        elif damage == "predecessor":
            retained["previous_sha256"] = "f" * 64
        elif damage == "audit":
            ledger._connection.execute(
                "UPDATE events SET details_json='{}' WHERE kind='entity_completed'"
            )
        elif damage in {"duplicate", "orphan"}:
            ledger._event(
                "entity_completion_record",
                run_id if damage == "duplicate" else "f" * 32,
                NOW,
                retained,
            )
        store_record(ledger, run_id, retained)
        injected = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            ledger._validate()
        assert ledger_rows(ledger) == injected
    assert ledger_rows(ledger) == before
    assert data_bytes(root) == data


@pytest.mark.parametrize("damage", ["record", "aggregate", "audit", "count"])
def test_success_record_budgets_refuse_without_pruning(
    successful_history, monkeypatch, damage
):
    from wintappy.coordination import entity_history

    root, _, prepared, ledger = successful_history
    before, data = ledger_rows(ledger), data_bytes(root)
    with monkeypatch.context() as limited:
        if damage == "record":
            limited.setattr(entity_history, "MAX_ENTITY_COMPLETION_BYTES", 1)
        elif damage == "aggregate":
            limited.setattr(entity_intent, "MAX_ENTITY_HISTORY_BYTES", 1)
        elif damage == "audit":
            limited.setattr(entity_intent, "MAX_ENTITY_AUDIT_BYTES", 1)
        else:
            limited.setattr(entity_history, "MAX_ENTITY_COMPLETIONS", 0)
        with pytest.raises(CoordinationError, match="budget"):
            ledger.entity_evidence(prepared.receipt["run_id"])
    assert ledger_rows(ledger) == before
    assert data_bytes(root) == data


def test_original_publication_remains_immutable_after_audited_clean_resolution(
    tmp_path,
    monkeypatch,
):
    from wintappy.coordination.entity_publication import get_retained_entity_publication
    from wintappy.coordination.entity_writer import resolve_entity_prepared

    with completed_fixture(tmp_path) as fixture:
        bound, prepared, ledger, intent, prelaunch = fixture
        original = retain(ledger, prepared, intent, prelaunch)
        # Explicitly retain the genuine prelaunch observation for original-clean recovery.
        from wintappy.coordination.supervisor import _publish

        _publish(prepared.path.parent / "prelaunch.json", prelaunch)
    monkeypatch.setattr(
        "wintappy.coordination.ledger._now", lambda: "2026-01-02T00:10:03Z"
    )
    with Ledger.open(tmp_path / "state", bound) as ledger:
        resolved = resolve_entity_prepared(
            ledger,
            prepared,
            "2026-01-02T00:10:04Z",
            clock=lambda: "2026-01-02T00:10:04Z",
            confirmed=True,
            reason="synthetic original clean resolution; cannot resume",
        )
        assert resolved["next_hour"] == HOUR
        assert (
            ledger.entity_evidence(prepared.receipt["run_id"])["operation"]
            == "entity-only-completion"
        )
        assert (
            get_retained_entity_publication(ledger, prepared.receipt["run_id"])
            == original["completion"]
        )
        assert ledger._connection.execute(
            "SELECT proof_json FROM entity_evidence"
        ).fetchall() == [(canonical(original),)]


@pytest.mark.parametrize(
    "phase", ["before_entity_completion_commit", "after_entity_completion_commit"]
)
def test_completion_commit_acknowledgment_loss_requires_explicit_resolution(
    tmp_path, phase
):
    from wintappy.coordination.entity_writer import resolve_entity_prepared

    bound, _ = writer_scope(tmp_path)
    prepared = prepare_entity(tmp_path, bound, "entities-init")
    before = data_bytes(tmp_path, ("raw", "compacted"))

    def fail(observed):
        if observed == phase:
            raise OSError("synthetic completion acknowledgment failure")

    with Ledger.open(tmp_path / "state", bound) as ledger:
        ledger.failure_hook = fail
        with pytest.raises(CoordinationError):
            execute_entity(ledger, prepared)
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.snapshot()["next_hour"] == HOUR
        assert (ledger.authority_high_water() is not None) == (
            phase.startswith("after")
        )
        assert ledger.entity_run(prepared.receipt["run_id"])["state"] == "BLOCKED"
    with Ledger.open(tmp_path / "state", bound) as ledger:
        result = resolve_entity_prepared(
            ledger,
            prepared,
            NOW,
            clock=lambda: NOW,
            confirmed=True,
            reason="synthetic completion acknowledgment resolution; cannot resume",
        )
        assert result["mode"] == "DRAINING"
        assert ledger.authority_high_water()["generation"] == 1
        assert ledger.entity_run(prepared.receipt["run_id"])["state"] == "COMPLETED"
        ledger._validate()
    assert data_bytes(tmp_path, ("raw", "compacted")) == before
