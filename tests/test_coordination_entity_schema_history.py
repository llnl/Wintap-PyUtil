"""Opt-in v5 preserves independently observed original v4 evidence byte-for-byte."""

import hashlib
import os
import subprocess
import sys

import duckdb
import pytest

from wintappy.coordination.execution import prepare_execution
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, make_request
from wintappy.coordination.verification import verify_authority_prepared

from .test_coordination_authority import prepared_entities
from .test_coordination_completion import LATER, NOW, complete, prepared_empty
from .test_coordination_entity_schema import rows, upgrade
from .test_coordination_execution import binding, config
from .test_coordination_intentional_retry import retry
from .test_coordination_retry import failed_merge

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux genuine independent completion, entity authority and failed-merge retry",
)


def files(root):
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file() and "state" not in path.relative_to(root).parts
    }


def forbid_runtime(monkeypatch):
    from wintappy.coordination import execution, verification

    def forbidden(*args, **kwargs):
        pytest.fail(
            "Schema/history reads must not reopen original runtime or launch data work"
        )

    monkeypatch.setattr(execution, "_read_bytes", forbidden)
    monkeypatch.setattr(verification, "_read", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)


def assert_preserved(before, after):
    assert after["metadata"][0] == (1, 5, *before["metadata"][0][2:])
    for table, original in before.items():
        if table not in {"metadata", "events"}:
            assert after[table] == original
    assert set(before["events"]) <= set(after["events"])
    assert after["entity_runs"] == after["entity_evidence"] == []


def test_opt_in_preserves_genuine_authority_completion_and_data_without_fresh_token(
    tmp_path, monkeypatch
):
    bound, prepared = prepared_entities(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        verify_authority_prepared(
            ledger,
            prepared,
            NOW,
            initialize=True,
            confirmed=True,
            reason="synthetic independently initialized original",
        )
        ledger.drain(NOW)
        complete(ledger, prepared)
        ledger.ingest(
            make_request(
                bound,
                "b" * 32,
                "dbt-build",
                prepared.receipt["hour_start"],
                prepared.receipt["hour_end"],
                NOW,
            ),
            NOW,
        )
        before, high_water = rows(ledger._connection), ledger.authority_high_water()
    assert before["authority_checks"] and before["completion_evidence"]
    data = files(tmp_path)
    with monkeypatch.context() as scoped:
        forbid_runtime(scoped)
        upgrade(tmp_path / "state", bound, now=NOW)
        with Ledger.open(tmp_path / "state", bound) as ledger:
            assert_preserved(before, rows(ledger._connection))
            assert ledger.authority_high_water() == high_water
            with pytest.raises(CoordinationError, match="freshly"):
                ledger.admit(NOW)
    assert files(tmp_path) == data
    with Ledger.open(tmp_path / "state", bound) as ledger:
        verify_authority_prepared(ledger, prepared, NOW)
        assert len(ledger.admit(NOW)) == 1


def test_opt_in_preserves_actual_failed_original_retry_disposition_and_new_completion(
    tmp_path, monkeypatch
):
    prepared, archive = failed_merge(tmp_path)
    bound = binding(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        retry(ledger, prepared)
        ledger.ingest(
            make_request(
                bound,
                "b" * 32,
                "dbt-build",
                prepared.receipt["hour_start"],
                prepared.receipt["hour_end"],
                NOW,
            ),
            NOW,
        )
        assert len(ledger.admit(NOW)) == 1
        before = rows(ledger._connection)
    assert before["retry_dispositions"] and before["completion_evidence"]
    data = files(tmp_path)
    with monkeypatch.context() as scoped:
        forbid_runtime(scoped)
        upgrade(tmp_path / "state", bound, now=NOW)
        with Ledger.open(tmp_path / "state", bound) as ledger:
            assert_preserved(before, rows(ledger._connection))
            assert ledger.writer_run(prepared.receipt["run_id"])["state"] == "BLOCKED"
            assert ledger.completion_evidence(prepared.receipt["run_id"]) is None
            assert ledger.snapshot()["readers"] == 1
    assert files(tmp_path) == data
    assert all(path.read_bytes() == payload for path, payload in archive.items())


def test_existing_raw_completion_still_works_after_explicit_v5_selection(tmp_path):
    first = prepared_empty(tmp_path, now=LATER)
    bound = binding(tmp_path)
    upgrade(tmp_path / "state", bound, now=LATER)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        ledger.drain(LATER)
        complete(ledger, first, LATER)
        first_proof = ledger.completion_evidence(first.receipt["run_id"])
    second = prepare_execution(
        tmp_path / "state", bound, first.receipt["hour_end"], LATER, config(tmp_path)
    )
    with Ledger.open(tmp_path / "state", bound) as ledger:
        ledger.drain(LATER)
        complete(ledger, second, LATER)
        assert ledger.completion_evidence(first.receipt["run_id"]) == first_proof
        assert ledger.snapshot()["next_hour"] == second.receipt["hour_end"]
        assert ledger._connection.execute(
            "SELECT schema_version FROM metadata"
        ).fetchone() == (5,)
        assert ledger.entity_evidence(first.receipt["run_id"]) is None


@pytest.mark.parametrize("evidence", ["completion_evidence", "retry_dispositions"])
def test_opt_in_refuses_damaged_genuine_history_before_any_schema_change(
    tmp_path, evidence
):
    prepared, archive = failed_merge(tmp_path)
    bound, state = binding(tmp_path), tmp_path / "state"
    with Ledger.open(state, bound) as ledger:
        retry(ledger, prepared)
        ledger._connection.execute(f"UPDATE {evidence} SET proof_sha256=?", ["f" * 64])
        before = rows(ledger._connection)
    with pytest.raises(CoordinationError):
        upgrade(state, bound, now=NOW)
    with duckdb.connect(str(state / "coordinator.duckdb")) as connection:
        assert rows(connection) == before
    assert all(path.read_bytes() == payload for path, payload in archive.items())


def test_opt_in_refuses_damaged_genuine_authority_before_any_schema_change(tmp_path):
    bound, prepared = prepared_entities(tmp_path)
    state = tmp_path / "state"
    with Ledger.open(state, bound) as ledger:
        verify_authority_prepared(
            ledger,
            prepared,
            NOW,
            initialize=True,
            confirmed=True,
            reason="synthetic original authority before corruption",
        )
        ledger._connection.execute(
            "UPDATE authority_checks SET proof_sha256=?", ["f" * 64]
        )
        before = rows(ledger._connection)
    data = files(tmp_path)
    with pytest.raises(CoordinationError):
        upgrade(state, bound, now=NOW)
    with duckdb.connect(str(state / "coordinator.duckdb")) as connection:
        assert rows(connection) == before
    assert files(tmp_path) == data
