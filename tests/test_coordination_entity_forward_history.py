"""Actual interleaved raw/entity history, never today's data as old evidence.

The fixture alone executes writers/verifiers. Original-scope corruption is staged
and rolled back; history and terminal-refusal controls may not read old artifacts.
"""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from wintappy.coordination import entity_history, entity_writer
from wintappy.coordination.execution import prepare_execution
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import (
    CoordinationError,
    canonical,
    digest,
    make_request,
)
from wintappy.coordination.verification import _observations, verify_authority_prepared
from wintappy.coordination.writer import execute_and_complete_prepared

from .test_coordination_entity_completion_history_qualification import no_original_reads
from .test_coordination_entity_operations import (
    HOUR,
    data_bytes,
    ledger_rows,
    prepare_entity,
)
from .test_coordination_entity_publications import originals, retain
from .test_coordination_entity_resolution import blocked_completion, resolve
from .test_coordination_entity_writer_history import record, staged, store_record
from .test_coordination_execution import PROJECT, config

END = "2026-01-02T00:00:00Z"
FINAL = "2026-01-02T01:00:00Z"
LATER = "2026-01-02T01:11:00Z"

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux genuine interleaved raw/entity completions and authority evolution",
)


def capture_owner(ledger, prepared):
    run_id = prepared.receipt["run_id"]
    return {
        "owner": ledger.entity_run(run_id),
        "completion": ledger.entity_evidence(run_id),
        "record": record(ledger, run_id),
        "publication": ledger._connection.execute(
            "SELECT * FROM entity_evidence WHERE run_id=?", [run_id]
        ).fetchall(),
        "resolutions": ledger._connection.execute(
            "SELECT observed_at,details_json FROM events WHERE kind='entity_resolution_record' "
            "AND identity=? ORDER BY observed_at",
            [run_id],
        ).fetchall(),
    }


def all_originals(prepared):
    return {
        name: payload for item in prepared for name, payload in originals(item).items()
    }


@pytest.fixture(scope="module")
def forward_history(tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic-interleaved-entity-raw-history")
    bound, first = blocked_completion(root, publication=True)
    with Ledger.open(root / "state", bound) as ledger:
        resolve(ledger, first)
        captured = {first.receipt["run_id"]: capture_owner(ledger, first)}
        first_files = all_originals((first,))
        waiting = make_request(
            bound, "a" * 32, "dbt-build", HOUR, END, "2026-01-02T00:10:07Z"
        )
        ledger.ingest(waiting, "2026-01-02T00:10:07Z")

        raw_first = prepare_execution(
            root / "state", bound, HOUR, "2026-01-02T00:11:00Z", config(root)
        )
        verify_authority_prepared(ledger, raw_first, "2026-01-02T00:11:00Z")
        ledger.drain("2026-01-02T00:11:00Z")
        result = execute_and_complete_prepared(
            ledger,
            raw_first,
            "2026-01-02T00:11:00Z",
            clock=lambda: "2026-01-02T00:11:00Z",
        )
        assert result["next_hour"] == END
        raw_original = ledger.completion_evidence(raw_first.receipt["run_id"])
        assert raw_original["verification"]["raw_source_rows"] == 1

        second = prepare_entity(root, bound, "entities-replay")

        def fail(phase):
            if phase == "after_entity_launch":
                intent = json.loads(
                    ledger.entity_run(second.receipt["run_id"])["intent_json"]
                )
                prelaunch = json.loads(
                    (second.path.parent / "prelaunch.json").read_text()
                )
                retain(
                    ledger,
                    second,
                    intent,
                    prelaunch,
                    now="2026-01-02T00:12:00Z",
                    clock=lambda: "2026-01-02T00:12:00Z",
                )
            if phase == "after_entity_completion_commit":
                entity_writer._block_uncertain(
                    ledger, second.receipt["run_id"], "2026-01-02T00:12:01Z"
                )
                raise OSError("synthetic actual replay completion acknowledgment loss")

        ledger.failure_hook = fail
        with pytest.raises(CoordinationError):
            entity_writer.execute_entity_and_complete_prepared(
                ledger,
                second,
                "2026-01-02T00:12:00Z",
                clock=lambda: "2026-01-02T00:12:00Z",
                confirmed=True,
                reason="synthetic replay after genuine raw cursor completion",
            )
        assert ledger.snapshot()["mode"] == "BLOCKED"
        ledger.failure_hook = lambda phase: None
        resolve(
            ledger,
            second,
            now="2026-01-02T00:12:02Z",
            clock=lambda: "2026-01-02T00:12:02Z",
        )
        captured[second.receipt["run_id"]] = capture_owner(ledger, second)
        assert all_originals((first,)) == first_files
        before_future = all_originals((first, second, raw_first))

        # Only standalone DuckDB 2.0 creates a genuinely new next-hour STOP input.
        source = list(
            (root / "compacted/raw_process/dayPK=20260101/hourPK=23").glob("*.parquet")
        )
        assert len(source) == 1
        target = root / "raw/raw_process/dayPK=20260102/hourPK=00/part.parquet"
        target.parent.mkdir(parents=True)
        script = """
import sys,duckdb
assert duckdb.__version__ == '2.0.0'
with duckdb.connect(config={'threads':2,'memory_limit':'128MB'}) as c:
    c.execute("COPY (SELECT * REPLACE ('STOP' AS activitytype,200::BIGINT AS eventtime) FROM read_parquet(?,hive_partitioning=false)) TO ? (FORMAT PARQUET)",sys.argv[1:])
"""
        child = subprocess.run(
            [
                str(PROJECT / ".venv/bin/python"),
                "-I",
                "-B",
                "-c",
                script,
                str(source[0]),
                str(target),
            ],
            env={},
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        (root / "next-input-child.json").write_text(
            canonical(
                {
                    "returncode": child.returncode,
                    "stdout": child.stdout,
                    "stderr": child.stderr,
                }
            )
            + "\n"
        )
        assert child.returncode == 0, (child.stdout, child.stderr)
        raw_second = prepare_execution(root / "state", bound, END, LATER, config(root))
        verify_authority_prepared(ledger, raw_second, LATER)
        ledger.drain(LATER)
        result = execute_and_complete_prepared(
            ledger, raw_second, LATER, clock=lambda: LATER
        )
        assert result["next_hour"] == FINAL and result["active_run_id"] is None
        latest = ledger.completion_evidence(raw_second.receipt["run_id"])
        assert latest["verification"]["raw_source_rows"] == 1
        assert ledger.authority_high_water()["generation"] == 4
        assert (
            latest["verification"]["authority_high_water"]
            == ledger.authority_high_water()
        )
        current_cli = _observations(
            Path(raw_second.receipt["observations_directory"]),
            raw_second.receipt,
            raw_second.command,
        )[2]
        assert current_cli["status"] == "completed"
        assert (
            current_cli["process_entities"]["generation_id"]
            == latest["verification"]["checkpoint_generation_id"]
        )
        assert current_cli["process_entities"]["hosts"][0]["bootstrap_start"] == HOUR
        assert ledger.job(waiting["job_id"])["state"] == "WAITING"
        assert ledger.pending_grants() == []
        assert all_originals((first, second, raw_first)) == before_future
        for prepared in (first, second):
            assert canonical(capture_owner(ledger, prepared)) == canonical(
                captured[prepared.receipt["run_id"]]
            )
        assert ledger.completion_evidence(raw_first.receipt["run_id"]) == raw_original
        captured["raw_original"] = raw_original
        captured["raw_latest"] = latest
        captured["current_cli"] = current_cli
        captured["artifact_sha256_before_future"] = {
            name: hashlib.sha256(payload).hexdigest()
            for name, payload in before_future.items()
        }
        (root / "original-history-capture.json").write_text(canonical(captured) + "\n")
        ledger._validate()
    return root, bound, (first, second), (raw_first, raw_second), captured


def state(root, ledger, prepared):
    return ledger_rows(ledger), data_bytes(root), all_originals(prepared)


@pytest.mark.parametrize("entry", ["validate", "scope", "accessors"])
def test_genuine_forward_history_keeps_originals_not_current_aggregate(
    forward_history, monkeypatch, entry
):
    root, bound, typed, raw, captured = forward_history
    with Ledger.open(root / "state", bound) as ledger:
        before = state(root, ledger, (*typed, *raw))
        current = ledger.authority_high_water()
        assert current["generation"] == 4
        with no_original_reads(monkeypatch):
            if entry == "validate":
                ledger._validate()
            for prepared in typed:
                original = captured[prepared.receipt["run_id"]]
                intent = json.loads(original["owner"]["intent_json"])
                proof = original["completion"]
                if entry == "scope":
                    entity_history.validate_original_scope(ledger, intent)
                if entry == "accessors":
                    assert canonical(capture_owner(ledger, prepared)) == canonical(
                        original
                    )
                assert proof["next_hour"] == intent["raw_next_hour"]
                assert (
                    proof["verification"]["authority_high_water"]["generation"]
                    < current["generation"]
                )
                assert (
                    proof["verification"]["authority_high_water"]["generation_id"]
                    != current["generation_id"]
                )
                assert (
                    proof["observation"]["execution_result"]["hosts"][0][
                        "semantic_sha256"
                    ]
                    != captured["current_cli"]["process_entities"]["hosts"][0][
                        "semantic_sha256"
                    ]
                )
                assert (
                    proof["observation"]["execution_result"]["hosts"][0][
                        "bootstrap_start"
                    ]
                    == HOUR
                )
            first, second = (captured[p.receipt["run_id"]]["completion"] for p in typed)
            assert first["raw_cursor_before_sha256"] is None
            assert second["raw_cursor_before_sha256"] == digest(
                captured["raw_original"]
            )
            assert second["next_hour"] == END < ledger.snapshot()["next_hour"] == FINAL
            assert (
                ledger._authority_checked is None and ledger._entity_live_intent is None
            )
            with pytest.raises(CoordinationError):
                ledger.admit(LATER)
            assert ledger.snapshot()["readers"] == 0 and ledger.pending_grants() == []
        assert state(root, ledger, (*typed, *raw)) == before


@pytest.mark.parametrize("entry", ["scope", "history"])
@pytest.mark.parametrize(
    "damage",
    [
        "authority-missing",
        "authority-current",
        "authority-late",
        "raw-missing",
        "raw-current",
        "raw-null",
        "raw-late",
        "cursor-rollback",
    ],
)
def test_historical_original_scope_damage_refuses_without_reopening_or_mutation(
    forward_history, monkeypatch, entry, damage
):
    root, bound, typed, raw, _ = forward_history
    prepared = typed[1]
    run_id = prepared.receipt["run_id"]
    with Ledger.open(root / "state", bound) as ledger:
        before = state(root, ledger, (*typed, *raw))
        intent = json.loads(ledger.entity_run(run_id)["intent_json"])
        with staged(ledger), no_original_reads(monkeypatch):
            entity_history.validate_original_scope(ledger, intent)
            retained = record(ledger, run_id)
            proof = retained["proof"]
            assert intent["raw_next_hour"] == END
            assert proof["raw_cursor_before_sha256"] == digest(
                ledger.completion_evidence(raw[0].receipt["run_id"])
            )
            if damage == "authority-missing":
                ledger._connection.execute(
                    "DELETE FROM authority_checks WHERE proof_sha256=?",
                    [intent["authority_proof_sha256"]],
                )
            elif damage == "authority-current":
                proof["authority_before_sha256"] = ledger._connection.execute(
                    "SELECT proof_sha256 FROM authority_checks ORDER BY sequence DESC LIMIT 1"
                ).fetchone()[0]
            elif damage == "authority-late":
                old = intent["authority_proof_sha256"]
                original = json.loads(
                    ledger._connection.execute(
                        "SELECT proof_json FROM authority_checks WHERE proof_sha256=?",
                        [old],
                    ).fetchone()[0]
                )
                original["observed_at"] = LATER
                intent["authority_proof_sha256"] = proof["authority_before_sha256"] = (
                    digest(original)
                )
                ledger._connection.execute(
                    "UPDATE authority_checks SET proof_json=?,proof_sha256=? WHERE proof_sha256=?",
                    [canonical(original), digest(original), old],
                )
            elif damage == "raw-missing":
                ledger._connection.execute(
                    "DELETE FROM completion_evidence WHERE run_id=?",
                    [raw[0].receipt["run_id"]],
                )
            elif damage == "raw-current":
                proof["raw_cursor_before_sha256"] = digest(
                    ledger.completion_evidence(raw[1].receipt["run_id"])
                )
            elif damage == "raw-null":
                proof["raw_cursor_before_sha256"] = None
            elif damage == "raw-late":
                original = ledger.completion_evidence(raw[0].receipt["run_id"])
                original["completed_at"] = LATER
                proof["raw_cursor_before_sha256"] = digest(original)
                ledger._connection.execute(
                    "UPDATE completion_evidence SET proof_json=?,proof_sha256=? WHERE run_id=?",
                    [canonical(original), digest(original), raw[0].receipt["run_id"]],
                )
            else:
                ledger._connection.execute(
                    "UPDATE dataset_state SET next_hour=?", [HOUR]
                )
            store_record(ledger, run_id, retained)
            if damage == "authority-late":
                ledger._connection.execute(
                    "UPDATE entity_runs SET intent_json=?,intent_sha256=? WHERE run_id=?",
                    [canonical(intent), digest(intent), run_id],
                )
            injected = ledger_rows(ledger)
            with pytest.raises(CoordinationError):
                if entry == "scope":
                    entity_history.validate_original_scope(ledger, intent)
                else:
                    ledger._validate()
            assert ledger_rows(ledger) == injected
        assert state(root, ledger, (*typed, *raw)) == before


@pytest.mark.parametrize("owner", [0, 1])
@pytest.mark.parametrize("entry", ["execute", "resolve"])
def test_old_completed_receipts_cannot_relaunch_or_resolve_after_forward_evolution(
    forward_history, monkeypatch, owner, entry
):
    root, bound, typed, raw, _ = forward_history
    with Ledger.open(root / "state", bound) as ledger:
        before = state(root, ledger, (*typed, *raw))
        function = (
            entity_writer.execute_entity_and_complete_prepared
            if entry == "execute"
            else entity_writer.resolve_entity_prepared
        )
        with (
            no_original_reads(monkeypatch, execution_receipt=True),
            pytest.raises(CoordinationError),
        ):
            function(
                ledger,
                typed[owner],
                LATER,
                clock=lambda: LATER,
                confirmed=True,
                reason="synthetic old completed original must not resume",
            )
        assert state(root, ledger, (*typed, *raw)) == before
        assert ledger.snapshot()["next_hour"] == FINAL
        assert ledger.authority_high_water()["generation"] == 4
        assert ledger.snapshot()["readers"] == 0 and ledger.pending_grants() == []
