"""Retained-proof semantics and real bounded-history/legacy compatibility controls."""

import copy
import hashlib
import json
import sys
from pathlib import Path

import duckdb
import pytest

from wintappy.coordination.completion import validate_completion_proof
from wintappy.coordination.ledger import SCHEMA_V2, Ledger
from wintappy.coordination.protocol import (
    CoordinationError,
    canonical,
    digest,
    make_request,
)

from .test_coordination_authority import prepared_entities
from .test_coordination_completion import (
    HOUR,
    NOW,
    complete,
    completed_empty,
    prepared_empty,
)
from .test_coordination_execution import binding

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="Real writer/verifier controls require Linux supervision",
)


@pytest.fixture(scope="module")
def retained_raw(tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic-retained-proof")
    prepared = completed_empty(root)
    with Ledger.open(root / "state", binding(root)) as ledger:
        return (
            ledger.completion_evidence(prepared.receipt["run_id"]),
            ledger.writer_run(prepared.receipt["run_id"]),
            binding(root),
            ledger.ledger_id,
        )


@pytest.mark.parametrize(
    "damage",
    [
        "proof_version",
        "ledger_identity",
        "run_identity",
        "clock",
        "cursor",
        "backlog",
        "unknown_field",
        "inactive_anchor",
        "inactive_checkpoint",
        "inactive_generation",
        "result_version",
        "result_identity",
        "result_count",
        "result_shape",
        "archive_identity",
        "shutdown",
        "command_identity",
        "capture_identity",
        "observations_path",
    ],
)
def test_retained_proof_semantic_negatives_are_independent_of_audit_rejection(
    retained_raw, damage
):
    proof, run, bound, identity = copy.deepcopy(retained_raw)
    if damage == "proof_version":
        proof["version"] = True
    elif damage == "ledger_identity":
        proof["ledger_id"] = "0" * 32
    elif damage == "run_identity":
        proof["run_id"] = "0" * 32
    elif damage == "clock":
        proof["completed_at"] = "2026-01-01T23:59:59Z"
    elif damage == "cursor":
        proof["next_hour"] = "2026-01-02T02:00:00Z"
    elif damage == "backlog":
        proof["mode"] = "DRAINING"
    elif damage == "unknown_field":
        proof["unexpected"] = "synthetic"
    elif damage == "inactive_anchor":
        proof["authority_before"] = {}
    elif damage == "inactive_checkpoint":
        proof["verification"]["checkpoint_sha256"] = "0" * 64
    elif damage == "inactive_generation":
        proof["verification"]["checkpoint_generation_id"] = "0" * 64
    elif damage == "result_version":
        proof["verification"]["version"] = True
    elif damage == "result_identity":
        proof["verification"]["execution_sha256"] = "0" * 64
    elif damage == "result_count":
        proof["verification"]["raw_source_rows"] = True
    elif damage == "result_shape":
        proof["verification"] = []
    elif damage == "archive_identity":
        proof["verification"]["archive_sha256"] = None
    elif damage == "shutdown":
        proof["writer_supervision"]["children_stopped"] = False
    elif damage == "command_identity":
        proof["verification"]["verifier"]["command_sha256"] = "0" * 64
    elif damage == "capture_identity":
        proof["verification"]["raw_source_rows"] = 1
        proof["verification"]["raw_output_rows"] = 1
    else:
        proof["verification"]["verifier_observations"] = (
            "/synthetic/unrelated-observations"
        )
    with pytest.raises(CoordinationError):
        validate_completion_proof(proof, run, bound, identity)


def test_three_real_completed_hours_validate_across_instrumented_two_row_pages(
    tmp_path,
):
    current = "2026-01-02T03:10:00Z"
    prepared = prepared_empty(tmp_path, now=current)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(current)
        for _ in range(3):
            complete(ledger, prepared, current)
            selected = prepared.receipt["run_id"]
            prepared = prepared_empty(tmp_path, prepared.receipt["hour_end"], current)
        original, pages = ledger._connection, []

        class TwoRowPages:
            def execute(self, sql, *args):
                if sql.startswith("SELECT * FROM completion_evidence WHERE sequence>"):
                    pages.append(args[0][0])
                    sql = sql.replace("LIMIT 256", "LIMIT 2")
                return original.execute(sql, *args)

        ledger._connection = TwoRowPages()
        try:
            ledger._validate_completion_history()
            assert pages == [0, 2, 3]
            proof = ledger.completion_evidence(selected)
            proof["version"] = True
            original.execute(
                "UPDATE completion_evidence SET proof_json=?,proof_sha256=? WHERE sequence=3",
                [canonical(proof), digest(proof)],
            )
            pages.clear()
            with pytest.raises(CoordinationError):
                ledger._validate_completion_history()
            assert pages == [0, 2]
        finally:
            ledger._connection = original


def test_v2_schema_fixture_preserves_genuine_authority_proof_and_reader_reservation(
    tmp_path,
):
    from wintappy.coordination.verification import verify_authority_prepared

    bound, prepared = prepared_entities(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        anchor = verify_authority_prepared(
            ledger,
            prepared,
            NOW,
            initialize=True,
            confirmed=True,
            reason="synthetic compatible baseline",
        )
        req = make_request(
            bound, "b" * 32, "dbt-build", HOUR, prepared.receipt["hour_end"], NOW
        )
        ledger.ingest(req, NOW)
        grant = ledger.admit(NOW)[0]
        identity = ledger.ledger_id
        # Reconstruct the old schema, preserving genuine proof bytes and supported
        # audits. This is an isolated compatibility fixture, not backup promotion
        # or manufactured data/recovery evidence; no reader process was launched.
        retained = {
            table: ledger._connection.execute(f"SELECT * FROM {table}").fetchall()
            for table in (
                "dataset_state",
                "jobs",
                "outbox",
                "compaction_runs",
                "authority_checks",
            )
        }
        events = ledger._connection.execute(
            "SELECT * FROM events WHERE kind <> 'completion_history_initialized'"
        ).fetchall()
    legacy = tmp_path / "legacy-state"
    legacy.mkdir()
    with duckdb.connect(str(legacy / "coordinator.duckdb")) as conn:
        conn.execute(SCHEMA_V2)
        conn.execute(
            "INSERT INTO metadata VALUES (1,2,?,?)", [identity, canonical(bound)]
        )
        for table, rows in {**retained, "events": events}.items():
            if rows:
                conn.executemany(
                    f"INSERT INTO {table} VALUES ({','.join('?' for _ in rows[0])})",
                    rows,
                )
    with pytest.raises(CoordinationError), Ledger.open(legacy, bound):
        pass
    Ledger.upgrade(
        legacy, bound, NOW, confirmed=True, reason="synthetic v2 compatibility upgrade"
    )
    with Ledger.open(legacy, bound) as ledger:
        assert ledger.ledger_id == identity
        assert ledger.authority_high_water() == anchor
        assert ledger.snapshot()["next_hour"] == HOUR
        assert ledger.snapshot()["readers"] == 1
        assert ledger.pending_grants() == [grant]
        for table, rows in retained.items():
            assert (
                ledger._connection.execute(f"SELECT * FROM {table}").fetchall() == rows
            )
        assert (
            ledger._connection.execute(
                "SELECT count(*) FROM completion_evidence"
            ).fetchone()[0]
            == 0
        )
        with pytest.raises(CoordinationError):
            ledger.admit(NOW)


@pytest.mark.parametrize("fail", [False, True])
def test_completion_excludes_ambient_and_exception_secrets_and_secret_hashes(
    tmp_path, monkeypatch, fail
):
    from wintappy.coordination import verification

    secret = "SYNTHETIC-COMPLETION-SECRET-NEVER-RETAIN"
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", secret)
    prepared = prepared_empty(tmp_path)
    if fail:

        def broken(*args, **kwargs):
            raise OSError(secret)

        monkeypatch.setattr(verification, "verify_prepared", broken)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        if fail:
            with pytest.raises(CoordinationError) as error:
                complete(ledger, prepared)
            assert secret not in str(error.value)
        else:
            complete(ledger, prepared)
        for table in ("compaction_runs", "completion_evidence", "events"):
            rows = ledger._connection.execute(f"SELECT * FROM {table}").fetchall()
            serialized = json.dumps(rows)
            assert secret not in serialized
            assert hashlib.sha256(secret.encode()).hexdigest() not in serialized
        assert secret not in repr(prepared)


def test_missing_evolving_clock_refuses_before_intent_or_launch(tmp_path):
    from wintappy.coordination.writer import execute_and_complete_prepared

    prepared = prepared_empty(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        before = ledger.snapshot()
        with pytest.raises(CoordinationError):
            execute_and_complete_prepared(ledger, prepared, NOW, clock=None)
        assert ledger.snapshot() == before
        assert ledger.writer_run(prepared.receipt["run_id"]) is None


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_retained_writer_capture_cannot_exceed_receipt_budget(retained_raw, stream):
    proof, run, bound, identity = copy.deepcopy(retained_raw)
    receipt = json.loads(run["intent_json"])["execution"]["receipt"]
    over = receipt["settings"]["max_log_bytes"] + 1
    proof["writer_supervision"][stream].update(bytes_saved=over, bytes_seen=over)
    # Keep all outer request/result/capture identities consistent, so an unrelated
    # hash/audit conflict cannot mask the effective-receipt budget defect.
    supervision_sha256 = digest(proof["writer_supervision"])
    result = proof["verification"]
    result["supervision_sha256"] = supervision_sha256
    request = {
        "version": 1,
        "arguments": receipt["command"][5:],
        "execution_sha256": digest(receipt),
        "supervision_sha256": supervision_sha256,
        "intent_sha256": run["intent_sha256"],
        "authority_high_water": None,
    }
    result["request_sha256"] = digest(request)
    observations = result["verifier_observations"]
    command = [
        receipt["runtime"]["executable"],
        "-I",
        "-B",
        "-m",
        "raw_sensor_compact.completion",
        "--request",
        str(Path(observations).parent / "request.json"),
        "--request-sha256",
        digest(request),
    ]
    settings = receipt["settings"]
    result["verifier"]["command_sha256"] = digest(
        {
            "command": command,
            "cwd": receipt["cwd"],
            "timeout_seconds": settings["supervision_timeout_seconds"],
            "stop_grace_seconds": settings["stop_grace_seconds"],
            "shutdown_seconds": settings["shutdown_seconds"],
            "max_log_bytes": settings["max_log_bytes"],
        }
    )
    data = {
        k: v
        for k, v in result.items()
        if k not in {"verifier", "verifier_observations"}
    }
    captured = (canonical(data) + "\n").encode()
    result["verifier"]["stdout"].update(
        bytes_saved=len(captured),
        bytes_seen=len(captured),
        sha256=hashlib.sha256(captured).hexdigest(),
    )
    with pytest.raises(CoordinationError):
        validate_completion_proof(proof, run, bound, identity)


def test_clock_before_prior_completion_cannot_reopen_backlog(tmp_path):
    late = "2026-01-02T03:10:00Z"
    first = prepared_empty(tmp_path, now=late)
    second = prepared_empty(tmp_path, first.receipt["hour_end"], "2026-01-02T01:10:00Z")
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(late)
        assert complete(ledger, first, late)["mode"] == "DRAINING"
        before = ledger.snapshot()
        with pytest.raises(CoordinationError):
            complete(ledger, second, "2026-01-02T01:10:00Z")
        assert ledger.snapshot() == before
        assert ledger.writer_run(second.receipt["run_id"]) is None
