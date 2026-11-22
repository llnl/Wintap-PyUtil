"""Fresh genuine retries: retained history, compatibility and operator boundaries."""

import copy
import json
import os
import sys
from pathlib import Path

import duckdb
import pytest

from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import (
    CoordinationError,
    canonical,
    digest,
    make_cancellation,
    make_release,
    make_request,
)

from .test_coordination_completion import NOW, complete, prepared_empty
from .test_coordination_execution import binding
from .test_coordination_intentional_retry import retry
from .test_coordination_ledger import HOUR
from .test_coordination_retry import failed_merge
from .test_coordination_service import settings

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux actual unprivileged failed-original controls",
)


@pytest.fixture(scope="module")
def retained_retry(tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic-retained-retry")
    prepared, _ = failed_merge(root)
    with Ledger.open(root / "state", binding(root)) as ledger:
        retry(ledger, prepared)
        row = ledger._connection.execute("SELECT * FROM retry_dispositions").fetchone()
        return (
            json.loads(row[3]),
            ledger.writer_run(row[1]),
            ledger.writer_run(row[2]),
            binding(root),
            ledger.ledger_id,
            ledger.completion_evidence(row[2]),
        )


@pytest.mark.parametrize(
    "damage",
    [
        "version",
        "confirmation",
        "reason",
        "original",
        "intent",
        "clock",
        "capture",
        "report",
        "preflight",
        "shutdown",
        "scope",
        "fresh_identity",
    ],
)
def test_retained_disposition_semantics_not_masked_by_outer_audits(
    retained_retry, damage
):
    from wintappy.coordination.intentional_retry import validate_disposition

    proof, original, new, bound, identity, _ = copy.deepcopy(retained_retry)
    if damage == "version":
        proof["version"] = True
    elif damage == "confirmation":
        proof["confirmed_cannot_resume"] = False
    elif damage == "reason":
        proof["reason"] = ""
    elif damage == "original":
        original["state"] = "COMPLETED"
        proof["original_run_sha256"] = digest(original)
    elif damage == "intent":
        proof["retry_intent_sha256"] = "0" * 64
    elif damage == "clock":
        proof["preflight"]["observed_at"] = "2026-01-02T00:11:00Z"
    elif damage == "capture":
        proof["original_capture"] += " "
    elif damage == "report":
        proof["original_report"]["phase"] = "checkpoint_publication"
    elif damage == "preflight":
        proof["preflight"]["verification"]["source_rows"] = True
    elif damage == "shutdown":
        proof["preflight"]["writer_supervision"]["children_stopped"] = False
    elif damage == "scope":
        intent = json.loads(new["intent_json"])
        intent["execution"]["receipt"]["settings"]["event_types"] = ["raw_process"]
        new["intent_json"] = canonical(intent)
        new["intent_sha256"] = digest(intent)
        proof["retry_intent_sha256"] = new["intent_sha256"]
    else:
        intent = json.loads(new["intent_json"])
        receipt = intent["execution"]["receipt"]
        receipt["cli_run_id"] = original["cli_run_id"]
        from wintappy.coordination.execution import command_for

        receipt["command"] = command_for(receipt)
        new["intent_json"] = canonical(intent)
        new["intent_sha256"] = digest(intent)
        proof["retry_intent_sha256"] = new["intent_sha256"]
    with pytest.raises(CoordinationError):
        validate_disposition(proof, original, new, bound, identity)


@pytest.mark.parametrize("budget", ["record", "count", "aggregate"])
def test_retry_budget_refusal_never_erases_original_or_reopens(
    tmp_path, monkeypatch, budget
):
    from wintappy.coordination import intentional_retry

    prepared, archive = failed_merge(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        state = ledger.snapshot()
        original = ledger.writer_run(prepared.receipt["run_id"])
        monkeypatch.setattr(
            intentional_retry,
            {
                "record": "MAX_RETRY_BYTES",
                "count": "MAX_RETRIES",
                "aggregate": "MAX_RETRY_TOTAL_BYTES",
            }[budget],
            0,
        )
        with pytest.raises(CoordinationError):
            retry(ledger, prepared)
        assert ledger.snapshot() == state
        assert ledger.writer_run(original["run_id"]) == original
        assert (
            ledger._connection.execute(
                "SELECT count(*) FROM retry_dispositions"
            ).fetchone()[0]
            == 0
        )
    assert all(path.read_bytes() == payload for path, payload in archive.items())


def test_clean_external_retry_requires_audited_resolution_after_uncertainty(tmp_path):
    from wintappy.coordination.recovery import load_original_execution, resolve_hour

    prepared, archive = failed_merge(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        original = ledger.writer_run(prepared.receipt["run_id"])

        def lost(phase):
            if phase == "after_retry_launch":
                raise OSError("synthetic acknowledgment loss after clean external work")

        ledger.failure_hook = lost
        with pytest.raises(CoordinationError):
            retry(ledger, prepared)
        assert ledger.snapshot()["next_hour"] == HOUR
        new_id = ledger.snapshot()["active_run_id"]
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        new = load_original_execution(ledger, new_id, {})
        result = resolve_hour(
            ledger,
            new,
            NOW,
            clock=lambda: NOW,
            confirmed=True,
            reason="synthetic audited clean retry resolution",
        )
        assert result["mode"] == "OPEN"
        assert result["next_hour"] == new.receipt["hour_end"]
        proof = ledger.completion_evidence(new_id)
        assert proof["version"] == 4
        assert (
            proof["retry"]["original_migration_sha256"]
            == proof["retry"]["retry_migration_sha256"]
        )
        assert ledger.writer_run(original["run_id"]) == original
        assert ledger.completion_evidence(original["run_id"]) is None
    assert all(path.read_bytes() == payload for path, payload in archive.items())


@pytest.mark.parametrize("damage", ["disposition", "prelaunch", "original_capture"])
def test_late_evidence_damage_cannot_complete_or_resolve(tmp_path, monkeypatch, damage):
    from wintappy.coordination import intentional_retry
    from wintappy.coordination.recovery import load_original_execution, resolve_hour

    prepared, _ = failed_merge(tmp_path)
    launch = intentional_retry._launch_prepared

    def corrupt(ledger, new, environment):
        result = launch(ledger, new, environment)
        path = (
            new.path.parent
            / (
                "retry-disposition.json"
                if damage == "disposition"
                else "retry-prelaunch.json"
            )
            if damage != "original_capture"
            else Path(prepared.receipt["observations_directory"]) / "stderr.log"
        )
        path.write_bytes(path.read_bytes() + b" ")
        return result

    monkeypatch.setattr(intentional_retry, "_launch_prepared", corrupt)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        with pytest.raises(CoordinationError):
            retry(ledger, prepared)
        state = ledger.snapshot()
        assert state["mode"] == "BLOCKED" and state["next_hour"] == HOUR
        new = load_original_execution(ledger, state["active_run_id"], {})
        with pytest.raises(CoordinationError):
            resolve_hour(
                ledger,
                new,
                NOW,
                clock=lambda: NOW,
                confirmed=True,
                reason="synthetic damaged evidence refusal",
            )
        assert ledger.completion_evidence(new.receipt["run_id"]) is None


@pytest.mark.parametrize(
    "damage", ["audit", "predecessor", "original", "completion_link"]
)
def test_corrupt_retained_retry_history_refuses_open(tmp_path, damage):
    prepared, _ = failed_merge(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        retry(ledger, prepared)
        if damage == "audit":
            ledger._connection.execute("DELETE FROM events WHERE kind='writer_retried'")
        elif damage == "predecessor":
            ledger._connection.execute(
                "UPDATE retry_dispositions SET previous_sha256=?", ["0" * 64]
            )
        elif damage == "original":
            ledger._connection.execute(
                "UPDATE compaction_runs SET block_reason='changed original failure' WHERE run_id=?",
                [prepared.receipt["run_id"]],
            )
        else:
            row = ledger._connection.execute(
                "SELECT run_id,proof_json FROM completion_evidence"
            ).fetchone()
            proof = json.loads(row[1])
            proof["retry"]["disposition_sha256"] = "0" * 64
            ledger._connection.execute(
                "UPDATE completion_evidence SET proof_json=?,proof_sha256=?",
                [canonical(proof), digest(proof)],
            )
            ledger._connection.execute(
                "UPDATE events SET details_json=? WHERE kind='writer_completed'",
                [
                    canonical(
                        {
                            "proof_sha256": digest(proof),
                            "next_hour": proof["next_hour"],
                            "mode": proof["mode"],
                        }
                    )
                ],
            )
    with (
        pytest.raises(CoordinationError),
        Ledger.open(tmp_path / "state", binding(tmp_path)),
    ):
        pass


def test_v3_upgrade_preserves_genuine_completion_readers_outbox_and_tombstones(
    tmp_path,
):
    from wintappy.coordination.ledger import SCHEMA_V3

    prepared = prepared_empty(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        complete(ledger, prepared)
        for token in ("a", "b"):
            request = make_request(
                binding(tmp_path),
                token * 32,
                "dbt-build",
                HOUR,
                prepared.receipt["hour_end"],
                NOW,
            )
            ledger.ingest(request, NOW)
        grants = ledger.admit(NOW)
        ledger.ingest(make_release(grants[0], NOW, 0), NOW)
        cancelled = make_request(
            binding(tmp_path),
            "c" * 32,
            "dbt-build",
            HOUR,
            prepared.receipt["hour_end"],
            NOW,
        )
        ledger.ingest(make_cancellation(cancelled, NOW), NOW)
        retained = {
            table: ledger._connection.execute(f"SELECT * FROM {table}").fetchall()
            for table in (
                "dataset_state",
                "jobs",
                "outbox",
                "compaction_runs",
                "authority_checks",
                "completion_evidence",
            )
        }
        metadata = ledger._connection.execute("SELECT * FROM metadata").fetchone()
        events = ledger._connection.execute(
            "SELECT * FROM events WHERE kind<>'retry_history_initialized'"
        ).fetchall()
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    with duckdb.connect(str(legacy / "coordinator.duckdb")) as connection:
        connection.execute(SCHEMA_V3)
        connection.execute(
            "INSERT INTO metadata VALUES (1,3,?,?,?)", list(metadata[2:])
        )
        for table, rows in {**retained, "events": events}.items():
            if rows:
                connection.executemany(
                    f"INSERT INTO {table} VALUES ({','.join('?' for _ in rows[0])})",
                    rows,
                )
    Ledger.upgrade(
        legacy,
        binding(tmp_path),
        NOW,
        confirmed=True,
        reason="synthetic completed-v3 compatible upgrade",
    )
    with Ledger.open(legacy, binding(tmp_path)) as ledger:
        assert ledger.ledger_id == metadata[2]
        for table, rows in retained.items():
            assert (
                ledger._connection.execute(f"SELECT * FROM {table}").fetchall() == rows
            )
        assert ledger.snapshot()["readers"] == 1
        assert ledger.job(cancelled["job_id"])["state"] == "CANCELLED"
        assert len(ledger.pending_grants()) == 1


def test_service_retry_dispatch_uses_original_receipt_and_never_transport(
    tmp_path, monkeypatch, capsys
):
    from wintappy.coordination import service

    prepared, _ = failed_merge(tmp_path)
    config = settings(tmp_path)
    monkeypatch.setattr(service, "load_config", lambda _: config)
    monkeypatch.setattr(service, "_now", lambda: NOW)
    monkeypatch.setattr(
        service,
        "open_message_store",
        lambda *a: pytest.fail("operator retry must not request transport or grants"),
    )
    assert (
        service.main(
            [
                "retry-hour",
                "--config",
                "synthetic",
                "--run-id",
                prepared.receipt["run_id"],
                "--confirm",
                "--reason",
                "synthetic operator retry",
            ]
        )
        == 0
    )
    output = json.loads(capsys.readouterr().out)
    assert (
        output["mode"] == "OPEN" and output["next_hour"] == prepared.receipt["hour_end"]
    )
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.writer_run(prepared.receipt["run_id"])["state"] == "BLOCKED"
        assert ledger.completion_evidence(prepared.receipt["run_id"]) is None


@pytest.mark.parametrize("option", ["no_confirmation", "no_reason", "maintenance_flag"])
def test_service_retry_requires_explicit_narrow_confirmation(
    tmp_path, monkeypatch, option
):
    from wintappy.coordination import service

    prepared, _ = failed_merge(tmp_path)
    monkeypatch.setattr(service, "load_config", lambda _: settings(tmp_path))
    monkeypatch.setattr(service, "_now", lambda: NOW)
    argv = [
        "retry-hour",
        "--config",
        "synthetic",
        "--run-id",
        prepared.receipt["run_id"],
    ]
    if option != "no_confirmation":
        argv.append("--confirm")
    if option != "no_reason":
        argv.extend(["--reason", "synthetic operator refusal"])
    if option == "maintenance_flag":
        argv.append("--confirm-replace-checkpoints")
    assert service.main(argv) == 1
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.snapshot()["active_run_id"] == prepared.receipt["run_id"]
        assert (
            ledger._connection.execute(
                "SELECT count(*) FROM retry_dispositions"
            ).fetchone()[0]
            == 0
        )
