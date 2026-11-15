"""Strict Linux entity dispatch: genuine originals, exclusion and explicit v5 only."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from wintappy.coordination import entity_writer, service
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical, make_request

from .test_coordination_entity_operations import (
    END,
    HOUR,
    NOW,
    data_bytes,
    entity_config,
    ledger_rows,
    prepare_entity,
    seed_scope,
    writer_scope,
)
from .test_coordination_entity_publications import assert_originals, originals
from .test_coordination_entity_resolution import RESOLVED, blocked_completion
from .test_coordination_entity_writer_history import record
from .test_coordination_service import settings

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux genuine unprivileged entity service qualification",
)


def configuration(root, bound, *, entities=True):
    value = settings(root)
    value["binding"] = bound
    if entities:
        value["entity_writer"] = entity_config(root)
    path = root / "service-config.json"
    path.write_text(canonical(value))
    return path, value


def arguments(operation, path):
    result = [operation, "--config", str(path)]
    if operation in {"entities-init", "entities-replay"}:
        result += ["--start-hour", HOUR, "--end-hour", END]
    return result + ["--confirm", "--reason", "synthetic explicit operator exclusion"]


def invoke(argv):
    # A missing parser choice in the frozen first red is a failure, not a refusal pass.
    try:
        return service.main(argv)
    except SystemExit as error:
        return error.code


def offline(monkeypatch, *, now=NOW):
    monkeypatch.setattr(service, "_now", lambda: now)
    monkeypatch.setattr(
        service,
        "open_message_store",
        lambda *a: pytest.fail("entity operator operation requested transport/grants"),
    )


def test_config_separates_entity_budget_from_one_hour_raw_tick(tmp_path):
    bound, _ = writer_scope(tmp_path)
    _, value = configuration(tmp_path, bound)
    value["entity_writer"]["max_hours"] = 24
    checked = service.validate_config(value)
    assert checked == value
    assert checked["writer"].get("max_hours", 1) == 1
    checked["entity_writer"]["max_hours"] = 168
    assert service.validate_config(checked) == checked
    assert value["entity_writer"]["max_hours"] == 24
    del value["entity_writer"]
    assert service.validate_config(value) == value  # Original v1 configs still work.


@pytest.mark.parametrize(
    "change",
    [
        {"max_hours": True},
        {"max_hours": 169},
        {"memory_limit": "unbounded"},
        {"event_types": ["raw_file"]},
        {"event_types": ["raw_process", "raw_file"]},
        {"target_size_bytes": 1},
        {"delete_source": False},
        {"grace_minutes": 10},
        {"resume_init": True},
        {"AWS_SECRET_ACCESS_KEY": "synthetic-sensitive-token"},
    ],
)
def test_entity_config_refuses_before_runtime_credentials_or_receipts(
    tmp_path, monkeypatch, change
):
    from wintappy.coordination import entity_execution

    bound, _ = writer_scope(tmp_path)
    path, value = configuration(tmp_path, bound)
    value["entity_writer"].update(change)
    path.write_text(canonical(value))
    offline(monkeypatch)
    monkeypatch.setattr(
        entity_execution, "runtime_identity", lambda *a: pytest.fail("runtime resolved")
    )
    monkeypatch.setattr(
        entity_execution,
        "resolve_storage",
        lambda *a: pytest.fail("credentials resolved"),
    )
    with Ledger.open(tmp_path / "state", bound) as ledger:
        before = ledger_rows(ledger)
    assert invoke(arguments("entities-init", path)) == 1
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger_rows(ledger) == before
    assert not list((tmp_path / "state/entity-executions").glob("*/execution.json"))


@pytest.mark.parametrize(
    "operation", ["entities-init", "entities-replay", "resolve-entity"]
)
@pytest.mark.parametrize(
    "flag",
    [
        "--job-id",
        "--maintenance-id",
        "--confirm-replace-checkpoints",
        "--after-job",
        "--after-run",
        "--after-maintenance",
        "--target-version",
    ],
)
def test_unrelated_entity_flags_refuse_before_configuration(
    monkeypatch, operation, flag
):
    monkeypatch.setattr(service, "load_config", lambda *a: pytest.fail("config loaded"))
    argv = arguments(operation, "synthetic-unused")
    if operation == "resolve-entity":
        argv += ["--run-id", "a" * 32]
    argv += [flag]
    if flag != "--confirm-replace-checkpoints":
        argv += ["5" if flag == "--target-version" else "b" * 32]
    assert invoke(argv) == 1


@pytest.mark.parametrize(
    "operation", ["entities-init", "entities-replay", "resolve-entity"]
)
@pytest.mark.parametrize("problem", ["confirmation", "reason", "scope", "platform"])
def test_incomplete_or_nonlinux_entity_dispatch_refuses_before_configuration(
    monkeypatch, operation, problem
):
    argv = arguments(operation, "synthetic-unused")
    if operation == "resolve-entity":
        argv += ["--run-id", "a" * 32]
    if problem == "confirmation":
        argv.remove("--confirm")
    elif problem == "reason":
        index = argv.index("--reason")
        del argv[index : index + 2]
    elif problem == "scope":
        option = "--run-id" if operation == "resolve-entity" else "--end-hour"
        index = argv.index(option)
        del argv[index : index + 2]
    else:
        monkeypatch.setattr(service.sys, "platform", "darwin")
    monkeypatch.setattr(service, "load_config", lambda *a: pytest.fail("config loaded"))
    assert invoke(argv) == 1


@pytest.mark.parametrize(
    "operation", ["init", "tick", "status", "retry-hour", "resolve-hour"]
)
@pytest.mark.parametrize(
    "flag,value",
    [("--start-hour", HOUR), ("--end-hour", END), ("--target-version", "5")],
)
def test_new_flags_cannot_leak_into_unrelated_existing_operations(
    monkeypatch, operation, flag, value
):
    monkeypatch.setattr(service, "load_config", lambda *a: pytest.fail("config loaded"))
    assert invoke(arguments(operation, "synthetic-unused") + [flag, value]) == 1


@pytest.mark.parametrize("operation", ["entities-init", "entities-replay"])
def test_genuine_entity_service_completion_keeps_raw_cursor_and_queued_reader(
    tmp_path, monkeypatch, capsys, operation
):
    replay = operation == "entities-replay"
    bound, _ = writer_scope(tmp_path, initialized=replay, anchored=replay)
    path, _ = configuration(tmp_path, bound)
    offline(monkeypatch)
    waiting = make_request(bound, "a" * 32, "dbt-build", HOUR, END, NOW)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        ledger.ingest(waiting, NOW)
        before = ledger_rows(ledger)
        anchor = ledger.authority_high_water()
    raw = data_bytes(tmp_path, ("raw", "compacted"))
    assert invoke(arguments(operation, path)) == 0
    output = json.loads(capsys.readouterr().out)
    run_id = output["entity_run_id"]
    assert output["next_hour"] == HOUR and output["mode"] == "DRAINING"
    with Ledger.open(tmp_path / "state", bound) as ledger:
        proof = ledger.entity_evidence(run_id)
        receipt = proof["receipt"]
        assert receipt["entity_command"] == operation
        assert receipt["hour_start"] == HOUR and receipt["hour_end"] == END
        assert receipt["settings"]["max_hours"] == 24
        assert ledger.authority_high_water()["generation"] >= (
            anchor["generation"] if anchor else 1
        )
        assert ledger.entity_run(run_id)["state"] == "COMPLETED"
        assert ledger.snapshot()["active_run_id"] is None
        assert ledger.job(waiting["job_id"])["state"] == "WAITING"
        assert ledger.pending_grants() == []
        after = ledger_rows(ledger)
        for table in ("compaction_runs", "completion_evidence", "jobs", "outbox"):
            assert after[table] == before[table]
        observations = Path(receipt["observations_directory"])
        assert (
            json.loads((observations / "command.json").read_text())["command"]
            == receipt["command"]
        )
        assert (
            json.loads((observations / "supervision.json").read_text())[
                "children_stopped"
            ]
            is True
        )
        assert (
            json.loads((observations / "stdout.log").read_text())["status"]
            == "completed"
        )
    assert data_bytes(tmp_path, ("raw", "compacted")) == raw


def test_entity_dispatch_never_automatically_upgrades_default_v4(tmp_path, monkeypatch):
    bound, _ = seed_scope(tmp_path)
    path, _ = configuration(tmp_path, bound)
    offline(monkeypatch)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        before = ledger_rows(ledger)
    assert invoke(arguments("entities-init", path)) == 1
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger_rows(ledger) == before
        assert ledger._connection.execute(
            "SELECT schema_version FROM metadata"
        ).fetchone() == (4,)
    assert not list((tmp_path / "state/entity-executions").glob("*/execution.json"))


def test_service_explicit_v5_upgrade_preserves_default_v4(
    tmp_path, monkeypatch, capsys
):
    bound, _ = seed_scope(tmp_path)
    path, _ = configuration(tmp_path, bound, entities=False)
    offline(monkeypatch)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        before = ledger_rows(ledger)
        identity = ledger.ledger_id
    assert invoke(arguments("upgrade", path) + ["--target-version", "5"]) == 0
    assert (
        json.loads(capsys.readouterr().out) is None
    )  # Preserve existing upgrade output.
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger._connection.execute(
            "SELECT schema_version FROM metadata"
        ).fetchone() == (5,)
        assert ledger.ledger_id == identity
        after = ledger_rows(ledger)
        for table in (
            "dataset_state",
            "jobs",
            "outbox",
            "compaction_runs",
            "authority_checks",
            "completion_evidence",
            "retry_dispositions",
        ):
            assert after[table] == before[table]


@pytest.mark.parametrize("committed", [False, True])
def test_service_resolution_loads_exact_original_not_current_configuration(
    tmp_path, monkeypatch, capsys, committed
):
    if committed:
        bound, prepared = blocked_completion(tmp_path)
    else:
        bound, _ = writer_scope(tmp_path)
        prepared = prepare_entity(tmp_path, bound, "entities-init")
        with Ledger.open(tmp_path / "state", bound) as ledger:

            def fail(phase):
                if phase == "after_entity_launch":
                    raise OSError("synthetic clean original acknowledgment loss")

            ledger.failure_hook = fail
            with pytest.raises(CoordinationError):
                from .test_coordination_entity_operations import execute_entity

                execute_entity(ledger, prepared)
    path, value = configuration(tmp_path, bound, entities=False)
    value["writer"]["event_types"] = ["raw_file"]
    value["writer"]["memory_limit"] = "256MB"
    path.write_text(canonical(value))
    offline(monkeypatch, now=RESOLVED)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        before = ledger_rows(ledger)
        original = record(ledger, prepared.receipt["run_id"]) if committed else None
    data, artifacts = data_bytes(tmp_path), originals(prepared)
    assert (
        invoke(
            arguments("resolve-entity", path) + ["--run-id", prepared.receipt["run_id"]]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["mode"] == "DRAINING"
    with Ledger.open(tmp_path / "state", bound) as ledger:
        proof = ledger.entity_evidence(prepared.receipt["run_id"])
        assert proof["receipt"] == prepared.receipt
        assert ledger.snapshot()["next_hour"] == HOUR
        after = ledger_rows(ledger)
        for table in ("compaction_runs", "completion_evidence", "jobs", "outbox"):
            assert after[table] == before[table]
        if committed:
            assert record(ledger, prepared.receipt["run_id"]) == original
            assert after["authority_checks"] == before["authority_checks"]
        else:
            assert proof["recovery"]["confirmed_cannot_resume"] is True
    assert data_bytes(tmp_path) == data
    assert_originals(artifacts)
    assert list((tmp_path / "state/entity-executions").glob("*/execution.json")) == [
        prepared.path
    ]


@pytest.mark.parametrize("damage", ["missing", "changed", "hardlink", "wrong_run"])
def test_original_entity_loader_refuses_actual_receipt_damage_before_verifier(
    tmp_path, monkeypatch, damage
):
    from wintappy.coordination import recovery

    bound, prepared = blocked_completion(tmp_path)
    if damage == "missing":
        prepared.path.unlink()
    elif damage == "changed":
        prepared.path.write_bytes(prepared.path.read_bytes() + b"\n")
    elif damage == "hardlink":
        os.link(prepared.path, tmp_path / "receipt-alias")
    run_id = "f" * 32 if damage == "wrong_run" else prepared.receipt["run_id"]
    monkeypatch.setattr(
        entity_writer,
        "run_supervised",
        lambda *a, **kw: pytest.fail("verifier launched"),
    )
    with Ledger.open(tmp_path / "state", bound) as ledger:
        before = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            recovery.load_original_entity_execution(ledger, run_id, {})
        assert ledger_rows(ledger) == before


@pytest.mark.parametrize(
    "phase", ["before_entity_completion_commit", "after_entity_completion_commit"]
)
def test_actual_service_death_retains_atomic_typed_history_and_blocks_relaunch(
    tmp_path, monkeypatch, phase
):
    bound, _ = writer_scope(tmp_path)
    path, _ = configuration(tmp_path, bound)
    raw = data_bytes(tmp_path, ("raw", "compacted"))
    script = """
import json,os,sys
from contextlib import contextmanager
from wintappy.coordination import service
from wintappy.coordination.ledger import Ledger
path,phase,now,start,end=sys.argv[1:]
service._now=lambda:now
opened=Ledger.open
@contextmanager
def open_hook(*args,**kwargs):
    with opened(*args,**kwargs) as ledger:
        def crash(observed):
            if observed==phase:
                print(json.dumps({'run_id':ledger.snapshot()['active_run_id']}),flush=True)
                os._exit(91)
        ledger.failure_hook=crash
        yield ledger
Ledger.open=open_hook
service.open_message_store=lambda *args:(_ for _ in ()).throw(AssertionError('transport invoked'))
code=service.main(['entities-init','--config',path,'--start-hour',start,'--end-hour',end,
    '--confirm','--reason','synthetic genuine service crash boundary'])
raise AssertionError('service crash boundary not reached: '+str(code))
"""
    child = subprocess.run(
        [sys.executable, "-B", "-c", script, str(path), phase, NOW, HOUR, END],
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    (tmp_path / "entity-child.json").write_text(
        canonical(
            {
                "returncode": child.returncode,
                "stdout": child.stdout,
                "stderr": child.stderr,
            }
        )
        + "\n"
    )
    assert child.returncode == 91, (child.stdout, child.stderr)
    receipt = json.loads(
        next(
            (tmp_path / "state/entity-executions").glob("*/execution.json")
        ).read_text()
    )
    run_id = receipt["run_id"]
    with Ledger.open(tmp_path / "state", bound) as ledger:
        before = ledger_rows(ledger)
        committed = phase.startswith("after")
        assert bool(ledger.entity_evidence(run_id)) == committed
        assert ledger.snapshot()["next_hour"] == HOUR
        assert (
            ledger.authority_high_water() is not None
            if committed
            else ledger.authority_high_water() is None
        )
        if committed:
            assert ledger.entity_run(run_id)["state"] == "COMPLETED"
        else:
            assert ledger.snapshot()["mode"] == "BLOCKED"
            assert ledger.entity_run(run_id)["state"] == "BLOCKED"
    offline(monkeypatch, now=RESOLVED)
    assert invoke(arguments("entities-init", path)) == 1
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger_rows(ledger) == before
    assert data_bytes(tmp_path, ("raw", "compacted")) == raw
