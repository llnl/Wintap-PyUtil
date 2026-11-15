"""Distinct entity-only ownership: genuine inputs, readers, crashes and verification."""

import hashlib
import json
import os
import subprocess
import sys

import pytest

from wintappy.coordination.execution import prepare_execution, validate_receipt
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical, make_request
from wintappy.coordination.verification import verify_authority_prepared
from wintappy.coordination.writer import execute_prepared

from .test_coordination_authority import HOUR, NOW, binding, prepared_entities
from .test_coordination_execution import PROJECT, config

END = "2026-01-02T00:00:00Z"

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux genuine entity-only supervision, unprivileged exclusion and crashes",
)


def data_bytes(root, names=("raw", "compacted", "entity", "metrics")):
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for name in names
        for path in sorted((root / name).rglob("*"))
        if path.is_file()
    }


def ledger_rows(ledger):
    tables = ledger._connection.execute("SHOW TABLES").fetchall()
    return {
        name: ledger._connection.execute(
            f"SELECT * FROM {name} ORDER BY ALL"
        ).fetchall()
        for (name,) in tables
    }


def seed_scope(root, *, initialized=False, anchored=False):
    if initialized:
        bound, compact = prepared_entities(root)
        if anchored:
            with Ledger.open(root / "state", bound) as ledger:
                verify_authority_prepared(
                    ledger,
                    compact,
                    NOW,
                    initialize=True,
                    confirmed=True,
                    reason="synthetic independently verified entity fixture",
                )
        return bound, compact
    bound = binding(root)
    Ledger.initialize(root / "state", bound, HOUR)
    script = """
import sys,duckdb
from pathlib import Path
root=Path(sys.argv[1])
for name in ('raw','compacted','entity','metrics'): (root/name).mkdir()
path=root/'raw/raw_process/dayPK=20260101/hourPK=23/part.parquet'
path.parent.mkdir(parents=True)
with duckdb.connect() as connection:
    connection.execute("COPY (SELECT 'SYNTHETIC-HOST' hostname,'synthetic-child' pidhash,10::BIGINT pid,'synthetic-parent' parentpidhash,9::BIGINT parentpid,'START' activitytype,100::BIGINT eventtime,'synthetic-process' processname,'/synthetic' processpath,'synthetic-account' username,'' processargs,'synthetic-agent' agentid) TO ? (FORMAT PARQUET)",[str(path)])
"""
    child = subprocess.run(
        [str(PROJECT / ".venv/bin/python"), "-I", "-B", "-c", script, str(root)],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
        env={},
    )
    assert child.returncode == 0, (child.stdout, child.stderr)
    return bound, prepare_execution(root / "state", bound, HOUR, NOW, config(root))


def entity_config(root):
    values = config(root)
    values.pop("target_size_bytes")
    return values


def prepare_entity(root, bound, operation):
    from wintappy.coordination.entity_execution import prepare_entity_execution

    return prepare_entity_execution(
        root / "state", bound, HOUR, END, NOW, entity_config(root), operation=operation
    )


def writer_scope(root, *, initialized=False, anchored=False):
    """Opt in only these writer controls; shared seed_scope remains default-v4."""
    bound, compact = seed_scope(root, initialized=initialized, anchored=anchored)
    Ledger.upgrade(
        root / "state",
        bound,
        confirmed=True,
        reason="synthetic explicit-v5 writer fixture",
        now=NOW,
        target_version=5,
    )
    return bound, compact


def execute_entity(ledger, prepared, **overrides):
    from wintappy.coordination.entity_writer import execute_entity_and_complete_prepared

    choices = {
        "clock": lambda: NOW,
        "confirmed": True,
        "reason": "synthetic exclusively supervised entity operation",
        **overrides,
    }
    return execute_entity_and_complete_prepared(ledger, prepared, NOW, **choices)


@pytest.mark.parametrize("operation", ["entities-init", "entities-replay"])
def test_real_entity_only_completion_never_forges_raw_completion_or_cursor(
    tmp_path, operation
):
    replay = operation == "entities-replay"
    bound, _ = writer_scope(tmp_path, initialized=replay, anchored=replay)
    before = data_bytes(tmp_path, ("raw", "compacted"))
    prepared = prepare_entity(tmp_path, bound, operation)
    # Public compact/authority receipts remain strict; entity-only has its own type.
    with pytest.raises(CoordinationError):
        validate_receipt(prepared.receipt)
    assert prepared.command[5] == operation
    assert "compact" not in prepared.command
    assert "--delete-source" not in prepared.command
    assert "--target-size-bytes" not in prepared.command
    with Ledger.open(tmp_path / "state", bound) as ledger:
        previous = ledger.authority_high_water()
        rows = ledger_rows(ledger)
        waiting = make_request(bound, "a" * 32, "dbt-build", HOUR, END, NOW)
        ledger.ingest(waiting, NOW)
        result = execute_entity(ledger, prepared)
        run_id = prepared.receipt["run_id"]
        assert result["entity_run_id"] == run_id
        assert result["next_hour"] == HOUR
        assert result["mode"] == "DRAINING"  # Raw due backlog still has priority.
        assert result["active_run_id"] is None
        assert result["readers"] == 0
        assert ledger.job(waiting["job_id"])["state"] == "WAITING"
        assert ledger.pending_grants() == []
        assert ledger.completion_evidence(run_id) is None
        assert ledger.entity_run(run_id)["state"] == "COMPLETED"
        proof = ledger.entity_evidence(run_id)
        assert proof["operation"] == "entity-only-completion"
        assert proof["receipt"] == prepared.receipt
        assert proof["verification"]["status"] == "verified"
        anchor = ledger.authority_high_water()
        assert anchor is not None
        assert anchor["generation"] >= (previous["generation"] if previous else 1)
        after = ledger_rows(ledger)
        assert after["compaction_runs"] == rows["compaction_runs"]
        assert after["completion_evidence"] == rows["completion_evidence"]
    assert data_bytes(tmp_path, ("raw", "compacted")) == before
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.entity_run(run_id)["state"] == "COMPLETED"
        assert ledger.snapshot()["next_hour"] == HOUR
        assert ledger.authority_high_water() == anchor
        assert ledger.completion_evidence(run_id) is None


@pytest.mark.parametrize("operation", ["entities-init", "entities-replay"])
@pytest.mark.parametrize("problem", ["confirmation", "reason", "clock", "receipt"])
def test_entity_operation_refuses_unbound_or_unconfirmed_launch_before_mutation(
    tmp_path, operation, problem
):
    replay = operation == "entities-replay"
    bound, _ = writer_scope(tmp_path, initialized=replay, anchored=replay)
    prepared = prepare_entity(tmp_path, bound, operation)
    before = data_bytes(tmp_path)
    options = {}
    if problem == "confirmation":
        options["confirmed"] = False
    elif problem == "reason":
        options["reason"] = None
    elif problem == "clock":
        options["clock"] = None
    else:
        prepared.path.write_bytes(prepared.path.read_bytes() + b"\n")
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            execute_entity(ledger, prepared, **options)
        assert ledger_rows(ledger) == rows
    assert data_bytes(tmp_path) == before


def test_entity_replay_refuses_outstanding_committed_reader(tmp_path):
    bound, compact = writer_scope(tmp_path, initialized=True, anchored=True)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        verify_authority_prepared(ledger, compact, NOW)
        waiting = make_request(bound, "b" * 32, "dbt-build", HOUR, END, NOW)
        ledger.ingest(waiting, NOW)
        assert len(ledger.admit(NOW)) == 1
    prepared = prepare_entity(tmp_path, bound, "entities-replay")
    before = data_bytes(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        with pytest.raises(CoordinationError):
            execute_entity(ledger, prepared)
        assert ledger.snapshot()["readers"] == 1
        assert ledger.snapshot()["active_run_id"] is None
        assert ledger.job(waiting["job_id"])["state"] == "GRANTED"
        assert ledger.entity_run(prepared.receipt["run_id"]) is None
    assert data_bytes(tmp_path) == before


def test_entity_replay_refuses_existing_unresolved_raw_owner(tmp_path):
    bound, compact = writer_scope(tmp_path, initialized=True, anchored=True)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        verify_authority_prepared(ledger, compact, NOW)
        ledger.drain(NOW)
        execute_prepared(ledger, compact, NOW)
        assert ledger.snapshot()["mode"] == "BLOCKED"
    prepared = prepare_entity(tmp_path, bound, "entities-replay")
    before = data_bytes(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            execute_entity(ledger, prepared)
        assert ledger_rows(ledger) == rows
        assert ledger.snapshot()["active_run_id"] == compact.receipt["run_id"]
    assert data_bytes(tmp_path) == before


@pytest.mark.parametrize("operation", ["entities-init", "entities-replay"])
def test_genesis_and_replay_cannot_adopt_unverified_existing_entity_state(
    tmp_path, operation
):
    bound, _ = writer_scope(tmp_path, initialized=True, anchored=False)
    prepared = prepare_entity(tmp_path, bound, operation)
    before = data_bytes(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            execute_entity(ledger, prepared)
        assert ledger_rows(ledger) == rows
        assert ledger.authority_high_water() is None
    assert data_bytes(tmp_path) == before


@pytest.mark.parametrize("phase", ["after_entity_commit", "after_entity_launch"])
def test_entity_independent_prelaunch_and_completion_refuse_new_raw_input(
    tmp_path, phase
):
    bound, _ = writer_scope(tmp_path)
    prepared = prepare_entity(tmp_path, bound, "entities-init")
    source = tmp_path / "raw/raw_process/dayPK=20260101/hourPK=23/part.parquet"

    def new_input(observed):
        if observed == phase:
            source.with_name("late.parquet").write_bytes(source.read_bytes())

    with Ledger.open(tmp_path / "state", bound) as ledger:
        ledger.failure_hook = new_input
        with pytest.raises(CoordinationError):
            execute_entity(ledger, prepared)
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.snapshot()["next_hour"] == HOUR
        assert ledger.authority_high_water() is None
        assert ledger.completion_evidence(prepared.receipt["run_id"]) is None
    assert source.with_name("late.parquet").exists()
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.entity_run(prepared.receipt["run_id"])["state"] == "BLOCKED"
        assert ledger.admit(NOW) == []


def test_clean_entity_launch_acknowledgment_loss_requires_original_bound_resolution(
    tmp_path,
):
    bound, _ = writer_scope(tmp_path)
    prepared = prepare_entity(tmp_path, bound, "entities-init")
    raw = data_bytes(tmp_path, ("raw", "compacted"))

    def lost_ack(phase):
        if phase == "after_entity_launch":
            raise OSError("synthetic acknowledgment loss after actual entity CLI")

    with Ledger.open(tmp_path / "state", bound) as ledger:
        ledger.failure_hook = lost_ack
        with pytest.raises(CoordinationError):
            execute_entity(ledger, prepared)
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.authority_high_water() is None
    with Ledger.open(tmp_path / "state", bound) as ledger:
        from wintappy.coordination.entity_writer import resolve_entity_prepared

        assert ledger.entity_run(prepared.receipt["run_id"])["state"] == "BLOCKED"
        with pytest.raises(CoordinationError):
            execute_entity(ledger, prepared)
        result = resolve_entity_prepared(
            ledger,
            prepared,
            NOW,
            clock=lambda: NOW,
            confirmed=True,
            reason="synthetic original clean entity-only resolution; cannot resume",
        )
        assert result["next_hour"] == HOUR
        assert result["mode"] == "DRAINING"
        assert ledger.entity_run(prepared.receipt["run_id"])["state"] == "COMPLETED"
        assert ledger.authority_high_water()["generation"] == 1
    assert data_bytes(tmp_path, ("raw", "compacted")) == raw


@pytest.mark.parametrize(
    "phase",
    [
        "before_entity_commit",
        "after_entity_commit",
        "before_entity_launch",
        "after_entity_launch",
    ],
)
def test_actual_entity_process_crashes_preserve_restart_visible_exclusive_ownership(
    tmp_path, phase
):
    bound, _ = writer_scope(tmp_path)
    raw = data_bytes(tmp_path, ("raw", "compacted"))
    script = """
import json,os,sys
from wintappy.coordination.entity_execution import prepare_entity_execution
from wintappy.coordination.entity_writer import execute_entity_and_complete_prepared
from wintappy.coordination.ledger import Ledger
root,bound,settings,phase,start,end,now=sys.argv[1:]
bound=json.loads(bound); settings=json.loads(settings)
prepared=prepare_entity_execution(root+'/state',bound,start,end,now,settings,operation='entities-init')
print(json.dumps({'run_id':prepared.receipt['run_id']}),flush=True)
with Ledger.open(root+'/state',bound) as ledger:
    def crash(observed):
        if observed==phase: os._exit(87)
    ledger.failure_hook=crash
    execute_entity_and_complete_prepared(ledger,prepared,now,clock=lambda:now,
        confirmed=True,reason='synthetic actual entity-only crash boundary')
raise AssertionError('entity crash boundary was not reached')
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
    assert child.returncode == 87, (child.stdout, child.stderr)
    run_id = json.loads(child.stdout)["run_id"]
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.snapshot()["next_hour"] == HOUR
        assert ledger.authority_high_water() is None
        assert ledger.completion_evidence(run_id) is None
        if phase == "before_entity_commit":
            assert ledger.snapshot()["active_run_id"] is None
            assert ledger.entity_run(run_id) is None
        else:
            assert ledger.snapshot()["mode"] == "BLOCKED"
            assert ledger.snapshot()["active_run_id"] == run_id
            assert ledger.entity_run(run_id)["state"] == "BLOCKED"
            assert ledger.admit(NOW) == []
    assert data_bytes(tmp_path, ("raw", "compacted")) == raw


def test_entity_operation_holds_one_lock_through_intent_launch_and_completion(tmp_path):
    bound, _ = writer_scope(tmp_path)
    prepared = prepare_entity(tmp_path, bound, "entities-init")
    observed = []
    script = """
import json,sys
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError
try:
    with Ledger.open(sys.argv[1],json.loads(sys.argv[2])): pass
except CoordinationError as error:
    assert 'busy' in str(error).lower()
    sys.exit(88)
raise AssertionError('competing process obtained entity-owned ledger')
"""

    def competitor(phase):
        if phase in {
            "after_entity_commit",
            "before_entity_launch",
            "after_entity_launch",
        }:
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
            (tmp_path / f"entity-lock-child-{phase}.json").write_text(
                canonical(
                    {
                        "returncode": child.returncode,
                        "stdout": child.stdout,
                        "stderr": child.stderr,
                    }
                )
                + "\n"
            )
            assert child.returncode == 88, (child.stdout, child.stderr)
            observed.append(phase)

    with Ledger.open(tmp_path / "state", bound) as ledger:
        ledger.failure_hook = competitor
        assert execute_entity(ledger, prepared)["next_hour"] == HOUR
    assert observed == [
        "after_entity_commit",
        "before_entity_launch",
        "after_entity_launch",
    ]
