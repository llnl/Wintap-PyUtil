"""Actual process death: fresh verifier first, atomic typed records, no raw release.

The subprocess exits without unwinding Python at the actual writer/recovery seams.
Captured observations come only from the real standalone verifier. Committed history
is all-or-none; an unresolved original needs a new audited current-only verification.
The scoped reopen clock belongs only to these explicitly synthetic January fixtures.
"""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from wintappy.coordination import entity_completion, entity_writer
from wintappy.coordination import ledger as ledger_module
from wintappy.coordination.entity_verification import _command
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import (
    CoordinationError,
    canonical,
    digest,
    make_request,
)
from wintappy.coordination.supervisor import _publish
from wintappy.coordination.verification import _observations

from .test_coordination_entity_completion_observations import completed_fixture
from .test_coordination_entity_operations import (
    END,
    HOUR,
    NOW,
    data_bytes,
    execute_entity,
    ledger_rows,
    prepare_entity,
    writer_scope,
)
from .test_coordination_entity_publication_qualification import no_runtime_reads
from .test_coordination_entity_publications import assert_originals, originals, retain
from .test_coordination_entity_resolution import BLOCK, RESOLVED, blocked_completion
from .test_coordination_entity_writer_history import record

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux actual entity completion process death and supervised independent evidence",
)

CHILD = """
import json,os,sys
from pathlib import Path
from wintappy.coordination import entity_history,entity_resolution,entity_writer
from wintappy.coordination.entity_execution import load_entity_execution
from wintappy.coordination.execution import PreparedExecution
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import canonical,digest
root,bound,path,sha256,operation,phase,now=sys.argv[1:]
root=Path(root); bound=json.loads(bound); receipt=load_entity_execution(path,sha256)
prepared=PreparedExecution(Path(path),sha256,receipt,tuple(receipt['command']),bound,{})
capture_path=root/'completion-boundary.json'
def capture(observation):
    value={'operation':operation,'phase':phase,'run_id':receipt['run_id'],
           'observation':observation}
    with capture_path.open('x') as out: out.write(canonical(value))
    print(canonical({'run_id':receipt['run_id'],'capture_sha256':digest(value)}),flush=True)
    if phase=='verified': os._exit(89)
original_completion=entity_history.complete_observation
def complete(ledger,prepared,intent,observation,now,*,recovery=None):
    capture(observation)
    return original_completion(ledger,prepared,intent,observation,now,recovery=recovery)
entity_history.complete_observation=complete
original_resolution=entity_resolution.complete_resolution
def resolve(ledger,prepared,intent,completion,pending,observation,now,reason):
    capture(observation)
    return original_resolution(ledger,prepared,intent,completion,pending,observation,now,reason)
entity_resolution.complete_resolution=resolve
kind='entity_resolution' if operation=='ack-resolution' else 'entity_completion'
target=('before_' if phase=='before-commit' else 'after_')+kind+'_commit'
with Ledger.open(root/'state',bound) as ledger:
    def crash(observed):
        if observed==target:
            assert capture_path.is_file()
            os._exit(89)
    ledger.failure_hook=crash
    if operation.startswith('writer-'):
        entity_writer.execute_entity_and_complete_prepared(ledger,prepared,now,
            clock=lambda:now,confirmed=True,reason='synthetic actual typed completion crash')
    else:
        entity_writer.resolve_entity_prepared(ledger,prepared,now,
            clock=lambda:now,confirmed=True,reason='synthetic original cannot resume; actual recovery crash')
raise AssertionError('actual completion/resolution boundary was not reached')
"""


def setup(root, operation):
    if operation == "ack-resolution":
        return blocked_completion(root, publication=True)
    if operation == "clean-recovery":
        with completed_fixture(root) as fixture:
            bound, prepared, ledger, intent, prelaunch = fixture
            _publish(prepared.path.parent / "prelaunch.json", prelaunch)
            retain(ledger, prepared, intent, prelaunch)
            from wintappy.coordination.entity_intent import block_entity

            block_entity(
                ledger,
                prepared.receipt["run_id"],
                BLOCK,
                "synthetic original clean blocked entity; child cannot resume",
            )
        return bound, prepared
    replay = operation == "writer-replay"
    bound, _ = writer_scope(root, initialized=replay, anchored=replay)
    return bound, prepare_entity(
        root, bound, "entities-replay" if replay else "entities-init"
    )


def record_rows(ledger, kind):
    return ledger._connection.execute(
        "SELECT observed_at,details_json FROM events WHERE kind=? ORDER BY ALL", [kind]
    ).fetchall()


@pytest.mark.parametrize(
    "operation", ["writer-init", "writer-replay", "clean-recovery", "ack-resolution"]
)
@pytest.mark.parametrize("phase", ["verified", "before-commit", "after-commit"])
def test_actual_typed_completion_death_authenticates_all_or_none_without_raw_release(
    tmp_path, monkeypatch, operation, phase
):
    bound, prepared = setup(tmp_path, operation)
    run_id = prepared.receipt["run_id"]
    # This is a synthetic fixture clock only, not permission to roll back real recovery.
    monkeypatch.setattr(ledger_module, "_now", lambda: BLOCK)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        waiting = make_request(bound, "c" * 32, "dbt-build", HOUR, END, NOW)
        ledger.ingest(waiting, NOW)
        before, anchor = ledger_rows(ledger), ledger.authority_high_water()
        prior_completions = record_rows(ledger, "entity_completion_record")
        prior_resolutions = record_rows(ledger, "entity_resolution_record")
    artifacts, raw = originals(prepared), data_bytes(tmp_path, ("raw", "compacted"))
    prior_data = data_bytes(tmp_path)
    now = NOW if operation.startswith("writer-") else RESOLVED
    child = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            CHILD,
            str(tmp_path),
            canonical(bound),
            str(prepared.path),
            prepared.sha256,
            operation,
            phase,
            now,
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=180,
    )
    # The unchanged diagnostic-retaining runner authenticates this actual child output.
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
    assert child.returncode == 89, (child.stdout, child.stderr)
    assert child.stderr == ""
    emitted = json.loads(child.stdout)
    capture_path = tmp_path / "completion-boundary.json"
    payload = capture_path.read_bytes()
    captured = json.loads(payload)
    assert capture_path.stat().st_nlink == 1
    assert payload == canonical(captured).encode()
    assert emitted == {
        "run_id": run_id,
        "capture_sha256": hashlib.sha256(payload).hexdigest(),
    }
    assert captured["operation"] == operation and captured["phase"] == phase
    assert captured["run_id"] == run_id
    observation = captured["observation"]
    directory = Path(observation["observations_directory"])
    request = observation["request"]
    assert (directory.parent / "request.json").read_bytes() == canonical(
        request
    ).encode()
    assert (
        request["operation"] == "entity-only-completion"
        and request["publication"] is None
    )
    command = _command(prepared.receipt, directory, request)
    assert command == [
        prepared.receipt["runtime"]["executable"],
        "-I",
        "-B",
        "-m",
        "raw_sensor_compact.entity_operation_verifier",
        "--request",
        str(directory.parent / "request.json"),
        "--request-sha256",
        digest(request),
    ]
    actual = _observations(directory, prepared.receipt, command)
    assert actual[0] == observation["supervision"]
    assert actual[2] == observation["result"]
    assert actual[0]["children_stopped"] is True
    assert json.loads((directory / "command.json").read_text())["command"] == command
    assert_originals(artifacts)
    assert data_bytes(tmp_path, ("raw", "compacted")) == raw
    if not operation.startswith("writer-"):
        assert data_bytes(tmp_path) == prior_data
    data = data_bytes(tmp_path)
    committed = phase == "after-commit"
    acknowledgment = operation == "ack-resolution"
    with Ledger.open(tmp_path / "state", bound) as ledger:
        state = ledger.snapshot()
        assert state["mode"] == ("DRAINING" if committed else "BLOCKED")
        assert state["next_hour"] == HOUR and state["readers"] == 0
        assert state["active_run_id"] == (None if committed else run_id)
        owner = ledger.entity_run(run_id)
        assert owner["state"] == ("COMPLETED" if committed else "BLOCKED")
        intent = json.loads(owner["intent_json"])
        core = entity_completion.validate_completion_observation(
            observation, bound, ledger.ledger_id, intent=intent
        )
        execution, cli = entity_completion._execution_artifacts(
            prepared.receipt, intent["input_observation"]["result"]
        )
        assert execution == observation["execution_supervision"]
        assert cli == observation["execution_result"]
        after = ledger_rows(ledger)
        for table in (
            "metadata",
            "compaction_runs",
            "completion_evidence",
            "retry_dispositions",
            "entity_evidence",
            "jobs",
            "outbox",
        ):
            assert after[table] == before[table]
        assert set(before["events"]) <= set(after["events"])
        assert (
            after["authority_checks"][: len(before["authority_checks"])]
            == before["authority_checks"]
        )
        assert len(after["authority_checks"]) == len(before["authority_checks"]) + int(
            committed and not acknowledgment
        )
        completions = record_rows(ledger, "entity_completion_record")
        resolutions = record_rows(ledger, "entity_resolution_record")
        if acknowledgment:
            assert completions == prior_completions
            assert len(resolutions) == len(prior_resolutions) + int(committed)
            assert ledger.authority_high_water() == anchor
        else:
            assert resolutions == prior_resolutions
            assert len(completions) == len(prior_completions) + int(committed)
            assert ledger.authority_high_water() == (
                core["authority_high_water"] if committed else anchor
            )
        if committed:
            retained = record(ledger, run_id)["proof"]
            assert retained["verification"] == core
            if not acknowledgment:
                assert retained["observation"] == observation
                assert (retained["recovery"] is not None) == (
                    operation == "clean-recovery"
                )
            else:
                resolution = json.loads(resolutions[-1][1])["proof"]
                assert resolution["observation"] == observation
                assert resolution["completion_sha256"] == digest(retained)
        elif not acknowledgment:
            assert ledger.entity_evidence(run_id) == (
                json.loads(before["entity_evidence"][0][2])
                if before["entity_evidence"]
                else None
            )
        assert ledger.completion_evidence(run_id) is None
        assert ledger._entity_live_intent is None and ledger._authority_checked is None
        assert ledger.admit(RESOLVED) == []
        assert ledger.job(waiting["job_id"])["state"] == "WAITING"
        assert ledger.pending_grants() == []
        rows = ledger_rows(ledger)
        with monkeypatch.context() as guarded:
            guarded.setattr(
                entity_writer,
                "run_supervised",
                lambda *a, **k: pytest.fail("old receipt relaunched"),
            )
            with pytest.raises(CoordinationError):
                execute_entity(ledger, prepared)
        assert ledger_rows(ledger) == rows
        if not committed:
            # The capture is not retained history or release authority. Reverify genuinely.
            calls, supervised = [], entity_writer.run_supervised

            def fresh(*args, **kwargs):
                calls.append(args)
                return supervised(*args, **kwargs)

            with monkeypatch.context() as guarded:
                guarded.setattr(entity_writer, "run_supervised", fresh)
                recovered = entity_writer.resolve_entity_prepared(
                    ledger,
                    prepared,
                    "2026-01-02T00:10:07Z",
                    clock=lambda: "2026-01-02T00:10:07Z",
                    confirmed=True,
                    reason="synthetic original cannot resume; fresh audit after process death",
                )
            assert recovered["mode"] == "DRAINING" and recovered["next_hour"] == HOUR
            assert len(calls) == 1
            assert calls[0][0][4] == "raw_sensor_compact.entity_operation_verifier"
            assert str(calls[0][1]) != str(directory)
            assert ledger.entity_evidence(run_id)["verification"] == core
            if acknowledgment:
                assert (
                    record_rows(ledger, "entity_completion_record") == prior_completions
                )
        ledger._validate()
        frozen = ledger_rows(ledger)
    with no_runtime_reads(monkeypatch), monkeypatch.context() as guarded:
        for name in ("run_supervised", "_read", "_observations"):
            guarded.setattr(
                entity_writer,
                name,
                lambda *a, **k: pytest.fail("reopen reread runtime originals"),
            )
        with Ledger.open(tmp_path / "state", bound) as ledger:
            assert ledger_rows(ledger) == frozen
    assert data_bytes(tmp_path) == data
    assert_originals(artifacts)
