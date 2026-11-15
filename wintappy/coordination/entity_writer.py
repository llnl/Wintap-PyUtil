"""Receipt-bound Linux entity launch and atomic typed completion, never raw compact.

Restart cannot resume a launch. Original-clean recovery runs only a fresh read-only
verifier against actual original artifacts; missing/partial/historical originals
stay blocked. Public read-only adapters retain their stricter live-session gate.
"""

import json
from pathlib import Path
from uuid import uuid4

from . import entity_completion, entity_history, entity_intent, entity_resolution
from .completion import _manifest
from .coordinator import _Clock
from .entity_execution import recheck_entity_execution
from .entity_verification import (
    _command,
    recheck_input_observation,
    verify_entity_inputs_prepared,
)
from .execution import execution_environment
from .ledger import _mkdir, _validate_block_reason
from .protocol import CoordinationError, canonical, digest, timestamp
from .supervisor import MAX_RECEIPT_BYTES, _publish, run_supervised
from .verification import _observations, _read


def _clock(now, clock, confirmed, reason):
    if confirmed is not True:
        raise CoordinationError("Entity writer requires explicit confirmation")
    _validate_block_reason(reason)
    if not callable(clock):
        raise CoordinationError("Entity writer requires a fresh evolving clock")
    return _Clock(now, clock)


def _block_uncertain(ledger, run_id, now):
    """Even a committed lost acknowledgment retains BLOCKED typed exclusion."""
    state = ledger.snapshot()
    run = ledger.entity_run(run_id)
    if run is None:
        return
    if run["state"] == "INTENT" and state["active_run_id"] == run_id:
        entity_intent.block_entity(
            ledger,
            run_id,
            now,
            "Entity launch/completion uncertain; preserve originals and require audited recovery",
        )
    elif run["state"] == "COMPLETED":
        ledger._entity_live_intent = None
        ledger._authority_checked = None
        completion = entity_history.retained_record(ledger, run_id)
        if ledger._connection.execute(
            "SELECT count(*) FROM events WHERE kind='entity_unresolved' AND identity=?",
            [run_id],
        ).fetchone()[0]:
            entity_resolution.block_acknowledgment(ledger, run, completion, now)
            return
        reason = "Entity completion acknowledgment uncertain; preserve committed originals and require audited recovery"
        details = {"reason": reason, "intent_sha256": run["intent_sha256"]}
        entity_intent._budget(ledger, size=len(canonical(details).encode()))
        with ledger._transaction():
            ledger._connection.execute(
                "UPDATE entity_runs SET state='BLOCKED',block_reason=? WHERE run_id=?",
                [reason, run_id],
            )
            ledger._connection.execute(
                "UPDATE dataset_state SET mode='BLOCKED',active_run_id=?,block_reason=? WHERE singleton=1",
                [run_id, reason],
            )
            ledger._event("entity_unresolved", run_id, now, details)
            ledger._validate()


def execute_entity_and_complete_prepared(
    ledger, prepared, now, *, clock, confirmed=False, reason=None
):
    """One uninterrupted locked session: intent COMMIT, fresh inputs, launch, verify."""
    observed = _clock(now, clock, confirmed, reason)
    receipt = recheck_entity_execution(prepared, ledger)
    run_id = receipt["run_id"]
    ledger._require_entity_schema()
    ledger._validate()
    entity_intent._budget(
        ledger,
        size=entity_history.MAX_ENTITY_COMPLETION_BYTES
        + 3 * entity_intent.MAX_ENTITY_AUDIT_BYTES,
    )
    intent = None
    try:
        intent = entity_intent.begin_entity_prepared(
            ledger,
            prepared,
            now,
            clock=observed.observe,
            confirmed=True,
            reason=reason,
        )
        ledger.failure_hook("before_entity_launch")
        prelaunch = verify_entity_inputs_prepared(
            ledger,
            prepared,
            observed.observe(),
            original=intent["input_observation"],
            entity_run_id=run_id,
        )
        _publish(prepared.path.parent / "prelaunch.json", prelaunch)
        current = observed.observe()
        entity_intent.recheck_live_original(
            ledger,
            prepared,
            run_id,
            intent["input_observation"],
            current,
        )
        recheck_input_observation(ledger, prepared, prelaunch)
        settings = receipt["settings"]
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
        ledger.failure_hook("after_entity_launch")
        observation = entity_completion.verify_entity_completion_prepared(
            ledger,
            prepared,
            observed.observe(),
            original=intent["input_observation"],
            prelaunch=prelaunch,
        )
        return entity_history.complete_observation(
            ledger,
            prepared,
            intent,
            observation,
            observed.observe(),
        )
    except BaseException as error:
        # A refusal to reuse an old completed receipt is not a new uncertain launch.
        # begin_entity_prepared handles its own intent-COMMIT acknowledgment errors.
        if intent is not None:
            _block_uncertain(ledger, run_id, observed.last)
        if isinstance(error, (KeyboardInterrupt, SystemExit, CoordinationError)):
            raise
        raise CoordinationError(
            "Entity writer refused/uncertain; details suppressed"
        ) from None


def _recovery_originals(ledger, prepared, now):
    ledger._require_entity_schema()
    ledger._validate()
    receipt = recheck_entity_execution(prepared, ledger)
    run = ledger.entity_run(receipt["run_id"])
    state = ledger.snapshot()
    if (
        run is None
        or run["state"] != "BLOCKED"
        or state["mode"] != "BLOCKED"
        or state["active_run_id"] != run["run_id"]
        or state["readers"]
        or ledger._entity_live_intent is not None
    ):
        raise CoordinationError(
            "Entity resolution requires the original blocked typed owner"
        )
    completion = entity_history.retained_record(ledger, run["run_id"])
    intent = entity_intent.validate_intent(ledger, run)
    pending = entity_resolution.blocked_original(ledger, run, intent, completion, now)
    if (
        intent["execution"]
        != {"path": str(prepared.path), "sha256": prepared.sha256, "receipt": receipt}
        or timestamp(now) < timestamp(run["started_at"])
        or _read(
            prepared.path.parent / "intent.json", entity_intent.MAX_ENTITY_INTENT_BYTES
        )
        != canonical(intent).encode()
    ):
        raise CoordinationError("Entity resolution actual original intent changed")
    prelaunch_payload = _read(
        prepared.path.parent / "prelaunch.json",
        entity_completion.MAX_ENTITY_OBSERVATION_BYTES,
    )
    prelaunch = json.loads(prelaunch_payload)
    if prelaunch_payload != canonical(prelaunch).encode():
        raise CoordinationError("Entity resolution prelaunch capture changed")
    original = entity_completion._prelaunch(
        intent,
        prelaunch,
        receipt,
        prepared.binding,
        ledger.ledger_id,
        now,
    )
    if (
        recheck_input_observation(ledger, prepared, intent["input_observation"])
        != original
        or recheck_input_observation(ledger, prepared, prelaunch) != original
    ):
        raise CoordinationError(
            "Entity resolution original/post-COMMIT observations changed"
        )
    if completion is not None:
        _recheck_retained_completion(
            ledger, prepared, intent, completion, original, prelaunch
        )
    return receipt, intent, prelaunch, original, completion, pending


def _recheck_retained_completion(
    ledger, prepared, intent, completion, original, prelaunch
):
    """Actual originals under blocked exclusion; never relax the public live adapter."""
    proof = completion["proof"]
    observation = proof["observation"]
    receipt = prepared.receipt
    core = entity_completion.validate_completion_observation(
        observation, prepared.binding, ledger.ledger_id, intent=intent
    )
    execution, cli = entity_completion._execution_artifacts(receipt, original)
    observations = Path(observation["observations_directory"])
    request = observation["request"]
    command = _command(receipt, observations, request)
    captured = _observations(observations, receipt, command)
    if (
        core != proof["verification"]
        or observation["prelaunch"] != prelaunch
        or observation["execution_supervision"] != execution
        or observation["execution_result"] != cli
        or captured[0] != observation["supervision"]
        or captured[2] != observation["result"]
        or _read(
            observations.parent / "request.json",
            entity_completion.MAX_ENTITY_REQUEST_BYTES,
        )
        != canonical(request).encode()
        or _read(
            observations / "command.json", entity_completion.MAX_ENTITY_REQUEST_BYTES
        )
        != canonical(_manifest(receipt, command)).encode()
        or _read(observations / "supervision.json", MAX_RECEIPT_BYTES)
        != canonical(observation["supervision"]).encode()
        or _read(
            observations / "stdout.log",
            min(
                entity_completion.MAX_ENTITY_RESULT_BYTES,
                receipt["settings"]["max_report_bytes"],
            ),
        )
        != (canonical(observation["result"]) + "\n").encode()
    ):
        raise CoordinationError(
            "Entity resolution actual original completion artifacts changed"
        )


def resolve_entity_prepared(
    ledger, prepared, now, *, clock, confirmed=False, reason=None
):
    """Audited original-clean current-only resolution; never replay or historical repair."""
    observed = _clock(now, clock, confirmed, reason)
    ledger._entity_clean_recovery = None
    ledger._entity_ack_resolution = None
    committing = False
    run_id = None
    try:
        receipt, intent, prelaunch, original, completion, pending = _recovery_originals(
            ledger, prepared, observed.observe()
        )
        run_id = receipt["run_id"]
        if completion is not None:
            entity_resolution._budget(ledger, records=1)
        entity_intent._budget(
            ledger,
            size=entity_history.MAX_ENTITY_COMPLETION_BYTES
            + 2 * entity_intent.MAX_ENTITY_AUDIT_BYTES,
        )
        state, anchor = ledger.snapshot(), ledger.authority_high_water()
        execution, cli = entity_completion._execution_artifacts(receipt, original)
        request = entity_completion._request(
            receipt, ledger.ledger_id, intent, original, execution
        )
        directory = prepared.path.parent / "completion-verifications" / uuid4().hex
        _mkdir(directory)
        _publish(directory / "request.json", request)
        observations = directory / "observations"
        settings = receipt["settings"]
        command = _command(receipt, observations, request)
        current = observed.observe()
        run_supervised(
            command,
            observations,
            timeout_seconds=settings["supervision_timeout_seconds"],
            stop_grace_seconds=settings["stop_grace_seconds"],
            shutdown_seconds=settings["shutdown_seconds"],
            max_log_bytes=settings["max_log_bytes"],
            cwd=receipt["cwd"],
            env=execution_environment(prepared),
        )
        verified = _observations(observations, receipt, command)
        observation = {
            "version": 1,
            "operation": "entity-completion-observation",
            "observed_at": current,
            "receipt": receipt,
            "intent_sha256": digest(intent),
            "prelaunch": prelaunch,
            "execution_supervision": execution,
            "execution_result": cli,
            "request": request,
            "result": verified[2],
            "supervision": verified[0],
            "observations_directory": str(observations),
        }
        core = entity_completion.validate_completion_observation(
            observation,
            prepared.binding,
            ledger.ledger_id,
            intent=intent,
        )
        if (
            _recovery_originals(ledger, prepared, observed.observe())
            != (receipt, intent, prelaunch, original, completion, pending)
            or entity_completion._execution_artifacts(receipt, original)
            != (execution, cli)
            or _observations(observations, receipt, command) != verified
            or _read(
                directory / "request.json", entity_completion.MAX_ENTITY_REQUEST_BYTES
            )
            != canonical(request).encode()
            or _read(
                observations / "command.json",
                entity_completion.MAX_ENTITY_REQUEST_BYTES,
            )
            != canonical(_manifest(receipt, command)).encode()
            or _read(observations / "supervision.json", MAX_RECEIPT_BYTES)
            != canonical(verified[0]).encode()
            or _read(
                observations / "stdout.log",
                min(
                    entity_completion.MAX_ENTITY_RESULT_BYTES,
                    settings["max_report_bytes"],
                ),
            )
            != (canonical(verified[2]) + "\n").encode()
            or ledger.snapshot() != state
            or ledger.authority_high_water() != anchor
        ):
            raise CoordinationError(
                "Entity resolution originals changed during fresh verification"
            )
        if completion is not None:
            if core != completion["proof"]["verification"]:
                raise CoordinationError(
                    "Fresh entity resolution differs from immutable completion"
                )
            ledger._entity_ack_resolution = (
                run_id,
                digest(observation),
                completion["proof_sha256"],
                digest(pending),
            )
            committing = True
            return entity_resolution.complete_resolution(
                ledger,
                prepared,
                intent,
                completion,
                pending,
                observation,
                observed.observe(),
                reason,
            )
        ledger._entity_clean_recovery = (run_id, digest(observation))
        committing = True
        return entity_history.complete_observation(
            ledger,
            prepared,
            intent,
            observation,
            observed.observe(),
            recovery={"reason": reason, "confirmed_cannot_resume": True},
        )
    except BaseException as error:
        if committing:
            _block_uncertain(ledger, run_id, observed.last)
        if isinstance(error, (KeyboardInterrupt, SystemExit, CoordinationError)):
            raise
        raise CoordinationError(
            "Entity original-clean resolution failed; details suppressed"
        ) from None
    finally:
        ledger._entity_clean_recovery = None
        ledger._entity_ack_resolution = None
