"""Read-only current-generation entity completion observations, never permission.

The main runtime authenticates original typed exclusion, post-COMMIT inputs and
actual CLI/verifier artifacts. Standalone owns data SQL. No entity command is
launched here; ownership, authority, cursor and acknowledgment remain unchanged,
including on refusal. Historical publication/recovery has no fallback.
"""

import hashlib
import json
from pathlib import Path
from uuid import uuid4

from .authority import validate_anchor
from .completion import _clean, _manifest
from .entity_execution import recheck_entity_execution, validate_entity_receipt
from .entity_intent import INTENT_FIELDS, recheck_live_original
from .entity_verification import (
    TRANSPORT_FIELDS,
    _command,
    _count,
    _data_binding,
    _hash,
    recheck_input_observation,
    validate_input_observation,
)
from .execution import execution_environment
from .ledger import _mkdir, _validate_block_reason
from .protocol import CoordinationError, canonical, digest, identifier, timestamp
from .supervisor import MAX_RECEIPT_BYTES, _publish, run_supervised
from .verification import _observations, _read

MAX_ENTITY_OBSERVATION_BYTES = 128 * 1024
MAX_ENTITY_RESULT_BYTES = 64 * 1024
MAX_ENTITY_REQUEST_BYTES = 1024 * 1024
COMPLETION_FIELDS = {
    "version",
    "operation",
    "status",
    "command",
    "run_id",
    "cli_run_id",
    "window_start",
    "window_end",
    "binding_sha256",
    "execution_sha256",
    "supervision_sha256",
    "inputs_sha256",
    "original_sha256",
    "archive_sha256",
    "archive_records",
    "authority_high_water",
    "entity_state_sha256",
    "checkpoint_sha256",
    "checkpoint_generation_id",
    "checkpoint_hours",
    "publication_sha256",
}
OBSERVATION_FIELDS = {
    "version",
    "operation",
    "observed_at",
    "receipt",
    "intent_sha256",
    "prelaunch",
    "execution_supervision",
    "execution_result",
    "request",
    "result",
    "supervision",
    "observations_directory",
}


def _prelaunch(intent, prelaunch, receipt, binding, ledger_id, now):
    """Contract-only checks; live callers must additionally recheck stored originals."""
    if (
        not isinstance(intent, dict)
        or set(intent) != INTENT_FIELDS
        or type(intent["version"]) is not int
        or intent["version"] != 1
        or intent["operation"] != "entity-only-intent"
        or intent["confirmed"] is not True
        or intent["ledger_id"] != ledger_id
        or canonical(intent["binding"]) != canonical(binding)
        or intent["execution"]
        != {
            "path": str(
                Path(receipt["observations_directory"]).parent / "execution.json"
            ),
            "sha256": digest(receipt),
            "receipt": receipt,
        }
        or any(
            intent[k] != receipt[k]
            for k in (
                "run_id",
                "cli_run_id",
                "entity_command",
                "hour_start",
                "hour_end",
            )
        )
    ):
        raise CoordinationError("Entity completion needs its original typed intent")
    _validate_block_reason(intent["reason"])
    _hash(intent["schema_upgrade_sha256"])
    timestamp(intent["raw_next_hour"], hour=True)
    original = intent["input_observation"]
    core = validate_input_observation(original, binding, ledger_id)
    checked = validate_input_observation(prelaunch, binding, ledger_id)
    if (
        canonical(original["receipt"]) != canonical(receipt)
        or original["request"]["original"] is not None
        or canonical(core["authority_high_water"])
        != canonical(intent["authority_before"])
        or canonical(prelaunch["receipt"]) != canonical(receipt)
        or canonical(prelaunch["request"]["original"]) != canonical(core)
        or canonical(checked) != canonical(core)
        or prelaunch["observations_directory"] == original["observations_directory"]
        or timestamp(original["observed_at"]) > timestamp(intent["started_at"])
        or timestamp(prelaunch["observed_at"]) < timestamp(intent["started_at"])
        or timestamp(now) < timestamp(prelaunch["observed_at"])
    ):
        raise CoordinationError("Entity completion original/post-COMMIT input mismatch")
    if intent["authority_before"] is not None:
        validate_anchor(intent["authority_before"], binding)
        _hash(intent["authority_proof_sha256"])
    elif intent["authority_proof_sha256"] is not None:
        raise CoordinationError("Entity genesis has unexpected authority evidence")
    return core


def _cli_result(report, receipt, original):
    """Bind the distinct pretty-printed CLI report, not a canonical verifier capture."""
    if (
        not isinstance(report, dict)
        or set(report)
        != {
            "status",
            "run_id",
            "coordinator_run_id",
            "hosts",
            "source_fingerprint",
            "compacted_fingerprint",
            "process_input_fingerprint",
            "identity_exclusions",
            "generation_id",
            "processing_metrics",
            "phase_evidence",
        }
        or len(canonical(report).encode()) > MAX_ENTITY_RESULT_BYTES
        or report["status"] != "completed"
        or report["run_id"] != receipt["cli_run_id"]
        or report["coordinator_run_id"] != receipt["run_id"]
        or any(
            report[k] != original["inputs"][k]
            for k in (
                "source_fingerprint",
                "compacted_fingerprint",
                "process_input_fingerprint",
            )
        )
    ):
        raise CoordinationError("Entity CLI result is not original-bound completion")
    _hash(report["generation_id"])
    hours = len(original["inputs"]["hours"])
    metrics = {
        "hours": hours,
        "root": receipt["binding"]["roots"]["metrics"],
        "run_id": receipt["cli_run_id"],
    }
    phase = report["phase_evidence"]
    if (
        canonical(report["processing_metrics"]) != canonical(metrics)
        or not isinstance(phase, dict)
        or set(phase) != {"directory", "last_sha256", "records", "migration_complete"}
        or phase["directory"]
        != str(
            Path(receipt["settings"]["run_evidence_directory"]) / receipt["cli_run_id"]
        )
        or type(phase["records"]) is not int
        or phase["records"] != hours + 5
        or phase["migration_complete"] is not False
    ):
        raise CoordinationError("Entity CLI checkpoint/archive scope mismatch")
    _hash(phase["last_sha256"])
    hosts = report["hosts"]
    configured_hosts = receipt["settings"]["entity"]["hosts"]
    reported_hosts = [host.get("hostname") for host in hosts]
    if (
        not isinstance(hosts, list)
        or not 0 < len(hosts) <= 1024
        or any(
            not isinstance(hostname, str) or not hostname for hostname in reported_hosts
        )
        or reported_hosts != sorted(set(reported_hosts))
        or not set(configured_hosts) <= set(reported_hosts)
        or not isinstance(report["identity_exclusions"], dict)
    ):
        raise CoordinationError("Entity CLI host/exclusion scope mismatch")
    for host in hosts:
        if (
            set(host)
            != {
                "hostname",
                "rows",
                "changed",
                "semantic_sha256",
                "bootstrap_start",
                "snapshot_bytes",
                "parent_conflicts",
            }
            or type(host["changed"]) is not bool
        ):
            raise CoordinationError("Entity CLI host statistics shape mismatch")
        for key in ("rows", "snapshot_bytes", "parent_conflicts"):
            _count(host[key])
        _hash(host["semantic_sha256"])
        # Replay preserves original bootstrap provenance, not its own window start.
        timestamp(host["bootstrap_start"], hour=True)
    return (json.dumps(report, indent=2, sort_keys=True) + "\n").encode()


def _cli_archive(receipt, report):
    """CLI host statistics must be the actual original archived publication report.

    Standalone independently authenticates the full archive chain and current data;
    main reads this bounded report record before/after that check, without data SQL.
    """
    path = (
        Path(receipt["settings"]["run_evidence_directory"])
        / receipt["cli_run_id"]
        / "000002-entities_verified.json"
    )
    payload = _read(path, MAX_ENTITY_RESULT_BYTES)
    record = json.loads(payload)
    if (
        not isinstance(record, dict)
        or set(record)
        != {
            "sha256",
            "evidence_version",
            "sequence",
            "previous_sha256",
            "run_id",
            "coordinator_run_id",
            "phase",
            "data",
        }
        or payload != (canonical(record) + "\n").encode()
        or record["sha256"]
        != digest({k: v for k, v in record.items() if k != "sha256"})
        or record["evidence_version"] != "1"
        or type(record["sequence"]) is not int
        or record["sequence"] != 2
        or record["phase"] != "entities_verified"
        or record["run_id"] != receipt["cli_run_id"]
        or record["coordinator_run_id"] != receipt["run_id"]
        or canonical(record["data"])
        != canonical(
            {
                k: v
                for k, v in report.items()
                if k
                not in {
                    "run_id",
                    "coordinator_run_id",
                    "phase_evidence",
                    "processing_metrics",
                }
            }
        )
    ):
        raise CoordinationError(
            "Entity CLI report differs from original publication archive"
        )
    _hash(record["previous_sha256"])


def _execution_artifacts(receipt, original):
    directory = Path(receipt["observations_directory"])
    observed = _observations(directory, receipt, receipt["command"])
    supervision, report = observed[0], observed[2]
    _clean(supervision, receipt, receipt["command"])
    captured = _cli_result(report, receipt, original)
    if (
        _read(directory / "command.json", MAX_ENTITY_REQUEST_BYTES)
        != canonical(_manifest(receipt, receipt["command"])).encode()
        or _read(directory / "supervision.json", MAX_RECEIPT_BYTES)
        != canonical(supervision).encode()
        or _read(
            directory / "stdout.log",
            min(MAX_ENTITY_RESULT_BYTES, receipt["settings"]["max_report_bytes"]),
        )
        != captured
    ):
        raise CoordinationError("Entity CLI canonical artifacts/capture changed")
    _cli_archive(receipt, report)
    return supervision, report


def _request(receipt, ledger_id, intent, original, supervision):
    return {
        "version": 1,
        "operation": "entity-only-completion",
        "arguments": receipt["command"][5:],
        "execution_sha256": digest(receipt),
        "ledger_id": ledger_id,
        "dataset_sha256": digest(receipt["binding"]),
        "authority_high_water": original["authority_high_water"],
        "original": original,
        "supervision_sha256": digest(supervision),
        "intent_sha256": digest(intent),
        "publication": None,
    }


def validate_completion_observation(proof, binding, ledger_id, *, intent):
    """Bounded retained contract checks only; no historical evidence or permission."""
    try:
        if (
            not isinstance(proof, dict)
            or set(proof) != OBSERVATION_FIELDS
            or type(proof["version"]) is not int
            or proof["version"] != 1
            or proof["operation"] != "entity-completion-observation"
            or len(canonical(proof).encode()) > MAX_ENTITY_OBSERVATION_BYTES
        ):
            raise CoordinationError("Invalid bounded entity completion observation")
        identifier(ledger_id)
        receipt = validate_entity_receipt(proof["receipt"])
        if canonical(receipt["binding"]) != canonical(binding):
            raise CoordinationError("Entity completion receipt binding mismatch")
        original = _prelaunch(
            intent,
            proof["prelaunch"],
            receipt,
            binding,
            ledger_id,
            proof["observed_at"],
        )
        execution = proof["execution_supervision"]
        _clean(execution, receipt, receipt["command"])
        cli = proof["execution_result"]
        capture = _cli_result(cli, receipt, original)
        if (
            len(capture)
            > min(MAX_ENTITY_RESULT_BYTES, receipt["settings"]["max_report_bytes"])
            or execution["stdout"]["bytes_saved"] != len(capture)
            or execution["stdout"]["sha256"] != hashlib.sha256(capture).hexdigest()
            or proof["intent_sha256"] != digest(intent)
        ):
            raise CoordinationError("Entity CLI captured result/intent mismatch")
        request = _request(receipt, ledger_id, intent, original, execution)
        if (
            canonical(proof["request"]) != canonical(request)
            or len(canonical(request).encode()) > MAX_ENTITY_REQUEST_BYTES
        ):
            raise CoordinationError("Entity completion request identity mismatch")
        result = proof["result"]
        hours = len(original["inputs"]["hours"])
        expected = {
            "version": 1,
            "operation": "entity-only-completion",
            "status": "verified",
            "command": receipt["entity_command"],
            "run_id": receipt["run_id"],
            "cli_run_id": receipt["cli_run_id"],
            "window_start": receipt["hour_start"],
            "window_end": receipt["hour_end"],
            "binding_sha256": digest(_data_binding(receipt)),
            "execution_sha256": digest(receipt),
            "supervision_sha256": digest(execution),
            "inputs_sha256": digest(original["inputs"]),
            "original_sha256": digest(original),
            "archive_sha256": cli["phase_evidence"]["last_sha256"],
            "archive_records": hours + 5,
            "checkpoint_generation_id": cli["generation_id"],
            "checkpoint_hours": hours,
            "publication_sha256": None,
            "intent_sha256": digest(intent),
            "ledger_id": ledger_id,
            "dataset_sha256": digest(binding),
            "request_sha256": digest(request),
        }
        if (
            not isinstance(result, dict)
            or set(result) != COMPLETION_FIELDS | TRANSPORT_FIELDS | {"intent_sha256"}
            or len(canonical(result).encode()) > MAX_ENTITY_RESULT_BYTES
            or any(
                result[k] != v or type(result[k]) is not type(v)
                for k, v in expected.items()
            )
        ):
            raise CoordinationError("Entity completion result scope/type mismatch")
        for key in ("entity_state_sha256", "checkpoint_sha256"):
            _hash(result[key])
        anchor = validate_anchor(
            result["authority_high_water"], binding, original["authority_high_water"]
        )
        before = original["authority_high_water"]
        if (
            anchor["generation"] != (before["generation"] + 1 if before else 1)
            or anchor["generation_id"] != result["checkpoint_generation_id"]
            or before is not None
            and anchor["generation_id"] == before["generation_id"]
        ):
            raise CoordinationError(
                "Entity completion requires its original current generation"
            )
        if not isinstance(proof["observations_directory"], str):
            raise CoordinationError("Entity completion observation path is missing")
        observations = Path(proof["observations_directory"])
        identifier(observations.parent.name)
        if observations != (
            Path(receipt["observations_directory"]).parent
            / "completion-verifications"
            / observations.parent.name
            / "observations"
        ):
            raise CoordinationError(
                "Entity completion observation is outside its execution"
            )
        _clean(proof["supervision"], receipt, _command(receipt, observations, request))
        captured = (canonical(result) + "\n").encode()
        stdout = proof["supervision"]["stdout"]
        if (
            len(captured) > receipt["settings"]["max_report_bytes"]
            or stdout["bytes_saved"] != len(captured)
            or stdout["sha256"] != hashlib.sha256(captured).hexdigest()
        ):
            raise CoordinationError(
                "Entity completion verifier capture identity mismatch"
            )
        return json.loads(canonical({k: result[k] for k in COMPLETION_FIELDS}))
    except (TypeError, ValueError, KeyError, AttributeError, RecursionError):
        raise CoordinationError("Malformed entity completion observation") from None


def _live(ledger, prepared, original, prelaunch, now):
    ledger._require_entity_schema()
    ledger._validate()  # Typed budgets/history precede every owner/payload lookup.
    receipt = recheck_entity_execution(prepared, ledger)
    intent = recheck_live_original(ledger, prepared, receipt["run_id"], original, now)
    core = _prelaunch(
        intent, prelaunch, receipt, prepared.binding, ledger.ledger_id, now
    )
    if recheck_input_observation(ledger, prepared, prelaunch) != core:
        raise CoordinationError("Entity post-COMMIT inputs changed")
    return receipt, intent, core


def recheck_completion_observation(ledger, prepared, proof):
    """Recheck actual originals/captures in the acknowledged live session, read-only."""
    try:
        ledger._require_entity_schema()
        ledger._validate()
        run = ledger.entity_run(prepared.receipt["run_id"])
        if run is None:
            raise CoordinationError("Entity completion has no typed original")
        intent = json.loads(run["intent_json"])
        core = validate_completion_observation(
            proof, prepared.binding, ledger.ledger_id, intent=intent
        )
        receipt, retained, original = _live(
            ledger,
            prepared,
            intent["input_observation"],
            proof["prelaunch"],
            proof["observed_at"],
        )
        supervision, report = _execution_artifacts(receipt, original)
        observations = Path(proof["observations_directory"])
        request = proof["request"]
        command = _command(receipt, observations, request)
        observed = _observations(observations, receipt, command)
        if (
            canonical(retained) != canonical(intent)
            or canonical(supervision) != canonical(proof["execution_supervision"])
            or canonical(report) != canonical(proof["execution_result"])
            or _read(observations.parent / "request.json", MAX_ENTITY_REQUEST_BYTES)
            != canonical(request).encode()
            or _read(observations / "command.json", MAX_ENTITY_REQUEST_BYTES)
            != canonical(_manifest(receipt, command)).encode()
            or _read(observations / "supervision.json", MAX_RECEIPT_BYTES)
            != canonical(proof["supervision"]).encode()
            or canonical(observed[0]) != canonical(proof["supervision"])
            or canonical(observed[2]) != canonical(proof["result"])
            or _read(
                observations / "stdout.log",
                min(MAX_ENTITY_RESULT_BYTES, receipt["settings"]["max_report_bytes"]),
            )
            != (canonical(proof["result"]) + "\n").encode()
        ):
            raise CoordinationError("Entity completion original/artifacts changed")
        return core
    except (TypeError, ValueError, KeyError, AttributeError, RecursionError):
        raise CoordinationError(
            "Malformed live entity completion observation"
        ) from None


def verify_entity_completion_prepared(ledger, prepared, now, *, original, prelaunch):
    """Launch only a read-only verifier, with no ledger or acknowledgment transition."""
    try:
        receipt, intent, inputs = _live(ledger, prepared, original, prelaunch, now)
        state, anchor = ledger.snapshot(), ledger.authority_high_water()
        acknowledgment = ledger._entity_live_intent
        execution, cli = _execution_artifacts(receipt, inputs)
        request = _request(receipt, ledger.ledger_id, intent, inputs, execution)
        if len(canonical(request).encode()) > MAX_ENTITY_REQUEST_BYTES:
            raise CoordinationError("Entity completion request exceeds byte budget")
        directory = prepared.path.parent / "completion-verifications" / uuid4().hex
        _mkdir(directory)
        _publish(directory / "request.json", request)
        observations = directory / "observations"
        settings = receipt["settings"]
        _mkdir(Path(settings["temp_directory"]))
        storage = settings["storage"]
        if storage and storage["extension_directory"]:
            _mkdir(Path(storage["extension_directory"]))
        run_supervised(
            _command(receipt, observations, request),
            observations,
            timeout_seconds=settings["supervision_timeout_seconds"],
            stop_grace_seconds=settings["stop_grace_seconds"],
            shutdown_seconds=settings["shutdown_seconds"],
            max_log_bytes=settings["max_log_bytes"],
            cwd=receipt["cwd"],
            env=execution_environment(prepared),
        )
        observed = _observations(
            observations, receipt, _command(receipt, observations, request)
        )
        proof = {
            "version": 1,
            "operation": "entity-completion-observation",
            "observed_at": now,
            "receipt": receipt,
            "intent_sha256": digest(intent),
            "prelaunch": prelaunch,
            "execution_supervision": execution,
            "execution_result": cli,
            "request": request,
            "result": observed[2],
            "supervision": observed[0],
            "observations_directory": str(observations),
        }
        recheck_completion_observation(ledger, prepared, proof)
        if (
            ledger.snapshot() != state
            or ledger.authority_high_water() != anchor
            or ledger._entity_live_intent != acknowledgment
        ):
            raise CoordinationError(
                "Entity completion scope changed during observation"
            )
        return json.loads(canonical(proof))
    except BaseException as error:
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        raise CoordinationError(
            "Independent entity completion observation failed; details suppressed"
        ) from None
