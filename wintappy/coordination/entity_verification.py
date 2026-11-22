"""Main-side authentication of read-only standalone entity input observations.

No launch, completion, authority, cursor or admission transition is implemented. The
standalone runtime owns data SQL; the main runtime binds canonical receipts,
requests, captures and shutdown. Original observations are never reconstructed.
"""

import hashlib
import json
import re
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from .authority import validate_anchor
from .completion import _clean, _manifest
from .entity_execution import recheck_entity_execution, validate_entity_receipt
from .execution import execution_environment
from .ledger import _mkdir
from .protocol import CoordinationError, canonical, digest, identifier, timestamp
from .supervisor import MAX_RECEIPT_BYTES, _publish, run_supervised
from .verification import _observations, _read

MAX_ENTITY_OBSERVATION_BYTES = 128 * 1024
MAX_ENTITY_RESULT_BYTES = 64 * 1024
MAX_ENTITY_REQUEST_BYTES = 1024 * 1024
INPUT_FIELDS = {
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
    "inputs",
    "authority_high_water",
    "entity_state_sha256",
    "checkpoint_state_sha256",
}
TRANSPORT_FIELDS = {"ledger_id", "dataset_sha256", "request_sha256"}


def _hash(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
        raise CoordinationError("Entity observation identity is missing")


def _count(value):
    if type(value) is not int or not 0 <= value < 2**63:
        raise CoordinationError("Entity observation requires exact bounded counts")


def _data_binding(receipt):
    roots, entity = receipt["binding"]["roots"], receipt["settings"]["entity"]
    return {
        "raw_root": roots["raw"],
        "compacted_root": roots["compacted"],
        "entity_root": roots["entity"],
        "metrics_root": roots["metrics"],
        "namespace": entity["namespace"],
        "hosts": entity["hosts"],
        "identity_policy": entity["identity_policy"],
        "identity_max_ratio": float(entity["identity_max_ratio"]),
        "window_start": receipt["hour_start"],
        "window_end": receipt["hour_end"],
        "event_types": receipt["settings"]["event_types"],
    }


def _core(result):
    return {key: result[key] for key in INPUT_FIELDS}


def _validate_inputs(result, receipt):
    """Validate the JSON interchange contract without importing standalone code."""
    expected = {
        "version": 1,
        "operation": "entity-only-inputs",
        "status": "verified",
        "command": receipt["entity_command"],
        "run_id": receipt["run_id"],
        "cli_run_id": receipt["cli_run_id"],
        "window_start": receipt["hour_start"],
        "window_end": receipt["hour_end"],
        "binding_sha256": digest(_data_binding(receipt)),
        "execution_sha256": digest(receipt),
    }
    if (
        not isinstance(result, dict)
        or set(result) != INPUT_FIELDS
        or len(canonical(result).encode()) > MAX_ENTITY_RESULT_BYTES
        or any(
            result[k] != v or type(result[k]) is not type(v)
            for k, v in expected.items()
        )
    ):
        raise CoordinationError("Entity input observation scope/type mismatch")
    _hash(result["checkpoint_state_sha256"])
    if receipt["entity_command"] == "entities-init":
        if (
            result["authority_high_water"] is not None
            or result["entity_state_sha256"] is not None
        ):
            raise CoordinationError("Entity genesis cannot adopt existing authority")
    else:
        validate_anchor(result["authority_high_water"], receipt["binding"])
        _hash(result["entity_state_sha256"])
    inputs = result["inputs"]
    if not isinstance(inputs, dict) or set(inputs) != {
        "raw",
        "compacted",
        "process_input_fingerprint",
        "source_fingerprint",
        "compacted_fingerprint",
        "hours",
    }:
        raise CoordinationError("Entity selected-input observation shape mismatch")
    for key in (
        "process_input_fingerprint",
        "source_fingerprint",
        "compacted_fingerprint",
    ):
        _hash(inputs[key])
    for name in ("raw", "compacted"):
        item = inputs[name]
        if not isinstance(item, dict) or set(item) != {
            "inventory_sha256",
            "content_sha256",
            "files",
            "bytes",
            "rows",
        }:
            raise CoordinationError("Entity selected-input evidence shape mismatch")
        for key in ("inventory_sha256", "content_sha256"):
            _hash(item[key])
        for key in ("files", "bytes", "rows"):
            _count(item[key])
    if (
        inputs["raw"]["files"] + inputs["compacted"]["files"]
        > receipt["settings"]["max_files"]
    ):
        raise CoordinationError("Entity selected-input file budget exhausted")
    first = timestamp(receipt["hour_start"], hour=True)
    last = timestamp(receipt["hour_end"], hour=True)
    hours = int((last - first).total_seconds() // 3600)
    if not isinstance(inputs["hours"], list) or len(inputs["hours"]) != hours:
        raise CoordinationError("Entity observations require the exact original window")
    for offset, row in enumerate(inputs["hours"]):
        hour = (first + timedelta(hours=offset)).strftime("%Y-%m-%dT%H:%M:%SZ")
        if (
            not isinstance(row, dict)
            or set(row) != {"hour_start", "raw", "compacted"}
            or row["hour_start"] != hour
        ):
            raise CoordinationError("Entity observation hour scope mismatch")
        for name in ("raw", "compacted"):
            item = row[name]
            if not isinstance(item, dict) or set(item) != {
                "files",
                "bytes",
                "fingerprint",
            }:
                raise CoordinationError("Entity hour-input evidence shape mismatch")
            _hash(item["fingerprint"])
            for key in ("files", "bytes"):
                _count(item[key])
                if item[key] > inputs[name][key]:
                    raise CoordinationError(
                        "Entity hour evidence exceeds selected inputs"
                    )
    if any(
        sum(row[name][key] for row in inputs["hours"]) != inputs[name][key]
        for name in ("raw", "compacted")
        for key in ("files", "bytes")
    ):
        raise CoordinationError("Entity hour totals differ from selected inputs")


def _command(receipt, observations, request):
    return [
        receipt["runtime"]["executable"],
        "-I",
        "-B",
        "-m",
        "raw_sensor_compact.entity_operation_verifier",
        "--request",
        str(observations.parent / "request.json"),
        "--request-sha256",
        digest(request),
    ]


def validate_input_observation(proof, binding, ledger_id):
    """Retained identity checks only; caller dictionaries never grant permission."""
    try:
        if (
            not isinstance(proof, dict)
            or set(proof)
            != {
                "version",
                "operation",
                "observed_at",
                "receipt",
                "request",
                "result",
                "supervision",
                "observations_directory",
            }
            or type(proof["version"]) is not int
            or proof["version"] != 1
            or proof["operation"] != "entity-input-observation"
            or len(canonical(proof).encode()) > MAX_ENTITY_OBSERVATION_BYTES
        ):
            raise CoordinationError("Invalid bounded entity input observation")
        identifier(ledger_id)
        receipt = validate_entity_receipt(proof["receipt"])
        if receipt["binding"] != binding or timestamp(proof["observed_at"]) < timestamp(
            receipt["created_at"]
        ):
            raise CoordinationError("Entity input receipt/clock binding mismatch")
        request = proof["request"]
        if not isinstance(request, dict) or set(request) != {
            "version",
            "operation",
            "arguments",
            "execution_sha256",
            "ledger_id",
            "dataset_sha256",
            "authority_high_water",
            "original",
        }:
            raise CoordinationError("Entity input request shape mismatch")
        result = proof["result"]
        if (
            not isinstance(result, dict)
            or set(result) != INPUT_FIELDS | TRANSPORT_FIELDS
        ):
            raise CoordinationError("Entity input result shape mismatch")
        core = _core(result)
        _validate_inputs(core, receipt)
        expected = {
            "version": 1,
            "operation": "entity-only-inputs",
            "arguments": receipt["command"][5:],
            "execution_sha256": digest(receipt),
            "ledger_id": ledger_id,
            "dataset_sha256": digest(binding),
            "authority_high_water": core["authority_high_water"],
            "original": request["original"],
        }
        if (
            canonical(request) != canonical(expected)
            or type(request["version"]) is not int
            or len(canonical(request).encode()) > MAX_ENTITY_REQUEST_BYTES
            or any(
                result[k] != value or type(result[k]) is not type(value)
                for k, value in {
                    "ledger_id": ledger_id,
                    "dataset_sha256": digest(binding),
                    "request_sha256": digest(request),
                }.items()
            )
        ):
            raise CoordinationError("Entity input request/result identity mismatch")
        if request["original"] is not None:
            _validate_inputs(request["original"], receipt)
            if canonical(request["original"]) != canonical(core):
                raise CoordinationError("Entity original observation changed")
        if not isinstance(proof["observations_directory"], str):
            raise CoordinationError("Entity input observations path is missing")
        observations = Path(proof["observations_directory"])
        identifier(observations.parent.name)
        if observations != (
            Path(receipt["observations_directory"]).parent
            / "input-verifications"
            / observations.parent.name
            / "observations"
        ):
            raise CoordinationError(
                "Entity input observations are outside the execution"
            )
        _clean(proof["supervision"], receipt, _command(receipt, observations, request))
        captured = (canonical(result) + "\n").encode()
        stdout = proof["supervision"]["stdout"]
        if (
            len(captured) > receipt["settings"]["max_report_bytes"]
            or stdout["bytes_saved"] != len(captured)
            or stdout["sha256"] != hashlib.sha256(captured).hexdigest()
        ):
            raise CoordinationError("Entity input captured result identity mismatch")
        return json.loads(canonical(core))
    except (TypeError, ValueError, KeyError, RecursionError):
        raise CoordinationError("Malformed entity input observation") from None


def recheck_input_observation(ledger, prepared, proof):
    """Authenticate actual bounded artifacts, not merely mutually rehashed JSON."""
    core = validate_input_observation(proof, prepared.binding, ledger.ledger_id)
    receipt = recheck_entity_execution(prepared, ledger)
    if canonical(receipt) != canonical(proof["receipt"]):
        raise CoordinationError("Entity input observation names another execution")
    observations = Path(proof["observations_directory"])
    request = proof["request"]
    command = _command(receipt, observations, request)
    observed = _observations(observations, receipt, command)
    if (
        _read(observations.parent / "request.json", MAX_ENTITY_REQUEST_BYTES)
        != canonical(request).encode()
        or _read(observations / "command.json", MAX_ENTITY_REQUEST_BYTES)
        != canonical(_manifest(receipt, command)).encode()
        or _read(observations / "supervision.json", MAX_RECEIPT_BYTES)
        != canonical(proof["supervision"]).encode()
        or observed[0] != proof["supervision"]
        or canonical(observed[2]) != canonical(proof["result"])
        or _read(observations / "stdout.log", receipt["settings"]["max_report_bytes"])
        != (canonical(proof["result"]) + "\n").encode()
    ):
        raise CoordinationError("Entity input observation artifacts changed")
    return core


def verify_entity_inputs_prepared(
    ledger, prepared, now, *, original=None, entity_run_id=None
):
    """Read-only input checks; a failed live typed check retains BLOCKED ownership.

    Owner-free checks grant no permission. Post-COMMIT checks require exact retained
    originals and this session's acknowledged typed intent, never a restarted owner.
    """
    try:
        observed_at = timestamp(now)
        ledger._require_entity_schema()
        ledger._validate()
        receipt = recheck_entity_execution(prepared, ledger)
        if observed_at < timestamp(receipt["created_at"]):
            raise CoordinationError("Entity input observation clock regressed")
        state, anchor = ledger.snapshot(), ledger.authority_high_water()
        live_intent = None
        if entity_run_id is not None:
            from .entity_intent import recheck_live_original

            live_intent = recheck_live_original(
                ledger, prepared, entity_run_id, original, now
            )
        if (
            (
                live_intent is None
                and (
                    state["mode"] not in {"OPEN", "DRAINING"}
                    or state["active_run_id"] is not None
                )
            )
            or state["readers"]
            or (receipt["entity_command"] == "entities-init") != (anchor is None)
        ):
            raise CoordinationError(
                "Entity input observation requires unowned exclusion"
            )
        previous = None
        if original is not None:
            previous = recheck_input_observation(ledger, prepared, original)
            if timestamp(original["observed_at"]) > timestamp(now):
                raise CoordinationError("Entity original observation clock regressed")
        request = {
            "version": 1,
            "operation": "entity-only-inputs",
            "arguments": receipt["command"][5:],
            "execution_sha256": prepared.sha256,
            "ledger_id": ledger.ledger_id,
            "dataset_sha256": digest(prepared.binding),
            "authority_high_water": anchor,
            "original": previous,
        }
        if len(canonical(request).encode()) > MAX_ENTITY_REQUEST_BYTES:
            raise CoordinationError("Entity input request exceeds byte budget")
        directory = prepared.path.parent / "input-verifications" / uuid4().hex
        _mkdir(directory)
        settings = receipt["settings"]
        _mkdir(Path(settings["temp_directory"]))
        storage = settings["storage"]
        if storage and storage["extension_directory"]:
            _mkdir(Path(storage["extension_directory"]))
        _publish(directory / "request.json", request)
        observations = directory / "observations"
        command = _command(receipt, observations, request)
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
        observed = _observations(observations, receipt, command)
        proof = {
            "version": 1,
            "operation": "entity-input-observation",
            "observed_at": now,
            "receipt": receipt,
            "request": request,
            "result": observed[2],
            "supervision": observed[0],
            "observations_directory": str(observations),
        }
        recheck_input_observation(ledger, prepared, proof)
        if (
            ledger.snapshot() != state
            or ledger.authority_high_water() != anchor
            or original is not None
            and recheck_input_observation(ledger, prepared, original) != previous
        ):
            raise CoordinationError("Entity input evidence changed during observation")
        if (
            live_intent is not None
            and recheck_live_original(ledger, prepared, entity_run_id, original, now)
            != live_intent
        ):
            raise CoordinationError(
                "Typed entity original changed during input observation"
            )
        return json.loads(canonical(proof))
    except BaseException as error:
        owner = getattr(ledger, "_entity_live_intent", None)
        state = ledger.snapshot()
        if (
            entity_run_id is not None
            and owner is not None
            and state["mode"] == "WRITING"
            and state["active_run_id"] == owner[0]
        ):
            from .entity_intent import block_entity

            run = ledger.entity_run(owner[0])
            blocked_at = run["started_at"]
            try:
                if timestamp(now) > timestamp(blocked_at):
                    blocked_at = now
            except CoordinationError:
                pass  # A broken caller clock cannot erase acknowledged ownership.
            block_entity(
                ledger,
                owner[0],
                blocked_at,
                "Typed entity input verification failed; preserve originals and require audited recovery",
            )
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        raise CoordinationError(
            "Independent entity input observation failed; details suppressed"
        ) from None
