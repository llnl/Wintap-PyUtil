"""Bounded retained authority observations, not caller-supplied initialization."""

import hashlib
import json
import re
from pathlib import Path

from .protocol import CoordinationError, canonical, digest, identifier, timestamp

MAX_AUTHORITY_BYTES = 64 * 1024
MAX_AUTHORITY_CHECKS = 10_000


def authority_binding_sha256(binding):
    entity, roots = binding["entity"], binding["roots"]
    if entity is None:
        raise CoordinationError("Explicit entity authority binding required")
    # The standalone journal identifies the canonical binding string itself.
    return digest(
        canonical(
            {
                "raw_root": roots["raw"],
                "compacted_root": roots["compacted"],
                "entity_root": roots["entity"],
                "metrics_root": roots["metrics"],
                "namespace": entity["namespace"],
                "hosts": entity["hosts"],
                "identity_policy": entity["identity_policy"],
                "identity_max_ratio": float(entity["identity_max_ratio"]),
            }
        )
    )


def validate_anchor(value, binding, previous=None):
    if (
        not isinstance(value, dict)
        or set(value) != {"generation", "generation_id", "binding_sha256"}
        or type(value["generation"]) is not int
        or not 0 < value["generation"] < 2**63
        or not isinstance(value["generation_id"], str)
        or not re.fullmatch(r"[a-f0-9]{64}", value["generation_id"])
        or value["binding_sha256"] != authority_binding_sha256(binding)
        or previous is not None
        and (
            value["generation"] < previous["generation"]
            or value["generation"] == previous["generation"]
            and value["generation_id"] != previous["generation_id"]
        )
    ):
        raise CoordinationError("Invalid or regressed entity authority high-water")
    return value


def validate_proof(proof, binding, ledger_id, previous):
    """Validate retained identities, without trusting or reopening external reports."""
    from .execution import validate_receipt
    from .supervisor import _validate_result

    if (
        not isinstance(proof, dict)
        or set(proof)
        != {
            "version",
            "observed_at",
            "initialization",
            "reason",
            "receipt",
            "execution_sha256",
            "request",
            "result",
            "supervision",
            "observations_directory",
        }
        or type(proof["version"]) is not int
        or proof["version"] != 1
        or type(proof["initialization"]) is not bool
        or proof["initialization"] != (previous is None)
        or not isinstance(proof["reason"], str)
        or not 0 < len(proof["reason"]) <= 4096
        or any(ord(c) < 32 for c in proof["reason"])
        or len(canonical(proof).encode()) > MAX_AUTHORITY_BYTES
    ):
        raise CoordinationError("Invalid bounded authority observation")
    timestamp(proof["observed_at"])
    receipt = validate_receipt(proof["receipt"])
    if receipt["binding"] != binding or digest(receipt) != proof["execution_sha256"]:
        raise CoordinationError("Authority execution binding mismatch")
    identifier(ledger_id)
    request = {
        "version": 2,
        "operation": "authority",
        "arguments": receipt["command"][5:],
        "execution_sha256": proof["execution_sha256"],
        "ledger_id": ledger_id,
        "dataset_sha256": digest(binding),
        "authority_high_water": previous,
    }
    result = proof["result"]
    expected = {
        "version": 2,
        "operation": "authority",
        "status": "verified",
        "execution_sha256": proof["execution_sha256"],
        "ledger_id": ledger_id,
        "dataset_sha256": digest(binding),
        "request_sha256": digest(request),
    }
    if (
        proof["request"] != request
        or not isinstance(result, dict)
        or set(result) != set(expected) | {"authority_high_water"}
        or any(
            result[k] != v or type(result[k]) is not type(v)
            for k, v in expected.items()
        )
    ):
        raise CoordinationError("Authority verifier request/result scope mismatch")
    validate_anchor(result["authority_high_water"], binding, previous)
    supervision = _validate_result(proof["supervision"])
    if (
        supervision["status"] != "exited"
        or supervision["exit_code"] != 0
        or supervision["children_stopped"] is not True
        or any(supervision[s]["truncated"] for s in ("stdout", "stderr"))
    ):
        raise CoordinationError("Authority verification shutdown is uncertain")
    if not isinstance(proof["observations_directory"], str):
        raise CoordinationError("Authority observations identity missing")
    observations = Path(proof["observations_directory"])
    if (
        observations.name != "observations"
        or observations.parent.parent.name != "authority-verifications"
        or observations.parent.parent.parent
        != Path(receipt["observations_directory"]).parent
    ):
        raise CoordinationError(
            "Authority observations path is outside the bound execution"
        )
    identifier(observations.parent.name)
    command = [
        receipt["runtime"]["executable"],
        "-I",
        "-B",
        "-m",
        "raw_sensor_compact.completion",
        "--authority-only",
        "--request",
        str(observations.parent / "request.json"),
        "--request-sha256",
        digest(request),
    ]
    settings = receipt["settings"]
    manifest = {
        "command": command,
        "cwd": receipt["cwd"],
        "timeout_seconds": settings["supervision_timeout_seconds"],
        "stop_grace_seconds": settings["stop_grace_seconds"],
        "shutdown_seconds": settings["shutdown_seconds"],
        "max_log_bytes": settings["max_log_bytes"],
    }
    captured = (canonical(result) + "\n").encode()
    if (
        supervision["command_sha256"] != digest(manifest)
        or len(captured) > settings["max_report_bytes"]
        or supervision["stdout"]["bytes_saved"] != len(captured)
        or supervision["stdout"]["sha256"] != hashlib.sha256(captured).hexdigest()
    ):
        raise CoordinationError("Authority execution/captured result identity mismatch")
    return json.loads(canonical(result["authority_high_water"]))
