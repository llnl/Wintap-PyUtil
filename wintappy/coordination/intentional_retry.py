"""Audited retry ownership; failed originals are never completed or erased.

Only a fresh independent preflight under uninterrupted exclusion can commit a new
owner. Restart/acknowledgment uncertainty never authorizes resuming that invocation.
The established retry path remains raw-only.  A separate, narrower
interrupted-before-publication path handles a supervised network/process
interruption before any migration evidence was committed.
"""

import hashlib
import json
import re
from pathlib import Path
from uuid import uuid4

from .completion import _clean, _complete_live_prepared, _manifest, _recheck_archive
from .coordinator import _Clock, writer_due
from .execution import (
    PreparedExecution,
    command_for,
    execution_environment,
    recheck_execution,
    validate_receipt,
)
from .ledger import _mkdir, _validate_block_reason
from .protocol import CoordinationError, canonical, digest, identifier, timestamp
from .retry import (
    _original_source_totals,
    _verify_retry_prepared,
    verify_retry_prepared,
)
from .supervisor import _parse_json, _publish, _validate_result
from .verification import _observations, _read, _verified_authority_observation
from .writer import _launch_prepared

MAX_RETRY_BYTES = 128 * 1024
MAX_RETRIES = 10_000
MAX_RETRY_TOTAL_BYTES = 64 * 1024 * 1024


def _hash(value):
    if not isinstance(value, str) or re.fullmatch(r"[a-f0-9]{64}", value) is None:
        raise CoordinationError("Retry identity must be a SHA-256 digest")


def _interrupted_binding(receipt):
    roots = receipt["binding"]["roots"]
    entity = receipt["settings"]["entity"]
    return {
        "raw_root": roots["raw"],
        "compacted_root": roots["compacted"],
        "entity_root": roots["entity"],
        "metrics_root": roots["metrics"],
        "namespace": entity["namespace"],
        "hosts": entity["hosts"],
        "identity_policy": entity["identity_policy"],
        "identity_max_ratio": entity["identity_max_ratio"],
        "window_start": receipt["hour_start"],
        "window_end": receipt["hour_end"],
        "event_types": receipt["settings"]["event_types"],
    }


def _interrupted_archive(receipt):
    """Validate the tiny archive produced before an interrupted launch.

    The compactor writes the entity intent before doing any entity publication and
    writes migration intent before creating raw output.  This exact two-record
    archive is therefore the only retryable pre-publication shape.
    """
    directory = (
        Path(receipt["settings"]["run_evidence_directory"]) / receipt["cli_run_id"]
    )
    try:
        paths = sorted(path for path in directory.iterdir() if path.is_file())
    except OSError:
        raise CoordinationError(
            "Interrupted retry evidence directory is unavailable"
        ) from None
    if [path.name for path in paths] != [
        "000000-run_intent.json",
        "000001-entities_intent.json",
    ]:
        raise CoordinationError("Interrupted retry requires pre-publication evidence")
    records, previous = [], ""
    expected_binding = _interrupted_binding(receipt)
    for sequence, path in enumerate(paths):
        payload = _read(path, MAX_RETRY_BYTES)
        try:
            value = json.loads(payload)
        except json.JSONDecodeError:
            raise CoordinationError("Interrupted retry archive is not JSON") from None
        if payload not in {
            canonical(value).encode(),
            (canonical(value) + "\n").encode(),
        }:
            raise CoordinationError("Interrupted retry archive bytes changed")
        if set(value) != {
            "coordinator_run_id",
            "data",
            "evidence_version",
            "phase",
            "previous_sha256",
            "run_id",
            "sequence",
            "sha256",
        }:
            raise CoordinationError("Interrupted retry archive shape changed")
        sha256 = value["sha256"]
        unsigned = {key: value[key] for key in value if key != "sha256"}
        if (
            digest(unsigned) != sha256
            or value["previous_sha256"] != previous
            or value["sequence"] != sequence
            or value["run_id"] != receipt["cli_run_id"]
            or value["coordinator_run_id"] != receipt["run_id"]
            or value["evidence_version"] != "1"
            or path.name != f"{sequence:06d}-{value['phase']}.json"
        ):
            raise CoordinationError("Interrupted retry archive chain changed")
        records.append(value)
        previous = sha256
    if (
        not isinstance(records[0]["data"], dict)
        or not isinstance(records[1]["data"], dict)
        or records[0]["phase"] != "run_intent"
        or records[0]["data"].get("command") != "compact"
        or records[0]["data"].get("binding") != expected_binding
        or records[1]["phase"] != "entities_intent"
        or records[1]["data"] != {"operation": "compact"}
    ):
        raise CoordinationError("Interrupted retry archive scope changed")
    return {"archive_sha256": previous, "archive_records": len(records)}


def _interrupted_supervision(receipt):
    directory = Path(receipt["observations_directory"])
    command = _parse_json(_read(directory / "command.json", MAX_RETRY_BYTES))
    supervision = _parse_json(_read(directory / "supervision.json", MAX_RETRY_BYTES))
    _validate_result(supervision)
    if (
        command != _manifest(receipt, receipt["command"])
        or supervision["command_sha256"] != digest(command)
        or supervision["status"] != "interrupted"
        or supervision["exit_code"] != -15
        or supervision["children_stopped"] is not True
        or any(
            supervision[stream]["bytes_saved"] != 0
            or supervision[stream]["bytes_seen"] != 0
            or supervision[stream]["sha256"]
            != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            for stream in ("stdout", "stderr")
        )
    ):
        raise CoordinationError("Interrupted retry requires a clean empty interruption")
    return supervision, digest(supervision)


def validate_interrupted_preflight(proof, original, binding, ledger_id):
    """Validate retained pre-publication evidence without reopening data."""
    from .authority import validate_proof

    if (
        not isinstance(proof, dict)
        or set(proof)
        != {
            "version",
            "operation",
            "observed_at",
            "receipt",
            "archive_sha256",
            "archive_records",
            "writer_supervision",
            "authority_before",
            "authority",
        }
        or proof["version"] != 1
        or proof["operation"] != "interrupted-preflight"
        or original is None
        or original["state"] != "BLOCKED"
        or len(canonical(proof).encode()) > MAX_RETRY_BYTES
    ):
        raise CoordinationError("Invalid interrupted pre-publication evidence")
    receipt = validate_receipt(proof["receipt"])
    intent = json.loads(original["intent_json"])
    if (
        digest(intent) != original["intent_sha256"]
        or receipt["binding"] != binding
        or intent.get("execution", {}).get("receipt") != receipt
        or receipt["settings"]["entity"] is None
        or receipt["settings"]["delete_source"] is not True
        or proof["archive_records"] != 2
        or not isinstance(proof["archive_sha256"], str)
        or not re.fullmatch(r"[a-f0-9]{64}", proof["archive_sha256"])
        or timestamp(proof["observed_at"]) < timestamp(original["started_at"])
    ):
        raise CoordinationError("Interrupted pre-publication scope mismatch")
    supervision = proof["writer_supervision"]
    _validate_result(supervision)
    if (
        supervision["status"] != "interrupted"
        or supervision["exit_code"] != -15
        or supervision["children_stopped"] is not True
    ):
        raise CoordinationError("Interrupted supervision evidence mismatch")
    validate_proof(
        proof["authority"],
        binding,
        ledger_id,
        proof["authority_before"],
    )
    if (
        proof["authority"]["result"]["authority_high_water"]
        != proof["authority_before"]
    ):
        raise CoordinationError("Interrupted retry authority changed")


def _verify_interrupted_prepared(ledger, prepared, now, *, retry_run_id=None):
    receipt = recheck_execution(prepared, ledger)
    state = ledger.snapshot()
    original = ledger.writer_run(receipt["run_id"])
    if (
        original is None
        or original["state"] != "BLOCKED"
        or state["readers"]
        or state["next_hour"] != receipt["hour_start"]
        or state["active_run_id"] != (retry_run_id or receipt["run_id"])
        or state["mode"] != ("WRITING" if retry_run_id else "BLOCKED")
    ):
        raise CoordinationError("Interrupted retry requires its exact blocked owner")
    if retry_run_id is not None:
        links = ledger._connection.execute(
            "SELECT original_run_id FROM retry_dispositions "
            "WHERE retry_run_id=? LIMIT 2",
            [retry_run_id],
        ).fetchall()
        if links != [(receipt["run_id"],)]:
            raise CoordinationError("Interrupted retry owner link changed")
    archive = _interrupted_archive(receipt)
    supervision, supervision_sha256 = _interrupted_supervision(receipt)
    before = ledger.authority_high_water()
    if before is None:
        raise CoordinationError("Interrupted retry requires entity authority")
    authority = _verified_authority_observation(
        ledger,
        prepared,
        receipt,
        before,
        now,
        reason="Fresh authority check before interrupted pre-publication retry",
    )
    proof = {
        "version": 1,
        "operation": "interrupted-preflight",
        "observed_at": now,
        "receipt": receipt,
        **archive,
        "writer_supervision": supervision,
        "authority_before": before,
        "authority": authority,
    }
    validate_interrupted_preflight(
        proof, original, json.loads(ledger.binding), ledger.ledger_id
    )
    if (
        recheck_execution(prepared, ledger) != receipt
        or ledger.writer_run(receipt["run_id"]) != original
        or ledger.authority_high_water() != before
        or digest(supervision) != supervision_sha256
        or ledger.snapshot() != state
    ):
        raise CoordinationError("Interrupted retry evidence changed during preflight")
    return proof


def verify_interrupted_prepared(ledger, prepared, now):
    """Fresh read-only qualification for an interrupted pre-publication run."""
    ledger._validate()
    return _verify_interrupted_prepared(ledger, prepared, now)


def _validate_interrupted_disposition(proof, original, new, binding, ledger_id):
    if (
        not isinstance(proof, dict)
        or set(proof)
        != {
            "version",
            "operation",
            "committed_at",
            "reason",
            "confirmed_cannot_resume",
            "original_run_sha256",
            "retry_intent_sha256",
            "preflight",
        }
        or proof["version"] != 1
        or proof["operation"] != "interrupted-retry"
        or proof["confirmed_cannot_resume"] is not True
        or original is None
        or new is None
        or len(canonical(proof).encode()) > MAX_RETRY_BYTES
        or proof["original_run_sha256"] != digest(original)
        or proof["retry_intent_sha256"] != new["intent_sha256"]
    ):
        raise CoordinationError("Invalid interrupted retry disposition")
    _validate_block_reason(proof["reason"])
    _validate_block_reason(original["block_reason"])
    original_intent = json.loads(original["intent_json"])
    if digest(original_intent) != original["intent_sha256"]:
        raise CoordinationError("Interrupted retry original intent changed")
    validate_interrupted_preflight(proof["preflight"], original, binding, ledger_id)
    first = validate_receipt(proof["preflight"]["receipt"])
    second = validate_receipt(json.loads(new["intent_json"])["execution"]["receipt"])
    if (
        len(
            {
                first["run_id"],
                first["cli_run_id"],
                second["run_id"],
                second["cli_run_id"],
            }
        )
        != 4
        or any(
            first[key] != second[key]
            for key in (
                "binding",
                "hour_start",
                "hour_end",
                "runtime",
                "settings",
                "cwd",
            )
        )
        or proof["committed_at"] != new["started_at"]
        or timestamp(second["created_at"]) > timestamp(proof["committed_at"])
        or timestamp(proof["committed_at"])
        < timestamp(proof["preflight"]["observed_at"])
    ):
        raise CoordinationError("Interrupted retry changed original scope/clock")


def _verifier_command(receipt, observations, request):
    return [
        receipt["runtime"]["executable"],
        "-I",
        "-B",
        "-m",
        "raw_sensor_compact.retry_verifier",
        "--request",
        str(observations.parent / "request.json"),
        "--request-sha256",
        digest(request),
    ]


def _capture(value, supervision, stream, receipt, *, canonical_json, raw=None):
    payload = (
        (
            canonical(value)
            if canonical_json
            else json.dumps(value, indent=2, sort_keys=True)
        )
        + "\n"
    ).encode()
    if raw is not None:
        if not isinstance(raw, str) or canonical(
            _parse_json(raw.encode())
        ) != canonical(value):
            raise CoordinationError("Retry original capture JSON changed")
        payload = raw.encode()
    if (
        len(payload) > receipt["settings"]["max_report_bytes"]
        or supervision[stream]["bytes_saved"] != len(payload)
        or supervision[stream]["sha256"] != hashlib.sha256(payload).hexdigest()
    ):
        raise CoordinationError("Retry retained report/capture mismatch")


def validate_preflight(proof, original, binding, ledger_id, report, capture):
    """Retained JSON integrity only; fresh external checks remain mandatory at entry."""
    if (
        not isinstance(proof, dict)
        or set(proof)
        != {
            "version",
            "operation",
            "observed_at",
            "receipt",
            "request",
            "writer_supervision",
            "verification",
            "verifier",
            "verifier_observations",
        }
        or type(proof["version"]) is not int
        or proof["version"] != 1
        or proof["operation"] != "retry-preflight"
        or len(canonical(proof).encode()) > MAX_RETRY_BYTES
    ):
        raise CoordinationError("Invalid retained retry preflight")
    receipt = validate_receipt(proof["receipt"])
    intent = json.loads(original["intent_json"])
    if (
        original["state"] != "BLOCKED"
        or digest(intent) != original["intent_sha256"]
        or intent.get("execution", {}).get("receipt") != receipt
        or receipt["binding"] != binding
        or receipt["settings"]["entity"] is not None
        or receipt["settings"]["delete_source"] is not False
        or len(receipt["settings"]["event_types"]) != 1
        or timestamp(proof["observed_at"]) < timestamp(original["started_at"])
    ):
        raise CoordinationError("Retry preflight is not the raw-only failed original")
    writer = proof["writer_supervision"]
    _clean(writer, receipt, receipt["command"], successful=False)
    _capture(
        report,
        writer,
        "stdout" if writer["stdout"]["bytes_saved"] else "stderr",
        receipt,
        canonical_json=False,
        raw=capture,
    )
    roots, settings = binding["roots"], receipt["settings"]
    hour = timestamp(receipt["hour_start"])
    parent = f"{settings['event_types'][0]}/dayPK={hour:%Y%m%d}/hourPK={hour:%H}/"
    phase = report.get("phase_evidence")
    if (
        report.get("status") != "error"
        or report.get("phase") != "merge"
        or report.get("run_id") != receipt["cli_run_id"]
        or report.get("coordinator_run_id") != receipt["run_id"]
        or report.get("source_hour") != roots["raw"].rstrip("/") + "/" + parent
        or report.get("destination_hour")
        != roots["compacted"].rstrip("/") + "/" + parent
        or not isinstance(phase, dict)
        or set(phase) != {"directory", "last_sha256", "records", "migration_complete"}
        or phase["migration_complete"] is not False
        or type(phase["records"]) is not int
        or phase["records"] != 3
        or phase["directory"]
        != str(Path(settings["run_evidence_directory"]) / receipt["cli_run_id"])
    ):
        raise CoordinationError(
            "Retry requires the actual original failed-merge archive"
        )
    _hash(phase["last_sha256"])
    request = {
        "version": 1,
        "operation": "retry-preflight",
        "arguments": receipt["command"][5:],
        "execution_sha256": digest(receipt),
        "supervision_sha256": digest(writer),
        "intent_sha256": original["intent_sha256"],
        "ledger_id": ledger_id,
        "dataset_sha256": digest(binding),
        "archive_sha256": phase["last_sha256"],
        "archive_records": 3,
    }
    if canonical(proof["request"]) != canonical(request):
        raise CoordinationError("Retry request does not authenticate its original")
    data_binding = {
        "raw_root": roots["raw"],
        "compacted_root": roots["compacted"],
        "entity_root": "",
        "metrics_root": "",
        "namespace": "",
        "hosts": [],
        "identity_policy": "reject",
        "identity_max_ratio": 0.05,
        "window_start": receipt["hour_start"],
        "window_end": receipt["hour_end"],
        "event_types": settings["event_types"],
    }
    expected = {
        "version": 1,
        "operation": "retry-preflight",
        "status": "verified",
        "disposition": "unchanged-inputs-absent-output",
        "output_absent": True,
        "run_id": receipt["run_id"],
        "cli_run_id": receipt["cli_run_id"],
        "hour_start": receipt["hour_start"],
        "hour_end": receipt["hour_end"],
        "binding_sha256": digest(data_binding),
        "request_sha256": digest(request),
        **{
            k: v
            for k, v in request.items()
            if k not in {"arguments", "operation", "version"}
        },
    }
    result = proof["verification"]
    if (
        not isinstance(result, dict)
        or set(result)
        != set(expected) | {"source_rows", "source_files", "source_bytes"}
        or any(
            type(result[k]) is not type(v) or result[k] != v
            for k, v in expected.items()
        )
        or any(
            type(result[k]) is not int or not 0 <= result[k] < 2**63
            for k in ("source_rows", "source_files", "source_bytes")
        )
        or not 1 <= result["source_files"] <= settings["max_files"]
    ):
        raise CoordinationError("Invalid retained retry disposition/counts")
    observations = Path(proof["verifier_observations"])
    if (
        observations.name != "observations"
        or observations.parent.parent.name != "retry-verifications"
        or observations.parent.parent.parent
        != Path(receipt["observations_directory"]).parent
    ):
        raise CoordinationError(
            "Retry verifier evidence is outside the original execution"
        )
    identifier(observations.parent.name)
    _clean(
        proof["verifier"], receipt, _verifier_command(receipt, observations, request)
    )
    _capture(result, proof["verifier"], "stdout", receipt, canonical_json=True)


def validate_disposition(proof, original, new, binding, ledger_id):
    if isinstance(proof, dict) and proof.get("operation") == "interrupted-retry":
        return _validate_interrupted_disposition(
            proof, original, new, binding, ledger_id
        )
    if (
        not isinstance(proof, dict)
        or set(proof)
        != {
            "version",
            "operation",
            "committed_at",
            "reason",
            "confirmed_cannot_resume",
            "original_run_sha256",
            "retry_intent_sha256",
            "preflight",
            "original_report",
            "original_capture",
        }
        or type(proof["version"]) is not int
        or proof["version"] != 1
        or proof["operation"] != "intentional-retry"
        or proof["confirmed_cannot_resume"] is not True
        or original is None
        or new is None
        or len(canonical(proof).encode()) > MAX_RETRY_BYTES
        or proof["original_run_sha256"] != digest(original)
        or proof["retry_intent_sha256"] != new["intent_sha256"]
    ):
        raise CoordinationError("Invalid bounded original-bound retry disposition")
    _validate_block_reason(proof["reason"])
    _validate_block_reason(original["block_reason"])
    validate_preflight(
        proof["preflight"],
        original,
        binding,
        ledger_id,
        proof["original_report"],
        proof["original_capture"],
    )
    first = proof["preflight"]["receipt"]
    second = validate_receipt(json.loads(new["intent_json"])["execution"]["receipt"])
    if (
        len(
            {
                first["run_id"],
                first["cli_run_id"],
                second["run_id"],
                second["cli_run_id"],
            }
        )
        != 4
        or any(
            first[k] != second[k]
            for k in ("binding", "hour_start", "hour_end", "runtime", "settings", "cwd")
        )
        or proof["committed_at"] != new["started_at"]
        or timestamp(second["created_at"]) > timestamp(proof["committed_at"])
        or timestamp(proof["committed_at"])
        < timestamp(proof["preflight"]["observed_at"])
    ):
        raise CoordinationError(
            "Retry invocation changed original scope/clock/identities"
        )


def _budget(ledger, *, records=0, size=0):
    count, total, largest = ledger._connection.execute(
        "SELECT count(*),coalesce(sum(octet_length(encode(proof_json))),0),"
        "coalesce(max(octet_length(encode(proof_json))),0) FROM retry_dispositions"
    ).fetchone()
    if (
        count + records > MAX_RETRIES
        or total + size > MAX_RETRY_TOTAL_BYTES
        or largest > MAX_RETRY_BYTES
    ):
        raise CoordinationError("Retry history exceeds bounded budgets; never prune")


def validate_history(ledger):
    """Bounded retained history; disposed originals remain BLOCKED, not active owners."""
    _budget(ledger)
    origin = ledger._connection.execute(
        "SELECT identity,observed_at,details_json FROM events WHERE kind='retry_history_initialized' LIMIT 2"
    ).fetchall()
    try:
        if len(origin) != 1 or origin[0][0] != "dataset":
            raise ValueError
        timestamp(origin[0][1])
        details = json.loads(origin[0][2])
        if (
            set(details) != {"source_schema"}
            or type(details["source_schema"]) is not int
            or details["source_schema"] not in {0, 1, 2, 3}
            or canonical(details) != origin[0][2]
        ):
            raise ValueError
        count, previous, seen = 0, "", set()
        while True:
            rows = ledger._connection.execute(
                "SELECT * FROM retry_dispositions WHERE sequence>? ORDER BY sequence LIMIT 256",
                [count],
            ).fetchall()
            if not rows:
                break
            for sequence, original_id, new_id, payload, sha256, predecessor in rows:
                count += 1
                if sequence != count or predecessor != previous or new_id in seen:
                    raise ValueError
                identifier(original_id)
                identifier(new_id)
                proof = json.loads(payload)
                if canonical(proof) != payload or digest(proof) != sha256:
                    raise ValueError
                original, new = (
                    ledger.writer_run(original_id),
                    ledger.writer_run(new_id),
                )
                validate_disposition(
                    proof, original, new, json.loads(ledger.binding), ledger.ledger_id
                )
                audit = ledger._connection.execute(
                    "SELECT observed_at,details_json FROM events WHERE kind='writer_retried' AND identity=? LIMIT 2",
                    [original_id],
                ).fetchall()
                if audit != [
                    (
                        proof["committed_at"],
                        canonical({"proof_sha256": sha256, "retry_run_id": new_id}),
                    )
                ]:
                    raise ValueError
                unresolved = ledger._connection.execute(
                    "SELECT details_json FROM events WHERE kind='writer_unresolved' AND identity=? LIMIT 2",
                    [original_id],
                ).fetchall()
                if (
                    len(unresolved) != 1
                    or json.loads(unresolved[0][0]).get("reason")
                    != original["block_reason"]
                ):
                    raise ValueError
                if ledger.completion_evidence(original_id) is not None:
                    raise ValueError
                seen.add(original_id)
                previous = sha256
        if (
            ledger._connection.execute(
                "SELECT count(*) FROM events WHERE kind='writer_retried'"
            ).fetchone()[0]
            != count
        ):
            raise ValueError
    except (TypeError, ValueError, KeyError, RecursionError):
        raise CoordinationError(
            "Retry history/ownership/audit integrity mismatch"
        ) from None


def _prepare_retry(ledger, original, now):
    receipt = json.loads(canonical(original.receipt))
    receipt.update(run_id=uuid4().hex, cli_run_id=uuid4().hex, created_at=now)
    directory = ledger.directory / "executions" / receipt["run_id"]
    receipt["observations_directory"] = str(directory / "observations")
    receipt["command"] = command_for(receipt)
    validate_receipt(receipt)
    _mkdir(directory)
    path = directory / "execution.json"
    _publish(path, receipt)
    return PreparedExecution(
        path,
        digest(receipt),
        receipt,
        tuple(receipt["command"]),
        original.binding,
        original.credentials,
    )


def _recheck_preflight(ledger, original, proof, report):
    receipt = recheck_execution(original, ledger)
    observations = Path(proof["verifier_observations"])
    captured = _observations(
        Path(receipt["observations_directory"]),
        receipt,
        original.command,
        successful=False,
    )
    verified = _observations(
        observations,
        receipt,
        _verifier_command(receipt, observations, proof["request"]),
    )
    if (
        captured[0] != proof["writer_supervision"]
        or captured[2] != report
        or verified[0] != proof["verifier"]
        or verified[2] != proof["verification"]
        or _read(observations.parent / "request.json", MAX_RETRY_BYTES)
        != canonical(proof["request"]).encode()
    ):
        raise CoordinationError("Retry original/preflight evidence changed")
    _recheck_archive(receipt, proof["verification"])


def validate_completion_link(ledger, run_id, completion):
    """Retained ledger-only binding; ordinary open never reopens old runtime files."""
    if (
        ledger._connection.execute("SELECT schema_version FROM metadata").fetchone()[0]
        < 4
    ):
        if completion.get("retry") is not None:
            raise CoordinationError("Legacy completion cannot supply retry permission")
        return
    links = ledger._connection.execute(
        "SELECT proof_sha256 FROM retry_dispositions WHERE retry_run_id=? LIMIT 2",
        [run_id],
    ).fetchall()
    retry = completion.get("retry")
    if (not links and retry is not None) or (
        links and (retry is None or links != [(retry["disposition_sha256"],)])
    ):
        raise CoordinationError(
            "Completion does not bind its original retry disposition"
        )


def _recheck_retry_completion(ledger, prepared, result):
    """Authenticate exact original/new source identities for completion AND recovery.

    Only bounded local JSON is read here. The fresh standalone completion verifier
    authenticates current Parquet against the new migration intent; equality with
    the failed original then prevents changed-content runs being blessed by recovery.
    """
    rows = ledger._connection.execute(
        "SELECT original_run_id,proof_json,proof_sha256 FROM retry_dispositions WHERE retry_run_id=? LIMIT 2",
        [prepared.receipt["run_id"]],
    ).fetchall()
    if not rows:
        return None
    if len(rows) != 1:
        raise CoordinationError("Retry completion requires one original disposition")
    original_id, payload, sha256 = rows[0]
    disposition = json.loads(payload)
    if canonical(disposition) != payload or digest(disposition) != sha256:
        raise CoordinationError("Retry completion disposition changed")
    validate_disposition(
        disposition,
        ledger.writer_run(original_id),
        ledger.writer_run(prepared.receipt["run_id"]),
        prepared.binding,
        ledger.ledger_id,
    )
    execution = json.loads(ledger.writer_run(original_id)["intent_json"])["execution"]
    original = PreparedExecution(
        Path(execution["path"]),
        execution["sha256"],
        execution["receipt"],
        tuple(execution["receipt"]["command"]),
        prepared.binding,
        prepared.credentials,
    )
    # The disposition binds unchanged storage scope. Reuse the already frozen
    # ephemeral credentials, never resolve ambient SDK/profile settings again.
    recheck_execution(original, ledger)
    if disposition["operation"] == "interrupted-retry":
        raw = _read(prepared.path.parent / "retry-prelaunch.json", MAX_RETRY_BYTES)
        prelaunch = json.loads(raw)
        if (
            raw != canonical(prelaunch).encode()
            or set(prelaunch)
            != {
                "version",
                "operation",
                "run_id",
                "execution_sha256",
                "disposition_sha256",
                "preflight",
            }
            or prelaunch["version"] != 1
            or prelaunch["operation"] != "interrupted-retry-prelaunch"
            or prelaunch["run_id"] != prepared.receipt["run_id"]
            or prelaunch["execution_sha256"] != prepared.sha256
            or prelaunch["disposition_sha256"] != sha256
            or timestamp(prelaunch["preflight"]["observed_at"])
            < timestamp(disposition["committed_at"])
            or prelaunch["preflight"]["authority"]["observations_directory"]
            == disposition["preflight"]["authority"]["observations_directory"]
        ):
            raise CoordinationError("Interrupted retry prelaunch evidence changed")
        validate_interrupted_preflight(
            prelaunch["preflight"],
            ledger.writer_run(original_id),
            prepared.binding,
            ledger.ledger_id,
        )
        archive = _interrupted_archive(original.receipt)
        supervision, supervision_sha256 = _interrupted_supervision(original.receipt)
        if (
            archive["archive_sha256"] != disposition["preflight"]["archive_sha256"]
            or archive["archive_records"] != disposition["preflight"]["archive_records"]
            or supervision != disposition["preflight"]["writer_supervision"]
            or supervision_sha256
            != digest(disposition["preflight"]["writer_supervision"])
        ):
            raise CoordinationError("Interrupted retry original evidence changed")
        if (
            _read(prepared.path.parent / "retry-disposition.json", MAX_RETRY_BYTES)
            != payload.encode()
        ):
            raise CoordinationError("Interrupted retry disposition receipt changed")
        return {
            "kind": "interrupted-prepublication",
            "disposition_sha256": sha256,
            "prelaunch_sha256": digest(prelaunch),
            "preflight_sha256": digest(prelaunch["preflight"]),
        }
    report = disposition["original_report"]
    _recheck_preflight(ledger, original, disposition["preflight"], report)
    if (
        _read(prepared.path.parent / "retry-disposition.json", MAX_RETRY_BYTES)
        != payload.encode()
    ):
        raise CoordinationError("Retry disposition receipt changed before completion")
    raw = _read(prepared.path.parent / "retry-prelaunch.json", MAX_RETRY_BYTES)
    prelaunch = json.loads(raw)
    if (
        raw != canonical(prelaunch).encode()
        or not isinstance(prelaunch, dict)
        or set(prelaunch)
        != {
            "version",
            "operation",
            "run_id",
            "execution_sha256",
            "disposition_sha256",
            "preflight",
        }
        or type(prelaunch["version"]) is not int
        or prelaunch["version"] != 1
        or prelaunch["operation"] != "intentional-retry-prelaunch"
        or prelaunch["run_id"] != prepared.receipt["run_id"]
        or prelaunch["execution_sha256"] != prepared.sha256
        or prelaunch["disposition_sha256"] != sha256
        or timestamp(prelaunch["preflight"]["observed_at"])
        < timestamp(disposition["committed_at"])
        or prelaunch["preflight"]["verifier_observations"]
        == disposition["preflight"]["verifier_observations"]
    ):
        raise CoordinationError("Retry requires its distinct fresh prelaunch evidence")
    validate_preflight(
        prelaunch["preflight"],
        ledger.writer_run(original_id),
        prepared.binding,
        ledger.ledger_id,
        report,
        disposition["original_capture"],
    )
    _recheck_preflight(ledger, original, prelaunch["preflight"], report)
    first = _recheck_archive(original.receipt, disposition["preflight"]["verification"])
    second = _recheck_archive(prepared.receipt, result)
    totals = _original_source_totals(
        original.receipt, disposition["preflight"]["verification"]
    )
    if (
        len(first) != 1
        or first != second
        or result["raw_source_rows"] != totals["source_rows"]
        or result["raw_output_rows"] != totals["source_rows"]
    ):
        raise CoordinationError(
            "Retry migration inputs differ from the failed original"
        )
    return {
        "disposition_sha256": sha256,
        "prelaunch_sha256": digest(prelaunch),
        "original_migration_sha256": first[0],
        "retry_migration_sha256": second[0],
    }


def retry_and_complete_prepared(
    ledger, original, now, *, clock, confirmed=False, reason=None
):
    """Explicit fresh retry and independent completion, never resume a committed retry."""
    from .maintenance import no_pending
    from .recovery import _confirmation

    _confirmation(confirmed, reason)
    if not callable(clock):
        raise CoordinationError("Intentional retry requires a fresh evolving clock")
    observed = _Clock(now, clock)
    prepared = None
    ledger._completion_live_run = None
    try:
        ledger._validate()
        no_pending(ledger)
        receipt = recheck_execution(original, ledger)
        state = ledger.snapshot()
        run = ledger.writer_run(receipt["run_id"])
        now = observed.observe()
        floor = ledger._completion_clock_floor()
        if floor is not None and timestamp(now) < timestamp(floor):
            raise CoordinationError("Retry clock predates retained completion")
        preflight = verify_retry_prepared(ledger, original, now)
        prepared = _prepare_retry(ledger, original, observed.observe())
        new_receipt = recheck_execution(prepared, ledger)
        environment = execution_environment(prepared)
        report = _observations(
            Path(receipt["observations_directory"]),
            receipt,
            original.command,
            successful=False,
        )[2]
        now = observed.observe()
        if not writer_due(
            receipt["hour_start"],
            now,
            grace_minutes=receipt["settings"]["grace_minutes"],
        ):
            raise CoordinationError("Retry selected hour is not due")
        new_run = {
            k: new_receipt[k]
            for k in (
                "run_id",
                "cli_run_id",
                "hour_start",
                "hour_end",
                "command",
                "binding",
            )
        }
        new_run.update(
            started_at=now,
            execution={
                "path": str(prepared.path),
                "sha256": prepared.sha256,
                "receipt": new_receipt,
            },
        )
        proof = {
            "version": 1,
            "operation": "intentional-retry",
            "committed_at": now,
            "reason": reason,
            "confirmed_cannot_resume": True,
            "original_run_sha256": digest(run),
            "retry_intent_sha256": digest(new_run),
            "preflight": preflight,
            "original_report": report,
            "original_capture": _read(
                Path(receipt["observations_directory"])
                / (
                    "stdout.log"
                    if preflight["writer_supervision"]["stdout"]["bytes_saved"]
                    else "stderr.log"
                ),
                receipt["settings"]["max_report_bytes"],
            ).decode(),
        }
        _budget(ledger, records=1, size=len(canonical(proof).encode()))
        _recheck_preflight(ledger, original, preflight, report)
        if ledger.snapshot() != state or ledger.writer_run(receipt["run_id"]) != run:
            raise CoordinationError("Retry original ownership changed before COMMIT")
        path = prepared.path.parent / "retry-disposition.json"
        _publish(
            path, proof
        )  # Both immutable receipts precede the single intent COMMIT.
        with ledger._transaction(kind="retry"):
            row = ledger._connection.execute(
                "SELECT sequence,proof_sha256 FROM retry_dispositions ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            sequence, previous = (row[0] + 1, row[1]) if row else (1, "")
            ledger._connection.execute(
                "INSERT INTO compaction_runs VALUES (?,?,?,?,'INTENT',?,?,?,'')",
                [
                    new_run["run_id"],
                    new_run["cli_run_id"],
                    new_run["hour_start"],
                    new_run["hour_end"],
                    now,
                    canonical(new_run),
                    digest(new_run),
                ],
            )
            ledger._connection.execute(
                "INSERT INTO retry_dispositions VALUES (?,?,?,?,?,?)",
                [
                    sequence,
                    run["run_id"],
                    new_run["run_id"],
                    canonical(proof),
                    digest(proof),
                    previous,
                ],
            )
            ledger._connection.execute(
                "UPDATE dataset_state SET mode='WRITING',active_run_id=?,block_reason='' WHERE singleton=1",
                [new_run["run_id"]],
            )
            ledger._event(
                "writer_intent",
                new_run["run_id"],
                now,
                {
                    "cli_run_id": new_run["cli_run_id"],
                    "hour_start": new_run["hour_start"],
                },
            )
            ledger._event(
                "writer_retried",
                run["run_id"],
                now,
                {"proof_sha256": digest(proof), "retry_run_id": new_run["run_id"]},
            )
            ledger._validate()
        ledger.failure_hook("before_retry_launch")
        if _read(path, MAX_RETRY_BYTES) != canonical(proof).encode():
            raise CoordinationError("Committed retry disposition bytes changed")
        _recheck_preflight(ledger, original, preflight, report)
        fresh = _verify_retry_prepared(
            ledger, original, observed.observe(), retry_run_id=new_run["run_id"]
        )
        prelaunch = {
            "version": 1,
            "operation": "intentional-retry-prelaunch",
            "run_id": new_run["run_id"],
            "execution_sha256": prepared.sha256,
            "disposition_sha256": digest(proof),
            "preflight": fresh,
        }
        if len(canonical(prelaunch).encode()) > MAX_RETRY_BYTES:
            raise CoordinationError("Retry prelaunch evidence exceeds byte budget")
        _publish(prepared.path.parent / "retry-prelaunch.json", prelaunch)
        _launch_prepared(ledger, prepared, environment)
        ledger.failure_hook("after_retry_launch")
        _recheck_preflight(ledger, original, preflight, report)
        ledger._completion_live_run = new_run["run_id"]
        return _complete_live_prepared(ledger, prepared, observed)
    except BaseException as error:
        ledger._completion_live_run = None
        ledger._authority_checked = None
        state = ledger.snapshot()
        if (
            prepared is not None
            and state["mode"] == "WRITING"
            and state["active_run_id"] == prepared.receipt["run_id"]
        ):
            ledger.block_writer(
                state["active_run_id"],
                observed.last,
                "Intentional retry uncertain; preserve original disposition and require audited recovery",
            )
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        raise CoordinationError(
            "Intentional retry refused/uncertain; details suppressed"
        ) from None
    finally:
        ledger._completion_live_run = None


def retry_interrupted_and_complete_prepared(
    ledger, original, now, *, clock, confirmed=False, reason=None
):
    """Retry one entity/deleting tick interrupted before publication evidence.

    This is deliberately narrower than a general retry: the original process
    must have been supervised to an empty SIGTERM interruption and its archive
    must contain only run/entity intent.  A fresh authority observation is
    required before the new owner is committed; the original owner remains
    retained as blocked history.
    """
    from .maintenance import no_pending
    from .recovery import _confirmation

    _confirmation(confirmed, reason)
    if not callable(clock):
        raise CoordinationError("Interrupted retry requires a fresh evolving clock")
    observed = _Clock(now, clock)
    prepared = None
    ledger._completion_live_run = None
    try:
        ledger._validate()
        no_pending(ledger)
        receipt = recheck_execution(original, ledger)
        state = ledger.snapshot()
        run = ledger.writer_run(receipt["run_id"])
        current = observed.observe()
        floor = ledger._completion_clock_floor()
        if floor is not None and timestamp(current) < timestamp(floor):
            raise CoordinationError("Interrupted retry clock predates completion")
        preflight = verify_interrupted_prepared(ledger, original, current)
        prepared = _prepare_retry(ledger, original, observed.observe())
        new_receipt = recheck_execution(prepared, ledger)
        environment = execution_environment(prepared)
        current = observed.observe()
        if not writer_due(
            receipt["hour_start"],
            current,
            grace_minutes=receipt["settings"]["grace_minutes"],
        ):
            raise CoordinationError("Interrupted retry selected hour is not due")
        new_run = {
            key: new_receipt[key]
            for key in (
                "run_id",
                "cli_run_id",
                "hour_start",
                "hour_end",
                "command",
                "binding",
            )
        }
        new_run.update(
            started_at=current,
            execution={
                "path": str(prepared.path),
                "sha256": prepared.sha256,
                "receipt": new_receipt,
            },
        )
        proof = {
            "version": 1,
            "operation": "interrupted-retry",
            "committed_at": current,
            "reason": reason,
            "confirmed_cannot_resume": True,
            "original_run_sha256": digest(run),
            "retry_intent_sha256": digest(new_run),
            "preflight": preflight,
        }
        _budget(ledger, records=1, size=len(canonical(proof).encode()))
        if ledger.snapshot() != state or ledger.writer_run(receipt["run_id"]) != run:
            raise CoordinationError("Interrupted retry owner changed before COMMIT")
        path = prepared.path.parent / "retry-disposition.json"
        _publish(path, proof)
        with ledger._transaction(kind="retry"):
            row = ledger._connection.execute(
                "SELECT sequence,proof_sha256 FROM retry_dispositions "
                "ORDER BY sequence DESC LIMIT 1"
            ).fetchone()
            sequence, previous = (row[0] + 1, row[1]) if row else (1, "")
            ledger._connection.execute(
                "INSERT INTO compaction_runs VALUES (?,?,?,?,'INTENT',?,?,?,'')",
                [
                    new_run["run_id"],
                    new_run["cli_run_id"],
                    new_run["hour_start"],
                    new_run["hour_end"],
                    current,
                    canonical(new_run),
                    digest(new_run),
                ],
            )
            ledger._connection.execute(
                "INSERT INTO retry_dispositions VALUES (?,?,?,?,?,?)",
                [
                    sequence,
                    run["run_id"],
                    new_run["run_id"],
                    canonical(proof),
                    digest(proof),
                    previous,
                ],
            )
            ledger._connection.execute(
                "UPDATE dataset_state SET mode='WRITING',active_run_id=?,"
                "block_reason='' WHERE singleton=1",
                [new_run["run_id"]],
            )
            ledger._event(
                "writer_intent",
                new_run["run_id"],
                current,
                {
                    "cli_run_id": new_run["cli_run_id"],
                    "hour_start": new_run["hour_start"],
                },
            )
            ledger._event(
                "writer_retried",
                run["run_id"],
                current,
                {"proof_sha256": digest(proof), "retry_run_id": new_run["run_id"]},
            )
            ledger._validate()
        ledger.failure_hook("before_retry_launch")
        if _read(path, MAX_RETRY_BYTES) != canonical(proof).encode():
            raise CoordinationError("Interrupted retry disposition bytes changed")
        fresh = _verify_interrupted_prepared(
            ledger, original, observed.observe(), retry_run_id=new_run["run_id"]
        )
        prelaunch = {
            "version": 1,
            "operation": "interrupted-retry-prelaunch",
            "run_id": new_run["run_id"],
            "execution_sha256": prepared.sha256,
            "disposition_sha256": digest(proof),
            "preflight": fresh,
        }
        if len(canonical(prelaunch).encode()) > MAX_RETRY_BYTES:
            raise CoordinationError("Interrupted retry prelaunch exceeds byte budget")
        _publish(prepared.path.parent / "retry-prelaunch.json", prelaunch)
        _launch_prepared(ledger, prepared, environment)
        ledger.failure_hook("after_retry_launch")
        ledger._completion_live_run = new_run["run_id"]
        return _complete_live_prepared(ledger, prepared, observed)
    except BaseException as error:
        ledger._completion_live_run = None
        ledger._authority_checked = None
        state = ledger.snapshot()
        if (
            prepared is not None
            and state["mode"] == "WRITING"
            and state["active_run_id"] == prepared.receipt["run_id"]
        ):
            ledger.block_writer(
                state["active_run_id"],
                observed.last,
                "Interrupted retry uncertain; preserve original and new evidence",
            )
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        raise CoordinationError(
            "Interrupted retry refused/uncertain; details suppressed"
        ) from None
    finally:
        ledger._completion_live_run = None
