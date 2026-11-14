"""Read-only standalone data evidence, never permission or a shutdown certificate.

The locked main-runtime adapter must authenticate execution and supervision and
retain the returned evidence atomically with its cursor/high-water transition.
This module never imports or opens the coordination ledger. Trusted local code,
cooperative exclusion and original phase records remain prerequisites.
"""

import argparse
import hashlib
import json
import re
import sys
from contextlib import contextmanager
from tempfile import TemporaryDirectory
from time import monotonic

import duckdb

from .cli import _verify_entity_state
from .cli import parser as compaction_parser
from .config import CompactionError, Dataset, Limits, hour_window, separate_roots
from .engine import connection
from .entities import EntityLimits, host_name
from .entity_authority import Authority
from .entity_authority import binding as authority_binding
from .entity_store import EntityStore
from .inventory import Store, discover, recheck
from .phase_evidence import (
    canonical,
    identity,
    load_archive,
    local_directory,
    read_local_record,
    run_identity,
    verify_migrations,
)
from .processing_metrics import (
    ProcessingMetricsStore,
    _report_hour,
    _sum_partitions,
    validate_checkpoint_receipts,
)
from .storage import S3Settings, open_s3

MAX_COMPLETION_BYTES = 64 * 1024
MAX_REQUEST_BYTES = 1024 * 1024


def _digest(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value):
        raise CompactionError("Completion context requires exact digest identities")


def _anchor(value, expected_binding):
    if (
        not isinstance(value, dict)
        or set(value) != {"generation", "generation_id", "binding_sha256"}
        or type(value["generation"]) is not int
        or not 0 < value["generation"] < 2**63
        or value["binding_sha256"] != identity(expected_binding)
    ):
        raise CompactionError("Explicit verified entity high-water baseline required")
    _digest(value["generation_id"])


@contextmanager
def _opened(args, client):
    if duckdb.__version__ != "2.0.0":
        raise CompactionError(
            "Completion verification requires standalone DuckDB 2.0.0"
        )
    if args.command != "compact":
        raise CompactionError("Completion verifier supports original compact runs only")
    limits = Limits(
        threads=args.threads,
        memory_limit=args.memory_limit,
        max_temp_size=args.max_temp_size,
        temp_directory=args.temp_directory,
        timeout=args.timeout,
        max_files=args.max_files,
        max_hours=args.max_hours,
    )
    hours = hour_window(args.start_hour, args.end_hour, max_hours=1)
    source = Dataset.parse(args.source_root, event_root=True)
    destination = Dataset.parse(args.destination_root, event_root=True)
    separate_roots(source, destination)
    events = tuple(args.event_type)
    if not events or list(events) != sorted(set(events)):
        raise CompactionError(
            "Completion requires a canonical explicit event allowlist"
        )
    entity_root = (
        Dataset.parse(args.process_entity_root) if args.process_entity_root else None
    )
    metrics_root = None
    if entity_root:
        if (
            not args.entity_namespace
            or not args.entity_host
            or "raw_process" not in events
        ):
            raise CompactionError("Completion entity scope is incomplete")
        metrics_root = (
            Dataset.parse(args.processing_metrics_root)
            if args.processing_metrics_root
            else source.sibling("processing_metrics")
        )
        for root in (source, destination):
            separate_roots(root, entity_root)
            separate_roots(root, metrics_root)
        separate_roots(entity_root, metrics_root)
    elif args.entity_host or args.entity_namespace:
        raise CompactionError("Inactive completion entities must be explicit")
    roots = [source, destination] + ([entity_root, metrics_root] if entity_root else [])
    remote = any(root.bucket for root in roots)
    if remote and (
        args.profile is not None or not args.region or not args.endpoint_url
    ):
        raise CompactionError("Completion storage requires previously resolved scope")
    expected_binding = {
        "raw_root": source.root,
        "compacted_root": destination.root,
        "entity_root": entity_root.root if entity_root else "",
        "metrics_root": metrics_root.root if metrics_root else "",
        "namespace": args.entity_namespace or "",
        "hosts": sorted({host_name(h) for h in args.entity_host}),
        "identity_policy": args.unknown_identity_policy,
        "identity_max_ratio": args.unknown_identity_max_ratio,
        "window_start": args.start_hour,
        "window_end": args.end_hour,
        "event_types": list(events),
    }
    deadline = monotonic() + limits.timeout
    with connection(limits) as conn:
        if remote and client is None:
            client = open_s3(
                conn,
                S3Settings(
                    None, args.region, args.endpoint_url, args.extension_directory
                ),
            )
        source_store, destination_store = (
            Store(root, client, deadline=deadline) for root in roots[:2]
        )
        entities = (
            EntityStore(
                Store(entity_root, client, deadline=deadline),
                args.entity_namespace,
                EntityLimits(args.max_entity_rows, args.max_entity_bytes),
            )
            if entity_root
            else None
        )
        metrics = (
            ProcessingMetricsStore(Store(metrics_root, client, deadline=deadline))
            if metrics_root
            else None
        )
        yield (
            conn,
            limits,
            hours,
            events,
            source_store,
            destination_store,
            entities,
            metrics,
            expected_binding,
        )


def _authority(conn, source, destination, entities, args, metrics, high_water=None):
    checked = _verify_entity_state(
        conn, source, destination, entities, args, metrics.store.dataset
    )
    expected = authority_binding(
        source,
        destination,
        entities,
        args.entity_host,
        args.unknown_identity_policy,
        args.unknown_identity_max_ratio,
        metrics.store.dataset.root,
    )
    with TemporaryDirectory(prefix="completion-authority-") as directory:
        authority = Authority(conn, entities, directory, expected)
        latest = authority.latest
        if (
            not latest
            or latest["status"] != "committed"
            or latest["generation_id"] != checked["generation_id"]
            or authority.identity != checked["authority_sha256"]
        ):
            raise CompactionError("Completion authority changed or is unresolved")
        candidate = {
            "generation": latest["generation"],
            "generation_id": latest["generation_id"],
            "binding_sha256": identity(expected),
        }
        if high_water is not None:
            _anchor(high_water, expected)
            if (
                conn.execute(
                    "SELECT count(*) FROM entity_authority WHERE generation=? "
                    "AND generation_id=? AND status='committed'",
                    [high_water["generation"], high_water["generation_id"]],
                ).fetchone()[0]
                != 1
            ):
                raise CompactionError(
                    "Entity authority rolled back or forked beyond retained high-water"
                )
        return candidate, checked


def verify_authority(args, *, client=None, authority_high_water=None):
    """Return verified evidence, not ledger initialization authority."""
    with _opened(args, client) as opened:
        conn, _, _, _, source, destination, entities, metrics, _ = opened
        if entities is None:
            raise CompactionError(
                "Explicit entity scope required for authority initialization"
            )
        current = _authority(
            conn, source, destination, entities, args, metrics, authority_high_water
        )
        if (
            _authority(conn, source, destination, entities, args, metrics, current[0])
            != current
        ):
            raise CompactionError("Authority initialization evidence changed")
        return current[0]


def _grammar(records, args, expected_binding):
    run_identity(args.run_id)
    run_identity(args.coordinator_run_id)
    if args.run_id == args.coordinator_run_id or any(
        r["coordinator_run_id"] != args.coordinator_run_id for r in records
    ):
        raise CompactionError("Completion archive run identity mismatch")
    if canonical(records[0]["data"]) != canonical(
        {"binding": expected_binding, "command": "compact"}
    ):
        raise CompactionError("Completion archive intent scope mismatch")
    raws = [r for r in records if r["phase"] == "raw_complete"]
    if len(raws) != 1:
        raise CompactionError("Completion raw phase missing or duplicated")
    raw = raws[0]["data"]
    if (
        raw.get("command") != "compact"
        or raw.get("duckdb_version") != "2.0.0"
        or raw.get("source_root") != expected_binding["raw_root"]
        or raw.get("destination_root") != expected_binding["compacted_root"]
        or type(raw.get("delete_source")) is not bool
        or raw["delete_source"] != args.delete_source
        or raw.get("event_types") != args.event_type
    ):
        raise CompactionError("Completion raw execution scope mismatch")
    for partition in raw.get("partitions", []):
        if (
            type(partition.get("passed")) is not bool
            or partition["passed"] is not True
            or any(
                type(partition.get(k)) is not int or partition[k] < 0
                for k in ("source_rows", "merged_rows", "source_files", "source_bytes")
            )
        ):
            raise CompactionError(
                "Completion migration counts/validation must have exact types"
            )
    expected = ["run_intent"]
    if args.process_entity_root:
        expected += ["entities_intent", "entities_verified"]
        if records[1]["data"] != {"operation": "compact"}:
            raise CompactionError("Completion entity operation intent mismatch")
    items = [h for h in raw.get("hours", []) if h.get("status") != "empty"]
    for item in items:
        parent = {k: item[k] for k in ("event_type", "dayPK", "hourPK")}
        before = [
            r
            for r in records
            if r["phase"] == "migration_intent" and r["data"].get("parent") == parent
        ]
        after = [
            r
            for r in records
            if r["phase"] == "migration_verified" and r["data"].get("parent") == parent
        ]
        if len(before) != 1 or len(after) != 1:
            raise CompactionError(
                "Original migration intent/evidence missing or duplicated"
            )
        migrated = after[0]["data"]
        for partition in migrated["partitions"]:
            if (
                type(partition.get("passed")) is not bool
                or partition["passed"] is not True
                or any(
                    type(partition.get(k)) is not int or partition[k] < 0
                    for k in (
                        "source_rows",
                        "merged_rows",
                        "source_files",
                        "source_bytes",
                    )
                )
            ):
                raise CompactionError(
                    "Completion migration counts/validation must have exact types"
                )
        partitions = [
            {
                k: p[k]
                for k in ("event_type", "dayPK", "hourPK", "schema", "source_rows")
            }
            | ({"protoPK": p["protoPK"]} if "protoPK" in p else {})
            for p in migrated["partitions"]
        ]
        if canonical(before[0]["data"]) != canonical(
            {"parent": parent, "source": migrated["source"], "partitions": partitions}
        ):
            raise CompactionError(
                "Original migration input/schema/count intent mismatch"
            )
        expected += ["migration_intent", "migration_verified"]
        if args.delete_source:
            deletes = [
                r
                for r in records
                if r["phase"] == "deletion_intent" and r["data"].get("parent") == parent
            ]
            if len(deletes) != 1 or deletes[0]["data"] != {"parent": parent}:
                raise CompactionError(
                    "Original deletion intent/source evidence mismatch"
                )
    if args.delete_source:
        expected += [p for _ in items for p in ("deletion_intent", "deletion_verified")]
    expected += ["raw_complete"]
    if args.process_entity_root:
        expected += ["checkpoint_intent", "checkpoint_verified"]
    expected += ["complete"]
    if [r["phase"] for r in records] != expected:
        raise CompactionError("Completion archive phase order/scope is incomplete")
    if canonical(records[-1]["data"]) != canonical(
        {
            "status": "completed" if items else "empty",
            "exit_code": 0,
            "migration_complete": True,
        }
    ):
        raise CompactionError("Completion archive terminal observation is not clean")
    return raw


def _checkpoint(conn, records, args, hours, expected_binding, metrics, raw):
    candidates = next(r["data"] for r in records if r["phase"] == "checkpoint_intent")
    if set(candidates) != {"rows"} or not isinstance(candidates["rows"], list):
        raise CompactionError("Original checkpoint candidates missing")
    rows = candidates["rows"]
    validate_checkpoint_receipts(conn, rows, hours, expected_binding)
    row = rows[0]
    generation = next(
        r["data"] for r in records if r["phase"] == "entities_verified"
    ).get("generation_id")
    statuses = {h["status"] for h in _report_hour(raw, hours[0])}
    if (
        row["run_id"] != args.run_id
        or row["operation"] != "compact"
        or row["entity_generation_id"] != generation
        or row["status"] != "completed"
        or row["raw_status"]
        != ("merged" if statuses & {"merged", "deleted"} else "empty")
        or row["deletion_failures"] != 0
        or row["deletion_status"]
        != ("completed" if args.delete_source else "not_requested")
        or row["raw_source_rows"] != _sum_partitions(raw, hours[0], "source_rows")
        or row["raw_output_rows"] != _sum_partitions(raw, hours[0], "merged_rows")
    ):
        raise CompactionError(
            "Original checkpoint migration/generation evidence mismatch"
        )
    observation = next(
        r["data"] for r in records if r["phase"] == "checkpoint_verified"
    )
    if (
        observation != {"hour_start": args.start_hour, "row": row}
        or metrics.read(conn, hours[0]) != row
    ):
        raise CompactionError(
            "Exact original selected-hour checkpoint is absent or changed"
        )
    return row


def verify_completion(
    args, *, execution_sha256, supervision_sha256, authority_high_water, client=None
):
    """Re-read data; execution/shutdown hashes are authenticated by the adapter."""
    _digest(execution_sha256)
    _digest(supervision_sha256)
    try:
        with _opened(args, client) as opened:
            (
                conn,
                limits,
                hours,
                events,
                source,
                destination,
                entities,
                metrics,
                expected_binding,
            ) = opened
            if entities is not None and authority_high_water is None:
                raise CompactionError(
                    "Explicit verified entity high-water baseline required"
                )
            if entities is None and authority_high_water is not None:
                raise CompactionError("Inactive entity high-water must be explicit")
            archive_root = local_directory(args.run_evidence_directory)
            archive_dataset = Dataset.parse(str(archive_root))
            for root in (
                source.dataset,
                destination.dataset,
                entities.store.dataset if entities else None,
                metrics.store.dataset if metrics else None,
            ):
                if root is not None:
                    separate_roots(archive_dataset, root)
            records = load_archive(str(archive_root), args.run_id)
            _grammar(records, args, expected_binding)
            # Aggregate selected-event file ceilings precede per-parent verification.
            inputs = discover(source, hours, events, max_files=limits.max_files)
            outputs = discover(destination, hours, events, max_files=limits.max_files)
            raw = verify_migrations(
                conn, records, source, destination, hours, events, limits
            )
            current, checked, checkpoint = None, None, None
            if entities is not None:
                current, checked = _authority(
                    conn,
                    source,
                    destination,
                    entities,
                    args,
                    metrics,
                    authority_high_water,
                )
                checkpoint = _checkpoint(
                    conn, records, args, hours, expected_binding, metrics, raw
                )
            verify_migrations(conn, records, source, destination, hours, events, limits)
            recheck(source, inputs, hours, max_files=limits.max_files)
            recheck(destination, outputs, hours, max_files=limits.max_files)
            if entities is not None:
                if _authority(
                    conn, source, destination, entities, args, metrics, current
                ) != (current, checked):
                    raise CompactionError(
                        "Entity authority changed during exclusive verification"
                    )
                if metrics.read(conn, hours[0]) != checkpoint:
                    raise CompactionError(
                        "Checkpoint changed during exclusive verification"
                    )
            if load_archive(str(archive_root), args.run_id) != records:
                raise CompactionError("Original archive changed during verification")
            result = {
                "version": 1,
                "status": "verified",
                "run_id": args.coordinator_run_id,
                "cli_run_id": args.run_id,
                "hour_start": args.start_hour,
                "hour_end": args.end_hour,
                "binding_sha256": identity(expected_binding),
                "execution_sha256": execution_sha256,
                "supervision_sha256": supervision_sha256,
                "archive_sha256": identity(records[-1]),
                "archive_records": len(records),
                "authority_high_water": current,
                "checkpoint_sha256": identity(checkpoint) if checkpoint else None,
                "checkpoint_generation_id": checkpoint["entity_generation_id"]
                if checkpoint
                else None,
                "raw_source_rows": _sum_partitions(raw, hours[0], "source_rows"),
                "raw_output_rows": _sum_partitions(raw, hours[0], "merged_rows"),
            }
            if len(canonical(result).encode()) > MAX_COMPLETION_BYTES:
                raise CompactionError("Completion evidence exceeds byte budget")
            return result
    except CompactionError:
        raise
    except Exception:
        raise CompactionError(
            "Independent completion verification failed; details suppressed"
        ) from None


def _arguments(arguments):
    if (
        not isinstance(arguments, list)
        or not 1 <= len(arguments) <= 256
        or any(
            not isinstance(arg, str) or not arg or len(arg) > 4096 or "\0" in arg
            for arg in arguments
        )
        or any(arg in {"-h", "--help"} for arg in arguments)
    ):
        raise CompactionError("Verification request arguments are invalid")
    selected = compaction_parser()

    def refuse(message):
        raise CompactionError("Verification request arguments are invalid")

    selected.error = refuse
    for action in selected._actions:
        if isinstance(action, argparse._SubParsersAction):
            for child in action.choices.values():
                child.error = refuse
    return selected.parse_args(arguments)


def _authority_request(path, expected_sha256):
    _digest(expected_sha256)
    payload = read_local_record(path, MAX_REQUEST_BYTES)
    try:
        value = json.loads(payload)
        if (
            hashlib.sha256(payload).hexdigest() != expected_sha256
            or canonical(value).encode() != payload
            or set(value)
            != {
                "version",
                "operation",
                "arguments",
                "execution_sha256",
                "ledger_id",
                "dataset_sha256",
                "authority_high_water",
            }
            or type(value["version"]) is not int
            or value["version"] != 2
            or value["operation"] != "authority"
        ):
            raise ValueError
        run_identity(value["ledger_id"])
        for field in ("execution_sha256", "dataset_sha256"):
            _digest(value[field])
        return value, _arguments(value["arguments"])
    except Exception:
        raise CompactionError(
            "Malformed authority request; details suppressed"
        ) from None


def _request(path, expected_sha256):
    _digest(expected_sha256)
    payload = read_local_record(path, MAX_REQUEST_BYTES)
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise CompactionError("Completion request checksum mismatch")
    try:
        value = json.loads(payload)
        if (
            canonical(value).encode() != payload
            or set(value)
            != {
                "version",
                "arguments",
                "execution_sha256",
                "supervision_sha256",
                "intent_sha256",
                "authority_high_water",
            }
            or type(value["version"]) is not int
            or value["version"] != 1
        ):
            raise ValueError
        for field in ("execution_sha256", "supervision_sha256", "intent_sha256"):
            _digest(value[field])
        return value, _arguments(value["arguments"])
    except CompactionError:
        raise
    except Exception:
        raise CompactionError(
            "Malformed completion request; details suppressed"
        ) from None


def main(argv=None):
    """Separate read-only invocation; no direct ledger or permission interface."""
    outer = argparse.ArgumentParser(description=__doc__)
    outer.add_argument("--request", required=True)
    outer.add_argument("--request-sha256", required=True)
    outer.add_argument("--authority-only", action="store_true")
    selected = outer.parse_args(argv)
    try:
        reader = _authority_request if selected.authority_only else _request
        request, args = reader(selected.request, selected.request_sha256)
        if selected.authority_only:
            result = {
                "version": 2,
                "operation": "authority",
                "status": "verified",
                "execution_sha256": request["execution_sha256"],
                "ledger_id": request["ledger_id"],
                "dataset_sha256": request["dataset_sha256"],
                "authority_high_water": verify_authority(
                    args, authority_high_water=request["authority_high_water"]
                ),
                "request_sha256": selected.request_sha256,
            }
        else:
            result = verify_completion(
                args,
                execution_sha256=request["execution_sha256"],
                supervision_sha256=request["supervision_sha256"],
                authority_high_water=request["authority_high_water"],
            )
            result.update(
                request_sha256=selected.request_sha256,
                intent_sha256=request["intent_sha256"],
            )
        if reader(selected.request, selected.request_sha256)[0] != request:
            raise CompactionError("Completion request changed during verification")
        print(canonical(result))
        return 0
    except Exception:
        print(
            canonical(
                {
                    "status": "error",
                    "message": (
                        "Independent completion verification failed; details suppressed"
                    ),
                }
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
