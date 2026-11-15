"""Read-only preflight for one raw-only failed merge; never authorize a retry.

The original archive stays failed. All original inputs must still match and the
selected output hour must be absent. Entities, deletion, multiple events/hours and
missing migration intent are deliberately outside this narrow prerequisite.
Cooperative exclusive access and separately authenticated shutdown remain required.
"""

import argparse
import hashlib
import json
import sys

from .completion import MAX_REQUEST_BYTES, _arguments, _digest, _opened
from .config import CompactionError, Dataset, Leaf, separate_roots
from .deletion_recovery import _files, prefix_inventory
from .engine import check_hive_fields, count_rows, schema
from .inventory import discover, recheck
from .phase_evidence import (
    canonical,
    identity,
    inventory_evidence,
    load_archive,
    local_directory,
    read_local_record,
    run_identity,
)

MAX_RESULT_BYTES = 64 * 1024


def verify_retry(args, *, client=None):
    """Return current disposition evidence only; no dataset writes or ledger access."""
    try:
        if (
            args.process_entity_root
            or args.entity_host
            or args.entity_namespace
            or args.delete_source is not False
            or len(args.event_type) != 1
        ):
            raise CompactionError(
                "Retry preflight supports raw-only single-event merges"
            )
        run_identity(args.run_id)
        run_identity(args.coordinator_run_id)
        if args.run_id == args.coordinator_run_id:
            raise CompactionError("Retry original run identities must be distinct")
        # Keep the existing public one-hour boundary, runtime and storage checks.
        with _opened(args, client) as opened:
            conn, limits, hours, events, source, destination, _, _, binding = opened
            root = local_directory(args.run_evidence_directory)
            for store in (source, destination):
                separate_roots(Dataset.parse(str(root)), store.dataset)
            records = load_archive(str(root), args.run_id)
            if (
                [r["phase"] for r in records]
                != ["run_intent", "migration_intent", "blocked"]
                or any(
                    r["coordinator_run_id"] != args.coordinator_run_id for r in records
                )
                or canonical(records[0]["data"])
                != canonical({"binding": binding, "command": "compact"})
                or set(records[-1]["data"]) != {"message"}
                or not isinstance(records[-1]["data"]["message"], str)
                or not records[-1]["data"]["message"]
            ):
                raise CompactionError(
                    "Retry requires exact original failed-merge grammar"
                )
            parent = Leaf(
                events[0], hours[0].strftime("%Y%m%d"), hours[0].strftime("%H")
            )
            original = records[1]["data"]
            if (
                set(original) != {"parent", "source", "partitions"}
                or canonical(original["parent"]) != canonical(parent.as_dict())
                or not isinstance(original["source"], list)
                or not 1 <= len(original["source"]) <= limits.max_files
                or not isinstance(original["partitions"], list)
                or not 1 <= len(original["partitions"]) <= len(original["source"])
            ):
                raise CompactionError(
                    "Retry original migration intent scope/budget mismatch"
                )
            _files(original["source"], source, parent.as_dict())

            def observe():
                # Bound every object and local directory node before Parquet discovery.
                prefixes = prefix_inventory(source, hours, events, limits.max_files)
                outputs = prefix_inventory(destination, hours, events, limits.max_files)
                if outputs[parent] or destination.exists(parent.relative):
                    raise CompactionError(
                        "Retry requires an absent original output hour"
                    )
                inputs = discover(source, hours, events, max_files=limits.max_files)
                current = inventory_evidence(source, inputs)
                keys = {
                    source.dataset.object_key(e["file"]["relative"])
                    if source.dataset.bucket
                    else e["file"]["uri"]
                    for e in current
                }
                if prefixes[parent] != keys or canonical(current) != canonical(
                    original["source"]
                ):
                    raise CompactionError(
                        "Retry original inputs are absent, changed or unarchived"
                    )
                partitions = []
                for leaf, files in sorted(inputs.groups.items()):
                    uris = [f.uri for f in files]
                    columns = schema(conn, uris)
                    check_hive_fields(conn, uris, leaf, columns)
                    partitions.append(
                        {
                            **leaf.as_dict(),
                            "schema": columns,
                            "source_rows": count_rows(conn, uris),
                        }
                    )
                if canonical(partitions) != canonical(original["partitions"]):
                    raise CompactionError(
                        "Retry original partition/schema/count intent changed"
                    )
                recheck(source, inputs, hours, max_files=limits.max_files)
                if (
                    inventory_evidence(source, inputs) != current
                    or prefix_inventory(source, hours, events, limits.max_files)
                    != prefixes
                    or prefix_inventory(destination, hours, events, limits.max_files)
                    != outputs
                    or destination.exists(parent.relative)
                ):
                    raise CompactionError(
                        "Retry input identities or source/output prefixes changed"
                    )
                return partitions, current

            partitions, current = observe()
            if (
                observe() != (partitions, current)
                or load_archive(str(root), args.run_id) != records
            ):
                raise CompactionError(
                    "Retry data or archive changed during verification"
                )
            rows = sum(p["source_rows"] for p in partitions)
            if not 0 <= rows < 2**63:
                raise CompactionError("Retry source row count exceeds supported range")
            result = {
                "version": 1,
                "operation": "retry-preflight",
                "status": "verified",
                "disposition": "unchanged-inputs-absent-output",
                "run_id": args.coordinator_run_id,
                "cli_run_id": args.run_id,
                "hour_start": args.start_hour,
                "hour_end": args.end_hour,
                "binding_sha256": identity(binding),
                "archive_sha256": identity(records[-1]),
                "archive_records": len(records),
                "source_rows": rows,
                "source_files": len(current),
                "source_bytes": sum(e["file"]["size"] for e in current),
                "output_absent": True,
            }
            if len(canonical(result).encode()) > MAX_RESULT_BYTES:
                raise CompactionError("Retry evidence exceeds byte budget")
            return result
    except CompactionError:
        raise
    except Exception:
        raise CompactionError(
            "Independent retry preflight failed; details suppressed"
        ) from None


def _request(path, expected_sha256):
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
                "supervision_sha256",
                "intent_sha256",
                "ledger_id",
                "dataset_sha256",
                "archive_sha256",
                "archive_records",
            }
            or type(value["version"]) is not int
            or value["version"] != 1
            or value["operation"] != "retry-preflight"
            or type(value["archive_records"]) is not int
            or value["archive_records"] != 3
        ):
            raise ValueError
        run_identity(value["ledger_id"])
        for field in (
            "execution_sha256",
            "supervision_sha256",
            "intent_sha256",
            "dataset_sha256",
            "archive_sha256",
        ):
            _digest(value[field])
        return value, _arguments(value["arguments"])
    except Exception:
        raise CompactionError("Malformed retry request; details suppressed") from None


def main(argv=None):
    outer = argparse.ArgumentParser(description=__doc__)
    outer.add_argument("--request", required=True)
    outer.add_argument("--request-sha256", required=True)
    selected = outer.parse_args(argv)
    try:
        request, args = _request(selected.request, selected.request_sha256)
        result = verify_retry(args)
        if any(result[k] != request[k] for k in ("archive_sha256", "archive_records")):
            raise CompactionError(
                "Retry archive is not bound to original failure capture"
            )
        result.update(
            {
                k: request[k]
                for k in (
                    "execution_sha256",
                    "supervision_sha256",
                    "intent_sha256",
                    "ledger_id",
                    "dataset_sha256",
                )
            }
        )
        result["request_sha256"] = selected.request_sha256
        if _request(selected.request, selected.request_sha256)[0] != request:
            raise CompactionError("Retry request changed during verification")
        print(canonical(result))
        return 0
    except Exception:
        print(
            canonical(
                {
                    "status": "error",
                    "message": "Independent retry preflight failed; details suppressed",
                }
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
