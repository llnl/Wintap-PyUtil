"""Original-bound deletion disposition. Never rewrite an original failed archive.

Requires cooperative exclusive access. Every original event/hour migration must be
complete before any survivor can be deleted; survivor row equality is insufficient.
Current entities are validated, never replayed. New checkpoints retain historical
entity provenance but carry this repair's identity and actual disposition evidence.
"""

import os
import re
import stat
from copy import copy
from datetime import datetime, timezone
from pathlib import Path

from .config import CompactionError, Leaf
from .engine import count_rows, schema
from .inventory import discover, recheck
from .phase_evidence import (
    canonical,
    identity,
    inventory_evidence,
    load_archive,
    run_identity,
)
from .processing_metrics import (
    _report_hour,
    _sum_partitions,
    validate_checkpoint_receipts,
)


def prefix_keys(store, parent, limit, *, budget=None):
    """Bound objects and local directory nodes, including non-Parquet markers."""
    keys = set()
    remaining = [limit] if budget is None else budget

    def consume():
        store.check_deadline()
        remaining[0] -= 1
        if remaining[0] < 0:
            raise CompactionError("Deletion recovery traversal/object budget exceeded")

    if store.dataset.bucket:
        prefix = store.dataset.object_key(parent.relative)
        for page in store.client.get_paginator("list_objects_v2").paginate(
            Bucket=store.dataset.bucket, Prefix=prefix
        ):
            store.check_deadline()
            for item in page.get("Contents", []):
                consume()
                key = item["Key"]
                if not key.startswith(prefix) or key in keys:
                    raise CompactionError(
                        "Deletion recovery listing scope/identity conflict"
                    )
                keys.add(key)
    else:
        path = Path(store.dataset.file_uri(parent.relative))
        store.exists(parent.relative)  # Reject symlinked prefix components.

        if path.exists():
            if not path.is_dir():
                raise CompactionError("Deletion recovery hour is not a directory")
            pending = [(path, 0)]
            while pending:
                directory, depth = pending.pop()
                if depth > 64:
                    raise CompactionError(
                        "Deletion recovery traversal depth budget exceeded"
                    )
                with os.scandir(directory) as children:
                    for child in children:
                        consume()
                        mode = child.stat(follow_symlinks=False).st_mode
                        if stat.S_ISDIR(mode):
                            pending.append((Path(child.path), depth + 1))
                        elif stat.S_ISREG(mode):
                            keys.add(child.path)
                        else:
                            raise CompactionError(
                                "Deletion recovery requires regular original objects"
                            )
    return keys


def prefix_inventory(store, hours, events, limit):
    """Apply one aggregate node/object ceiling before Parquet traversal or reads."""
    budget = [limit]
    result = {}
    for hour in hours:
        for event in events:
            leaf = Leaf(event, hour.strftime("%Y%m%d"), hour.strftime("%H"))
            result[leaf] = prefix_keys(store, leaf, limit, budget=budget)
    return result


def _files(values, store, parent):
    seen = set()
    if not isinstance(values, list) or not values:
        raise CompactionError("Original migration objects missing")
    for entry in values:
        file = entry["file"]
        leaf = Leaf.from_relative(file["relative"])
        if (
            set(entry) != {"leaf", "file", "sha256"}
            or set(file) != {"relative", "uri", "size", "etag", "modified"}
            or entry["leaf"] != leaf.as_dict()
            or {k: entry["leaf"][k] for k in parent} != parent
            or file["uri"] != store.dataset.file_uri(file["relative"])
            or type(file["size"]) is not int
            or file["size"] < 0
            or any(not isinstance(file[k], str) for k in ("etag", "modified"))
            or not isinstance(entry["sha256"], str)
            or not re.fullmatch(r"[a-f0-9]{64}", entry["sha256"])
            or file["uri"] in seen
        ):
            raise CompactionError("Original migration object scope/identity mismatch")
        seen.add(file["uri"])


def original_deletions(
    conn, args, records, binding, source, destination, hours, events, limits
):
    """Authenticate all originals and every current survivor/output, without writes."""
    from .completion import _grammar

    original = copy(args)
    original.command = "compact"
    original.run_id = records[0]["run_id"]
    original.coordinator_run_id = records[0]["coordinator_run_id"]
    original.delete_source = True
    raw = _grammar(records, original, binding, deletion_repaired=True)
    expected = [
        Leaf(event, hour.strftime("%Y%m%d"), hour.strftime("%H")).as_dict()
        for hour in hours
        for event in events
    ]
    items = raw.get("hours", [])
    if (
        not events
        or list(events) != sorted(set(events))
        or [{k: item[k] for k in ("event_type", "dayPK", "hourPK")} for item in items]
        != expected
        or any(
            item["status"] not in {"empty", "merged", "merged_with_deletion_error"}
            for item in items
        )
        or not any(item["status"] == "merged_with_deletion_error" for item in items)
        or raw["deletion_failures"]
        != [
            {**parent, **item["deletion_error"]}
            for parent, item in zip(expected, items, strict=True)
            if item["status"] == "merged_with_deletion_error"
        ]
    ):
        raise CompactionError("Original partial deletion hour/failure scope mismatch")
    migrations = [r["data"] for r in records if r["phase"] == "migration_verified"]
    if raw["partitions"] != [
        p for migration in migrations for p in migration["partitions"]
    ]:
        raise CompactionError("Original migration partition/count report mismatch")
    for side in ("source", "destination"):
        if sum(len(m[side]) for m in migrations) > limits.max_files:
            raise CompactionError(
                "Original migration aggregate object ceiling exceeded"
            )
    prefixes = [
        prefix_inventory(store, hours, events, limits.max_files)
        for store in (source, destination)
    ]
    inputs = discover(source, hours, events, max_files=limits.max_files)
    outputs = discover(destination, hours, events, max_files=limits.max_files)
    observed_source = inventory_evidence(source, inputs)
    observed_output = inventory_evidence(destination, outputs)
    observations = []
    for parent, item in zip(expected, items, strict=True):
        leaf = Leaf(parent["event_type"], parent["dayPK"], parent["hourPK"])
        surviving = [
            entry
            for entry in observed_source
            if all(entry["leaf"][k] == v for k, v in parent.items())
        ]
        current_output = [
            entry
            for entry in observed_output
            if all(entry["leaf"][k] == v for k, v in parent.items())
        ]
        for index, (store, entries) in enumerate(
            ((source, surviving), (destination, current_output))
        ):
            keys = {
                store.dataset.object_key(entry["file"]["relative"])
                if store.dataset.bucket
                else entry["file"]["uri"]
                for entry in entries
            }
            if prefixes[index][leaf] != keys:
                raise CompactionError(
                    "Unarchived source/output objects prohibit deletion recovery"
                )
        if item["status"] == "empty":
            if surviving or current_output or destination.exists(leaf.relative):
                raise CompactionError("Originally empty migration changed")
        else:
            selected = [m for m in migrations if m["parent"] == parent]
            if len(selected) != 1:
                raise CompactionError("Original migration partition missing/duplicated")
            migration = selected[0]
            _files(migration["source"], source, parent)
            _files(migration["destination"], destination, parent)
            if current_output != migration["destination"] or any(
                entry not in migration["source"] for entry in surviving
            ):
                raise CompactionError("Original survivor/output identity changed")
            if item["status"] == "merged" and source.exists(leaf.relative):
                raise CompactionError("Originally verified deletion source reappeared")
            terminal = [
                r
                for r in records
                if r["phase"] in {"deletion_verified", "deletion_uncertain"}
                and all(r["data"].get(k) == v for k, v in parent.items())
            ]
            if len(terminal) != 1 or terminal[0]["data"] != item:
                raise CompactionError("Original deletion outcome/report mismatch")
            partitions = migration["partitions"]
            keys = [
                {k: p[k] for k in ("event_type", "dayPK", "hourPK")}
                | ({"protoPK": p["protoPK"]} if "protoPK" in p else {})
                for p in partitions
            ]
            if (
                len({canonical(k) for k in keys}) != len(keys)
                or {canonical(k) for k in keys}
                != {canonical(e["leaf"]) for e in migration["source"]}
                or {canonical(k) for k in keys}
                != {canonical(e["leaf"]) for e in current_output}
            ):
                raise CompactionError("Original migration protocol scope mismatch")
            for partition, key in zip(partitions, keys, strict=True):
                original_files = [
                    e["file"] for e in migration["source"] if e["leaf"] == key
                ]
                files = [e["file"]["uri"] for e in current_output if e["leaf"] == key]
                if (
                    partition["source_files"] != len(original_files)
                    or partition["source_bytes"]
                    != sum(f["size"] for f in original_files)
                    or partition["source_rows"] != partition["merged_rows"]
                    or count_rows(conn, files) != partition["merged_rows"]
                    or schema(conn, files) != partition["schema"]
                ):
                    raise CompactionError(
                        "Original migration row/schema/count mismatch"
                    )
        observations.append({"parent": parent, "survivors": surviving})
    recheck(source, inputs, hours, max_files=limits.max_files)
    recheck(destination, outputs, hours, max_files=limits.max_files)
    if [
        prefix_inventory(store, hours, events, limits.max_files)
        for store in (source, destination)
    ] != prefixes:
        raise CompactionError("Deletion recovery prefix inventory changed")
    return raw, observations


def original_checkpoints(conn, records, binding, hours, raw):
    rows = next(r["data"]["rows"] for r in records if r["phase"] == "checkpoint_intent")
    entities = next(r["data"] for r in records if r["phase"] == "entities_verified")
    validate_checkpoint_receipts(conn, rows, hours, binding)
    for row, hour in zip(rows, hours, strict=True):
        failures = sum(
            item["status"] == "merged_with_deletion_error"
            for item in _report_hour(raw, hour)
        )
        if (
            row["run_id"] != records[0]["run_id"]
            or row["operation"] != "compact"
            or row["entity_generation_id"] != entities.get("generation_id")
            or row["deletion_failures"] != failures
            or row["deletion_status"] != ("failed" if failures else "completed")
            or row["status"]
            != ("completed_with_deletion_errors" if failures else "completed")
            or row["raw_status"]
            != (
                "completed_with_deletion_errors"
                if failures
                else "merged"
                if any(i["status"] == "merged" for i in _report_hour(raw, hour))
                else "empty"
            )
            or row["raw_source_rows"] != _sum_partitions(raw, hour, "source_rows")
            or row["raw_output_rows"] != _sum_partitions(raw, hour, "merged_rows")
        ):
            raise CompactionError(
                "Original failed checkpoint migration/provenance mismatch"
            )
    observed = [r["data"] for r in records if r["phase"] == "checkpoint_verified"]
    expected = [{"hour_start": row["hour_start"], "row": row} for row in rows]
    # A publication failure can leave only a prefix, never duplicates or reorderings.
    if canonical(observed) != canonical(expected[: len(observed)]) or len(
        observed
    ) > len(expected):
        raise CompactionError(
            "Original checkpoint observation/candidate order mismatch"
        )
    return rows


def disposition_rows(original, raw, hours, repair_id, completed_at):
    """New candidates, not edits or fabricated successes in the original archive."""
    return [
        {
            **row,
            "run_id": repair_id,
            "operation": "deletions-repair",
            "status": "completed",
            "raw_status": "merged"
            if any(i["status"] != "empty" for i in _report_hour(raw, hour))
            else "empty",
            "deletion_status": "completed",
            "deletion_failures": 0,
            "completed_at": completed_at,
        }
        for row, hour in zip(original, hours, strict=True)
    ]


def repair_deletions(
    conn,
    args,
    archive,
    binding,
    source,
    destination,
    metrics,
    hours,
    events,
    limits,
    *,
    verify_current_entities,
):
    from . import deletion

    original_id = run_identity(args.repair_run_id)
    if original_id == archive.run_id:
        raise CompactionError("Deletion repair requires a separate run identity")
    current = verify_current_entities()
    records = load_archive(args.run_evidence_directory, original_id)
    expected_observations = None
    expected_checkpoints = None

    def verify():
        if load_archive(args.run_evidence_directory, original_id) != records:
            raise CompactionError("Original deletion archive changed")
        result = original_deletions(
            conn, args, records, binding, source, destination, hours, events, limits
        )
        if verify_current_entities() != current:
            raise CompactionError("Entity authority changed during deletion repair")
        if expected_observations is not None and result[1] != expected_observations:
            raise CompactionError("Survivors changed during deletion repair")
        if (
            expected_checkpoints is not None
            and [metrics.read(conn, hour) for hour in hours] != expected_checkpoints
        ):
            raise CompactionError("Checkpoint changed during deletion repair")
        return result

    raw, observations = verify()
    original_rows = (
        original_checkpoints(conn, records, binding, hours, raw) if metrics else []
    )
    existing = [metrics.read(conn, hour) for hour in hours] if metrics else []
    expected_observations = [
        {**o, "survivors": list(o["survivors"])} for o in observations
    ]
    expected_checkpoints = list(existing) if metrics else None
    conflicts = any(
        old is not None and old != row
        for old, row in zip(existing, original_rows, strict=True)
    )
    if conflicts and not args.confirm_replace_checkpoints:
        raise CompactionError(
            "Conflicting checkpoint coverage; explicit "
            "--confirm-replace-checkpoints required"
        )
    rows = (
        disposition_rows(
            original_rows,
            raw,
            hours,
            archive.run_id,
            datetime.now(timezone.utc).isoformat(),
        )
        if metrics
        else []
    )
    archive.append(
        "repair_intent",
        {
            "original_run_id": original_id,
            "original_last_sha256": identity(records[-1]),
            "replaced_checkpoints": existing,
            "confirmed_replacement": conflicts,
            "rows": rows,
        },
    )
    if verify()[1] != observations:
        raise CompactionError("Survivors changed before deletion disposition")
    for index, observed in enumerate(observations):
        parent = observed["parent"]
        leaf = Leaf(parent["event_type"], parent["dayPK"], parent["hourPK"])
        # Validate every event/hour again before each mutation; prior deletions may
        # legitimately reduce only the originally uncertain survivor inventories.
        current_observed = next(o for o in verify()[1] if o["parent"] == parent)
        if current_observed != observed:
            raise CompactionError("Original survivors changed before deletion")
        archive.append("deletion_repair_intent", observed)
        if next(o for o in verify()[1] if o["parent"] == parent) != observed:
            raise CompactionError("Survivors changed during deletion repair intent")
        details = {}
        try:
            count = deletion.delete_hour(
                source,
                leaf,
                confirm_attempts=args.delete_confirm_attempts,
                confirm_initial_delay_seconds=args.delete_confirm_initial_delay_seconds,
                report=details,
            )
        except deletion.DeletionError:
            archive.append("deletion_repair_uncertain", {"parent": parent})
            raise
        if (
            type(count) is not int
            or count != len(observed["survivors"])
            or source.exists(leaf.relative)
        ):
            raise CompactionError(
                "Deletion repair count/empty-prefix confirmation failed"
            )
        expected_observations[index] = {"parent": parent, "survivors": []}
        archive.append(
            "deletion_repair_verified",
            {
                "parent": parent,
                "deleted_objects": count,
                "remaining_keys": [],
                "details": details,
            },
        )
    if any(o["survivors"] for o in verify()[1]):
        raise CompactionError("Deletion repair survivors remain")
    for index, (hour, row, old) in enumerate(
        zip(hours if metrics else (), rows, existing, strict=True)
    ):
        verify()  # Recheck all hours and already-published rows before each mutation.
        if metrics.read(conn, hour) != old:
            raise CompactionError("Checkpoint changed during deletion repair")
        metrics.publish(conn, hour, row)
        if metrics.read(conn, hour) != row:
            raise CompactionError("Deletion repair checkpoint GET failed")
        expected_checkpoints[index] = row
        archive.append(
            "checkpoint_verified", {"hour_start": row["hour_start"], "row": row}
        )
    if any(o["survivors"] for o in verify()[1]) or any(
        metrics.read(conn, hour) != row
        for hour, row in zip(hours if metrics else (), rows, strict=True)
    ):
        raise CompactionError("Deletion repair final verification failed")
    archive.migration_complete = True
    return {"status": "repaired", "repair_run_id": original_id}


def verify_disposition(
    conn,
    args,
    original,
    repaired,
    binding,
    source,
    destination,
    metrics,
    hours,
    events,
    limits,
    *,
    repair_run_id,
    repair_coordinator_run_id,
    replacement_authorized,
):
    """Read-only independent validation of actual separate disposition records."""
    raw, observations = original_deletions(
        conn, args, original, binding, source, destination, hours, events, limits
    )
    expected = (
        ["run_intent"] + (["entities_verified"] if metrics else []) + ["repair_intent"]
    )
    expected += [
        p
        for _ in observations
        for p in ("deletion_repair_intent", "deletion_repair_verified")
    ]
    expected += ["checkpoint_verified"] * (len(hours) if metrics else 0) + ["complete"]
    if (
        len(
            {
                args.run_id,
                args.coordinator_run_id,
                repair_run_id,
                repair_coordinator_run_id,
            }
        )
        != 4
        or any(
            r["run_id"] != repair_run_id
            or r["coordinator_run_id"] != repair_coordinator_run_id
            for r in repaired
        )
        or [r["phase"] for r in repaired] != expected
        or repaired[0]["data"] != {"binding": binding, "command": "deletions-repair"}
        or canonical(repaired[-1]["data"])
        != canonical({"status": "repaired", "exit_code": 0, "migration_complete": True})
        or any(
            o["survivors"]
            or source.exists(
                Leaf(
                    o["parent"]["event_type"],
                    o["parent"]["dayPK"],
                    o["parent"]["hourPK"],
                ).relative
            )
            for o in observations
        )
    ):
        raise CompactionError(
            "Exact deletion repair scope/order/empty-source evidence missing"
        )
    rows = original_checkpoints(conn, original, binding, hours, raw) if metrics else []
    intent = next(r["data"] for r in repaired if r["phase"] == "repair_intent")
    replaced = intent.get("replaced_checkpoints")
    new_rows = intent.get("rows")
    if (
        not isinstance(replaced, list)
        or len(replaced) != len(rows)
        or any(old is not None and not isinstance(old, dict) for old in replaced)
        or not isinstance(new_rows, list)
        or len(new_rows) != len(rows)
    ):
        raise CompactionError("Deletion repair checkpoint disposition missing")
    conflicts = any(
        old is not None and old != row for old, row in zip(replaced, rows, strict=True)
    )
    if conflicts and not replacement_authorized:
        raise CompactionError(
            "Deletion repair replacement lacks explicit authorization"
        )
    if metrics:
        completed_at = new_rows[0]["completed_at"]
        if (
            not isinstance(completed_at, str)
            or datetime.fromisoformat(completed_at).tzinfo is None
        ):
            raise CompactionError("Deletion repair observation time missing")
        if new_rows != disposition_rows(rows, raw, hours, repair_run_id, completed_at):
            raise CompactionError(
                "Deletion repair altered original checkpoint provenance"
            )
        validate_checkpoint_receipts(conn, new_rows, hours, binding)
    if canonical(intent) != canonical(
        {
            "original_run_id": args.run_id,
            "original_last_sha256": identity(original[-1]),
            "replaced_checkpoints": replaced,
            "confirmed_replacement": conflicts,
            "rows": new_rows,
        }
    ):
        raise CompactionError("Deletion repair intent original identity mismatch")
    deletion_intents = [
        r["data"] for r in repaired if r["phase"] == "deletion_repair_intent"
    ]
    verified = [r["data"] for r in repaired if r["phase"] == "deletion_repair_verified"]
    migrations = [r["data"] for r in original if r["phase"] == "migration_verified"]
    for observed, before, after in zip(
        observations, deletion_intents, verified, strict=True
    ):
        parent = observed["parent"]
        survivors = before.get("survivors")
        migration = next((m for m in migrations if m["parent"] == parent), None)
        allowed = migration["source"] if migration else []
        if (
            set(before) != {"parent", "survivors"}
            or before["parent"] != parent
            or not isinstance(survivors, list)
            or any(e not in allowed for e in survivors)
            or len({canonical(e) for e in survivors}) != len(survivors)
            or set(after) != {"parent", "deleted_objects", "remaining_keys", "details"}
            or after["parent"] != parent
            or type(after["deleted_objects"]) is not int
            or after["deleted_objects"] != len(survivors)
            or after["remaining_keys"] != []
            or not isinstance(after["details"], dict)
        ):
            raise CompactionError(
                "Deletion repair actual survivor/count evidence mismatch"
            )
    checkpoints = [r["data"] for r in repaired if r["phase"] == "checkpoint_verified"]
    if checkpoints != [
        {"hour_start": row["hour_start"], "row": row} for row in new_rows
    ] or any(
        metrics.read(conn, hour) != row
        for hour, row in zip(hours if metrics else (), new_rows, strict=True)
    ):
        raise CompactionError("Exact deletion disposition checkpoint missing/changed")
    return raw, new_rows[0] if new_rows else None
