"""Bounded, immutable local run archive; not the coordination authority ledger.

External reader/writer exclusion is required. Coverage can be overwritten; this
archive retains original migration evidence and checkpoint candidates separately.
"""

import hashlib
import json
import os
import re
from dataclasses import asdict
from pathlib import Path
from tempfile import NamedTemporaryFile

from .config import CompactionError, Dataset, Leaf
from .engine import count_rows, schema
from .inventory import discover, recheck

MAX_RECORD_BYTES = 64 * 1024 * 1024
MAX_ARCHIVE_BYTES = 256 * 1024 * 1024
MAX_RECORDS = 10_000


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def identity(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def run_identity(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{32}", value):
        raise CompactionError(
            "Run identity must be 32 lowercase hexadecimal characters"
        )
    return value


def local_directory(value):
    dataset = Dataset.parse(str(value))
    if dataset.bucket:
        raise CompactionError("Run evidence requires a server-local directory")
    path = Path(value).expanduser().absolute()
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise CompactionError("Symlinked run evidence paths unsupported")
    return Path(dataset.root)


def fsync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def durable_mkdir(path):
    missing = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    for directory in reversed(missing):
        directory.mkdir(exist_ok=True)
        fsync_directory(directory.parent)


class RunEvidence:
    def __init__(self, root, run_id, coordinator_run_id, binding, command):
        self.root = local_directory(root)
        self.run_id = run_identity(run_id)
        self.coordinator_run_id = (
            run_identity(coordinator_run_id) if coordinator_run_id else ""
        )
        self.directory = self.root / run_id
        self.sequence = 0
        self.previous = ""
        self.bytes = 0
        self.migration_complete = False
        durable_mkdir(self.root)
        try:
            self.directory.mkdir()
        except FileExistsError:
            raise CompactionError(
                "Run evidence already exists; use a new run identity"
            ) from None
        fsync_directory(self.root)
        self.append("run_intent", {"binding": binding, "command": command})

    def append(self, phase, data):
        if not re.fullmatch(r"[a-z_]+", phase):
            raise CompactionError("Invalid run evidence phase")
        value = {
            "evidence_version": "1",
            "sequence": self.sequence,
            "previous_sha256": self.previous,
            "run_id": self.run_id,
            "coordinator_run_id": self.coordinator_run_id,
            "phase": phase,
            "data": data,
        }
        digest = identity(value)
        encoded = (canonical({**value, "sha256": digest}) + "\n").encode()
        if (
            len(encoded) > MAX_RECORD_BYTES
            or self.bytes + len(encoded) > MAX_ARCHIVE_BYTES
            or self.sequence >= MAX_RECORDS
        ):
            raise CompactionError(
                "Run evidence exceeds archive budget; history not truncated"
            )
        target = self.directory / f"{self.sequence:06d}-{phase}.json"
        staging = None
        try:
            with NamedTemporaryFile(dir=self.directory, delete=False) as output:
                staging = output.name
                output.write(encoded)
                output.flush()
                os.fsync(output.fileno())
            # Publish without replacing an existing record, even after a crash.
            os.link(staging, target)
            fsync_directory(self.directory)
        except Exception:
            raise CompactionError(
                "Run evidence publication uncertain; exclusive recovery required"
            ) from None
        finally:
            if staging and os.path.exists(staging):
                os.unlink(staging)
        self.sequence += 1
        self.previous = digest
        self.bytes += len(encoded)

    def summary(self):
        return {
            "directory": str(self.directory),
            "last_sha256": self.previous,
            "records": self.sequence,
            "migration_complete": self.migration_complete,
        }


def load_archive(root, run_id):
    directory = local_directory(root) / run_identity(run_id)
    if directory.is_symlink() or not directory.is_dir():
        raise CompactionError("Original run evidence is missing")
    paths = []
    for path in directory.iterdir():
        if path.suffix != ".json":
            continue  # Unpublished staging files never establish trust.
        paths.append(path)
        if len(paths) > MAX_RECORDS:
            raise CompactionError("Original run evidence record budget/shape invalid")
    paths.sort()
    if not paths or len(paths) > MAX_RECORDS:
        raise CompactionError("Original run evidence record budget/shape invalid")
    records, previous, total = [], "", 0
    for sequence, path in enumerate(paths):
        size = path.stat().st_size
        total += size
        if path.is_symlink() or size > MAX_RECORD_BYTES or total > MAX_ARCHIVE_BYTES:
            raise CompactionError("Original run evidence exceeds archive budget")
        try:
            with path.open("rb") as stream:
                encoded = stream.read(
                    min(MAX_RECORD_BYTES, MAX_ARCHIVE_BYTES - total + size) + 1
                )
            if len(encoded) != size:
                raise ValueError
            record = json.loads(encoded)
            if encoded != (canonical(record) + "\n").encode():
                raise ValueError
            if set(record) != {
                "sha256",
                "evidence_version",
                "sequence",
                "previous_sha256",
                "run_id",
                "coordinator_run_id",
                "phase",
                "data",
            }:
                raise ValueError
            digest = record.pop("sha256")
            if (
                digest != identity(record)
                or type(record["sequence"]) is not int
                or record["sequence"] != sequence
                or record["previous_sha256"] != previous
                or record["evidence_version"] != "1"
                or record["run_id"] != run_id
                or not isinstance(record["phase"], str)
                or not re.fullmatch(r"[a-z_]+", record["phase"])
                or not isinstance(record["data"], dict)
                or path.name != f"{sequence:06d}-{record['phase']}.json"
            ):
                raise ValueError
            if record["coordinator_run_id"]:
                run_identity(record["coordinator_run_id"])
            if (
                records
                and record["coordinator_run_id"] != records[0]["coordinator_run_id"]
            ):
                raise ValueError
        except Exception:
            raise CompactionError(
                "Original run evidence identity/lineage invalid"
            ) from None
        records.append(record)
        previous = digest
    if records[0]["phase"] != "run_intent":
        raise CompactionError("Original run evidence intent missing")
    return records


def object_digest(store, file):
    store.check_deadline()
    if store.dataset.bucket:
        stream = store.client.get_object(
            Bucket=store.dataset.bucket, Key=store.dataset.object_key(file.relative)
        )["Body"]
    else:
        stream = open(file.uri, "rb")
    digest, size = hashlib.sha256(), 0
    try:
        while chunk := stream.read(1024 * 1024):
            store.check_deadline()
            size += len(chunk)
            if size > file.size:
                raise CompactionError(
                    "Migration evidence object changed during hashing"
                )
            digest.update(chunk)
    finally:
        stream.close()
    if size != file.size:
        raise CompactionError("Migration evidence object changed during hashing")
    return digest.hexdigest()


def inventory_evidence(store, inventory):
    return [
        {
            "leaf": leaf.as_dict(),
            "file": asdict(file),
            "sha256": object_digest(store, file),
        }
        for leaf, files in sorted(inventory.groups.items())
        for file in files
    ]


def migration_evidence(source, destination, inputs, outputs, partitions, parent):
    return {
        "parent": parent.as_dict(),
        "source": inventory_evidence(source, inputs),
        "destination": inventory_evidence(destination, outputs),
        "partitions": partitions,
    }


def migration_complete(report):
    entries = report.get("hours", [])
    return (
        bool(entries)
        and not report.get("deletion_failures")
        and all(
            item.get("status") in {"merged", "empty", "deleted"}
            and (
                not report.get("delete_source")
                or item["status"] == "empty"
                or type(item.get("source_deleted_objects")) is int
                and item["source_deleted_objects"] >= 0
            )
            for item in entries
        )
    )


def verify_migrations(conn, records, source, destination, hours, events, limits):
    """No deletion or state repair: verify the original surviving copies strictly."""
    complete = [r["data"] for r in records if r["phase"] == "raw_complete"]
    if len(complete) != 1 or not migration_complete(complete[0]):
        raise CompactionError(
            "Original migration is incomplete/uncertain; "
            "explicit reconciliation required"
        )
    report = complete[0]
    families = report["event_types"]
    if events and sorted(set(events)) != families:
        raise CompactionError("Original migration event scope mismatch")
    expected = {
        (event, h.strftime("%Y%m%d"), h.strftime("%H"))
        for h in hours
        for event in families
    }
    actual = {(h["event_type"], h["dayPK"], h["hourPK"]) for h in report["hours"]}
    if actual != expected or len(actual) != len(report["hours"]):
        raise CompactionError("Original migration hour scope mismatch")
    migrations = [r["data"] for r in records if r["phase"] == "migration_verified"]
    expected_migrations = [h for h in report["hours"] if h["status"] != "empty"]
    if len(migrations) != len(expected_migrations):
        raise CompactionError("Original migration evidence scope mismatch")
    raw_sequence = next(r["sequence"] for r in records if r["phase"] == "raw_complete")
    for item in report["hours"]:
        parent = {k: item[k] for k in ("event_type", "dayPK", "hourPK")}
        selected = tuple(
            h
            for h in hours
            if h.strftime("%Y%m%d") == item["dayPK"]
            and h.strftime("%H") == item["hourPK"]
        )
        inputs = discover(
            source, selected, (item["event_type"],), max_files=limits.max_files
        )
        outputs = discover(
            destination, selected, (item["event_type"],), max_files=limits.max_files
        )
        if item["status"] == "empty":
            if inputs.groups or outputs.groups:
                raise CompactionError("Originally empty migration hour changed")
        else:
            evidence = [m for m in migrations if m["parent"] == parent]
            if len(evidence) != 1:
                raise CompactionError(
                    "Original migration partition evidence missing/duplicated"
                )
            evidence = evidence[0]
            verified_record = next(
                r
                for r in records
                if r["phase"] == "migration_verified" and r["data"] == evidence
            )
            if verified_record["sequence"] >= raw_sequence:
                raise CompactionError("Original migration phase order invalid")
            if report["delete_source"]:
                intent = [
                    r
                    for r in records
                    if r["phase"] == "deletion_intent"
                    and r["data"].get("parent") == parent
                ]
                deleted = [
                    r
                    for r in records
                    if r["phase"] == "deletion_verified"
                    and {k: r["data"].get(k) for k in parent} == parent
                ]
                if (
                    len(intent) != 1
                    or len(deleted) != 1
                    or deleted[0]["data"] != item
                    or not verified_record["sequence"]
                    < intent[0]["sequence"]
                    < deleted[0]["sequence"]
                    < raw_sequence
                ):
                    raise CompactionError(
                        "Original migration deletion evidence missing/uncertain"
                    )
            if inventory_evidence(destination, outputs) != evidence["destination"]:
                raise CompactionError("Original migration output identity changed")
            if report["delete_source"]:
                if source.exists(
                    Leaf(item["event_type"], item["dayPK"], item["hourPK"]).relative
                ):
                    raise CompactionError(
                        "Original migration source prefix is not empty"
                    )
            elif inventory_evidence(source, inputs) != evidence["source"]:
                raise CompactionError("Original migration source identity changed")
            keys = [
                {k: partition[k] for k in ("event_type", "dayPK", "hourPK")}
                | ({"protoPK": partition["protoPK"]} if "protoPK" in partition else {})
                for partition in evidence["partitions"]
            ]
            if (
                not evidence["source"]
                or not evidence["destination"]
                or len(keys) != len(outputs.groups)
                or sorted(keys, key=canonical)
                != sorted([leaf.as_dict() for leaf in outputs.groups], key=canonical)
            ):
                raise CompactionError("Original migration protocol scope mismatch")
            for partition, key in zip(evidence["partitions"], keys, strict=True):
                if partition not in report["partitions"]:
                    raise CompactionError(
                        "Original migration report/count evidence mismatch"
                    )
                files = [
                    f.uri
                    for leaf, values in outputs.groups.items()
                    if leaf.as_dict() == key
                    for f in values
                ]
                if (
                    not files
                    or not partition["passed"]
                    or partition["source_rows"] != partition["merged_rows"]
                    or count_rows(conn, files) != partition["merged_rows"]
                    or schema(conn, files) != partition["schema"]
                ):
                    raise CompactionError(
                        "Original migration output row/schema evidence mismatch"
                    )
        recheck(source, inputs, selected, max_files=limits.max_files)
        recheck(destination, outputs, selected, max_files=limits.max_files)
    return report


def repair_checkpoints(
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
    """Externally-exclusive repair; no entity replay or ledger reopening."""
    from .processing_metrics import (
        _report_hour,
        _sum_partitions,
        validate_checkpoint_receipts,
    )

    original_id = run_identity(args.repair_run_id)
    current_entities = verify_current_entities()
    records = load_archive(args.run_evidence_directory, original_id)
    intent = records[0]["data"]
    if intent.get("binding") != binding or intent.get("command") not in {
        "compact",
        "reconcile-delete",
    }:
        raise CompactionError("Original migration run binding/operation mismatch")
    raw = verify_migrations(conn, records, source, destination, hours, events, limits)
    checkpoints = [r for r in records if r["phase"] == "checkpoint_intent"]
    entities = [r for r in records if r["phase"] == "entities_verified"]
    raw_record = next(r for r in records if r["phase"] == "raw_complete")
    if (
        len(checkpoints) != 1
        or len(entities) != 1
        or not entities[0]["sequence"]
        < raw_record["sequence"]
        < checkpoints[0]["sequence"]
    ):
        raise CompactionError(
            "Original migration checkpoint/entity phase evidence missing"
        )
    rows = checkpoints[0]["data"].get("rows")
    if not isinstance(rows, list):
        raise CompactionError("Original checkpoint candidates missing")
    validate_checkpoint_receipts(conn, rows, hours, binding)
    for row, hour in zip(rows, hours, strict=True):
        statuses = {h["status"] for h in _report_hour(raw, hour)}
        expected_status = "merged" if statuses & {"merged", "deleted"} else "empty"
        if (
            row["run_id"] != original_id
            or row["operation"] != intent["command"]
            or row["entity_generation_id"] != entities[0]["data"].get("generation_id")
            or row["status"] != "completed"
            or row["raw_status"] != expected_status
            or row["deletion_failures"] != 0
            or row["deletion_status"] != "completed"
            or row["raw_source_rows"] != _sum_partitions(raw, hour, "source_rows")
            or row["raw_output_rows"] != _sum_partitions(raw, hour, "merged_rows")
        ):
            raise CompactionError(
                "Original checkpoint migration/generation evidence mismatch"
            )
    # Preflight *all* replacements before the first PUT, retaining conflicting coverage
    # in this new repair archive. An exit status cannot silently dispose of newer rows.
    existing = [metrics.read(conn, hour) for hour in hours]
    conflicts = any(
        old is not None and old != row for old, row in zip(existing, rows, strict=True)
    )
    if conflicts and not getattr(args, "confirm_replace_checkpoints", False):
        raise CompactionError(
            "Conflicting checkpoint coverage; "
            "explicit --confirm-replace-checkpoints required"
        )
    archive.append(
        "repair_intent",
        {
            "original_run_id": original_id,
            "original_last_sha256": identity(records[-1]),
            "replaced_checkpoints": existing,
            "rows": rows,
            "confirmed_replacement": bool(conflicts),
        },
    )
    verify_migrations(conn, records, source, destination, hours, events, limits)
    if verify_current_entities() != current_entities:
        raise CompactionError(
            "Entity authority changed during exclusive checkpoint repair"
        )
    for hour, row, old in zip(hours, rows, existing, strict=True):
        if metrics.read(conn, hour) != old:
            raise CompactionError("Checkpoint changed during exclusive repair")
        if old != row:
            metrics.publish(conn, hour, row)
        if metrics.read(conn, hour) != row:
            raise CompactionError("Repaired checkpoint GET verification failed")
        archive.append(
            "checkpoint_verified", {"hour_start": row["hour_start"], "row": row}
        )
    verify_migrations(conn, records, source, destination, hours, events, limits)
    if verify_current_entities() != current_entities:
        raise CompactionError(
            "Entity authority changed during exclusive checkpoint repair"
        )
    if any(
        metrics.read(conn, hour) != row for hour, row in zip(hours, rows, strict=True)
    ):
        raise CompactionError("Repaired checkpoint final GET verification failed")
    archive.migration_complete = True
    return {
        "status": "repaired",
        "repair_run_id": original_id,
        "processing_metrics": {
            "root": metrics.store.dataset.root,
            "hours": len(rows),
            "run_id": original_id,
        },
    }
