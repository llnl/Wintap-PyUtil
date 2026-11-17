"""Independent bounded entity-only observations, never permission or raw completion.

The main-runtime adapter owns exclusion, original observation authentication and
shutdown. This standalone runtime owns data SQL; it never opens the ledger, updates
entities, publishes checkpoints or prunes history. Public compact validators retain
their one-hour boundary.
"""

import argparse
import hashlib
import json
import math
import os
import re
import stat
import sys
from contextlib import contextmanager
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic
from urllib.parse import urlsplit

import duckdb

from .completion import _anchor, _arguments, _authority, _digest
from .config import (
    CompactionError,
    Dataset,
    Limits,
    component,
    hour_window,
    separate_roots,
)
from .deletion_recovery import prefix_inventory
from .engine import check_hive_fields, connection, count_rows, schema
from .entities import EntityLimits, host_name
from .entity_authority import Authority
from .entity_authority import binding as authority_binding
from .entity_input_verifier import load_process_inputs
from .entity_store import EntityStore
from .inventory import Inventory, Store, discover
from .phase_evidence import (
    canonical,
    identity,
    load_archive,
    local_directory,
    read_local_record,
    run_identity,
)
from .processing_metrics import (
    ProcessingMetricsStore,
    inventory_hour_stats,
    metric_rows,
    validate_checkpoint_receipts,
)
from .storage import S3Settings, open_s3

MAX_REQUEST_BYTES = 1024 * 1024
MAX_RESULT_BYTES = 64 * 1024
MAX_ENTITY_HOURS = 168
COMMANDS = {"entities-init", "entities-replay"}
ORIGINAL_FIELDS = {
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


def _regular(path, ceiling, *, optional=False):
    path = Path(path)
    if any(p.is_symlink() for p in (path, *path.parents)):
        raise CompactionError("Entity verification refuses aliased local paths")
    try:
        info = path.lstat()
    except FileNotFoundError:
        if optional:
            return None
        raise CompactionError("Entity verification object is missing") from None
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > ceiling:
        raise CompactionError(
            "Entity verification requires bounded single-link objects"
        )
    return info


def _stream_local(path, ceiling, deadline, *, target=None, optional=False):
    """Stream without retaining data rows or trusting a path replaced before open."""
    before = _regular(path, ceiling, optional=optional)
    if before is None:
        return None
    descriptor = os.open(
        path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
    )
    digest, size = hashlib.sha256(), 0
    with os.fdopen(descriptor, "rb") as stream:
        opened = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or (opened.st_dev, opened.st_ino, opened.st_size)
            != (before.st_dev, before.st_ino, before.st_size)
        ):
            raise CompactionError("Entity verification object changed before read")
        while chunk := stream.read(1024 * 1024):
            deadline()
            size += len(chunk)
            if size > before.st_size:
                raise CompactionError("Entity verification object changed during read")
            digest.update(chunk)
            if target is not None:
                target.write(chunk)
        after = os.fstat(stream.fileno())
    current = _regular(path, ceiling)

    def fields(info):
        return (
            info.st_dev,
            info.st_ino,
            info.st_size,
            info.st_mtime_ns,
            info.st_ctime_ns,
            info.st_nlink,
        )

    if (
        size != before.st_size
        or fields(before) != fields(after)
        or fields(before) != fields(current)
    ):
        raise CompactionError("Entity verification object changed during read")
    deadline()
    return digest.hexdigest()


class _Entities(EntityStore):
    def read_object(self, relative, path):
        if self.store.dataset.bucket:
            return super().read_object(relative, path)
        source = Path(self.store.dataset.root) / relative
        if _regular(source, self.limits.max_bytes, optional=True) is None:
            return None
        with Path(path).open("wb") as output:
            return _stream_local(
                source, self.limits.max_bytes, self.store.check_deadline, target=output
            )


class _Metrics(ProcessingMetricsStore):
    def read(self, conn, hour):
        if self.store.dataset.bucket:
            return super().read(conn, hour)
        from .processing_metrics import MAX_CHECKPOINT_BYTES, SCHEMA

        source = Path(self.store.dataset.root) / self.relative(hour)
        if _regular(source, MAX_CHECKPOINT_BYTES, optional=True) is None:
            return None
        with TemporaryDirectory(prefix="entity-checkpoint-") as directory:
            path = Path(directory) / "checkpoint.parquet"
            with path.open("wb") as output:
                _stream_local(
                    source,
                    MAX_CHECKPOINT_BYTES,
                    self.store.check_deadline,
                    target=output,
                )
            self._validate(conn, path)
            values = conn.execute(
                "SELECT * FROM read_parquet(?, hive_partitioning=false)", [str(path)]
            ).fetchone()
            self.store.check_deadline()
            return dict(zip(SCHEMA, values, strict=True))


def _validate_args(args):
    if (
        args.command not in COMMANDS
        or getattr(args, "resume_init", False) is not False
        or getattr(args, "allow_empty_host", False) is not False
        or any(
            hasattr(args, k)
            for k in (
                "target_size_bytes",
                "delete_source",
                "grace_minutes",
                "delete_confirm_attempts",
            )
        )
        or not args.process_entity_root
        or not args.processing_metrics_root
        or not isinstance(args.entity_namespace, str)
        or not 1 <= len(args.entity_namespace) <= 128
        or any(ord(c) < 32 or c in "/\\" for c in args.entity_namespace)
    ):
        raise CompactionError(
            "Entity verifier requires distinct explicit entity-only scope"
        )
    run_identity(args.run_id)
    run_identity(args.coordinator_run_id)
    if args.run_id == args.coordinator_run_id:
        raise CompactionError("Entity verification run identities must be distinct")
    for key, high in (
        ("threads", 64),
        ("max_files", 1_000_000),
        ("max_hours", MAX_ENTITY_HOURS),
        ("max_entity_rows", 1_000_000_000),
        ("max_entity_bytes", 64 * 1024**3),
    ):
        value = getattr(args, key)
        if type(value) is not int or not 1 <= value <= high:
            raise CompactionError("Entity verification integer budget is invalid")
    if (
        type(args.timeout) not in {int, float}
        or not math.isfinite(args.timeout)
        or not 0 < args.timeout <= 86400
        or args.unknown_identity_policy not in {"reject", "quarantine"}
        or type(args.unknown_identity_max_ratio) not in {int, float}
        or not math.isfinite(args.unknown_identity_max_ratio)
        or not 0 <= args.unknown_identity_max_ratio <= 1
    ):
        raise CompactionError("Invalid entity time or identity budget")
    factors = {
        "B": 1,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "KiB": 1024,
        "MiB": 1024**2,
        "GiB": 1024**3,
    }
    for key, ceiling in (
        ("memory_limit", 32 * 1000**3),
        ("max_temp_size", 64 * 1000**3),
    ):
        value = getattr(args, key)
        match = (
            re.fullmatch(r"([1-9]\d{0,11})(B|KB|MB|GB|KiB|MiB|GiB)", value)
            if isinstance(value, str)
            else None
        )
        if match is None or int(match[1]) * factors[match[2]] > ceiling:
            raise CompactionError(
                "Entity resource budget exceeds qualification envelope"
            )
    if not isinstance(args.event_type, list) or not 1 <= len(args.event_type) <= 128:
        raise CompactionError("Entity verification event allowlist is missing")
    for event in args.event_type:
        if not isinstance(event, str) or not re.fullmatch(
            r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", event
        ):
            raise CompactionError("Entity verification event allowlist is invalid")
        component(event)
    if (
        args.event_type != sorted(set(args.event_type))
        or "raw_process" not in args.event_type
    ):
        raise CompactionError(
            "Entity verification requires canonical raw_process scope"
        )
    if (
        not isinstance(args.entity_host, list)
        or not 1 <= len(args.entity_host) <= 64
        or any(
            not isinstance(h, str) or len(h) > 256 or any(ord(c) < 32 for c in h)
            for h in args.entity_host
        )
        or args.entity_host != sorted({host_name(h) for h in args.entity_host})
    ):
        raise CompactionError("Entity verification requires canonical expected hosts")


def _storage(args, remote):
    if not remote:
        if any(
            getattr(args, k) is not None
            for k in ("profile", "region", "endpoint_url", "extension_directory")
        ):
            raise CompactionError("Inactive entity storage must be absent")
        return
    region, origin = args.region, args.endpoint_url
    if (
        args.profile is not None
        or not isinstance(region, str)
        or not re.fullmatch(r"[A-Za-z0-9_.-]{1,256}", region)
        or not isinstance(origin, str)
        or not 1 <= len(origin) <= 4096
        or any(ord(c) < 33 for c in origin)
    ):
        raise CompactionError("Entity verification requires resolved storage scope")
    parsed = urlsplit(origin)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
        or origin != parsed.scheme + "://" + parsed.netloc
    ):
        raise CompactionError("Entity storage must be a non-secret canonical origin")
    try:
        _ = parsed.port
    except ValueError:
        raise CompactionError("Entity storage port is invalid") from None


@contextmanager
def _opened(args, client):
    _validate_args(args)
    if duckdb.__version__ != "2.0.0":
        raise CompactionError("Entity verification requires standalone DuckDB 2.0.0")
    limits = Limits(
        **{
            k: getattr(args, k)
            for k in (
                "threads",
                "memory_limit",
                "max_temp_size",
                "temp_directory",
                "timeout",
                "max_files",
                "max_hours",
            )
        }
    )
    hours = hour_window(args.start_hour, args.end_hour, max_hours=limits.max_hours)
    if args.start_hour != hours[0].strftime("%Y-%m-%dT%H:00:00Z") or args.end_hour != (
        hours[-1] + timedelta(hours=1)
    ).strftime("%Y-%m-%dT%H:00:00Z"):
        raise CompactionError("Entity verification windows require canonical UTC hours")
    roots = [
        Dataset.parse(args.source_root, event_root=True),
        Dataset.parse(args.destination_root, event_root=True),
        Dataset.parse(args.process_entity_root),
        Dataset.parse(args.processing_metrics_root),
    ]
    for i, root in enumerate(roots):
        if not root.bucket:
            # Dataset.parse resolves paths; reject aliases in the original spelling too.
            original = (
                args.source_root,
                args.destination_root,
                args.process_entity_root,
                args.processing_metrics_root,
            )[i]
            path = Path(original).expanduser().absolute()
            if any(p.is_symlink() for p in (path, *path.parents)):
                raise CompactionError("Entity verification refuses aliased data roots")
            if str(path) != root.root:
                raise CompactionError("Entity verification roots must be canonical")
        for other in roots[i + 1 :]:
            separate_roots(root, other)
    archive = Dataset.parse(str(local_directory(args.run_evidence_directory)))
    for root in roots:
        separate_roots(archive, root)
    remote = any(root.bucket for root in roots)
    _storage(args, remote)
    work = [archive]
    for key in ("temp_directory", "extension_directory"):
        value = getattr(args, key)
        if value is None:
            continue
        path = str(local_directory(value))
        if path != value:
            raise CompactionError("Entity work paths require canonical local scope")
        dataset = Dataset.parse(path)
        for other in roots + work:
            separate_roots(dataset, other)
        work.append(dataset)
    expected = {
        "raw_root": roots[0].root,
        "compacted_root": roots[1].root,
        "entity_root": roots[2].root,
        "metrics_root": roots[3].root,
        "namespace": args.entity_namespace,
        "hosts": args.entity_host,
        "identity_policy": args.unknown_identity_policy,
        "identity_max_ratio": args.unknown_identity_max_ratio,
        "window_start": args.start_hour,
        "window_end": args.end_hour,
        "event_types": args.event_type,
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
        source, compacted = [
            Store(root, client, deadline=deadline) for root in roots[:2]
        ]
        entities = _Entities(
            Store(roots[2], client, deadline=deadline),
            args.entity_namespace,
            EntityLimits(args.max_entity_rows, args.max_entity_bytes),
        )
        metrics = _Metrics(Store(roots[3], client, deadline=deadline))
        yield (
            conn,
            limits,
            hours,
            tuple(args.event_type),
            source,
            compacted,
            entities,
            metrics,
            expected,
        )


def _inventory(store, hours, events, ceiling):
    if not store.dataset.bucket and not Path(store.dataset.root).exists():
        return Inventory(
            store.dataset, events, events, tuple(h.isoformat() for h in hours), {}, ()
        )
    return discover(store, hours, events, max_files=ceiling)


def _object_evidence(store, inventory, conn):
    for leaf, files in sorted(inventory.groups.items()):
        for file in files:
            if store.dataset.bucket:
                from .phase_evidence import object_digest

                hashed = object_digest(store, file)
            else:
                hashed = _stream_local(file.uri, file.size, store.check_deadline)
            columns = schema(conn, [file.uri])
            check_hive_fields(conn, [file.uri], leaf, columns)
            yield {
                "leaf": leaf.as_dict(),
                "file": asdict(file),
                "sha256": hashed,
                "rows": count_rows(conn, [file.uri]),
                "schema": columns,
            }


def _inputs(conn, source, compacted, hours, events, limits):
    """Exact selected-event inventories/content/schema/counts, plus process scope."""
    stores = source, compacted
    result = {}
    total_files = 0
    for name, store in zip(("raw", "compacted"), stores, strict=True):
        prefixes = prefix_inventory(store, hours, events, limits.max_files)
        inventory = _inventory(store, hours, events, limits.max_files)
        keys = {
            store.dataset.object_key(f.relative) if store.dataset.bucket else f.uri
            for files in inventory.groups.values()
            for f in files
        }
        if set().union(*prefixes.values()) != keys:
            raise CompactionError("Entity inputs contain unrecorded prefix objects")
        total_files += sum(len(files) for files in inventory.groups.values())
        if total_files > limits.max_files:
            raise CompactionError(
                "Entity selected-input file budget exceeded; no pruning"
            )
        digest, files, size, rows = hashlib.sha256(b"["), 0, 0, 0
        for item in _object_evidence(store, inventory, conn):
            if files:
                digest.update(b",")
            digest.update(canonical(item).encode())
            files += 1
            size += item["file"]["size"]
            rows += item["rows"]
        digest.update(b"]")
        result[name] = {
            "inventory_sha256": inventory.fingerprint,
            "content_sha256": digest.hexdigest(),
            "files": files,
            "bytes": size,
            "rows": rows,
        }
    process = [_inventory(s, hours, ("raw_process",), limits.max_files) for s in stores]
    result.update(
        process_input_fingerprint=hashlib.sha256(
            json.dumps([i.fingerprint for i in process], sort_keys=True).encode()
        ).hexdigest(),
        source_fingerprint=process[0].fingerprint,
        compacted_fingerprint=process[1].fingerprint,
        hours=[
            {
                "hour_start": h.strftime("%Y-%m-%dT%H:00:00Z"),
                "raw": inventory_hour_stats(process[0], h),
                "compacted": inventory_hour_stats(process[1], h),
            }
            for h in hours
        ],
    )
    return result


def _process(conn, source, compacted, args, hours, limits):
    inventories = [
        _inventory(s, hours, ("raw_process",), limits.max_files)
        for s in (source, compacted)
    ]
    exclusions = load_process_inputs(
        conn,
        inventories,
        args,
        hours,
        deadline=source.check_deadline,
    )
    if args.command == "entities-init" and conn.execute(
        "SELECT count(*) FROM unnest(?::VARCHAR[]) h "
        "WHERE NOT EXISTS (SELECT 1 FROM entity_input e WHERE e.hostname=h.unnest)",
        [args.entity_host],
    ).fetchone()[0]:
        raise CompactionError(
            "Entity genesis requires observations for every expected host"
        )
    return inventories, exclusions


def _fresh(entities, limits):
    store = entities.store
    if store.dataset.bucket:
        pages = 0
        prefix = store.dataset.prefix.rstrip("/") + "/" if store.dataset.prefix else ""
        for page in store.client.get_paginator("list_objects_v2").paginate(
            Bucket=store.dataset.bucket, Prefix=prefix
        ):
            store.check_deadline()
            pages += 1
            if pages > 10_000 or page.get("Contents"):
                raise CompactionError(
                    "Entity genesis requires a fresh uninitialized root"
                )
    else:
        root = Path(store.dataset.root)
        if not root.exists():
            return
        remaining, pending = limits.max_files, [(root, 0)]
        while pending:
            directory, depth = pending.pop()
            if depth > 64:
                raise CompactionError("Entity genesis traversal budget exceeded")
            with os.scandir(directory) as entries:
                for entry in entries:
                    store.check_deadline()
                    remaining -= 1
                    if remaining < 0 or not stat.S_ISDIR(
                        entry.stat(follow_symlinks=False).st_mode
                    ):
                        raise CompactionError(
                            "Entity genesis requires a fresh uninitialized root"
                        )
                    pending.append((Path(entry.path), depth + 1))


def _prelaunch_state(
    conn, source, compacted, entities, metrics, args, hours, limits, anchor
):
    if args.command == "entities-init":
        if anchor is not None:
            raise CompactionError("Entity genesis cannot adopt retained authority")
        _fresh(entities, limits)
        checked = None
    else:
        if anchor is None:
            raise CompactionError("Entity replay requires explicit retained authority")
        _authority(conn, source, compacted, entities, args, metrics, anchor)
        # The original retains the observed state, not merely a generation label.
        from .cli import _verify_entity_state

        checked = _verify_entity_state(
            conn, source, compacted, entities, args, metrics.store.dataset
        )
    rows = [metrics.read(conn, h) for h in hours]
    if args.command == "entities-init" and any(r is not None for r in rows):
        raise CompactionError("Entity genesis cannot adopt prior selected checkpoints")
    if checked:
        selected = [(r, h) for r, h in zip(rows, hours, strict=True) if r is not None]
        for row, hour in selected:
            validate_checkpoint_receipts(
                conn,
                [row],
                [hour],
                {
                    "namespace": args.entity_namespace,
                    "hosts": args.entity_host,
                    "identity_policy": args.unknown_identity_policy,
                    "identity_max_ratio": args.unknown_identity_max_ratio,
                },
            )
    return identity(checked) if checked else None, identity(rows)


def _original(value, args, execution_sha256, binding):
    if (
        not isinstance(value, dict)
        or set(value) != ORIGINAL_FIELDS
        or type(value["version"]) is not int
        or value["version"] != 1
        or value["operation"] != "entity-only-inputs"
        or value["status"] != "verified"
        or value["command"] != args.command
        or value["run_id"] != args.coordinator_run_id
        or value["cli_run_id"] != args.run_id
        or value["window_start"] != args.start_hour
        or value["window_end"] != args.end_hour
        or value["execution_sha256"] != execution_sha256
        or value["binding_sha256"] != identity(binding)
    ):
        raise CompactionError("Entity original observation scope/type mismatch")
    _digest(value["checkpoint_state_sha256"])
    if args.command == "entities-init":
        if (
            value["authority_high_water"] is not None
            or value["entity_state_sha256"] is not None
        ):
            raise CompactionError(
                "Entity genesis original must have absent authority/state"
            )
    else:
        _digest(value["entity_state_sha256"])
        if value["authority_high_water"] is None:
            raise CompactionError("Entity replay original must retain authority")
        expected = canonical(
            {
                k: binding[k]
                for k in (
                    "raw_root",
                    "compacted_root",
                    "entity_root",
                    "metrics_root",
                    "namespace",
                    "hosts",
                    "identity_policy",
                    "identity_max_ratio",
                )
            }
        )
        _anchor(value["authority_high_water"], expected)
    inputs = value["inputs"]
    if not isinstance(inputs, dict) or set(inputs) != {
        "raw",
        "compacted",
        "process_input_fingerprint",
        "source_fingerprint",
        "compacted_fingerprint",
        "hours",
    }:
        raise CompactionError("Entity original input observation shape mismatch")
    for name in ("raw", "compacted"):
        item = inputs[name]
        if not isinstance(item, dict) or set(item) != {
            "inventory_sha256",
            "content_sha256",
            "files",
            "bytes",
            "rows",
        }:
            raise CompactionError("Entity original selected-input shape mismatch")
        for key in ("inventory_sha256", "content_sha256"):
            _digest(item[key])
        for key in ("files", "bytes", "rows"):
            if type(item[key]) is not int or not 0 <= item[key] < 2**63:
                raise CompactionError(
                    "Entity original input counts require exact types"
                )
    for key in (
        "process_input_fingerprint",
        "source_fingerprint",
        "compacted_fingerprint",
    ):
        _digest(inputs[key])
    hours = hour_window(args.start_hour, args.end_hour, max_hours=args.max_hours)
    selected = inputs["hours"]
    if not isinstance(selected, list) or len(selected) != len(hours):
        raise CompactionError(
            "Entity original hour observations must cover exact window"
        )
    for row, hour in zip(selected, hours, strict=True):
        if (
            not isinstance(row, dict)
            or set(row) != {"hour_start", "raw", "compacted"}
            or row["hour_start"] != hour.strftime("%Y-%m-%dT%H:00:00Z")
        ):
            raise CompactionError("Entity original hour observation scope mismatch")
        for name in ("raw", "compacted"):
            item = row[name]
            if not isinstance(item, dict) or set(item) != {
                "files",
                "bytes",
                "fingerprint",
            }:
                raise CompactionError("Entity original hour statistics shape mismatch")
            _digest(item["fingerprint"])
            for key in ("files", "bytes"):
                if type(item[key]) is not int or not 0 <= item[key] < 2**63:
                    raise CompactionError(
                        "Entity original hour counts require exact types"
                    )
    if len(canonical(value).encode()) > MAX_RESULT_BYTES:
        raise CompactionError("Entity original observation exceeds byte budget")
    return value


def verify_entity_inputs(
    args, *, execution_sha256, authority_high_water, original=None, client=None
):
    """Observe before COMMIT, or require exact equality after COMMIT/before launch."""
    try:
        _digest(execution_sha256)
        with _opened(args, client) as opened:
            (
                conn,
                limits,
                hours,
                events,
                source,
                compacted,
                entities,
                metrics,
                binding,
            ) = opened
            if original is not None:
                _original(original, args, execution_sha256, binding)
            inputs = _inputs(conn, source, compacted, hours, events, limits)
            _process(conn, source, compacted, args, hours, limits)
            state = _prelaunch_state(
                conn,
                source,
                compacted,
                entities,
                metrics,
                args,
                hours,
                limits,
                authority_high_water,
            )
            result = {
                "version": 1,
                "operation": "entity-only-inputs",
                "status": "verified",
                "command": args.command,
                "run_id": args.coordinator_run_id,
                "cli_run_id": args.run_id,
                "window_start": args.start_hour,
                "window_end": args.end_hour,
                "binding_sha256": identity(binding),
                "execution_sha256": execution_sha256,
                "inputs": inputs,
                "authority_high_water": authority_high_water,
                "entity_state_sha256": state[0],
                "checkpoint_state_sha256": state[1],
            }
            if (
                original is not None
                and canonical(result) != canonical(original)
                or _inputs(conn, source, compacted, hours, events, limits) != inputs
                or _prelaunch_state(
                    conn,
                    source,
                    compacted,
                    entities,
                    metrics,
                    args,
                    hours,
                    limits,
                    authority_high_water,
                )
                != state
            ):
                raise CompactionError(
                    "Entity inputs/authority/checkpoints changed before launch"
                )
            _original(result, args, execution_sha256, binding)
            return result
    except CompactionError:
        raise
    except Exception:
        raise CompactionError(
            "Independent entity input verification failed; details suppressed"
        ) from None


def _grammar(records, args, binding, original):
    if (
        [r["phase"] for r in records]
        != ["run_intent", "entities_intent", "entities_verified", "checkpoint_intent"]
        + ["checkpoint_verified"] * len(original["inputs"]["hours"])
        + ["complete"]
        or any(r["coordinator_run_id"] != args.coordinator_run_id for r in records)
        or canonical(records[0]["data"])
        != canonical({"binding": binding, "command": args.command})
        or canonical(records[1]["data"]) != canonical({"operation": args.command})
        or canonical(records[-1]["data"])
        != canonical(
            {"status": "completed", "exit_code": 0, "migration_complete": False}
        )
    ):
        raise CompactionError("Entity original archive scope/order/terminal mismatch")
    report = records[2]["data"]
    if (
        set(report)
        != {
            "status",
            "hosts",
            "source_fingerprint",
            "compacted_fingerprint",
            "process_input_fingerprint",
            "identity_exclusions",
            "generation_id",
        }
        or report["status"] != "completed"
        or not isinstance(report["hosts"], list)
        or any(
            not isinstance(host, dict)
            or not isinstance(host.get("hostname"), str)
            for host in report["hosts"]
        )
        or [h.get("hostname") for h in report["hosts"]]
        != sorted({h.get("hostname") for h in report["hosts"]})
        or not set(args.entity_host)
        <= {h.get("hostname") for h in report["hosts"]}
        or any(
            report[k] != original["inputs"][k]
            for k in (
                "source_fingerprint",
                "compacted_fingerprint",
                "process_input_fingerprint",
            )
        )
    ):
        raise CompactionError("Entity original publication report scope mismatch")
    _digest(report["generation_id"])
    for host in report["hosts"]:
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
            or any(
                type(host[k]) is not int or not 0 <= host[k] < 2**63
                for k in ("rows", "snapshot_bytes", "parent_conflicts")
            )
        ):
            raise CompactionError("Entity original host statistics require exact types")
        _digest(host["semantic_sha256"])
    return report


def _publication(
    conn,
    source,
    compacted,
    entities,
    metrics,
    args,
    report,
    original,
    current,
    checked,
):
    expected = authority_binding(
        source,
        compacted,
        entities,
        args.entity_host,
        args.unknown_identity_policy,
        args.unknown_identity_max_ratio,
        metrics.store.dataset.root,
    )
    with TemporaryDirectory(prefix="entity-original-lineage-") as directory:
        authority = Authority(conn, entities, directory, expected)
        cursor = conn.execute(
            "SELECT generation, parent_id, status, bootstrap_starts, before_hashes, "
            "snapshot_hashes, semantic_hashes, window_start, window_end, "
            "input_fingerprint FROM entity_authority WHERE generation_id=?",
            [report["generation_id"]],
        )
        row = cursor.fetchone()
        if row is None or cursor.fetchone() is not None:
            raise CompactionError(
                "Entity original generation is absent from retained lineage"
            )
        (
            generation,
            parent,
            status,
            bootstraps,
            before,
            snapshots,
            semantics,
            start,
            end,
            fingerprint,
        ) = row
        anchor = original["authority_high_water"]
        if (
            status != "committed"
            or generation != (anchor["generation"] + 1 if anchor else 1)
            or parent != (anchor["generation_id"] if anchor else "")
            or start != args.start_hour
            or end != args.end_hour
            or fingerprint != original["inputs"]["process_input_fingerprint"]
            or authority.latest["generation_id"] != current["generation_id"]
        ):
            raise CompactionError(
                "Entity original publication/window/input lineage mismatch"
            )
        before, snapshots, semantics, bootstraps = map(
            json.loads, (before, snapshots, semantics, bootstraps)
        )
        report_hosts = {host["hostname"] for host in report["hosts"]}
        expected_hosts = set(args.entity_host)
        expected_hosts.update(
            row[0]
            for row in conn.execute(
                "SELECT DISTINCT hostname FROM entity_input"
            ).fetchall()
        )
        anchor = original["authority_high_water"]
        if anchor is not None:
            previous = conn.execute(
                "SELECT snapshot_hashes FROM entity_authority WHERE generation=? "
                "AND generation_id=? AND status='committed'",
                [anchor["generation"], anchor["generation_id"]],
            ).fetchone()
            if previous is None:
                raise CompactionError("Entity original prior host receipt is missing")
            expected_hosts.update(json.loads(previous[0]))
        if report_hosts != expected_hosts or report_hosts != set(snapshots):
            raise CompactionError(
                "Entity original publication host discovery mismatch"
            )
        for host in report["hosts"]:
            name = host["hostname"]
            if (
                host["semantic_sha256"] != semantics[name]
                or host["bootstrap_start"] != bootstraps[name]
                or host["changed"] != (before[name] != snapshots[name])
            ):
                raise CompactionError(
                    "Entity original host publication/provenance mismatch"
                )
            if current["generation_id"] == report["generation_id"]:
                observed = next(h for h in checked["hosts"] if h["hostname"] == name)
                path = Path(directory) / "snapshot.parquet"
                if (
                    entities.read(name, path) != snapshots[name]
                    or host["rows"] != observed["rows"]
                    or host["snapshot_bytes"] != path.stat().st_size
                    or entities.sql_state_hash(conn, name, path) != semantics[name]
                    or host["parent_conflicts"]
                    != conn.execute(
                        "SELECT count(*) FROM read_parquet(?, hive_partitioning=false) "
                        "WHERE parent_status='conflicted'",
                        [str(path)],
                    ).fetchone()[0]
                ):
                    raise CompactionError(
                        "Entity original current snapshot statistics mismatch"
                    )


def _checkpoints(
    conn, records, report, inventories, exclusions, args, binding, hours, metrics
):
    candidates = records[3]["data"]
    if set(candidates) != {"rows"} or not isinstance(candidates["rows"], list):
        raise CompactionError("Entity original checkpoint candidates missing")
    rows = candidates["rows"]
    validate_checkpoint_receipts(conn, rows, hours, binding)
    if canonical(report["identity_exclusions"]) != canonical(exclusions):
        raise CompactionError(
            "Entity original identity-exclusion input evidence mismatch"
        )
    expected = metric_rows(
        {},
        report,
        *inventories,
        hours,
        namespace=args.entity_namespace,
        hosts=tuple(args.entity_host),
        run_id=args.run_id,
        operation=args.command,
        completed_at=rows[0]["completed_at"],
    )
    if canonical(rows) != canonical(expected):
        raise CompactionError(
            "Entity original checkpoint counts/input/operation mismatch"
        )
    for record, row, hour in zip(records[4:-1], rows, hours, strict=True):
        if canonical(record["data"]) != canonical(
            {"hour_start": row["hour_start"], "row": row}
        ) or canonical(metrics.read(conn, hour)) != canonical(row):
            raise CompactionError(
                "Entity original checkpoint publication missing or changed"
            )
    return rows


def _publication_observation(publication, result, original, binding):
    """Caller authenticates retained independent evidence, never reconstructs it.

    A past generation's row/byte/conflict statistics cannot be checked against a
    forward-evolved snapshot. Require a separate observation made while that exact
    generation was current, binding this same original archive/checkpoints. A
    self-consistent rehashed archive is not a replacement for that observation.
    """
    historical = (
        result["checkpoint_generation_id"]
        != result["authority_high_water"]["generation_id"]
    )
    if publication is None:
        if historical:
            raise CompactionError(
                "Historical entity publication needs independent original evidence"
            )
        return
    if (
        not isinstance(publication, dict)
        or set(publication) != COMPLETION_FIELDS
        or type(publication["version"]) is not int
        or publication["version"] != 1
        or publication["publication_sha256"] is not None
        or len(canonical(publication).encode()) > MAX_RESULT_BYTES
    ):
        raise CompactionError("Malformed independent entity publication observation")
    for key in COMPLETION_FIELDS - {
        "authority_high_water",
        "entity_state_sha256",
        "publication_sha256",
    }:
        if canonical(publication[key]) != canonical(result[key]):
            raise CompactionError(
                "Independent entity publication archive/scope mismatch"
            )
    expected = canonical(
        {
            k: binding[k]
            for k in (
                "raw_root",
                "compacted_root",
                "entity_root",
                "metrics_root",
                "namespace",
                "hosts",
                "identity_policy",
                "identity_max_ratio",
            )
        }
    )
    anchor = publication["authority_high_water"]
    _anchor(anchor, expected)
    retained = original["authority_high_water"]
    if (
        anchor is None
        or anchor["generation_id"] != result["checkpoint_generation_id"]
        or anchor["generation"] != (retained["generation"] + 1 if retained else 1)
    ):
        raise CompactionError(
            "Independent publication must observe original generation"
        )
    _digest(publication["entity_state_sha256"])
    if (
        not historical
        and publication["entity_state_sha256"] != result["entity_state_sha256"]
    ):
        raise CompactionError("Independent publication snapshot observation changed")


def verify_entity_completion(
    args,
    *,
    execution_sha256,
    supervision_sha256,
    original,
    publication=None,
    client=None,
):
    """Authenticate original entity/checkpoint phases and unchanged selected data."""
    try:
        _digest(execution_sha256)
        _digest(supervision_sha256)
        with _opened(args, client) as opened:
            (
                conn,
                limits,
                hours,
                events,
                source,
                compacted,
                entities,
                metrics,
                binding,
            ) = opened
            _original(original, args, execution_sha256, binding)
            inputs = _inputs(conn, source, compacted, hours, events, limits)
            if canonical(inputs) != canonical(original["inputs"]):
                raise CompactionError("Entity original raw/compacted inputs changed")
            inventories, exclusions = _process(
                conn, source, compacted, args, hours, limits
            )
            root = str(local_directory(args.run_evidence_directory))
            records = load_archive(root, args.run_id)
            report = _grammar(records, args, binding, original)
            current, checked = _authority(
                conn,
                source,
                compacted,
                entities,
                args,
                metrics,
                original["authority_high_water"],
            )
            _publication(
                conn,
                source,
                compacted,
                entities,
                metrics,
                args,
                report,
                original,
                current,
                checked,
            )
            rows = _checkpoints(
                conn,
                records,
                report,
                inventories,
                exclusions,
                args,
                binding,
                hours,
                metrics,
            )
            if (
                _inputs(conn, source, compacted, hours, events, limits) != inputs
                or _authority(conn, source, compacted, entities, args, metrics, current)
                != (current, checked)
                or canonical([metrics.read(conn, h) for h in hours]) != canonical(rows)
                or load_archive(root, args.run_id) != records
            ):
                raise CompactionError(
                    "Entity completion evidence changed during verification"
                )
            result = {
                "version": 1,
                "operation": "entity-only-completion",
                "status": "verified",
                "command": args.command,
                "run_id": args.coordinator_run_id,
                "cli_run_id": args.run_id,
                "window_start": args.start_hour,
                "window_end": args.end_hour,
                "binding_sha256": identity(binding),
                "execution_sha256": execution_sha256,
                "supervision_sha256": supervision_sha256,
                "inputs_sha256": identity(inputs),
                "original_sha256": identity(original),
                "archive_sha256": identity(records[-1]),
                "archive_records": len(records),
                "authority_high_water": current,
                "entity_state_sha256": identity(checked),
                "checkpoint_sha256": identity(rows),
                "checkpoint_generation_id": report["generation_id"],
                "checkpoint_hours": len(hours),
                "publication_sha256": (
                    identity(publication) if publication is not None else None
                ),
            }
            _publication_observation(publication, result, original, binding)
            if len(canonical(result).encode()) > MAX_RESULT_BYTES:
                raise CompactionError("Entity completion evidence exceeds byte budget")
            return result
    except CompactionError:
        raise
    except Exception:
        raise CompactionError(
            "Independent entity completion verification failed; details suppressed"
        ) from None


def _request(path, expected_sha256):
    try:
        _digest(expected_sha256)
        payload = read_local_record(path, MAX_REQUEST_BYTES)
        value = json.loads(payload)
        completion = value.get("operation") == "entity-only-completion"
        fields = {
            "version",
            "operation",
            "arguments",
            "execution_sha256",
            "ledger_id",
            "dataset_sha256",
            "authority_high_water",
            "original",
        } | (
            {"supervision_sha256", "intent_sha256", "publication"}
            if completion
            else set()
        )
        if (
            not isinstance(value, dict)
            or set(value) != fields
            or type(value["version"]) is not int
            or value["version"] != 1
            or value["operation"]
            not in {"entity-only-inputs", "entity-only-completion"}
            or hashlib.sha256(payload).hexdigest() != expected_sha256
            or canonical(value).encode() != payload
            or completion
            and value["original"] is None
        ):
            raise ValueError
        run_identity(value["ledger_id"])
        for key in ("execution_sha256", "dataset_sha256") + (
            ("supervision_sha256", "intent_sha256") if completion else ()
        ):
            _digest(value[key])
        args = _arguments(value["arguments"])
        _validate_args(args)
        return value, args
    except Exception:
        raise CompactionError(
            "Malformed entity verification request; details suppressed"
        ) from None


def main(argv=None):
    outer = argparse.ArgumentParser(description=__doc__)
    outer.add_argument("--request", required=True)
    outer.add_argument("--request-sha256", required=True)
    selected = outer.parse_args(argv)
    try:
        request, args = _request(selected.request, selected.request_sha256)
        if request["operation"] == "entity-only-inputs":
            result = verify_entity_inputs(
                args,
                execution_sha256=request["execution_sha256"],
                authority_high_water=request["authority_high_water"],
                original=request["original"],
            )
        else:
            if canonical(request["authority_high_water"]) != canonical(
                request["original"]["authority_high_water"]
            ):
                raise CompactionError(
                    "Entity completion request original authority mismatch"
                )
            result = verify_entity_completion(
                args,
                execution_sha256=request["execution_sha256"],
                supervision_sha256=request["supervision_sha256"],
                original=request["original"],
                publication=request["publication"],
            )
            result["intent_sha256"] = request["intent_sha256"]
        result.update(
            ledger_id=request["ledger_id"],
            dataset_sha256=request["dataset_sha256"],
            request_sha256=selected.request_sha256,
        )
        if (
            _request(selected.request, selected.request_sha256)[0] != request
            or len(canonical(result).encode()) > MAX_RESULT_BYTES
        ):
            raise CompactionError(
                "Entity verification request changed or result exceeds budget"
            )
        print(canonical(result))
        return 0
    except Exception:
        print(
            canonical(
                {
                    "status": "error",
                    "message": (
                        "Independent entity verification failed; details suppressed"
                    ),
                }
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
