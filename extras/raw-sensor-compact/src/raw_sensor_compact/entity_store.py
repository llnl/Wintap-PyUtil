"""Validated fixed-object snapshots. Requires external writer/reader exclusion."""

import hashlib
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

from botocore.exceptions import ClientError

from .config import CompactionError, separate_roots
from .entities import (
    SCHEMA,
    VERSION,
    EntityLimits,
    Evidence,
    canonical,
    host_key,
    host_name,
    rows,
)
from .inventory import Inventory, discover
from .storage import quote


def digest(path):
    result = hashlib.sha256()
    with open(path, "rb") as stream:
        while chunk := stream.read(1024 * 1024):
            result.update(chunk)
    return result.hexdigest()


class EntityStore:
    def __init__(self, store, namespace, limits=None):
        if not namespace or any(c in namespace for c in "\x00\n\r"):
            raise CompactionError("An explicit entity identity namespace is required")
        self.store = store
        self.namespace = namespace
        self.limits = limits or EntityLimits()

    def relative(self, host):
        return f"hostPK={host_key(host)}/entities.parquet"

    def read(self, host, path):
        """Return content identity, or None only for an exact absent object."""
        self.store.check_deadline()
        dataset = self.store.dataset
        relative = self.relative(host)
        if dataset.bucket:
            try:
                response = self.store.client.get_object(
                    Bucket=dataset.bucket, Key=dataset.object_key(relative)
                )
            except ClientError as error:
                if error.response["Error"]["Code"] in {"NoSuchKey", "404"}:
                    return None
                raise CompactionError("Entity GET failed; source retained") from None
            body = response["Body"]
            try:
                if response.get("ContentLength", 0) > self.limits.max_bytes:
                    raise CompactionError("Entity snapshot exceeds byte budget")
                with open(path, "wb") as output:
                    size = 0
                    while chunk := body.read(1024 * 1024):
                        self.store.check_deadline()
                        size += len(chunk)
                        if size > self.limits.max_bytes:
                            raise CompactionError("Entity snapshot exceeds byte budget")
                        output.write(chunk)
            finally:
                body.close()
        else:
            source = Path(dataset.root) / relative
            if any(p.is_symlink() for p in (source, *source.parents)):
                raise CompactionError("Symlinked entity paths unsupported")
            if not source.exists():
                return None
            if source.stat().st_size > self.limits.max_bytes:
                raise CompactionError("Entity snapshot exceeds byte budget")
            with source.open("rb") as stream, open(path, "wb") as output:
                while chunk := stream.read(1024 * 1024):
                    self.store.check_deadline()
                    output.write(chunk)
        return digest(path)

    def publish(self, host, candidate):
        self.store.check_deadline()
        dataset = self.store.dataset
        relative = self.relative(host)
        if dataset.bucket:
            # One complete PUT; no conditional-write assumption or remote temp keys.
            with open(candidate, "rb") as stream:
                self.store.client.put_object(
                    Bucket=dataset.bucket, Key=dataset.object_key(relative), Body=stream
                )
        else:
            target = Path(dataset.root) / relative
            if any(p.is_symlink() for p in (target, *target.parents)):
                raise CompactionError("Symlinked entity paths unsupported")
            target.parent.mkdir(parents=True, exist_ok=True)
            # Candidate and live root can be on different filesystems.
            from tempfile import NamedTemporaryFile

            staging = None
            try:
                with (
                    NamedTemporaryFile(dir=target.parent, delete=False) as output,
                    open(candidate, "rb") as source,
                ):
                    staging = output.name
                    while chunk := source.read(1024 * 1024):
                        output.write(chunk)
                    output.flush()
                    os.fsync(output.fileno())
                os.replace(staging, target)
            finally:
                if staging and os.path.exists(staging):
                    os.unlink(staging)

    def metadata(self, host, bootstrap):
        return {
            "entity_version": VERSION,
            "hostname": host_name(host),
            "identity_namespace": self.namespace,
            "bootstrap_start": bootstrap,
            "time_basis": "producer_filetime;publication_partition",
        }

    def validate(self, conn, path, host):
        actual = {
            r[0]: r[1]
            for r in conn.execute(
                "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)",
                [str(path)],
            ).fetchall()
        }
        if actual != SCHEMA:
            raise CompactionError("Entity snapshot schema/version mismatch")
        metadata = dict(
            conn.execute(
                "SELECT decode(key), decode(value) FROM parquet_kv_metadata(?)",
                [str(path)],
            ).fetchall()
        )
        if metadata != self.metadata(host, metadata.get("bootstrap_start", "")):
            raise CompactionError("Entity host/namespace/coverage metadata mismatch")
        from .config import utc_hour

        utc_hour(metadata["bootstrap_start"])
        count = conn.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(path)]
        ).fetchone()[0]
        if (
            count > self.limits.max_rows
            or Path(path).stat().st_size > self.limits.max_bytes
        ):
            raise CompactionError("Entity snapshot exceeds row/byte budget")
        state = {}
        budget = 0
        for row in rows(
            conn, "SELECT * FROM read_parquet(?, hive_partitioning=false)", [str(path)]
        ):
            self.store.check_deadline()
            key = row["pid_hash"]
            from .entities import unknown

            if row["hostname"] != host_name(host) or unknown(key, host) or key in state:
                raise CompactionError("Invalid/duplicate entity instance key")
            evidence = Evidence.restore(row)
            from datetime import datetime

            try:
                for field in ("first_partition", "last_partition"):
                    datetime.strptime(row[field], "%Y%m%d%H")
                if row["first_partition"] > row["last_partition"]:
                    raise ValueError()
            except (ValueError, TypeError):
                raise CompactionError("Invalid entity publication bounds") from None
            if evidence.row(host_name(host), key) != row:
                raise CompactionError("Entity scalar fields disagree with evidence")
            state[key] = evidence
            budget += evidence.bytes + 1024
            if budget > self.limits.max_bytes:
                raise CompactionError("Entity decoded state exceeds byte budget")
        return state, metadata

    def write(self, conn, path, host, state, metadata):
        if len(state) > self.limits.max_rows:
            raise CompactionError("Entity snapshot exceeds row budget")
        conn.execute("DROP TABLE IF EXISTS entity_output")
        conn.execute(
            "CREATE TEMP TABLE entity_output ("
            + ",".join(f"{k} {v}" for k, v in SCHEMA.items())
            + ")"
        )
        budget = 0
        for key in sorted(state):
            self.store.check_deadline()
            row = state[key].row(host_name(host), key)
            budget += len(canonical(row)) + 1024
            if budget > self.limits.max_bytes:
                raise CompactionError("Entity decoded state exceeds byte budget")
            conn.execute(
                "INSERT INTO entity_output VALUES ("
                + ",".join("?" for _ in SCHEMA)
                + ")",
                list(row.values()),
            )
        kv = ",".join(quote(k) + ": " + quote(v) for k, v in metadata.items())
        conn.execute(
            "COPY entity_output TO ? (FORMAT PARQUET, COMPRESSION ZSTD, "
            f"KV_METADATA {{{kv}}})",
            [str(path)],
        )
        self.validate(conn, path, host)


def entity_inventory(store, hours, max_files):
    # Either archive root may be absent locally, e.g. first bootstrap before COPY.
    # Preserve read-only planning and detect a root appearing during processing.
    if not store.dataset.bucket and not Path(store.dataset.root).exists():
        from .config import Leaf

        return Inventory(
            store.dataset,
            ("raw_process",),
            ("raw_process",),
            tuple(h.isoformat() for h in hours),
            {},
            tuple(
                Leaf("raw_process", h.strftime("%Y%m%d"), h.strftime("%H")).relative
                for h in hours
            ),
        )
    return discover(store, hours, ("raw_process",), max_files=max_files)


def recheck_entity_inputs(store, inventory, hours, max_files):
    if entity_inventory(store, hours, max_files).fingerprint != inventory.fingerprint:
        raise CompactionError("Process archive inventory changed during entity update")


def update_entities(
    conn,
    source,
    compacted,
    entities,
    hours,
    hosts,
    *,
    initialize=False,
    plan=False,
    allow_empty=False,
    max_files=10000,
    resume_init=False,
    identity_policy="reject",
):
    """Prepare all hosts, recheck every input, publish/verify; no source deletion."""
    from .entities import load_inputs

    hosts = tuple(sorted({host_name(h) for h in hosts}))
    if not hosts:
        raise CompactionError("Explicit expected entity hosts are required")
    for other in (source, compacted):
        separate_roots(other.dataset, entities.store.dataset)
    inventories = [entity_inventory(s, hours, max_files) for s in (source, compacted)]
    if (
        sum(
            len(files)
            for inventory in inventories
            for files in inventory.groups.values()
        )
        > max_files
    ):
        raise CompactionError(
            "Entity archive file limit exceeded; inputs not truncated"
        )
    identity_exclusions = load_inputs(
        conn, inventories, identity_policy=identity_policy
    )
    observed = {
        r[0]
        for r in conn.execute("SELECT DISTINCT hostname FROM entity_input").fetchall()
    }
    if not observed <= set(hosts):
        raise CompactionError("Observed host is not explicitly initialized/expected")
    if initialize and not allow_empty and observed != set(hosts):
        raise CompactionError("Empty host bootstrap requires --allow-empty-host")
    report = {
        "status": "planned" if plan else "completed",
        "hosts": [],
        "source_fingerprint": inventories[0].fingerprint,
        "compacted_fingerprint": inventories[1].fingerprint,
        "process_input_fingerprint": hashlib.sha256(
            json.dumps(
                [inventory.fingerprint for inventory in inventories], sort_keys=True
            ).encode()
        ).hexdigest(),
        "identity_exclusions": identity_exclusions,
    }
    with TemporaryDirectory(prefix="process-entities-") as directory:
        directory = Path(directory)
        prepared = []
        for host in hosts:
            old = directory / (host_key(host) + "-old.parquet")
            identity = entities.read(host, old)
            if initialize and identity is not None and not resume_init:
                raise CompactionError(
                    "Entity init requires absent host snapshots; use replay"
                )
            if not initialize and identity is None:
                raise CompactionError(
                    "Expected entity snapshot missing; initialize/rebuild explicitly"
                )
            if identity is None:
                state, metadata = (
                    {},
                    entities.metadata(host, hours[0].strftime("%Y-%m-%dT%H:00:00Z")),
                )
            else:
                state, metadata = entities.validate(conn, old, host)
            before = canonical(
                {key: e.row(host, key) for key, e in sorted(state.items())}
            )
            decoded_bytes = sum(e.bytes + 1024 for e in state.values())
            for row in rows(
                conn, "SELECT * FROM entity_input WHERE hostname=?", [host]
            ):
                key = row["pidhash"]
                if key not in state:
                    state[key] = Evidence()
                    decoded_bytes += 1024
                previous_bytes = state[key].bytes
                state[key].observe(row)
                decoded_bytes += state[key].bytes - previous_bytes
                if decoded_bytes > entities.limits.max_bytes:
                    raise CompactionError("Entity decoded state exceeds byte budget")
                if len(state) > entities.limits.max_rows:
                    raise CompactionError("Entity snapshot exceeds row budget")
                entities.store.check_deadline()
            after = canonical(
                {key: e.row(host, key) for key, e in sorted(state.items())}
            )
            if len(after) > entities.limits.max_bytes:
                raise CompactionError("Entity decoded state exceeds byte budget")
            changed = identity is None or before != after
            candidate = directory / (host_key(host) + "-new.parquet")
            if changed:
                entities.write(conn, candidate, host, state, metadata)
            report["hosts"].append(
                {
                    "hostname": host,
                    "rows": len(state),
                    "changed": changed,
                    "semantic_sha256": hashlib.sha256(after.encode()).hexdigest(),
                    "bootstrap_start": metadata["bootstrap_start"],
                    "snapshot_bytes": candidate.stat().st_size
                    if changed
                    else old.stat().st_size,
                    "parent_conflicts": sum(
                        e.row(host, k)["parent_status"] == "conflicted"
                        for k, e in state.items()
                    ),
                }
            )
            prepared.append((host, candidate, identity, changed))
        for store, inventory in zip((source, compacted), inventories, strict=False):
            recheck_entity_inputs(store, inventory, hours, max_files)
        for host, _candidate, identity, _changed in prepared:
            check = directory / "check.parquet"
            if entities.read(host, check) != identity:
                raise CompactionError("Entity state changed during preparation")
        if not plan:
            published = []
            try:
                for host, candidate, _identity, changed in prepared:
                    if changed:
                        published.append(host)
                        entities.publish(host, candidate)
                        check = directory / "published.parquet"
                        if entities.read(host, check) != digest(candidate):
                            raise CompactionError(
                                "Published entity content verification failed"
                            )
                        entities.validate(conn, check, host)
            except Exception:
                raise CompactionError(
                    "Entity publication/verification failed; no source deletion; "
                    "possibly advanced hosts: " + ",".join(published)
                ) from None
        for store, inventory in zip((source, compacted), inventories, strict=False):
            recheck_entity_inputs(store, inventory, hours, max_files)
    return report, inventories[0]
