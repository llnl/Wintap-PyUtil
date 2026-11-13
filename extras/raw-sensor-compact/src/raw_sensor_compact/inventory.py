"""Read-only, prefix-bounded discovery; no source mutation API."""

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from time import monotonic

from .config import CompactionError, Dataset, Leaf, component


@dataclass(frozen=True)
class ObjectFile:
    relative: str
    uri: str
    size: int
    etag: str = ""
    modified: str = ""


class Store:
    def __init__(self, dataset: Dataset, client=None, *, deadline: float | None = None):
        self.dataset = dataset
        self.client = client
        self.deadline = deadline
        if dataset.bucket and client is None:
            raise CompactionError("S3 discovery requires a configured client")

    def check_deadline(self):
        if self.deadline is not None and monotonic() >= self.deadline:
            raise CompactionError("Command timeout exceeded")

    def events(self) -> tuple[str, ...]:
        self.check_deadline()
        dataset = self.dataset
        if not dataset.bucket:
            root = Path(dataset.root)
            if not root.is_dir():
                raise CompactionError("Local dataset root does not exist")
            raw = root if dataset.event_root else root / "raw_sensor"
            if raw.is_symlink():
                raise CompactionError(
                    "Symlinked raw_sensor directories are unsupported"
                )
            if not raw.exists():
                return ()
            events = []
            for path in raw.iterdir():
                self.check_deadline()
                if path.is_symlink():
                    raise CompactionError("Symlinked event directories are unsupported")
                if path.is_dir():
                    events.append(component(path.name))
            return tuple(sorted(events))
        prefix = dataset.object_key("raw_sensor/")
        events = set()
        try:
            for page in self.client.get_paginator("list_objects_v2").paginate(
                Bucket=dataset.bucket, Prefix=prefix, Delimiter="/"
            ):
                self.check_deadline()
                for item in page.get("CommonPrefixes", []):
                    self.check_deadline()
                    value = item["Prefix"]
                    if not value.startswith(prefix) or not value.endswith("/"):
                        raise CompactionError("Unexpected S3 event prefix")
                    event = component(value[len(prefix) : -1])
                    events.add(event)
                    if len(events) > 100:
                        raise CompactionError(
                            "Too many event families; select explicit events"
                        )
        except CompactionError:
            raise
        except Exception:
            raise CompactionError(
                "S3 event listing failed; details suppressed"
            ) from None
        return tuple(sorted(events))

    def objects(self, prefix: str):
        """Yield only the requested relative hour prefix's Parquet files."""
        self.check_deadline()
        dataset = self.dataset
        if not dataset.bucket:
            root = Path(dataset.root)
            if not root.is_dir():
                raise CompactionError("Local dataset root does not exist")
            prefix = dataset.storage_relative(prefix)
            directory = root / prefix
            # Reject symlinks in every existing prefix component, not just files.
            check = root
            for part in Path(prefix).parts:
                check /= part
                if check.is_symlink():
                    raise CompactionError(
                        "Symlinked partition directories are unsupported"
                    )
            if not directory.exists():
                return
            yield from self._local_objects(directory)
            return
        key_prefix = dataset.object_key(prefix)
        try:
            for page in self.client.get_paginator("list_objects_v2").paginate(
                Bucket=dataset.bucket, Prefix=key_prefix
            ):
                self.check_deadline()
                for item in page.get("Contents", []):
                    self.check_deadline()
                    key = item["Key"]
                    if not key.startswith(key_prefix):
                        raise CompactionError("S3 returned an out-of-scope object")
                    if not key.endswith(".parquet"):
                        continue
                    relative = key[len(dataset.prefix) + 1 :] if dataset.prefix else key
                    relative = dataset.inventory_relative(relative)
                    yield ObjectFile(
                        relative,
                        dataset.file_uri(relative),
                        int(item["Size"]),
                        item.get("ETag", ""),
                        str(item.get("LastModified", "")),
                    )
        except CompactionError:
            raise
        except Exception:
            raise CompactionError(
                "S3 partition listing failed; details suppressed"
            ) from None

    def _local_objects(self, directory: Path):
        for path in sorted(directory.iterdir()):
            self.check_deadline()
            if path.is_symlink():
                raise CompactionError("Symlinked source paths are unsupported")
            if path.is_dir():
                yield from self._local_objects(path)
            elif path.name.endswith(".parquet"):
                stat = path.stat()
                relative = self.dataset.inventory_relative(
                    path.relative_to(self.dataset.root).as_posix()
                )
                yield ObjectFile(
                    relative, str(path), stat.st_size, modified=str(stat.st_mtime_ns)
                )

    def exists(self, prefix: str) -> bool:
        """Any destination hour path/object means skip, not verified completion."""
        self.check_deadline()
        if not self.dataset.bucket:
            path = Path(self.dataset.root)
            for part in Path(self.dataset.storage_relative(prefix)).parts:
                path /= part
                if path.is_symlink():
                    raise CompactionError("Symlinked destination paths are unsupported")
            return path.exists()
        key_prefix = self.dataset.object_key(prefix)
        try:
            page = self.client.list_objects_v2(
                Bucket=self.dataset.bucket, Prefix=key_prefix, MaxKeys=1
            )
            self.check_deadline()
            contents = page.get("Contents", [])
            if any(not item["Key"].startswith(key_prefix) for item in contents):
                raise CompactionError("S3 returned an out-of-scope destination object")
            return bool(contents)
        except CompactionError:
            raise
        except Exception:
            raise CompactionError("Destination existence check failed") from None


@dataclass
class Inventory:
    dataset: Dataset
    events: tuple[str, ...]
    requested_events: tuple[str, ...]
    hours: tuple[str, ...]
    groups: dict[Leaf, tuple[ObjectFile, ...]]
    empty_hours: tuple[str, ...]

    @property
    def fingerprint(self) -> str:
        content = {
            "dataset": self.dataset.root,
            "events": self.events,
            "hours": self.hours,
            "objects": [
                asdict(file)
                for leaf in sorted(self.groups)
                for file in self.groups[leaf]
            ],
        }
        return hashlib.sha256(json.dumps(content, sort_keys=True).encode()).hexdigest()


def discover(
    store: Store,
    hours: tuple[datetime, ...],
    events: tuple[str, ...] = (),
    *,
    max_files: int = 10_000,
) -> Inventory:
    families = tuple(sorted({component(event) for event in events})) or store.events()
    groups: dict[Leaf, list[ObjectFile]] = {}
    empty_hours = []
    seen = {}
    for event in families:
        for hour in hours:
            expected = Leaf(event, hour.strftime("%Y%m%d"), hour.strftime("%H"))
            found = False
            for file in store.objects(expected.relative):
                leaf = Leaf.from_relative(file.relative)
                if (leaf.event_type, leaf.day, leaf.hour) != (
                    expected.event_type,
                    expected.day,
                    expected.hour,
                ) or file.size < 0:
                    raise CompactionError(
                        "Object does not match the requested partition"
                    )
                found = True
                previous = seen.get(file.uri)
                if previous is not None:
                    if previous != file:
                        raise CompactionError(
                            "Conflicting identities for one source object"
                        )
                    continue
                seen[file.uri] = file
                if len(seen) > max_files:
                    raise CompactionError(
                        "File limit exceeded; input was not truncated"
                    )
                groups.setdefault(leaf, []).append(file)
            if not found:
                empty_hours.append(expected.relative)
    return Inventory(
        store.dataset,
        families,
        events,
        tuple(h.isoformat() for h in hours),
        {
            leaf: tuple(sorted(files, key=lambda f: f.uri))
            for leaf, files in groups.items()
        },
        tuple(empty_hours),
    )


def recheck(store: Store, inventory: Inventory, hours, *, max_files: int) -> None:
    # Preserve automatic family discovery, including newly appearing families.
    current = discover(store, hours, inventory.requested_events, max_files=max_files)
    if current.fingerprint != inventory.fingerprint:
        raise CompactionError("Dataset inventory changed during processing; retry")
