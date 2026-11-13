"""Explicit dataset roots, UTC windows, and resource limits."""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit


class CompactionError(Exception):
    """Safe, operator-facing failure (never raw SDK or secret SQL text)."""


def component(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value) or value in {".", ".."}:
        raise CompactionError("Invalid event/protocol path component")
    return value


@dataclass(frozen=True)
class Dataset:
    root: str
    bucket: str = ""
    prefix: str = ""
    event_root: bool = False

    @classmethod
    def parse(cls, value: str, *, event_root: bool = False) -> "Dataset":
        if not value or any(c in value for c in "*?[]\\\x00\n\r"):
            raise CompactionError("Dataset roots must be explicit paths without globs")
        if value.startswith("s3://"):
            uri = urlsplit(value)
            if (
                not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", uri.netloc)
                or uri.query
                or uri.fragment
                or "%" in uri.path
            ):
                raise CompactionError("Invalid S3 dataset root")
            prefix = uri.path.strip("/")
            if any(p in {"", ".", ".."} for p in prefix.split("/")) and prefix:
                raise CompactionError("Dataset prefixes cannot contain traversal")
            if not event_root and prefix.split("/")[-1] == "raw_sensor":
                raise CompactionError("Dataset root must be the parent of raw_sensor")
            root = f"s3://{uri.netloc}" + (f"/{prefix}" if prefix else "")
            return cls(root, uri.netloc, prefix, event_root)
        if "://" in value:
            raise CompactionError(
                "Only local directories and s3:// roots are supported"
            )
        path = Path(value).expanduser().resolve()
        if not event_root and path.name == "raw_sensor":
            raise CompactionError("Dataset root must be the parent of raw_sensor")
        return cls(str(path), event_root=event_root)

    def storage_relative(self, relative: str) -> str:
        """Map normalized inventory paths to a direct event root when requested."""
        if not self.event_root or not relative:
            return relative
        if not relative.startswith("raw_sensor/"):
            raise CompactionError("Unexpected normalized dataset-relative path")
        return relative.removeprefix("raw_sensor/")

    def inventory_relative(self, relative: str) -> str:
        return "raw_sensor/" + relative if self.event_root else relative

    def file_uri(self, relative: str) -> str:
        return self.root.rstrip("/") + "/" + self.storage_relative(relative)

    def object_key(self, relative: str) -> str:
        return (self.prefix + "/" if self.prefix else "") + self.storage_relative(
            relative
        )


def separate_roots(source: Dataset, destination: Dataset) -> None:
    if bool(source.bucket) != bool(destination.bucket):
        return
    if source.bucket != destination.bucket:
        return
    a, b = source.root.rstrip("/") + "/", destination.root.rstrip("/") + "/"
    if a.startswith(b) or b.startswith(a):
        raise CompactionError("Source and destination dataset namespaces overlap")


def utc_hour(value: str) -> datetime:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:00:00Z", value):
        raise CompactionError("Hours must use whole-hour UTC YYYY-MM-DDTHH:00:00Z")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:00:00Z").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        raise CompactionError("Invalid UTC hour") from None


def hour_window(
    start: str, end: str, *, max_hours: int = 24, now: datetime | None = None
) -> tuple[datetime, ...]:
    first, stop = utc_hour(start), utc_hour(end)
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    completed = current.replace(minute=0, second=0, microsecond=0)
    count = int((stop - first).total_seconds() // 3600)
    if count <= 0 or count > max_hours or stop > completed:
        raise CompactionError("Select 1–max-hours completed UTC hours (end exclusive)")
    return tuple(first + timedelta(hours=i) for i in range(count))


@dataclass(frozen=True, order=True)
class Leaf:
    event_type: str
    day: str
    hour: str
    protocol: str = ""

    def __post_init__(self):
        component(self.event_type)
        if self.protocol:
            component(self.protocol)
        try:
            parsed = datetime.strptime(self.day + self.hour, "%Y%m%d%H")
        except ValueError:
            raise CompactionError("Invalid dayPK/hourPK leaf") from None
        if parsed.strftime("%Y%m%d%H") != self.day + self.hour:
            raise CompactionError("dayPK/hourPK require YYYYMMDD/HH")

    @property
    def relative(self) -> str:
        path = f"raw_sensor/{self.event_type}/dayPK={self.day}/hourPK={self.hour}/"
        return path + (f"protoPK={self.protocol}/" if self.protocol else "")

    def as_dict(self) -> dict:
        return {
            "event_type": self.event_type,
            "dayPK": self.day,
            "hourPK": self.hour,
            **({"protoPK": self.protocol} if self.protocol else {}),
        }

    @classmethod
    def from_relative(cls, relative: str) -> "Leaf":
        match = re.fullmatch(
            r"raw_sensor/([^/]+)/dayPK=(\d{8})/hourPK=(\d{2})/"
            r"(?:protoPK=([^/]+)/)?([^/]+\.parquet)",
            relative,
        )
        if not match or any(c in relative for c in "*?[]\\\x00\n\r"):
            raise CompactionError("Unexpected raw_sensor Parquet leaf layout")
        return cls(match[1], match[2], match[3], match[4] or "")


@dataclass(frozen=True)
class Limits:
    threads: int = 2
    memory_limit: str = "1GB"
    max_temp_size: str = "1GB"
    temp_directory: str | None = None
    timeout: float = 300
    max_files: int = 10_000
    max_hours: int = 24

    def __post_init__(self):
        if not 1 <= self.threads <= 64 or not 1 <= self.max_files <= 1_000_000:
            raise CompactionError("Invalid threads/max-files limit")
        if not 1 <= self.max_hours <= 744 or not 0 < self.timeout <= 86_400:
            raise CompactionError("Invalid timeout/max-hours limit")
        for size in (self.memory_limit, self.max_temp_size):
            if not re.fullmatch(r"[1-9]\d*(?:B|KB|MB|GB|KiB|MiB|GiB)", size):
                raise CompactionError(
                    "Resource sizes require positive B/KB/MB/GB units"
                )
