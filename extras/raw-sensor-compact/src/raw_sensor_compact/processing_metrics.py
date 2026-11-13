"""Durable per-hour processing metrics and coverage checkpoints."""

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

from .config import CompactionError
from .entities import DEFAULT_IDENTITY_MAX_RATIO
from .inventory import Inventory, Store

VERSION = "3"
SCHEMA = {
    "metric_version": "VARCHAR",
    "run_id": "VARCHAR",
    "operation": "VARCHAR",
    "status": "VARCHAR",
    "window_start": "VARCHAR",
    "window_end": "VARCHAR",
    "hour_start": "VARCHAR",
    "hour_end": "VARCHAR",
    "entity_mode": "BOOLEAN",
    "entity_namespace": "VARCHAR",
    "expected_host_count": "BIGINT",
    "entity_rows": "BIGINT",
    "entity_changed_hosts": "BIGINT",
    "entity_parent_conflicts": "BIGINT",
    "entity_bootstrap_start": "VARCHAR",
    "entity_snapshot_hashes": "VARCHAR",
    "entity_input_fingerprint": "VARCHAR",
    "identity_policy": "VARCHAR",
    "identity_exclusion_input_rows": "BIGINT",
    "identity_exclusion_rows": "BIGINT",
    "identity_exclusion_ratio": "DOUBLE",
    "identity_exclusion_max_ratio": "DOUBLE",
    "identity_exclusion_fingerprint": "VARCHAR",
    "identity_exclusion_reasons": "VARCHAR",
    "identity_exclusion_hosts": "VARCHAR",
    "process_source_fingerprint": "VARCHAR",
    "process_compacted_fingerprint": "VARCHAR",
    "raw_status": "VARCHAR",
    "raw_source_files": "BIGINT",
    "raw_source_bytes": "BIGINT",
    "raw_source_rows": "BIGINT",
    "raw_output_files": "BIGINT",
    "raw_output_bytes": "BIGINT",
    "raw_output_rows": "BIGINT",
    "deletion_status": "VARCHAR",
    "deletion_failures": "BIGINT",
    "completed_at": "VARCHAR",
}


def _hour_string(hour: datetime) -> str:
    return hour.strftime("%Y-%m-%dT%H:00:00Z")


def _hour_key(hour: datetime) -> tuple[str, str]:
    return hour.strftime("%Y%m%d"), hour.strftime("%H")


def _inventory_hour_files(inventory: Inventory, hour: datetime):
    day, value = _hour_key(hour)
    return [
        file
        for leaf, files in inventory.groups.items()
        if leaf.day == day and leaf.hour == value
        for file in files
    ]


def inventory_hour_fingerprint(inventory: Inventory, hour: datetime) -> str:
    files = [
        {
            "relative": file.relative,
            "uri": file.uri,
            "size": file.size,
            "etag": file.etag,
            "modified": file.modified,
        }
        for file in sorted(
            _inventory_hour_files(inventory, hour), key=lambda item: item.uri
        )
    ]
    return hashlib.sha256(
        json.dumps(
            {
                "dataset": inventory.dataset.root,
                "hour": _hour_string(hour),
                "files": files,
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()


def inventory_hour_stats(inventory: Inventory, hour: datetime) -> dict:
    files = _inventory_hour_files(inventory, hour)
    return {
        "files": len(files),
        "bytes": sum(file.size for file in files),
        "fingerprint": inventory_hour_fingerprint(inventory, hour),
    }


def _report_hour(report: dict, hour: datetime) -> list[dict]:
    day, value = _hour_key(hour)
    return [
        entry
        for entry in report.get("hours", [])
        if entry.get("dayPK") == day and entry.get("hourPK") == value
    ]


def _sum_partitions(report: dict, hour: datetime, field: str) -> int:
    day, value = _hour_key(hour)
    return sum(
        int(partition.get(field, 0) or 0)
        for partition in report.get("partitions", [])
        if partition.get("dayPK") == day and partition.get("hourPK") == value
    )


def metric_rows(
    report: dict,
    entity_report: dict,
    source_inventory: Inventory,
    compacted_inventory: Inventory,
    hours: tuple[datetime, ...],
    *,
    namespace: str,
    hosts: tuple[str, ...],
    run_id: str,
    operation: str = "compact",
    completed_at: str | None = None,
) -> list[dict]:
    """Build one durable checkpoint row for each requested UTC hour."""
    completed_at = completed_at or datetime.now(timezone.utc).isoformat()
    hosts_report = entity_report.get("hosts", [])
    entity_hashes = {
        str(host.get("hostname", "")): host.get("semantic_sha256", "")
        for host in hosts_report
    }
    entity_rows = sum(int(host.get("rows", 0) or 0) for host in hosts_report)
    changed_hosts = sum(bool(host.get("changed")) for host in hosts_report)
    conflicts = sum(int(host.get("parent_conflicts", 0) or 0) for host in hosts_report)
    bootstrap_starts = sorted(
        str(host.get("bootstrap_start", ""))
        for host in hosts_report
        if host.get("bootstrap_start")
    )
    entity_bootstrap_start = bootstrap_starts[0] if bootstrap_starts else ""
    input_fingerprint = entity_report.get("process_input_fingerprint", "")
    identity_report = entity_report.get("identity_exclusions", {})
    identity_policy = identity_report.get("policy", "reject")

    rows = []
    for hour in hours:
        entries = _report_hour(report, hour)
        statuses = {entry.get("status") for entry in entries}
        deletion_failures = sum(
            1
            for failure in report.get("deletion_failures", [])
            if failure.get("dayPK") == hour.strftime("%Y%m%d")
            and failure.get("hourPK") == hour.strftime("%H")
        )
        raw_status = (
            "completed_with_deletion_errors"
            if deletion_failures
            else "merged"
            if "merged" in statuses
            else "skipped_existing"
            if "skipped_existing" in statuses
            else "empty"
            if "empty" in statuses
            else "not_run"
        )
        source = inventory_hour_stats(source_inventory, hour)
        compacted = inventory_hour_stats(compacted_inventory, hour)
        identity_hour = identity_report.get("hours", {}).get(
            hour.strftime("%Y%m%d%H"), {}
        )
        rows.append(
            {
                "metric_version": VERSION,
                "run_id": run_id,
                "operation": operation,
                "status": "completed_with_deletion_errors"
                if deletion_failures
                else "completed",
                "window_start": _hour_string(hours[0]),
                "window_end": _hour_string(hours[-1] + timedelta(hours=1)),
                "hour_start": _hour_string(hour),
                "hour_end": _hour_string(hour + timedelta(hours=1)),
                "entity_mode": True,
                "entity_namespace": namespace,
                "expected_host_count": len(hosts),
                "entity_rows": entity_rows,
                "entity_changed_hosts": changed_hosts,
                "entity_parent_conflicts": conflicts,
                "entity_bootstrap_start": entity_bootstrap_start,
                "entity_snapshot_hashes": json.dumps(entity_hashes, sort_keys=True),
                "entity_input_fingerprint": input_fingerprint,
                "identity_policy": identity_policy,
                "identity_exclusion_input_rows": int(
                    identity_hour.get("input_rows", 0) or 0
                ),
                "identity_exclusion_rows": int(identity_hour.get("rows", 0) or 0),
                "identity_exclusion_ratio": float(
                    identity_hour.get("ratio", 0.0) or 0.0
                ),
                "identity_exclusion_max_ratio": float(
                    identity_hour.get(
                        "max_ratio",
                        identity_report.get("max_ratio", DEFAULT_IDENTITY_MAX_RATIO),
                    )
                    or 0.0
                ),
                "identity_exclusion_fingerprint": identity_hour.get("fingerprint", ""),
                "identity_exclusion_reasons": json.dumps(
                    identity_hour.get("reasons", {}), sort_keys=True
                ),
                "identity_exclusion_hosts": json.dumps(
                    identity_hour.get("hosts", {}), sort_keys=True
                ),
                "process_source_fingerprint": source["fingerprint"],
                "process_compacted_fingerprint": compacted["fingerprint"],
                "raw_status": raw_status,
                "raw_source_files": source["files"],
                "raw_source_bytes": source["bytes"],
                "raw_source_rows": _sum_partitions(report, hour, "source_rows"),
                "raw_output_files": compacted["files"],
                "raw_output_bytes": compacted["bytes"],
                "raw_output_rows": _sum_partitions(report, hour, "merged_rows"),
                "deletion_status": "failed" if deletion_failures else "completed",
                "deletion_failures": deletion_failures,
                "completed_at": completed_at,
            }
        )
    return rows


class ProcessingMetricsStore:
    """Fixed per-hour Parquet objects under a processing_metrics root."""

    def __init__(self, store: Store):
        self.store = store

    @staticmethod
    def relative(hour: datetime) -> str:
        day, value = _hour_key(hour)
        return f"dayPK={day}/hourPK={value}/metrics.parquet"

    def _validate(self, conn, path):
        actual = {
            row[0]: row[1]
            for row in conn.execute(
                "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)",
                [str(path)],
            ).fetchall()
        }
        if actual != SCHEMA:
            raise CompactionError("Processing metrics schema mismatch")
        if (
            conn.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(path)]
            ).fetchone()[0]
            != 1
        ):
            raise CompactionError("Processing metrics object must contain one row")

    def publish(self, conn, hour: datetime, row: dict) -> None:
        self.store.check_deadline()
        relative = self.relative(hour)
        with TemporaryDirectory(prefix="processing-metrics-") as directory:
            candidate = Path(directory) / "metrics.parquet"
            conn.execute("DROP TABLE IF EXISTS processing_metric_output")
            conn.execute(
                "CREATE TEMP TABLE processing_metric_output ("
                + ",".join(f"{name} {dtype}" for name, dtype in SCHEMA.items())
                + ")"
            )
            conn.execute(
                "INSERT INTO processing_metric_output VALUES ("
                + ",".join("?" for _ in SCHEMA)
                + ")",
                [row[name] for name in SCHEMA],
            )
            conn.execute(
                "COPY processing_metric_output TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
                [str(candidate)],
            )
            self._validate(conn, candidate)
            dataset = self.store.dataset
            if dataset.bucket:
                with candidate.open("rb") as stream:
                    self.store.client.put_object(
                        Bucket=dataset.bucket,
                        Key=dataset.object_key(relative),
                        Body=stream,
                    )
                try:
                    response = self.store.client.get_object(
                        Bucket=dataset.bucket, Key=dataset.object_key(relative)
                    )
                    body = response["Body"]
                    try:
                        remote = body.read()
                    finally:
                        body.close()
                except Exception:
                    raise CompactionError(
                        "Processing metrics GET verification failed"
                    ) from None
                if (
                    hashlib.sha256(remote).digest()
                    != hashlib.sha256(candidate.read_bytes()).digest()
                ):
                    raise CompactionError(
                        "Processing metrics content verification failed"
                    )
            else:
                target = Path(dataset.root) / relative
                if any(path.is_symlink() for path in (target, *target.parents)):
                    raise CompactionError(
                        "Symlinked processing metrics paths unsupported"
                    )
                target.parent.mkdir(parents=True, exist_ok=True)
                staging = None
                try:
                    with (
                        NamedTemporaryFile(dir=target.parent, delete=False) as output,
                        candidate.open("rb") as source,
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
            self.store.check_deadline()
