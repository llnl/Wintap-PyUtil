"""Fixed destination hours: skip existing, otherwise merge; manual recovery."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import CompactionError, Leaf, component, separate_roots
from .deletion import delete_hour
from .engine import check_hive_fields, copy_parquet, count_rows, finish_leaf, schema
from .inventory import discover, recheck


class HourError(CompactionError):
    def __init__(self, message, destination_hour, *, source_hour=None, phase="merge"):
        super().__init__(message)
        self.destination_hour = destination_hour
        self.source_hour = source_hour
        self.phase = phase


def process_hours(
    conn,
    source,
    destination,
    hours,
    events,
    limits,
    *,
    target_size_bytes=None,
    plan=False,
    delete_source=False,
    grace_minutes=10,
    now=None,
    process_inventory=None,
):
    separate_roots(source.dataset, destination.dataset)
    if not plan and (type(target_size_bytes) is not int or target_size_bytes <= 0):
        raise CompactionError("A positive explicit target-size byte count is required")
    if delete_source:
        if type(grace_minutes) is not int or not 5 <= grace_minutes <= 1440:
            raise CompactionError("Deletion grace must be 5–1440 minutes")
        current = now or datetime.now(timezone.utc)
        if any(h + timedelta(hours=1, minutes=grace_minutes) > current for h in hours):
            raise CompactionError(
                "Source deletion requires closed UTC hours past the grace period"
            )
    families = tuple(sorted({component(e) for e in events})) or source.events()
    report = {
        "command": "plan" if plan else "compact",
        "duckdb_version": "2.0.0",
        "source_root": source.dataset.root,
        "destination_root": destination.dataset.root,
        "hours": [],
        "partitions": [],
        "delete_source": delete_source,
    }
    processed_files = 0
    for event in families:
        for hour in hours:
            parent = Leaf(event, hour.strftime("%Y%m%d"), hour.strftime("%H"))
            destination_hour = destination.dataset.file_uri(parent.relative)
            if destination.exists(parent.relative):
                report["hours"].append(
                    {**parent.as_dict(), "status": "skipped_existing"}
                )
                continue
            # A skipped hour is never scanned or opened on the source side.
            inputs = discover(
                source, (hour,), (event,), max_files=limits.max_files - processed_files
            )
            if process_inventory is not None and event == "raw_process":
                expected = {
                    leaf: files for leaf, files in process_inventory.groups.items()
                    if leaf.day == parent.day and leaf.hour == parent.hour
                }
                if inputs.groups != expected:
                    raise CompactionError("Process inputs differ from published entity evidence; no deletion")
            if not inputs.groups:
                report["hours"].append({**parent.as_dict(), "status": "empty"})
                continue
            processed_files += sum(len(f) for f in inputs.groups.values())
            phase = "merge"
            try:
                columns = {}
                for leaf, files in sorted(inputs.groups.items()):
                    uris = [f.uri for f in files]
                    columns[leaf] = schema(conn, uris)
                    check_hive_fields(conn, uris, leaf, columns[leaf])
                recheck(source, inputs, (hour,), max_files=limits.max_files)
                if plan:
                    for leaf, files in sorted(inputs.groups.items()):
                        report["partitions"].append(
                            {
                                **leaf.as_dict(),
                                "source_files": len(files),
                                "source_bytes": sum(f.size for f in files),
                                "schema": columns[leaf],
                            }
                        )
                    report["hours"].append({**parent.as_dict(), "status": "planned"})
                    continue
                # No lock/atomic publish guarantee. Only one writer may process
                # a destination at a time; this second check avoids stale plans.
                if destination.exists(parent.relative):
                    report["hours"].append(
                        {**parent.as_dict(), "status": "skipped_existing"}
                    )
                    continue
                if not destination.dataset.bucket:
                    try:
                        Path(destination_hour).mkdir(parents=True)
                    except FileExistsError:
                        report["hours"].append(
                            {**parent.as_dict(), "status": "skipped_existing"}
                        )
                        continue
                expected_files = set()
                results = []
                for leaf, files in sorted(inputs.groups.items()):
                    destination.check_deadline()
                    uris = [f.uri for f in files]
                    directory = destination.dataset.file_uri(leaf.relative).rstrip("/")
                    if not destination.dataset.bucket:
                        Path(directory).mkdir(parents=True, exist_ok=True)
                    written = copy_parquet(
                        conn, uris, directory, count_rows(conn, uris), target_size_bytes
                    )
                    objects = list(destination.objects(leaf.relative))
                    merged = sorted(f.uri for f in objects)
                    result = finish_leaf(
                        conn, uris, merged, written, leaf, columns[leaf]
                    )
                    result.update(
                        status="merged",
                        source_bytes=sum(f.size for f in files),
                        merged_bytes=sum(f.size for f in objects),
                        shard_sizes=[f.size for f in objects],
                    )
                    expected_files.update(merged)
                    results.append(result)
                observed = discover(
                    destination, (hour,), (event,), max_files=limits.max_files
                )
                if observed.groups.keys() != inputs.groups.keys() or expected_files != {
                    f.uri for files in observed.groups.values() for f in files
                }:
                    raise CompactionError(
                        "Destination hour differs from written file set"
                    )
                recheck(source, inputs, (hour,), max_files=limits.max_files)
                recheck(destination, observed, (hour,), max_files=limits.max_files)
                deleted = 0
                if delete_source:
                    # UTC producer contract + grace makes the entire hour closed.
                    # Every protocol leaf and inventory check must pass first.
                    phase = "delete_source"
                    deleted = delete_hour(source, parent)
                report["partitions"].extend(results)
                report["hours"].append(
                    {
                        **parent.as_dict(),
                        "status": "merged",
                        "source_deleted_objects": deleted,
                    }
                )
            except Exception as error:
                message = (
                    str(error)
                    if isinstance(error, CompactionError)
                    else ("Hour merge failed; backend details suppressed")
                )
                raise HourError(
                    message,
                    destination_hour,
                    source_hour=source.dataset.file_uri(parent.relative),
                    phase=phase,
                ) from None
    statuses = {h["status"] for h in report["hours"]}
    report["status"] = (
        "planned"
        if "planned" in statuses
        else "completed"
        if "merged" in statuses
        else "skipped"
        if "skipped_existing" in statuses
        else "empty"
    )
    return report
