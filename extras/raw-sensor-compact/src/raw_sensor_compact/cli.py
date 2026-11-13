"""Plan, merge absent destination hours, and verify partition row totals."""

import argparse
import json
import sys
import uuid
from datetime import datetime, timedelta, timezone
from time import monotonic

from .compaction import process_hours, reconcile_deletions
from .config import CompactionError, Dataset, Limits, hour_window, separate_roots
from .engine import connection, validate_counts
from .entities import EntityLimits, host_name
from .entity_store import EntityStore, entity_inventory, update_entities
from .inventory import Store, discover, recheck
from .processing_metrics import ProcessingMetricsStore, metric_rows
from .storage import S3Settings, open_s3


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    for name in (
        "plan",
        "compact",
        "verify",
        "reconcile-delete",
        "entities-init",
        "entities-replay",
        "entities-verify",
    ):
        command = commands.add_parser(name)
        command.add_argument(
            "--source-root",
            required=True,
            help="Direct event root, e.g. s3://bucket/lintap/raw_sensor",
        )
        command.add_argument(
            "--destination-root",
            required=True,
            help="Direct event root, e.g. s3://bucket/lintap/compacted",
        )
        if name == "compact":
            command.add_argument("--target-size-bytes", type=int, required=True)
            command.add_argument(
                "--delete-source",
                action="store_true",
                help="After validation, delete the closed source event/hour prefix",
            )
            command.add_argument(
                "--grace-minutes",
                type=int,
                default=10,
                help="Minimum age after hour end for deletion (default 10; minimum 5)",
            )
        if name in {"compact", "reconcile-delete"}:
            command.add_argument(
                "--delete-confirm-attempts",
                type=int,
                default=5,
                help="Post-delete empty-list confirmations (default 5)",
            )
            command.add_argument(
                "--delete-confirm-initial-delay-seconds",
                type=float,
                default=1.0,
                help="Initial post-delete confirmation delay (default 1 second)",
            )
        if name == "reconcile-delete":
            command.add_argument(
                "--grace-minutes",
                type=int,
                default=10,
                help="Minimum age after hour end for deletion (default 10; minimum 5)",
            )
            command.add_argument(
                "--confirm-delete",
                action="store_true",
                help="Explicitly authorize deletion after output validation",
            )
        command.add_argument("--start-hour", required=True)
        command.add_argument("--end-hour", required=True)
        command.add_argument("--event-type", action="append", default=[])
        command.add_argument("--profile")
        command.add_argument("--region")
        command.add_argument("--endpoint-url")
        command.add_argument("--extension-directory")
        command.add_argument("--threads", type=int, default=2)
        command.add_argument("--memory-limit", default="1GB")
        command.add_argument("--max-temp-size", default="1GB")
        command.add_argument("--temp-directory")
        command.add_argument("--timeout", type=float, default=300)
        command.add_argument("--max-files", type=int, default=10_000)
        command.add_argument("--max-hours", type=int, default=24)
        command.add_argument("--process-entity-root")
        command.add_argument(
            "--processing-metrics-root",
            help="Sibling Parquet root for hourly processing metrics/checkpoints",
        )
        command.add_argument("--entity-host", action="append", default=[])
        command.add_argument("--entity-namespace")
        command.add_argument("--max-entity-rows", type=int, default=1_000_000)
        command.add_argument("--max-entity-bytes", type=int, default=256 * 1024 * 1024)
        if name == "entities-init":
            command.add_argument("--allow-empty-host", action="store_true")
            command.add_argument(
                "--resume-init",
                action="store_true",
                help=(
                    "Explicitly resume partial initialization; validate existing hosts"
                ),
            )
    return result


def run(args) -> tuple[dict, int]:
    started = monotonic()
    limits = Limits(
        threads=args.threads,
        memory_limit=args.memory_limit,
        max_temp_size=args.max_temp_size,
        temp_directory=args.temp_directory,
        timeout=args.timeout,
        max_files=args.max_files,
        max_hours=args.max_hours,
    )
    source = Dataset.parse(args.source_root, event_root=True)
    destination = Dataset.parse(args.destination_root, event_root=True)
    separate_roots(source, destination)
    if args.command == "reconcile-delete" and not args.confirm_delete:
        raise CompactionError(
            "Deletion reconciliation requires explicit --confirm-delete"
        )
    hours = hour_window(args.start_hour, args.end_hour, max_hours=limits.max_hours)
    if args.command == "reconcile-delete":
        if not 5 <= args.grace_minutes <= 1440 or any(
            h + timedelta(hours=1, minutes=args.grace_minutes)
            > datetime.now(timezone.utc)
            for h in hours
        ):
            raise CompactionError(
                "Source deletion requires closed UTC hours past a 5–1440 minute grace"
            )
    events = tuple(args.event_type)
    settings = S3Settings(
        args.profile, args.region, args.endpoint_url, args.extension_directory
    )
    entity_root = (
        Dataset.parse(args.process_entity_root) if args.process_entity_root else None
    )
    if args.command.startswith("entities-") and entity_root is None:
        raise CompactionError("Entity commands require --process-entity-root")
    if args.processing_metrics_root and entity_root is None:
        raise CompactionError(
            "Processing metrics require --process-entity-root; do not run raw-only mode"
        )
    if args.command == "compact" and args.target_size_bytes <= 0:
        raise CompactionError("A positive explicit target-size byte count is required")
    if entity_root:
        separate_roots(source, entity_root)
        separate_roots(destination, entity_root)
        if not args.entity_namespace or not args.entity_host:
            raise CompactionError(
                "Entity mode requires --entity-namespace and --entity-host"
            )
    metrics_root = None
    if entity_root:
        metrics_root = Dataset.parse(
            args.processing_metrics_root
            if args.processing_metrics_root
            else source.sibling("processing_metrics").root
        )
        separate_roots(source, metrics_root)
        separate_roots(destination, metrics_root)
        separate_roots(entity_root, metrics_root)
    with connection(limits) as conn:
        client = (
            open_s3(conn, settings)
            if source.bucket
            or destination.bucket
            or (entity_root and entity_root.bucket)
            or (metrics_root and metrics_root.bucket)
            else None
        )
        source_store = Store(source, client, deadline=started + limits.timeout)
        merged_store = Store(destination, client, deadline=started + limits.timeout)
        entity_report, process_inventory = None, None
        metrics = (
            ProcessingMetricsStore(
                Store(metrics_root, client, deadline=started + limits.timeout)
            )
            if metrics_root
            else None
        )
        run_id = uuid.uuid4().hex

        def publish_metrics(report, operation):
            if metrics is None or process_inventory is None or args.command == "plan":
                return
            compacted_inventory = entity_inventory(
                merged_store, hours, limits.max_files
            )
            rows = metric_rows(
                report,
                entity_report or {},
                process_inventory,
                compacted_inventory,
                hours,
                namespace=args.entity_namespace,
                hosts=tuple(args.entity_host),
                run_id=run_id,
                operation=operation,
                completed_at=datetime.now(timezone.utc).isoformat(),
            )
            for hour, row in zip(hours, rows, strict=False):
                metrics.publish(conn, hour, row)
            report["processing_metrics"] = {
                "root": metrics_root.root,
                "hours": len(rows),
                "run_id": run_id,
            }

        if entity_root:
            entities = EntityStore(
                Store(entity_root, client, deadline=started + limits.timeout),
                args.entity_namespace,
                EntityLimits(args.max_entity_rows, args.max_entity_bytes),
            )
            if args.command in {"entities-verify", "verify"}:
                from pathlib import Path
                from tempfile import TemporaryDirectory

                report = {"status": "passed", "hosts": []}
                with TemporaryDirectory(prefix="entity-verify-") as directory:
                    for host in args.entity_host:
                        path = Path(directory) / "snapshot.parquet"
                        if entities.read(host, path) is None:
                            raise CompactionError("Expected entity snapshot missing")
                        state, metadata = entities.validate(conn, path, host)
                        report["hosts"].append(
                            {
                                "hostname": host_name(host),
                                "rows": len(state),
                                "metadata": metadata,
                            }
                        )
                if args.command == "entities-verify":
                    return report, 0
                entity_report = report
            if args.command != "verify":
                # Check raw deletion preflight before any entity publication.
                if getattr(args, "delete_source", False):
                    grace = args.grace_minutes
                    if not 5 <= grace <= 1440 or any(
                        h + timedelta(hours=1, minutes=grace)
                        > datetime.now(timezone.utc)
                        for h in hours
                    ):
                        raise CompactionError(
                            "Source deletion requires closed UTC hours past a "
                            "5–1440 minute grace"
                        )
                entity_report, process_inventory = update_entities(
                    conn,
                    source_store,
                    merged_store,
                    entities,
                    hours,
                    args.entity_host,
                    initialize=args.command == "entities-init",
                    plan=args.command == "plan",
                    allow_empty=getattr(args, "allow_empty_host", False),
                    max_files=limits.max_files,
                    resume_init=getattr(args, "resume_init", False),
                )
                if args.command.startswith("entities-"):
                    if args.command in {"entities-init", "entities-replay"}:
                        publish_metrics({"hours": [], "partitions": []}, args.command)
                    return entity_report, 0
        if args.command in {"plan", "compact"}:
            report = process_hours(
                conn,
                source_store,
                merged_store,
                hours,
                events,
                limits,
                plan=args.command == "plan",
                target_size_bytes=getattr(args, "target_size_bytes", None),
                delete_source=getattr(args, "delete_source", False),
                grace_minutes=getattr(args, "grace_minutes", 10),
                process_inventory=process_inventory,
                delete_confirm_attempts=getattr(args, "delete_confirm_attempts", 5),
                delete_confirm_initial_delay_seconds=getattr(
                    args, "delete_confirm_initial_delay_seconds", 1.0
                ),
            )
            if entity_report is not None:
                report["process_entities"] = entity_report
                publish_metrics(report, "compact")
            return (
                report,
                2 if report["status"] == "completed_with_deletion_errors" else 0,
            )
        if args.command == "reconcile-delete":
            report = reconcile_deletions(
                conn,
                source_store,
                merged_store,
                hours,
                events,
                limits,
                confirm_delete=args.confirm_delete,
                grace_minutes=args.grace_minutes,
                delete_confirm_attempts=args.delete_confirm_attempts,
                delete_confirm_initial_delay_seconds=args.delete_confirm_initial_delay_seconds,
            )
            return (
                report,
                2 if report["status"] == "completed_with_deletion_errors" else 0,
            )
        inputs = discover(source_store, hours, events, max_files=limits.max_files)
        merged = discover(merged_store, hours, events, max_files=limits.max_files)
        report = {
            "command": "verify",
            "duckdb_version": "2.0.0",
            "source_root": source.root,
            "destination_root": destination.root,
            "source_fingerprint": inputs.fingerprint,
            "merged_fingerprint": merged.fingerprint,
            "partitions": [],
        }
        for leaf in sorted(inputs.groups.keys() | merged.groups.keys()):
            check = validate_counts(
                conn,
                [f.uri for f in inputs.groups.get(leaf, ())],
                [f.uri for f in merged.groups.get(leaf, ())],
            )
            report["partitions"].append({**leaf.as_dict(), **check.as_dict()})
        recheck(source_store, inputs, hours, max_files=limits.max_files)
        recheck(merged_store, merged, hours, max_files=limits.max_files)
        passed = all(p["passed"] for p in report["partitions"])
        report["status"] = (
            ("passed" if passed else "failed") if report["partitions"] else "empty"
        )
        if entity_report is not None:
            report["process_entities"] = entity_report
        return report, 0 if passed else 1


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    try:
        report, code = run(args)
        print(json.dumps(report, indent=2, sort_keys=True))
        return code
    except CompactionError as error:
        report = {"status": "error", "message": str(error)}
        if hasattr(error, "destination_hour"):
            report["destination_hour"] = error.destination_hour
            report["source_hour"] = error.source_hour
            report["phase"] = error.phase
        print(json.dumps(report), file=sys.stderr)
        return 2
    except Exception:
        print(
            json.dumps(
                {"status": "error", "message": "Operation failed; details suppressed"}
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
