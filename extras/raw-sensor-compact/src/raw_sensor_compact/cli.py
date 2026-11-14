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
from .entities import DEFAULT_IDENTITY_MAX_RATIO, EntityLimits, host_name
from .entity_store import EntityStore, entity_inventory, update_entities
from .inventory import Store, discover, recheck
from .phase_evidence import (
    RunEvidence,
    load_archive,
    local_directory,
    migration_complete,
    run_identity,
    verify_migrations,
)
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
        "checkpoints-repair",
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
        command.add_argument(
            "--run-id", help="Stable 32-character lowercase hex CLI identity"
        )
        command.add_argument(
            "--coordinator-run-id", help="Independent coordinator run identity"
        )
        command.add_argument(
            "--run-evidence-directory",
            help="Separate server-local immutable run archive",
        )
        if name == "checkpoints-repair":
            command.add_argument("--repair-run-id", required=True)
            command.add_argument(
                "--confirm-repair",
                action="store_true",
                help="Confirm all readers/writers are excluded for repair",
            )
            command.add_argument(
                "--confirm-replace-checkpoints",
                action="store_true",
                help="Explicitly replace conflicting coverage; archive its disposition",
            )
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
        command.add_argument(
            "--unknown-identity-policy",
            choices=("reject", "quarantine"),
            default="reject",
            help=(
                "How entity mode handles unknown process identities; reject is "
                "the safe default, quarantine retains explicit non-entity evidence"
            ),
        )
        command.add_argument(
            "--unknown-identity-max-ratio",
            type=float,
            default=DEFAULT_IDENTITY_MAX_RATIO,
            help=(
                "Maximum per-hour fraction of process rows that may be "
                "quarantined (default 0.05)"
            ),
        )
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
    run_id = run_identity(getattr(args, "run_id", None) or uuid.uuid4().hex)
    coordinator_id = getattr(args, "coordinator_run_id", None) or ""
    if coordinator_id:
        run_identity(coordinator_id)
        if not getattr(args, "run_evidence_directory", None):
            raise CompactionError("Coordinated runs require --run-evidence-directory")
    state = {}
    try:
        report, code = _run(args, run_id, coordinator_id, state)
        archive = state.get("archive")
        if archive:
            archive.append(
                "complete" if code == 0 else "blocked",
                {
                    "status": report.get("status", ""),
                    "exit_code": code,
                    "migration_complete": archive.migration_complete,
                },
            )
            report["phase_evidence"] = archive.summary()
        else:
            report["phase_evidence"] = {"migration_complete": False, "archived": False}
        report.update(run_id=run_id, coordinator_run_id=coordinator_id)
        return report, code
    except Exception as error:
        archive = state.get("archive")
        if archive:
            try:
                archive.append(
                    "blocked",
                    {
                        "message": str(error)
                        if isinstance(error, CompactionError)
                        else "Operation failed; details suppressed"
                    },
                )
            except CompactionError:
                pass  # Archiving failure cannot clear the original uncertainty.
        if not isinstance(error, CompactionError):
            error = CompactionError("Operation failed; details suppressed")
        error.run_id = run_id
        error.coordinator_run_id = coordinator_id
        error.phase_evidence = (
            archive.summary() if archive else {"migration_complete": False}
        )
        raise error


def _verify_entity_state(conn, source, merged, entities, args, metrics_root):
    from pathlib import Path
    from tempfile import TemporaryDirectory

    from .entity_authority import Authority, binding

    report = {"status": "passed", "hosts": []}
    with TemporaryDirectory(prefix="entity-verify-") as directory:
        identities, bootstraps = {}, {}
        for host in sorted({host_name(h) for h in args.entity_host}):
            path = Path(directory) / "snapshot.parquet"
            snapshot_identity = entities.read(host, path)
            if snapshot_identity is None:
                raise CompactionError("Expected entity snapshot missing")
            entities.validate_sql(conn, path, host, None)
            metadata = entities.sql_metadata(conn, path)
            identities[host] = snapshot_identity
            bootstraps[host] = metadata["bootstrap_start"]
            rows = conn.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(path)]
            ).fetchone()[0]
            report["hosts"].append(
                {
                    "hostname": host,
                    "rows": rows,
                    "metadata": metadata,
                    "snapshot_sha256": snapshot_identity,
                }
            )
        authority = Authority(
            conn,
            entities,
            directory,
            binding(
                source,
                merged,
                entities,
                identities,
                args.unknown_identity_policy,
                args.unknown_identity_max_ratio,
                metrics_root.root,
            ),
        )
        authority.check(identities, bootstraps)
        report["generation_id"] = authority.latest["generation_id"]
        report["authority_sha256"] = authority.identity
    return report


def _run(args, run_id, coordinator_id, state) -> tuple[dict, int]:
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
    if args.command == "checkpoints-repair" and (
        entity_root is None
        or not getattr(args, "run_evidence_directory", None)
        or not getattr(args, "confirm_repair", False)
    ):
        raise CompactionError(
            "Checkpoint repair requires entity mode, original run evidence "
            "and explicit --confirm-repair"
        )
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
    archive = None
    evidence_binding = {
        "raw_root": source.root,
        "compacted_root": destination.root,
        "entity_root": entity_root.root if entity_root else "",
        "metrics_root": metrics_root.root if metrics_root else "",
        "namespace": args.entity_namespace or "",
        "hosts": sorted({host_name(h) for h in args.entity_host})
        if entity_root
        else [],
        "identity_policy": args.unknown_identity_policy,
        "identity_max_ratio": float(args.unknown_identity_max_ratio),
        "window_start": args.start_hour,
        "window_end": args.end_hour,
        "event_types": sorted(set(events)),
    }
    if getattr(args, "run_evidence_directory", None):
        archive_root = Dataset.parse(str(local_directory(args.run_evidence_directory)))
        for dataset in (source, destination, entity_root, metrics_root):
            if dataset:
                separate_roots(archive_root, dataset)
        archive = RunEvidence(
            archive_root.root, run_id, coordinator_id, evidence_binding, args.command
        )
        state["archive"] = archive
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

        def publish_metrics(report, operation):
            if metrics is None or process_inventory is None or args.command == "plan":
                return
            current_entities = None
            if archive:
                current_entities = _verify_entity_state(
                    conn, source_store, merged_store, entities, args, metrics_root
                )
                if current_entities["generation_id"] != entity_report.get(
                    "generation_id"
                ):
                    raise CompactionError(
                        "Entity authority changed before checkpoint publication"
                    )
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
            if archive:
                archive.append("checkpoint_intent", {"rows": rows})
            for hour, row in zip(hours, rows, strict=False):
                metrics.publish(conn, hour, row)
                if archive:
                    archive.append(
                        "checkpoint_verified",
                        {"hour_start": row["hour_start"], "row": row},
                    )
            if archive:
                if any(
                    metrics.read(conn, h) != r for h, r in zip(hours, rows, strict=True)
                ):
                    raise CompactionError("Checkpoint final GET verification failed")
                if (
                    _verify_entity_state(
                        conn, source_store, merged_store, entities, args, metrics_root
                    )
                    != current_entities
                ):
                    raise CompactionError(
                        "Entity authority changed during checkpoint publication"
                    )
            report["processing_metrics"] = {
                "root": metrics_root.root,
                "hours": len(rows),
                "run_id": run_id,
            }

        def recheck_raw():
            if archive and archive.migration_complete:
                verify_migrations(
                    conn,
                    load_archive(archive.root.as_posix(), run_id),
                    source_store,
                    merged_store,
                    hours,
                    events,
                    limits,
                )

        def record_raw(report):
            if archive:
                archive.append("raw_complete", report)
                archive.migration_complete = migration_complete(report)
                recheck_raw()

        if entity_root:
            entities = EntityStore(
                Store(entity_root, client, deadline=started + limits.timeout),
                args.entity_namespace,
                EntityLimits(args.max_entity_rows, args.max_entity_bytes),
            )
            if args.command in {"entities-verify", "verify", "checkpoints-repair"}:

                def verify_current_entities():
                    return _verify_entity_state(
                        conn, source_store, merged_store, entities, args, metrics_root
                    )

                report = verify_current_entities()
                if archive:
                    archive.append("entities_verified", report)
                if args.command == "checkpoints-repair":
                    from .phase_evidence import repair_checkpoints

                    return repair_checkpoints(
                        conn,
                        args,
                        archive,
                        evidence_binding,
                        source_store,
                        merged_store,
                        metrics,
                        hours,
                        events,
                        limits,
                        verify_current_entities=verify_current_entities,
                    ), 0
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
                if archive and args.command != "plan":
                    archive.append("entities_intent", {"operation": args.command})
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
                    identity_policy=args.unknown_identity_policy,
                    identity_max_ratio=args.unknown_identity_max_ratio,
                    metrics_root=metrics_root.root,
                )
                if archive and args.command != "plan":
                    archive.append("entities_verified", entity_report)
                if args.command.startswith("entities-"):
                    if args.command in {"entities-init", "entities-replay"}:
                        metrics_report = {"hours": [], "partitions": []}
                        publish_metrics(metrics_report, args.command)
                        if "processing_metrics" in metrics_report:
                            entity_report["processing_metrics"] = metrics_report[
                                "processing_metrics"
                            ]
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
                evidence=archive if args.command != "plan" else None,
            )
            if archive and args.command != "plan":
                record_raw(report)
            if entity_report is not None:
                report["process_entities"] = entity_report
                publish_metrics(report, "compact")
            recheck_raw()
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
                evidence=archive,
            )
            if archive:
                record_raw(report)
            if entity_report is not None:
                report["process_entities"] = entity_report
                publish_metrics(report, "reconcile-delete")
            recheck_raw()
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
        for field in ("run_id", "coordinator_run_id", "phase_evidence"):
            if hasattr(error, field):
                report[field] = getattr(error, field)
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
