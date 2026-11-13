"""Plan, merge absent destination hours, and verify partition row totals."""

import argparse
import json
import sys
from time import monotonic

from .compaction import process_hours
from .config import CompactionError, Dataset, Limits, hour_window, separate_roots
from .engine import connection, validate_counts
from .inventory import Store, discover, recheck
from .storage import S3Settings, open_s3


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    for name in ("plan", "compact", "verify"):
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
    hours = hour_window(args.start_hour, args.end_hour, max_hours=limits.max_hours)
    events = tuple(args.event_type)
    settings = S3Settings(
        args.profile, args.region, args.endpoint_url, args.extension_directory
    )
    with connection(limits) as conn:
        client = (
            open_s3(conn, settings) if source.bucket or destination.bucket else None
        )
        source_store = Store(source, client, deadline=started + limits.timeout)
        merged_store = Store(destination, client, deadline=started + limits.timeout)
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
            )
            return report, 0
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
