"""Qualified Linux scheduling path; the conservative callback tick stays unchanged."""

import argparse
import json
import logging
import os
import sys
from itertools import chain
from pathlib import Path
from uuid import uuid4

import boto3
from botocore.config import Config

from .coordinator import _Clock, _grace, writer_due
from .execution import (
    DEFAULTS,
    OPTIONAL,
    REQUIRED,
    _artifact_layout,
    _label,
    _local,
    _origin,
    _validate_settings,
    prepare_execution,
    resolve_storage,
)
from .ledger import Ledger, _fsync_directory, _now
from .protocol import (
    CoordinationError,
    MessageStore,
    canonical,
    dataset_binding,
    separate_transport_root,
    timestamp,
    validate_binding,
)
from .supervisor import _parse_json
from .verification import verify_authority_prepared
from .writer import execute_and_complete_prepared

LOG = logging.getLogger(__name__)
MAX_EXPORT_BYTES = 1024 * 1024
EXPORT_PAGE_SIZE = 100


def validate_config(value):
    required = {
        "version",
        "binding",
        "coordination_root",
        "state_directory",
        "first_hour",
        "writer",
        "storage",
    }
    if (
        not isinstance(value, dict)
        or not required <= set(value)
        or set(value) - required - {"entity_writer"}
        or type(value["version"]) is not int
        or value["version"] != 1
    ):
        raise CoordinationError("Invalid service configuration fields/version")
    validate_binding(value["binding"])
    separate_transport_root(value["coordination_root"], value["binding"])
    if not value["coordination_root"].startswith("s3://"):
        raise CoordinationError("Service transport must use S3")
    _local(value["state_directory"])
    timestamp(value["first_hour"], hour=True)
    writer = value["writer"]
    if (
        not isinstance(writer, dict)
        or not REQUIRED <= set(writer)
        or set(writer) - REQUIRED - set(DEFAULTS) - OPTIONAL
    ):
        raise CoordinationError("Writer configuration must be explicit and allowlisted")
    _grace(writer.get("grace_minutes", DEFAULTS["grace_minutes"]))
    if writer.get("max_hours", 1) != 1 or type(writer.get("max_hours", 1)) is not int:
        raise CoordinationError("Exactly one hour per service tick is supported")
    if not isinstance(value["storage"], dict) or set(value["storage"]) - {
        "profile",
        "region",
        "endpoint_url",
    }:
        raise CoordinationError("Transport storage settings must be allowlisted")
    settings = {
        **DEFAULTS,
        **{k: v for k, v in writer.items() if k in set(DEFAULTS) | REQUIRED},
    }
    settings.pop("executable")
    settings.pop("project")
    settings.update(
        entity=value["binding"]["entity"],
        storage=None,
        archive_limits={
            "record_bytes": 64 * 1024**2,
            "run_bytes": 256 * 1024**2,
            "records": 10_000,
        },
    )
    for key in ("temp_directory", "run_evidence_directory"):
        settings[key] = _local(settings[key])
    _validate_settings(settings, value["binding"], check_storage=False)
    _artifact_layout(settings, Path(_local(value["state_directory"])))
    storage_configs = [value["storage"], writer]
    if "entity_writer" in value:
        from . import entity_execution

        entity_writer = value["entity_writer"]
        if (
            not isinstance(entity_writer, dict)
            or not entity_execution.REQUIRED <= set(entity_writer)
            or set(entity_writer)
            - entity_execution.REQUIRED
            - set(entity_execution.DEFAULTS)
            - entity_execution.OPTIONAL
        ):
            raise CoordinationError(
                "Entity writer configuration must be explicit and allowlisted"
            )
        entity_settings = {
            **entity_execution.DEFAULTS,
            **{
                k: v
                for k, v in entity_writer.items()
                if k in set(entity_execution.DEFAULTS) | entity_execution.REQUIRED
            },
        }
        entity_settings.pop("executable")
        entity_settings.pop("project")
        for key in ("temp_directory", "run_evidence_directory"):
            entity_settings[key] = _local(entity_settings[key])
        entity_settings.update(
            entity=value["binding"]["entity"],
            storage=None,
            archive_limits=dict(entity_execution.ARCHIVE_LIMITS),
        )
        entity_execution._validate_settings(
            entity_settings, value["binding"], check_storage=False
        )
        _artifact_layout(entity_settings, Path(_local(value["state_directory"])))
        storage_configs.append(entity_writer)
    for storage in storage_configs:
        for key in ("profile", "region"):
            _label(storage.get(key))
        if storage.get("endpoint_url"):
            _origin(storage["endpoint_url"])
    # No secret-bearing unrecognized config is copied into runtime records.
    return json.loads(canonical(value))


def load_config(path):
    from .execution import _read_bytes

    return validate_config(_parse_json(_read_bytes(path, 1024 * 1024)))


def open_message_store(root, storage):
    # Resolve transport once, even for locally mounted data roots. No data I/O here.
    transport_binding = dataset_binding(
        "transport", {"raw": root + "/_scope-a", "compacted": root + "/_scope-b"}
    )
    scope, credentials = resolve_storage(transport_binding, storage)
    client = boto3.client(
        "s3",
        endpoint_url=scope["endpoint_url"],
        region_name=scope["region"],
        aws_access_key_id=credentials["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=credentials["AWS_SECRET_ACCESS_KEY"],
        aws_session_token=credentials.get("AWS_SESSION_TOKEN"),
        config=Config(
            s3={"addressing_style": "path"},
            connect_timeout=10,
            read_timeout=30,
            retries={"mode": "standard", "total_max_attempts": 3},
        ),
    )
    store = MessageStore(root, client)
    store.transport_scope = {
        key: scope[key] for key in ("endpoint_url", "region", "addressing_style")
    }
    return store


def status_page(ledger, now, *, after_job="", after_run="", after_maintenance=""):
    """Bounded local advisory producer interface, never a permission token."""
    from .protocol import identifier

    timestamp(now)
    for identity in (after_job, after_run, after_maintenance):
        if identity:
            identifier(identity)
    jobs = ledger._connection.execute(
        "SELECT job_id,state,first_seen,grant_id FROM jobs WHERE job_id>? ORDER BY job_id LIMIT ?",
        [after_job, EXPORT_PAGE_SIZE + 1],
    ).fetchall()
    runs = ledger._connection.execute(
        "SELECT run_id,cli_run_id,hour_start,hour_end,state,started_at FROM compaction_runs WHERE run_id>? ORDER BY run_id LIMIT ?",
        [after_run, EXPORT_PAGE_SIZE + 1],
    ).fetchall()
    from .maintenance import history

    operations = history(ledger)
    maintenance_ids = sorted(
        identity for identity in operations if identity > after_maintenance
    )
    value = {
        "version": 1,
        "advisory": True,
        "observed_at": now,
        "ledger_id": ledger.ledger_id,
        "binding": json.loads(ledger.binding),
        "state": ledger.snapshot(),
        "authority_high_water": ledger.authority_high_water(),
        "jobs": [
            dict(zip(("job_id", "state", "first_seen", "grant_id"), row, strict=True))
            for row in jobs[:EXPORT_PAGE_SIZE]
        ],
        "runs": [
            dict(
                zip(
                    (
                        "run_id",
                        "cli_run_id",
                        "hour_start",
                        "hour_end",
                        "state",
                        "started_at",
                    ),
                    row,
                    strict=True,
                )
            )
            for row in runs[:EXPORT_PAGE_SIZE]
        ],
        "maintenance": [
            {
                "maintenance_id": identity,
                "run_id": operations[identity]["receipt"]["execution"]["run_id"],
                "operation": operations[identity]["receipt"]["operation"],
                "created_at": operations[identity]["receipt"]["created_at"],
                "state": "FINISHED"
                if operations[identity]["finished"] is not None
                else "ABANDONED"
                if operations[identity]["abandoned"] is not None
                else "PENDING",
            }
            for identity in maintenance_ids[:EXPORT_PAGE_SIZE]
        ],
        "next_maintenance": maintenance_ids[EXPORT_PAGE_SIZE - 1]
        if len(maintenance_ids) > EXPORT_PAGE_SIZE
        else None,
        "next_job": jobs[EXPORT_PAGE_SIZE - 1][0]
        if len(jobs) > EXPORT_PAGE_SIZE
        else None,
        "next_run": runs[EXPORT_PAGE_SIZE - 1][0]
        if len(runs) > EXPORT_PAGE_SIZE
        else None,
        "scope": "bounded ledger summaries; not a partition inventory or completion certificate",
    }
    if len(canonical(value).encode()) > MAX_EXPORT_BYTES:
        raise CoordinationError("Advisory page exceeds byte budget")
    return value


def export_status(ledger, now):
    value = status_page(ledger, now)
    path = ledger.directory / "status.json"
    temporary = ledger.directory / (".status-" + uuid4().hex)
    try:
        with temporary.open("xb") as output:
            os.chmod(temporary, 0o600)
            output.write(canonical(value).encode())
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        _fsync_directory(ledger.directory)
    finally:
        temporary.unlink(missing_ok=True)
    return value


def _export(ledger, now):
    try:
        export_status(ledger, now)
        return True
    except Exception:  # noqa: BLE001 -- advisory failure neither authorizes nor undoes a transition
        LOG.warning(
            "Advisory export failed; details suppressed; ledger remains authoritative"
        )
        return False


def service_tick(config, store, *, clock=_now):
    config = validate_config(config)
    if sys.platform != "linux":
        raise CoordinationError("Service/writer integration is qualified only on Linux")
    if store.root != config["coordination_root"]:
        raise CoordinationError("Service transport configuration mismatch")
    observed = _Clock(clock(), clock)
    grace = config["writer"].get("grace_minutes", DEFAULTS["grace_minutes"])
    with Ledger.open(config["state_directory"], config["binding"]) as ledger:
        invoked = False

        def observe():
            value = observed.observe()
            floor = ledger._completion_clock_floor()
            if floor is not None and timestamp(value) < timestamp(floor):
                raise CoordinationError(
                    "Service clock predates retained prior completion"
                )
            return value

        try:
            observe()
            messages = [
                store.messages(kind) for kind in ("release", "cancellation", "request")
            ]
            ledger.ingest_many(chain.from_iterable(messages), observe())
            now = observe()
            state = ledger.snapshot()
            if state["mode"] == "BLOCKED":
                return {
                    **state,
                    "writer_invoked": False,
                    "advisory_exported": _export(ledger, now),
                }
            prepared = None
            if config["binding"]["entity"]:
                prepared = prepare_execution(
                    ledger.directory,
                    config["binding"],
                    state["next_hour"],
                    now,
                    config["writer"],
                )
                verify_authority_prepared(ledger, prepared, now)
                now = observe()
            if writer_due(state["next_hour"], now, grace_minutes=grace):
                ledger.drain(now)
            if ledger.snapshot()["mode"] == "OPEN":
                ledger.admit(now)
            ledger.flush_grants(store)
            now = observe()
            state = ledger.snapshot()
            if writer_due(state["next_hour"], now, grace_minutes=grace):
                ledger.drain(now)
                if not state["readers"]:
                    prepared = prepared or prepare_execution(
                        ledger.directory,
                        config["binding"],
                        state["next_hour"],
                        now,
                        config["writer"],
                    )
                    invoked = True
                    execute_and_complete_prepared(
                        ledger,
                        prepared,
                        now,
                        clock=observe,
                        on_intent=lambda: _export(ledger, observed.last),
                    )
                    # Completion is COMMITTED before any new reservations/outbox.
                    now = observe()
                    state = ledger.snapshot()
                    if writer_due(state["next_hour"], now, grace_minutes=grace):
                        ledger.drain(now)
                    if ledger.snapshot()["mode"] == "OPEN":
                        ledger.admit(now)
                    ledger.flush_grants(store)
            return {
                **ledger.snapshot(),
                "writer_invoked": invoked,
                "advisory_exported": _export(ledger, observe()),
            }
        except BaseException:
            _export(ledger, observed.last)
            raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "operation",
        choices=[
            "init",
            "upgrade",
            "tick",
            "status",
            "init-authority",
            "release-reader",
            "resolve-hour",
            "retry-hour",
            "repair-checkpoints",
            "repair-deletions",
            "abandon-maintenance",
            "entities-init",
            "entities-replay",
            "resolve-entity",
        ],
    )
    parser.add_argument("--config", required=True)
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Original processes cannot resume; externally established exclusion",
    )
    parser.add_argument("--reason")
    parser.add_argument("--job-id")
    parser.add_argument("--run-id")
    parser.add_argument("--maintenance-id")
    parser.add_argument("--confirm-replace-checkpoints", action="store_true")
    parser.add_argument("--after-job")
    parser.add_argument("--after-run")
    parser.add_argument("--after-maintenance")
    parser.add_argument("--start-hour")
    parser.add_argument("--end-hour")
    parser.add_argument("--target-version")
    args = parser.parse_args(argv)
    try:
        entity_launch = args.operation in {"entities-init", "entities-replay"}
        if (
            not entity_launch
            and (args.start_hour is not None or args.end_hour is not None)
            or args.operation != "upgrade"
            and args.target_version is not None
        ):
            raise CoordinationError("Operation refuses unrelated entity/upgrade flags")
        if args.target_version is not None:
            from .recovery import _confirmation

            if sys.platform != "linux" or args.target_version not in {"4", "5"}:
                raise CoordinationError(
                    "Explicit service upgrade target requires Linux and 4 or 5"
                )
            _confirmation(args.confirm, args.reason)
        if entity_launch or args.operation == "resolve-entity":
            from .entity_execution import _window
            from .protocol import identifier
            from .recovery import _confirmation

            if sys.platform != "linux":
                raise CoordinationError(
                    "Entity service operations are qualified only on Linux"
                )
            if (
                args.job_id is not None
                or args.maintenance_id is not None
                or args.confirm_replace_checkpoints
                or args.after_job is not None
                or args.after_run is not None
                or args.after_maintenance is not None
                or entity_launch
                and args.run_id is not None
            ):
                raise CoordinationError(
                    "Entity service operation refuses unrelated flags"
                )
            _confirmation(args.confirm, args.reason)
            if entity_launch:
                _window(args.start_hour, args.end_hour)
            else:
                identifier(args.run_id)
        if args.operation == "retry-hour":
            from .recovery import _confirmation

            if sys.platform != "linux":
                raise CoordinationError("Intentional retry is qualified only on Linux")
            if (
                args.job_id is not None
                or args.maintenance_id is not None
                or args.confirm_replace_checkpoints
                or args.after_job
                or args.after_run
                or args.after_maintenance
            ):
                raise CoordinationError("Intentional retry refuses unrelated flags")
            _confirmation(args.confirm, args.reason)
        config = load_config(args.config)
        directory, binding = config["state_directory"], config["binding"]
        now = _now()
        if args.operation == "init":
            if not args.confirm or not args.reason:
                raise CoordinationError(
                    "Initialization requires confirmation/reason and external exclusion"
                )
            Ledger.initialize(directory, binding, config["first_hour"])
            value = {"initialized": True}
        elif args.operation == "upgrade":
            selected = (
                {"target_version": int(args.target_version)}
                if args.target_version is not None
                else {}
            )
            value = Ledger.upgrade(
                directory,
                binding,
                now,
                confirmed=args.confirm,
                reason=args.reason,
                **selected,
            )
        elif args.operation == "tick":
            value = service_tick(
                config,
                open_message_store(config["coordination_root"], config["storage"]),
            )
        else:
            with Ledger.open(directory, binding) as ledger:
                if args.operation == "status":
                    value = status_page(
                        ledger,
                        now,
                        after_job=args.after_job or "",
                        after_run=args.after_run or "",
                        after_maintenance=args.after_maintenance or "",
                    )
                elif entity_launch:
                    from .entity_execution import prepare_entity_execution
                    from .entity_writer import execute_entity_and_complete_prepared

                    ledger._require_entity_schema()
                    state = ledger.snapshot()
                    if (
                        "entity_writer" not in config
                        or state["mode"] not in {"OPEN", "DRAINING"}
                        or state["active_run_id"] is not None
                        or state["readers"]
                    ):
                        raise CoordinationError(
                            "Entity launch requires separate configuration and unowned zero-reader exclusion"
                        )
                    prepared = prepare_entity_execution(
                        ledger.directory,
                        binding,
                        args.start_hour,
                        args.end_hour,
                        now,
                        config["entity_writer"],
                        operation=args.operation,
                    )
                    value = execute_entity_and_complete_prepared(
                        ledger,
                        prepared,
                        now,
                        clock=_now,
                        confirmed=args.confirm,
                        reason=args.reason,
                    )
                elif args.operation == "resolve-entity":
                    from .entity_writer import resolve_entity_prepared
                    from .recovery import load_original_entity_execution

                    prepared = load_original_entity_execution(
                        ledger, args.run_id, config["storage"]
                    )
                    value = resolve_entity_prepared(
                        ledger,
                        prepared,
                        now,
                        clock=_now,
                        confirmed=args.confirm,
                        reason=args.reason,
                    )
                elif args.operation == "release-reader":
                    from .recovery import release_reader

                    value = release_reader(
                        ledger,
                        args.job_id,
                        now,
                        confirmed=args.confirm,
                        reason=args.reason,
                    )
                elif args.operation == "abandon-maintenance":
                    from .maintenance import abandon

                    value = abandon(
                        ledger,
                        args.maintenance_id,
                        now,
                        confirmed=args.confirm,
                        reason=args.reason,
                    )
                elif args.operation == "retry-hour":
                    from .intentional_retry import retry_and_complete_prepared
                    from .recovery import load_original_execution

                    prepared = load_original_execution(
                        ledger, args.run_id, config["writer"]
                    )
                    value = retry_and_complete_prepared(
                        ledger,
                        prepared,
                        now,
                        clock=_now,
                        confirmed=args.confirm,
                        reason=args.reason,
                    )
                elif args.operation in {
                    "resolve-hour",
                    "repair-checkpoints",
                    "repair-deletions",
                }:
                    from .recovery import (
                        load_original_execution,
                        repair_checkpoints,
                        repair_deletions,
                        resolve_hour,
                    )

                    prepared = load_original_execution(
                        ledger, args.run_id, config["writer"]
                    )
                    if args.operation in {"repair-checkpoints", "repair-deletions"}:
                        repair = (
                            repair_deletions
                            if args.operation == "repair-deletions"
                            else repair_checkpoints
                        )
                        value = {
                            "maintenance_id": repair(
                                ledger,
                                prepared,
                                now,
                                confirmed=args.confirm,
                                reason=args.reason,
                                replace_checkpoints=args.confirm_replace_checkpoints,
                            )
                        }
                    else:
                        value = resolve_hour(
                            ledger,
                            prepared,
                            now,
                            clock=_now,
                            confirmed=args.confirm,
                            reason=args.reason,
                            maintenance_id=args.maintenance_id,
                        )
                else:
                    prepared = prepare_execution(
                        directory,
                        binding,
                        ledger.snapshot()["next_hour"],
                        now,
                        config["writer"],
                    )
                    value = verify_authority_prepared(
                        ledger,
                        prepared,
                        now,
                        initialize=True,
                        confirmed=args.confirm,
                        reason=args.reason,
                    )
                _export(ledger, _now())
        print(canonical(value))
        return 0
    except (CoordinationError, OSError, ValueError):
        print(
            "Coordination operation refused/uncertain; retain original state and evidence",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
