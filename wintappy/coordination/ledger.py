"""Server-local DuckDB permissions, guarded by one persistent OS lock inode.

The caller must provision persistent local disk (not a network filesystem).
This module records writer intent, but cannot certify completion or reopen it.
No object listing, time limit or process ID can retire a reader reservation.
"""

import fcntl
import json
import os
import stat
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import duckdb

from .protocol import (
    MAX_LIST_BYTES,
    MAX_MESSAGES,
    CoordinationError,
    canonical,
    decode_message,
    digest,
    identifier,
    make_grant,
    separate_transport_root,
    timestamp,
    validate_binding,
    validate_message,
)

SCHEMA = """
CREATE TABLE metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton=1),
    schema_version INTEGER NOT NULL CHECK (schema_version=1),
    ledger_id VARCHAR NOT NULL UNIQUE,
    binding_json VARCHAR NOT NULL
);
CREATE TABLE dataset_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton=1),
    mode VARCHAR NOT NULL CHECK (mode IN ('OPEN','DRAINING','WRITING','BLOCKED')),
    next_hour VARCHAR NOT NULL,
    active_run_id VARCHAR,
    block_reason VARCHAR NOT NULL
);
CREATE TABLE jobs (
    job_id VARCHAR PRIMARY KEY,
    request_sha256 VARCHAR NOT NULL,
    request_json VARCHAR,
    first_seen VARCHAR NOT NULL,
    state VARCHAR NOT NULL CHECK (state IN ('WAITING','GRANTED','RELEASED','CANCELLED')),
    grant_id VARCHAR UNIQUE,
    grant_json VARCHAR,
    terminal_json VARCHAR
);
CREATE TABLE outbox (
    job_id VARCHAR PRIMARY KEY REFERENCES jobs(job_id),
    payload VARCHAR NOT NULL,
    payload_sha256 VARCHAR NOT NULL,
    state VARCHAR NOT NULL CHECK (state IN ('PENDING','PUBLISHED','SUPPRESSED'))
);
CREATE TABLE compaction_runs (
    run_id VARCHAR PRIMARY KEY,
    cli_run_id VARCHAR NOT NULL UNIQUE,
    hour_start VARCHAR NOT NULL,
    hour_end VARCHAR NOT NULL,
    state VARCHAR NOT NULL CHECK (state IN ('INTENT','BLOCKED')),
    started_at VARCHAR NOT NULL,
    intent_json VARCHAR NOT NULL,
    intent_sha256 VARCHAR NOT NULL,
    block_reason VARCHAR NOT NULL
);
CREATE TABLE events (
    event_id VARCHAR PRIMARY KEY,
    observed_at VARCHAR NOT NULL,
    kind VARCHAR NOT NULL,
    identity VARCHAR NOT NULL,
    details_json VARCHAR NOT NULL
);
"""
TABLES = {"metadata", "dataset_state", "jobs", "outbox", "compaction_runs", "events"}
COLUMNS = {
    "metadata": ("singleton", "schema_version", "ledger_id", "binding_json"),
    "dataset_state": (
        "singleton",
        "mode",
        "next_hour",
        "active_run_id",
        "block_reason",
    ),
    "jobs": (
        "job_id",
        "request_sha256",
        "request_json",
        "first_seen",
        "state",
        "grant_id",
        "grant_json",
        "terminal_json",
    ),
    "outbox": ("job_id", "payload", "payload_sha256", "state"),
    "compaction_runs": (
        "run_id",
        "cli_run_id",
        "hour_start",
        "hour_end",
        "state",
        "started_at",
        "intent_json",
        "intent_sha256",
        "block_reason",
    ),
    "events": ("event_id", "observed_at", "kind", "identity", "details_json"),
}


def _fsync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _mkdir(path):
    missing = []
    current = path
    while not current.exists():
        missing.append(current)
        current = current.parent
    for directory in reversed(missing):
        directory.mkdir(mode=0o700)
        _fsync_directory(directory.parent)
        _fsync_directory(directory)


def _state_directory(directory, binding):
    if "://" in str(directory):
        raise CoordinationError("Ledger directory must be server-local")
    path = Path(directory).expanduser().absolute()
    if any(part.is_symlink() for part in (path, *path.parents)):
        raise CoordinationError("Ledger directory must not traverse symlinks")
    path = path.resolve()
    if not path.is_absolute():
        raise CoordinationError("Ledger directory must be server-local")
    for root in binding["roots"].values():
        if not root.startswith("s3://"):
            source = Path(root)
            if path == source or source in path.parents or path in source.parents:
                raise CoordinationError("Ledger directory must not overlap data roots")
    return path


@contextmanager
def _locked(directory, *, initialize=False):
    if not directory.exists():
        if not initialize:
            raise CoordinationError(
                "Ledger directory/database missing; explicit init required"
            )
        _mkdir(directory)
    if not directory.is_dir():
        raise CoordinationError("Ledger directory is not a directory")
    descriptor = os.open(
        directory / "coordinator.lock",
        os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC,
        0o600,
    )
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise CoordinationError("Ledger lock must be one regular persistent inode")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise CoordinationError("Coordinator busy; ledger not opened") from None
        os.fsync(descriptor)
        _fsync_directory(directory)
        yield
    finally:
        # Never unlink: other processes must acquire this same inode.
        os.close(descriptor)


def _connect(path):
    if duckdb.__version__ != "1.5.6":
        raise CoordinationError(
            "Coordination ledger requires the main DuckDB 1.5.6 runtime"
        )
    if path.is_symlink() or Path(str(path) + ".wal").is_symlink():
        raise CoordinationError("Ledger database/WAL must not be symlinks")
    return duckdb.connect(str(path), config={"threads": 2, "memory_limit": "128MB"})


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Ledger:
    """Use initialize/open only; hold the local lock throughout all external effects."""

    def __init__(self, connection, directory, binding):
        self._connection = connection
        self.directory = directory
        self.binding = canonical(binding)
        self.failure_hook = lambda phase: None

    @classmethod
    def initialize(cls, directory, binding, first_hour):
        validate_binding(binding)
        timestamp(first_hour, hour=True)
        directory = _state_directory(directory, binding)
        with _locked(directory, initialize=True):
            path = directory / "coordinator.duckdb"
            if path.exists() or path.is_symlink() or Path(str(path) + ".wal").exists():
                raise CoordinationError(
                    "Ledger already exists; initialization cannot replace authority"
                )
            connection = _connect(path)
            try:
                ledger = cls(connection, directory, binding)
                with ledger._transaction():
                    connection.execute(SCHEMA)
                    connection.execute(
                        "INSERT INTO metadata VALUES (1,1,?,?)",
                        [uuid4().hex, ledger.binding],
                    )
                    connection.execute(
                        "INSERT INTO dataset_state VALUES (1,'OPEN',?,NULL,'')",
                        [first_hour],
                    )
                    ledger._event(
                        "initialized", "dataset", _now(), {"first_hour": first_hour}
                    )
                connection.execute("CHECKPOINT")
            finally:
                connection.close()
            _fsync_directory(directory)

    @classmethod
    @contextmanager
    def open(cls, directory, binding):
        validate_binding(binding)
        directory = _state_directory(directory, binding)
        with _locked(directory):
            path = directory / "coordinator.duckdb"
            if not path.is_file():
                raise CoordinationError(
                    "Ledger database missing; explicit recovery/init required"
                )
            connection = None
            try:
                connection = _connect(path)
                ledger = cls(connection, directory, binding)
                ledger._validate()
                ledger._block_unresolved_writer()
            except duckdb.Error as error:
                if connection is not None:
                    connection.close()
                raise CoordinationError(
                    "Invalid/unreadable coordination ledger; explicit recovery required"
                ) from error
            except BaseException:
                if connection is not None:
                    connection.close()
                raise
            try:
                yield ledger
            finally:
                connection.close()

    @contextmanager
    def _transaction(self):
        connection = self._connection
        connection.execute("BEGIN TRANSACTION")
        committed = False
        try:
            yield
            self.failure_hook("before_commit")
            connection.execute("COMMIT")
            committed = True
            self.failure_hook("after_commit")
        except BaseException:
            if not committed:
                connection.execute("ROLLBACK")
            raise

    def _event(self, kind, identity, now, details):
        self._connection.execute(
            "INSERT INTO events VALUES (?,?,?,?,?)",
            [uuid4().hex, now, kind, identity, canonical(details)],
        )

    def _validate(self):
        connection = self._connection
        tables = {r[0] for r in connection.execute("SHOW TABLES").fetchall()}
        if tables != TABLES:
            raise CoordinationError(
                "Ledger schema mismatch; explicit recovery required"
            )
        for table, expected in COLUMNS.items():
            actual = connection.execute(f"DESCRIBE {table}").fetchall()
            expected_types = tuple(
                "INTEGER"
                if name == "singleton" or name == "schema_version"
                else "VARCHAR"
                for name in expected
            )
            if (
                tuple(row[0] for row in actual) != expected
                or tuple(row[1] for row in actual) != expected_types
            ):
                raise CoordinationError(
                    "Ledger column/schema mismatch; explicit recovery required"
                )
        metadata = connection.execute("SELECT * FROM metadata").fetchall()
        if (
            len(metadata) != 1
            or metadata[0][:2] != (1, 1)
            or metadata[0][3] != self.binding
        ):
            raise CoordinationError("Ledger schema/binding mismatch")
        identifier(metadata[0][2])
        state = connection.execute("SELECT * FROM dataset_state").fetchall()
        if len(state) != 1 or state[0][0] != 1:
            raise CoordinationError("Invalid ledger dataset state")
        mode, hour, run_id, reason = state[0][1:]
        timestamp(hour, hour=True)
        if mode not in {"OPEN", "DRAINING", "WRITING", "BLOCKED"}:
            raise CoordinationError("Invalid ledger mode")
        if mode in {"OPEN", "DRAINING"} and (run_id is not None or reason):
            raise CoordinationError("Unresolved writer cannot be an open ledger")
        if mode in {"WRITING", "BLOCKED"}:
            if mode == "BLOCKED":
                _validate_block_reason(reason)
            elif reason:
                raise CoordinationError(
                    "Unresolved writer intent has an invalid block reason"
                )
            run = connection.execute(
                "SELECT hour_start,state,block_reason FROM compaction_runs WHERE run_id=?",
                [run_id],
            ).fetchall()
            if run != [(hour, "INTENT" if mode == "WRITING" else "BLOCKED", reason)]:
                raise CoordinationError("Unresolved writer run/ledger mismatch")
            if connection.execute(
                "SELECT count(*) FROM jobs WHERE state='GRANTED'"
            ).fetchone()[0]:
                raise CoordinationError("Reader reservation overlaps unresolved writer")
        runs = connection.execute("SELECT run_id FROM compaction_runs").fetchall()
        if runs != ([(run_id,)] if run_id is not None else []):
            raise CoordinationError(
                "Orphan/unresolved writer history requires explicit recovery"
            )
        invalid = connection.execute("""
            SELECT count(*) FROM jobs j LEFT JOIN outbox o USING (job_id) WHERE
                NOT coalesce(regexp_full_match(j.job_id,'[a-f0-9]{32}'),false)
                OR NOT coalesce(regexp_full_match(j.request_sha256,'[a-f0-9]{64}'),false)
                OR j.state IS NULL OR j.state NOT IN ('WAITING','GRANTED','RELEASED','CANCELLED')
                OR (j.state='WAITING' AND (j.request_json IS NULL OR j.grant_id IS NOT NULL
                    OR j.terminal_json IS NOT NULL OR o.job_id IS NOT NULL))
                OR (j.state='GRANTED' AND (j.request_json IS NULL OR j.grant_id IS NULL
                    OR j.grant_json IS NULL OR j.terminal_json IS NOT NULL
                    OR o.state IS NULL OR o.state NOT IN ('PENDING','PUBLISHED')))
                OR (j.state IN ('RELEASED','CANCELLED') AND j.terminal_json IS NULL)
                OR (j.state='RELEASED' AND (j.grant_id IS NULL OR j.grant_json IS NULL))
                OR (j.grant_id IS NULL) IS DISTINCT FROM (j.grant_json IS NULL)
                OR (j.grant_id IS NULL) IS DISTINCT FROM (o.job_id IS NULL)
                OR (o.job_id IS NOT NULL AND (o.payload IS DISTINCT FROM j.grant_json
                    OR o.payload_sha256 IS DISTINCT FROM sha256(o.payload)))
                OR (j.state IN ('RELEASED','CANCELLED') AND o.state IS NOT NULL
                    AND o.state <> 'SUPPRESSED')
        """).fetchone()[0]
        if invalid:
            raise CoordinationError(
                "Invalid reader reservation/outbox; explicit recovery required"
            )
        cursor = connection.execute("SELECT * FROM jobs")
        while batch := cursor.fetchmany(256):
            for row in batch:
                timestamp(row[3])
                for field, kind in (
                    (2, "request"),
                    (6, "grant"),
                    (7, "release" if row[4] == "RELEASED" else "cancellation"),
                ):
                    payload = row[field]
                    if payload is None:
                        continue
                    message = decode_message(payload.encode())
                    if (
                        canonical(message) != payload
                        or message["kind"] != kind
                        or message["job_id"] != row[0]
                        or message["request_sha256"] != row[1]
                        or canonical(message["binding"]) != self.binding
                        or kind in {"grant", "release"}
                        and message["grant_id"] != row[5]
                    ):
                        raise CoordinationError(
                            "Stored reader message/identity conflict"
                        )
        # No completion API exists in this slice: every retained run is unresolved.
        for (
            payload,
            expected_hash,
            stored_run_id,
            cli_run_id,
            start,
            end,
            started,
        ) in connection.execute(
            "SELECT intent_json,intent_sha256,run_id,cli_run_id,hour_start,hour_end,started_at FROM compaction_runs"
        ).fetchall():
            try:
                run = json.loads(payload)
                identifier(stored_run_id)
                identifier(cli_run_id)
                first = timestamp(start, hour=True)
                timestamp(started)
                valid = (
                    set(run)
                    == {
                        "run_id",
                        "cli_run_id",
                        "hour_start",
                        "hour_end",
                        "started_at",
                        "command",
                        "binding",
                    }
                    and run["run_id"] == stored_run_id
                    and run["cli_run_id"] == cli_run_id
                    and run["hour_start"] == start
                    and run["hour_end"] == end
                    and run["started_at"] == started
                    and stored_run_id != cli_run_id
                    and end
                    == (first + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
                    and canonical(run["binding"]) == self.binding
                    and canonical(run) == payload
                    and digest(run) == expected_hash
                )
            except (TypeError, ValueError, KeyError, RecursionError):
                valid = False
            if not valid:
                raise CoordinationError("Stored writer intent/identity conflict")
            validate_writer_command(run["command"])

    def _block_unresolved_writer(self):
        state = self.snapshot()
        if state["mode"] != "WRITING":
            return
        reason = "Unresolved committed writer intent; confirm original child stopped and reconcile all phases"
        self.block_writer(state["active_run_id"], _now(), reason)

    def block_writer(self, run_id, now, reason, *, observation=None):
        """Retain unresolved intent, never complete/retry it or move the cursor."""
        identifier(run_id)
        timestamp(now)
        _validate_block_reason(reason)
        details = {"reason": reason}
        if observation is not None:
            encoded = canonical(observation)
            if not isinstance(observation, dict) or len(encoded.encode()) > 4096:
                raise CoordinationError(
                    "Writer observation must be a bounded JSON object"
                )
            details["observation"] = json.loads(encoded)
        with self._transaction():
            state = self.snapshot()
            if state["mode"] != "WRITING" or state["active_run_id"] != run_id:
                raise CoordinationError(
                    "Only the active committed writer intent may be blocked"
                )
            self._connection.execute(
                "UPDATE dataset_state SET mode='BLOCKED',block_reason=? WHERE singleton=1",
                [reason],
            )
            self._connection.execute(
                "UPDATE compaction_runs SET state='BLOCKED',block_reason=? WHERE run_id=?",
                [reason, run_id],
            )
            self._event("writer_unresolved", run_id, now, details)

    def snapshot(self):
        row = self._connection.execute("SELECT * FROM dataset_state").fetchone()
        readers, waiting = self._connection.execute(
            "SELECT count(*) FILTER (WHERE state='GRANTED'),"
            "count(*) FILTER (WHERE state='WAITING') FROM jobs"
        ).fetchone()
        return {
            "mode": row[1],
            "next_hour": row[2],
            "active_run_id": row[3],
            "block_reason": row[4],
            "readers": readers,
            "waiting": waiting,
        }

    def job(self, job_id):
        cursor = self._connection.execute("SELECT * FROM jobs WHERE job_id=?", [job_id])
        row = cursor.fetchone()
        return (
            dict(zip((c[0] for c in cursor.description), row, strict=True))
            if row
            else None
        )

    def ingest(self, value, observed_at=None):
        self.ingest_many((value,), observed_at)

    def ingest_many(self, values, observed_at=None):
        """Atomically ingest a bounded tick input, including terminal conflicts.

        The transport must finish all listings first. A failure during iteration
        or any conflict rolls back every terminal/request in this batch. Nothing
        retires a reservation merely because it was earlier in the input order.
        """
        now = observed_at or _now()
        timestamp(now)
        total_bytes = 0
        with self._transaction():
            for count, value in enumerate(values, 1):
                if count > 3 * MAX_MESSAGES:
                    raise CoordinationError("Tick messages exceed count budget")
                payload = canonical(validate_message(value))
                total_bytes += len(payload.encode())
                if total_bytes > 3 * MAX_LIST_BYTES:
                    raise CoordinationError(
                        "Tick messages exceed aggregate byte budget"
                    )
                self._ingest(payload, now)

    def _ingest(self, payload, now):
        # Decode the frozen canonical payload, never retain caller-owned state.
        message = decode_message(payload.encode())
        if canonical(message["binding"]) != self.binding:
            raise CoordinationError("Message dataset binding conflict")
        kind, job_id = message["kind"], message["job_id"]
        if kind == "grant":
            raise CoordinationError("Only the ledger may create grants")
        job = self.job(job_id)
        if job is not None and job["request_sha256"] != message["request_sha256"]:
            raise CoordinationError("Immutable request/terminal conflict")
        if kind == "request":
            if job is None:
                self._connection.execute(
                    "INSERT INTO jobs VALUES (?,?,?,?,'WAITING',NULL,NULL,NULL)",
                    [job_id, message["request_sha256"], payload, now],
                )
                self._event(
                    "request",
                    job_id,
                    now,
                    {"request_sha256": message["request_sha256"]},
                )
            elif job["request_json"] is None:
                self._connection.execute(
                    "UPDATE jobs SET request_json=? WHERE job_id=?",
                    [payload, job_id],
                )
            elif job["request_json"] != payload:
                raise CoordinationError("Immutable request payload conflict")
            return
        if job is not None and job["terminal_json"] is not None:
            if job["terminal_json"] != payload:
                raise CoordinationError("Immutable terminal message conflict")
            return
        if kind == "release":
            if (
                job is None
                or job["state"] != "GRANTED"
                or job["grant_id"] != message["grant_id"]
            ):
                raise CoordinationError("Release does not name a recorded reader grant")
            state = "RELEASED"
        else:
            state = "CANCELLED"
            if job is None:
                self._connection.execute(
                    "INSERT INTO jobs VALUES (?,?,NULL,?,'CANCELLED',NULL,NULL,?)",
                    [job_id, message["request_sha256"], now, payload],
                )
        self._connection.execute(
            "UPDATE jobs SET state=?,terminal_json=? WHERE job_id=?",
            [state, payload, job_id],
        )
        self._connection.execute(
            "UPDATE outbox SET state='SUPPRESSED' WHERE job_id=?", [job_id]
        )
        self._event(kind, job_id, now, {"request_sha256": message["request_sha256"]})

    def admit(self, now, *, limit=1000):
        timestamp(now)
        if type(limit) is not int or not 0 < limit <= 1000:
            raise CoordinationError("Admission limit must be between 1 and 1000")
        admitted = []
        with self._transaction():
            if self.snapshot()["mode"] != "OPEN":
                return admitted
            requests = self._connection.execute(
                "SELECT request_json FROM jobs WHERE state='WAITING' ORDER BY first_seen,job_id LIMIT ?",
                [limit],
            ).fetchall()
            for (payload,) in requests:
                request = decode_message(payload.encode())
                if canonical(request["binding"]) != self.binding:
                    raise CoordinationError("Stored request binding conflict")
                grant = make_grant(request, uuid4().hex, now)
                serialized = canonical(grant)
                self._connection.execute(
                    "UPDATE jobs SET state='GRANTED',grant_id=?,grant_json=? WHERE job_id=?",
                    [grant["grant_id"], serialized, grant["job_id"]],
                )
                self._connection.execute(
                    "INSERT INTO outbox VALUES (?,?,?,'PENDING')",
                    [grant["job_id"], serialized, digest(grant)],
                )
                self._event(
                    "reader_reserved",
                    grant["job_id"],
                    now,
                    {"grant_id": grant["grant_id"]},
                )
                admitted.append(grant)
        return admitted

    def pending_grants(self):
        return [
            decode_message(payload.encode())
            for (payload,) in self._connection.execute(
                "SELECT o.payload FROM outbox o JOIN jobs j USING(job_id) "
                "WHERE o.state='PENDING' AND j.state='GRANTED' ORDER BY j.first_seen,j.job_id"
            ).fetchall()
        ]

    def flush_grants(self, store):
        separate_transport_root(store.root, json.loads(self.binding))
        for grant in self.pending_grants():
            self.failure_hook("before_publish")
            store.publish(grant)
            self.failure_hook("after_publish")
            with self._transaction():
                self._connection.execute(
                    "UPDATE outbox SET state='PUBLISHED' WHERE job_id=? AND state='PENDING'",
                    [grant["job_id"]],
                )
                self._event(
                    "grant_published",
                    grant["job_id"],
                    _now(),
                    {"grant_id": grant["grant_id"]},
                )

    def drain(self, now):
        timestamp(now)
        with self._transaction():
            mode = self.snapshot()["mode"]
            if mode not in {"OPEN", "DRAINING"}:
                raise CoordinationError("Unresolved writer blocks draining/admission")
            if mode == "OPEN":
                self._connection.execute(
                    "UPDATE dataset_state SET mode='DRAINING' WHERE singleton=1"
                )
                self._event("draining", "dataset", now, {})

    def begin_writer(self, hour, now, command):
        start = timestamp(hour, hour=True)
        timestamp(now)
        validate_writer_command(command)
        run = {
            "run_id": uuid4().hex,
            "cli_run_id": uuid4().hex,
            "hour_start": hour,
            "hour_end": (start + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "started_at": now,
            "command": list(command),
            "binding": json.loads(self.binding),
        }
        with self._transaction():
            state = self.snapshot()
            if state["readers"]:
                raise CoordinationError(
                    "Outstanding reader reservations exclude the writer"
                )
            if state["mode"] != "DRAINING" or state["next_hour"] != hour:
                raise CoordinationError(
                    "Writer requires the selected draining cursor with no unresolved intent"
                )
            self._connection.execute(
                "INSERT INTO compaction_runs VALUES (?,?,?,?,'INTENT',?,?,?,'')",
                [
                    run["run_id"],
                    run["cli_run_id"],
                    hour,
                    run["hour_end"],
                    now,
                    canonical(run),
                    digest(run),
                ],
            )
            self._connection.execute(
                "UPDATE dataset_state SET mode='WRITING',active_run_id=? WHERE singleton=1",
                [run["run_id"]],
            )
            self._event(
                "writer_intent",
                run["run_id"],
                now,
                {"cli_run_id": run["cli_run_id"], "hour_start": hour},
            )
        return run


def validate_writer_command(command):
    if (
        not isinstance(command, list)
        or not 0 < len(command) <= 256
        or any(
            not isinstance(part, str) or not part or len(part) > 4096 or "\x00" in part
            for part in command
        )
    ):
        raise CoordinationError("Writer command must be a bounded argument list")


def _validate_block_reason(reason):
    if (
        not isinstance(reason, str)
        or not reason
        or len(reason) > 4096
        or any(ord(char) < 32 for char in reason)
    ):
        raise CoordinationError("Blocked writer requires a bounded diagnostic")
