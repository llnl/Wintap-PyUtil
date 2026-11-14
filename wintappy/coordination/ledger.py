"""Server-local DuckDB permissions, guarded by one persistent OS lock inode.

The caller must provision persistent local disk (not a network filesystem).
Only the qualified same-session completion adapter can finish a clean live intent.
Previously blocked/restarted intents still require explicit audited recovery.
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

SCHEMA_V1 = """
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
AUTHORITY_SCHEMA = """
CREATE TABLE authority_checks (
    sequence BIGINT PRIMARY KEY,
    proof_json VARCHAR NOT NULL,
    proof_sha256 VARCHAR NOT NULL UNIQUE,
    previous_sha256 VARCHAR NOT NULL
);
"""
SCHEMA_V2 = (
    SCHEMA_V1.replace("CHECK (schema_version=1)", "CHECK (schema_version=2)")
    + AUTHORITY_SCHEMA
)
SCHEMA_VERSION = 3
COMPLETION_SCHEMA = """
CREATE TABLE completion_evidence (
    sequence BIGINT PRIMARY KEY,
    run_id VARCHAR NOT NULL UNIQUE,
    proof_json VARCHAR NOT NULL,
    proof_sha256 VARCHAR NOT NULL UNIQUE,
    previous_sha256 VARCHAR NOT NULL
);
"""
SCHEMA = (
    SCHEMA_V2.replace("CHECK (schema_version=2)", "CHECK (schema_version=3)")
    .replace(
        "binding_json VARCHAR NOT NULL\n",
        "binding_json VARCHAR NOT NULL,\n    first_hour VARCHAR NOT NULL\n",
    )
    .replace(
        "state IN ('INTENT','BLOCKED')", "state IN ('INTENT','BLOCKED','COMPLETED')"
    )
    + COMPLETION_SCHEMA
)
TABLES_V1 = {"metadata", "dataset_state", "jobs", "outbox", "compaction_runs", "events"}
TABLES_V2 = TABLES_V1 | {"authority_checks"}
TABLES = TABLES_V2 | {"completion_evidence"}
COLUMNS = {
    "metadata": (
        "singleton",
        "schema_version",
        "ledger_id",
        "binding_json",
        "first_hour",
    ),
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
    "authority_checks": ("sequence", "proof_json", "proof_sha256", "previous_sha256"),
    "completion_evidence": (
        "sequence",
        "run_id",
        "proof_json",
        "proof_sha256",
        "previous_sha256",
    ),
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
        self._authority_checked = None
        self._completion_live_run = None

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
                        "INSERT INTO metadata VALUES (1,3,?,?,?)",
                        [uuid4().hex, ledger.binding, first_hour],
                    )
                    connection.execute(
                        "INSERT INTO dataset_state VALUES (1,'OPEN',?,NULL,'')",
                        [first_hour],
                    )
                    ledger._event(
                        "initialized", "dataset", _now(), {"first_hour": first_hour}
                    )
                    ledger._event(
                        "completion_history_initialized",
                        "dataset",
                        _now(),
                        {"first_hour": first_hour, "source_schema": 0},
                    )
                connection.execute("CHECKPOINT")
            finally:
                connection.close()
            _fsync_directory(directory)

    @classmethod
    def upgrade(cls, directory, binding, now, *, confirmed=False, reason=None):
        """Explicit compatible v1/v2 upgrade; preserve all permissions and evidence."""
        validate_binding(binding)
        timestamp(now)
        if confirmed is not True:
            raise CoordinationError("Ledger upgrade requires explicit confirmation")
        _validate_block_reason(reason)
        directory = _state_directory(directory, binding)
        with _locked(directory):
            path = directory / "coordinator.duckdb"
            if not path.is_file():
                raise CoordinationError(
                    "Ledger missing; upgrade cannot create authority"
                )
            connection = None
            try:
                connection = _connect(path)
                ledger = cls(connection, directory, binding)
                source = connection.execute(
                    "SELECT schema_version FROM metadata"
                ).fetchall()
                if source not in [[(1,)], [(2,)]]:
                    raise CoordinationError(
                        "Only compatible v1/v2 ledger upgrade is supported"
                    )
                version = source[0][0]
                ledger._validate(version=version)
                ledger._block_unresolved_writer()
                with ledger._transaction():
                    connection.execute("ALTER TABLE metadata RENAME TO metadata_old")
                    connection.execute("""CREATE TABLE metadata (
                        singleton INTEGER PRIMARY KEY CHECK (singleton=1),
                        schema_version INTEGER NOT NULL CHECK (schema_version=3),
                        ledger_id VARCHAR NOT NULL UNIQUE, binding_json VARCHAR NOT NULL,
                        first_hour VARCHAR NOT NULL
                    )""")
                    connection.execute(
                        "INSERT INTO metadata "
                        "SELECT singleton,3,ledger_id,binding_json,"
                        "(SELECT next_hour FROM dataset_state) FROM metadata_old"
                    )
                    connection.execute("DROP TABLE metadata_old")
                    connection.execute(
                        "ALTER TABLE compaction_runs RENAME TO compaction_runs_old"
                    )
                    run_schema = SCHEMA_V1.split("CREATE TABLE compaction_runs (", 1)[
                        1
                    ].split("CREATE TABLE events", 1)[0]
                    connection.execute(
                        "CREATE TABLE compaction_runs ("
                        + run_schema.replace(
                            "state IN ('INTENT','BLOCKED')",
                            "state IN ('INTENT','BLOCKED','COMPLETED')",
                        )
                    )
                    connection.execute(
                        "INSERT INTO compaction_runs SELECT * FROM compaction_runs_old"
                    )
                    connection.execute("DROP TABLE compaction_runs_old")
                    if version == 1:
                        connection.execute(AUTHORITY_SCHEMA)
                    connection.execute(COMPLETION_SCHEMA)
                    ledger._event(
                        "schema_upgraded",
                        "dataset",
                        now,
                        {"from": version, "to": SCHEMA_VERSION, "reason": reason},
                    )
                    ledger._event(
                        "completion_history_initialized",
                        "dataset",
                        now,
                        {
                            "first_hour": ledger.snapshot()["next_hour"],
                            "source_schema": version,
                        },
                    )
                    ledger._validate()
                connection.execute("CHECKPOINT")
            except duckdb.Error:
                raise CoordinationError(
                    "Invalid/unreadable ledger; upgrade refused without replacement"
                ) from None
            finally:
                if connection is not None:
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
    def _transaction(self, *, kind=None):
        connection = self._connection
        connection.execute("BEGIN TRANSACTION")
        committed = False
        try:
            yield
            self.failure_hook("before_commit")
            if kind:
                self.failure_hook(f"before_{kind}_commit")
            connection.execute("COMMIT")
            committed = True
            if kind:
                self.failure_hook(f"after_{kind}_commit")
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

    def _validate(self, *, version=SCHEMA_VERSION):
        connection = self._connection
        tables = {r[0] for r in connection.execute("SHOW TABLES").fetchall()}
        if tables != {1: TABLES_V1, 2: TABLES_V2, 3: TABLES}.get(version):
            raise CoordinationError(
                "Ledger schema mismatch; explicit upgrade/recovery required"
            )
        for table, expected in COLUMNS.items():
            if (version == 1 and table == "authority_checks") or (
                version < 3 and table == "completion_evidence"
            ):
                continue
            if table == "metadata" and version < 3:
                expected = expected[:-1]
            actual = connection.execute(f"DESCRIBE {table}").fetchall()
            expected_types = tuple(
                "INTEGER"
                if name == "singleton" or name == "schema_version"
                else "BIGINT"
                if name == "sequence"
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
            or metadata[0][:2] != (1, version)
            or metadata[0][3] != self.binding
        ):
            raise CoordinationError("Ledger schema/binding mismatch")
        identifier(metadata[0][2])
        if version == 3:
            timestamp(metadata[0][4], hour=True)
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
        runs = connection.execute(
            "SELECT run_id FROM compaction_runs WHERE state<>'COMPLETED'"
        ).fetchall()
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
        cursor = connection.execute(
            "SELECT intent_json,intent_sha256,run_id,cli_run_id,hour_start,hour_end,started_at FROM compaction_runs"
        )
        while batch := cursor.fetchmany(256):
            for row in batch:
                self._validate_intent_row(row)
        if version >= 2:
            self._validate_authority_checks()
        if version == 3:
            self._validate_completion_history()

    def _validate_intent_row(self, row):
        (
            payload,
            expected_hash,
            stored_run_id,
            cli_run_id,
            start,
            end,
            started,
        ) = row
        try:
            run = json.loads(payload)
            identifier(stored_run_id)
            identifier(cli_run_id)
            first = timestamp(start, hour=True)
            timestamp(started)
            valid = (
                set(run) - {"execution"}
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
                and end == (first + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
                and canonical(run["binding"]) == self.binding
                and canonical(run) == payload
                and digest(run) == expected_hash
            )
        except (TypeError, ValueError, KeyError, RecursionError):
            valid = False
        if not valid:
            raise CoordinationError("Stored writer intent/identity conflict")
        validate_writer_command(run["command"])
        if "execution" in run:
            from .execution import validate_intent_execution

            validate_intent_execution(run["execution"], run)

    @property
    def ledger_id(self):
        return self._connection.execute("SELECT ledger_id FROM metadata").fetchone()[0]

    def authority_high_water(self):
        row = self._connection.execute(
            "SELECT proof_json FROM authority_checks ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        return json.loads(row[0])["result"]["authority_high_water"] if row else None

    def _validate_authority_checks(self):
        from .authority import MAX_AUTHORITY_BYTES, MAX_AUTHORITY_CHECKS, validate_proof

        previous, previous_hash, count = None, "", 0
        binding = json.loads(self.binding)
        ledger_id = self.ledger_id
        rows = self._connection.execute(
            "SELECT * FROM authority_checks ORDER BY sequence"
        )
        while batch := rows.fetchmany(256):
            for sequence, payload, sha256, predecessor in batch:
                count += 1
                if (
                    count > MAX_AUTHORITY_CHECKS
                    or sequence != count
                    or predecessor != previous_hash
                    or len(payload.encode()) > MAX_AUTHORITY_BYTES
                ):
                    raise CoordinationError(
                        "Authority high-water history is invalid or exceeds budget"
                    )
                try:
                    proof = json.loads(payload)
                    if canonical(proof) != payload or digest(proof) != sha256:
                        raise ValueError
                    previous = validate_proof(proof, binding, ledger_id, previous)
                except (TypeError, ValueError, KeyError, RecursionError):
                    raise CoordinationError(
                        "Authority high-water evidence is malformed"
                    ) from None
                previous_hash = sha256

    def _record_authority_check(self, proof):
        """Internal qualified-verifier seam; no public caller-supplied anchor setter."""
        with self._transaction():
            self._insert_authority_check(proof)
        self._authority_checked = digest(proof)

    def _insert_authority_check(self, proof):
        """Called only within an authority or completion transaction."""
        from .authority import MAX_AUTHORITY_CHECKS, validate_proof

        previous = self.authority_high_water()
        validate_proof(proof, json.loads(self.binding), self.ledger_id, previous)
        row = self._connection.execute(
            "SELECT sequence,proof_sha256 FROM authority_checks ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        sequence, predecessor = (row[0] + 1, row[1]) if row else (1, "")
        if sequence > MAX_AUTHORITY_CHECKS:
            raise CoordinationError(
                "Authority history budget exhausted; never truncate"
            )
        self._connection.execute(
            "INSERT INTO authority_checks VALUES (?,?,?,?)",
            [sequence, canonical(proof), digest(proof), predecessor],
        )
        self._event(
            "authority_initialized" if previous is None else "authority_checked",
            "dataset",
            proof["observed_at"],
            {"proof_sha256": digest(proof)},
        )

    def completion_evidence(self, run_id):
        identifier(run_id)
        row = self._connection.execute(
            "SELECT proof_json FROM completion_evidence WHERE run_id=?", [run_id]
        ).fetchone()
        return json.loads(row[0]) if row else None

    def _completion_clock_floor(self):
        row = self._connection.execute(
            "SELECT json_extract_string(proof_json,'$.completed_at') "
            "FROM completion_evidence ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        return row[0] if row else None

    def _validate_completion_history(self):
        from .completion import (
            MAX_COMPLETED_RUNS,
            MAX_HISTORY_BYTES,
            validate_completion_proof,
        )

        first = self._connection.execute("SELECT first_hour FROM metadata").fetchone()[
            0
        ]
        origin = self._connection.execute(
            "SELECT identity,observed_at,details_json FROM events "
            "WHERE kind='completion_history_initialized' LIMIT 2"
        ).fetchall()
        try:
            if len(origin) != 1 or origin[0][0] != "dataset":
                raise ValueError
            timestamp(origin[0][1])
            details = json.loads(origin[0][2])
            if (
                canonical(details) != origin[0][2]
                or set(details) != {"first_hour", "source_schema"}
                or details["first_hour"] != first
                or type(details["source_schema"]) is not int
                or details["source_schema"] not in {0, 1, 2}
            ):
                raise ValueError
        except (TypeError, ValueError, KeyError, RecursionError):
            raise CoordinationError(
                "Completion history origin/audit conflict"
            ) from None
        expected_hour, previous_hash, count, previous_completed = first, "", 0, None
        while True:
            rows = self._connection.execute(
                "SELECT * FROM completion_evidence WHERE sequence>? ORDER BY sequence LIMIT 256",
                [count],
            ).fetchall()
            if not rows:
                break
            for sequence, run_id, payload, sha256, predecessor in rows:
                count += 1
                if (
                    count > MAX_COMPLETED_RUNS
                    or sequence != count
                    or predecessor != previous_hash
                    or len(payload.encode()) > MAX_HISTORY_BYTES
                ):
                    raise CoordinationError(
                        "Completion history is invalid or exceeds budget"
                    )
                try:
                    proof = json.loads(payload)
                    run = self.writer_run(run_id)
                    if (
                        canonical(proof) != payload
                        or digest(proof) != sha256
                        or run is None
                        or run["state"] != "COMPLETED"
                        or run["block_reason"]
                        or run["hour_start"] != expected_hour
                        or previous_completed is not None
                        and timestamp(run["started_at"]) < previous_completed
                    ):
                        raise ValueError
                    validate_completion_proof(
                        proof, run, json.loads(self.binding), self.ledger_id
                    )
                    audit = self._connection.execute(
                        "SELECT observed_at,details_json FROM events "
                        "WHERE kind='writer_completed' AND identity=? LIMIT 2",
                        [run_id],
                    ).fetchall()
                    if audit != [
                        (
                            proof["completed_at"],
                            canonical(
                                {
                                    "proof_sha256": sha256,
                                    "next_hour": proof["next_hour"],
                                    "mode": proof["mode"],
                                }
                            ),
                        )
                    ]:
                        raise ValueError
                    if proof["authority_proof"] is not None:
                        retained = self._connection.execute(
                            "SELECT proof_json FROM authority_checks WHERE proof_sha256=?",
                            [digest(proof["authority_proof"])],
                        ).fetchone()
                        if retained != (canonical(proof["authority_proof"]),):
                            raise ValueError
                except (TypeError, ValueError, KeyError, RecursionError):
                    raise CoordinationError(
                        "Completion history evidence is malformed"
                    ) from None
                expected_hour, previous_hash = run["hour_end"], sha256
                previous_completed = timestamp(proof["completed_at"])
        if (
            self.snapshot()["next_hour"] != expected_hour
            or self._connection.execute(
                "SELECT count(*) FROM compaction_runs WHERE state='COMPLETED'"
            ).fetchone()[0]
            != count
            or self._connection.execute(
                "SELECT count(*) FROM events WHERE kind='writer_completed'"
            ).fetchone()[0]
            != count
        ):
            raise CoordinationError("Completion history/run/cursor conflict")

    def _complete_writer(self, proof):
        """Internal fresh adapter seam, not a public report-to-permission API."""
        from .completion import MAX_COMPLETED_RUNS, validate_completion_proof

        self._authority_checked = None
        try:
            self._validate()
            with self._transaction(kind="completion"):
                state = self.snapshot()
                run = self.writer_run(proof["run_id"])
                if (
                    self._completion_live_run != proof["run_id"]
                    or state["mode"] != "WRITING"
                    or state["active_run_id"] != proof["run_id"]
                    or state["readers"]
                    or run is None
                    or run["state"] != "INTENT"
                    or state["next_hour"] != run["hour_start"]
                    or self.authority_high_water() != proof["authority_before"]
                ):
                    raise CoordinationError(
                        "Completion requires this session's clean live writer"
                    )
                validate_completion_proof(
                    proof, run, json.loads(self.binding), self.ledger_id
                )
                row = self._connection.execute(
                    "SELECT sequence,proof_sha256 FROM completion_evidence ORDER BY sequence DESC LIMIT 1"
                ).fetchone()
                sequence, predecessor = (row[0] + 1, row[1]) if row else (1, "")
                if sequence > MAX_COMPLETED_RUNS:
                    raise CoordinationError(
                        "Completed history budget exhausted; never prune"
                    )
                if proof["authority_proof"] is not None:
                    self._insert_authority_check(proof["authority_proof"])
                self._connection.execute(
                    "INSERT INTO completion_evidence VALUES (?,?,?,?,?)",
                    [
                        sequence,
                        proof["run_id"],
                        canonical(proof),
                        digest(proof),
                        predecessor,
                    ],
                )
                self._connection.execute(
                    "UPDATE compaction_runs SET state='COMPLETED',block_reason='' WHERE run_id=?",
                    [proof["run_id"]],
                )
                self._connection.execute(
                    "UPDATE dataset_state SET mode=?,next_hour=?,active_run_id=NULL,block_reason='' WHERE singleton=1",
                    [proof["mode"], proof["next_hour"]],
                )
                self._event(
                    "writer_completed",
                    proof["run_id"],
                    proof["completed_at"],
                    {
                        "proof_sha256": digest(proof),
                        "next_hour": proof["next_hour"],
                        "mode": proof["mode"],
                    },
                )
            if proof["authority_proof"] is not None:
                self._authority_checked = digest(proof["authority_proof"])
            return self.snapshot()
        finally:
            self._completion_live_run = None

    def _require_entity_authority(self):
        if json.loads(self.binding)["entity"] is None:
            return
        row = self._connection.execute(
            "SELECT proof_sha256 FROM authority_checks ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        if row is None or self._authority_checked != row[0]:
            raise CoordinationError(
                "Entity permissions require independently initialized and freshly "
                "verified authority high-water"
            )

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

    def writer_run(self, run_id):
        identifier(run_id)
        cursor = self._connection.execute(
            "SELECT * FROM compaction_runs WHERE run_id=?", [run_id]
        )
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
            self._require_entity_authority()
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

    def begin_writer(self, hour, now, command, *, execution=None):
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
        if execution is not None:
            from .execution import validate_intent_execution

            run["run_id"] = execution["receipt"]["run_id"]
            run["cli_run_id"] = execution["receipt"]["cli_run_id"]
            run["execution"] = json.loads(canonical(execution))
            validate_intent_execution(run["execution"], run)
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
            self._require_entity_authority()
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
        self._authority_checked = None
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
