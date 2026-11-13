"""Replay-safe process evidence; snapshots are context, never raw observations."""

import hashlib
import json
from collections import Counter, defaultdict
from dataclasses import dataclass

from .config import CompactionError

VERSION = "1"
IDENTITY_POLICIES = {"reject", "quarantine"}
FIELDS = ("pids", "parents", "births", "stops", "names", "paths", "users", "args", "agents", "activities")
SCHEMA = {
    "hostname": "VARCHAR", "pid_hash": "VARCHAR", "os_pid": "BIGINT",
    "parent_pid_hash": "VARCHAR", "parent_os_pid": "BIGINT",
    "parent_status": "VARCHAR", "identity_status": "VARCHAR",
    "process_name": "VARCHAR", "process_path": "VARCHAR", "user_name": "VARCHAR",
    "args": "VARCHAR", "agent_id": "VARCHAR", "birth_filetime": "BIGINT",
    "stop_filetime": "BIGINT", "lifetime_status": "VARCHAR",
    "seen_start": "BOOLEAN", "seen_refresh": "BOOLEAN", "seen_stop": "BOOLEAN",
    "evidence_json": "VARCHAR", "first_partition": "VARCHAR", "last_partition": "VARCHAR",
}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def host_name(value):
    if not isinstance(value, str) or not value.strip() or any(c in value for c in "\x00\n\r"):
        raise CompactionError("Process evidence requires a nonempty hostname")
    return value.strip().upper()


def host_key(host):
    return hashlib.sha256(host_name(host).encode()).hexdigest()


def unknown_hash(host):
    # Classify the producer's pid-v2 unknown sentinel; never rekey real instances.
    attributes = f"Process_ID`llnl`0`{host_name(host)}`-1`"
    inner = hashlib.md5(attributes.encode()).hexdigest().upper()
    return hashlib.md5(f"N`Process`{inner}`".encode()).hexdigest().upper()


def unknown(value, host):
    return value is None or str(value).strip().upper() in {
        "", "UNKNOWN", "NA", "-1", "0", unknown_hash(host),
        "00000000000000000000000000000000",
    }


def identity_reasons(row):
    """Return explicit reasons a row cannot become durable entity state."""
    hostname = row.get("hostname")
    if not isinstance(hostname, str) or not hostname.strip() or any(
        c in hostname for c in "\x00\n\r"
    ):
        return ("invalid_hostname",)
    host = host_name(hostname)
    reasons = []
    if unknown(row.get("pidhash"), host):
        reasons.append("unknown_pid_hash")
    pid = row.get("pid")
    if pid is None:
        reasons.append("missing_pid")
    elif pid < 0:
        reasons.append("negative_pid")
    return tuple(reasons)


@dataclass(frozen=True)
class EntityLimits:
    max_rows: int = 1_000_000
    max_bytes: int = 256 * 1024 * 1024

    def __post_init__(self):
        if self.max_rows <= 0 or self.max_bytes <= 0:
            raise CompactionError("Entity row/byte budgets must be positive")


class Evidence:
    def __init__(self):
        self.sets = {k: set() for k in FIELDS}
        self.first = self.last = None
        self.bytes = 0

    def add(self, field, value):
        if value is not None and value != "":
            encoded = canonical(value)
            if encoded not in self.sets[field]:
                self.sets[field].add(encoded)
                self.bytes += len(encoded) + 128

    def partition(self, first, last):
        self.first = min(v for v in (self.first, first) if v is not None)
        self.last = max(v for v in (self.last, last) if v is not None)

    def observe(self, row):
        kind = (row.get("activitytype") or "").upper()
        if kind not in {"START", "REFRESH", "STOP"}:
            raise CompactionError("Unsupported process lifecycle evidence kind")
        self.add("activities", kind)
        for field, column in (
            ("pids", "pid"), ("names", "processname"), ("paths", "processpath"),
            ("users", "username"), ("args", "processargs"), ("agents", "agentid"),
        ):
            if field == "users" and str(row.get(column) or "").lower() == "na":
                continue
            self.add(field, row.get(column))
        self.add("parents", [row.get("parentpidhash"), row.get("parentpid"), kind])
        time = row.get("eventtime")
        if time is not None and time > 0:
            self.add("stops" if kind == "STOP" else "births", time)
        self.partition(row["partition"], row["partition"])

    @classmethod
    def restore(cls, row):
        result = cls()
        try:
            values = json.loads(row["evidence_json"])
            if set(values) != set(FIELDS) or any(not isinstance(v, list) for v in values.values()):
                raise ValueError()
            for field in ("pids", "births", "stops"):
                if any(type(v) is not int or not 0 <= v < 2**63 for v in values[field]):
                    raise ValueError()
            for field in ("names", "paths", "users", "args", "agents", "activities"):
                if any(not isinstance(v, str) for v in values[field]):
                    raise ValueError()
            if not values["pids"] or not values["activities"] or not set(values["activities"]) <= {"START", "REFRESH", "STOP"}:
                raise ValueError()
            for parent in values["parents"]:
                if not isinstance(parent, list) or len(parent) != 3:
                    raise ValueError()
                if parent[0] is not None and not isinstance(parent[0], str):
                    raise ValueError()
                if parent[1] is not None and (type(parent[1]) is not int or not -(2**63) <= parent[1] < 2**63):
                    raise ValueError()
                if parent[2] not in {"START", "REFRESH", "STOP"}:
                    raise ValueError()
            result.sets = {k: {canonical(v) for v in values[k]} for k in FIELDS}
            result.bytes = sum(len(v) + 128 for field in result.sets.values() for v in field)
            result.partition(row["first_partition"], row["last_partition"])
        except (ValueError, TypeError, KeyError):
            raise CompactionError("Invalid process entity evidence") from None
        return result

    def values(self, field):
        return [json.loads(v) for v in sorted(self.sets[field])]

    def one(self, field):
        values = self.values(field)
        return values[0] if len(values) == 1 else None

    def display(self, field):
        values = self.values(field)
        return values[0] if values else None

    def row(self, host, key):
        parents = self.values("parents")
        # Missing PID can be completed only within the same real parent hash.
        real = {}
        sentinel = False
        for parent, pid, _ in parents:
            if unknown(parent, host) or (pid is not None and pid < 0):
                sentinel |= parent is not None or (pid is not None and pid < 0)
                continue
            real.setdefault(parent, set())
            if pid is not None:
                real[parent].add(pid)
        conflict = len(real) > 1 or any(len(pids) > 1 for pids in real.values())
        status = "conflicted" if conflict else "resolved" if real else "sentinel" if sentinel else "missing"
        parent = next(iter(real)) if status == "resolved" else None
        parent_pid = next(iter(real[parent])) if parent and real[parent] else None
        birth, stop = self.one("births"), self.one("stops")
        lifetime_conflict = len(self.sets["births"]) > 1 or len(self.sets["stops"]) > 1 or (
            birth is not None and stop is not None and stop < birth
        )
        if lifetime_conflict:
            birth = stop = None
        return {
            "hostname": host, "pid_hash": key, "os_pid": self.one("pids"),
            "parent_pid_hash": parent, "parent_os_pid": parent_pid,
            "parent_status": status,
            "identity_status": "conflicted" if len(self.sets["pids"]) > 1 else "recorded",
            "process_name": self.display("names"), "process_path": self.display("paths"),
            "user_name": self.display("users"), "args": self.display("args"),
            "agent_id": self.display("agents"), "birth_filetime": birth, "stop_filetime": stop,
            "lifetime_status": "conflicted" if lifetime_conflict else "exited" if canonical("STOP") in self.sets["activities"] else "observed",
            "seen_start": canonical("START") in self.sets["activities"],
            "seen_refresh": canonical("REFRESH") in self.sets["activities"],
            "seen_stop": canonical("STOP") in self.sets["activities"],
            "evidence_json": canonical({k: self.values(k) for k in FIELDS}),
            "first_partition": self.first, "last_partition": self.last,
        }


def rows(conn, query, params=()):
    cursor = conn.execute(query, params)
    names = [c[0].lower() for c in cursor.description]
    while batch := cursor.fetchmany(1024):
        for values in batch:
            yield dict(zip(names, values, strict=False))


def load_inputs(conn, inventories, *, identity_policy="reject"):
    """Validate physical schemas/partitions before projecting lifecycle evidence."""
    from .engine import check_hive_fields, schema

    if identity_policy not in IDENTITY_POLICIES:
        raise CompactionError("Unknown identity policy; use reject or quarantine")
    conn.execute("DROP TABLE IF EXISTS entity_input")
    conn.execute("DROP TABLE IF EXISTS identity_exclusions")
    conn.execute("CREATE TEMP TABLE entity_input (hostname VARCHAR, pidhash VARCHAR, pid BIGINT, "
                 "parentpidhash VARCHAR, parentpid BIGINT, activitytype VARCHAR, eventtime BIGINT, "
                 "processname VARCHAR, processpath VARCHAR, username VARCHAR, processargs VARCHAR, "
                 "agentid VARCHAR, partition VARCHAR)")
    types = {"pid": "BIGINT", "parentpid": "BIGINT", "eventtime": "BIGINT"}
    for inventory in inventories:
        for leaf, files in sorted(inventory.groups.items()):
            uris = [f.uri for f in files]
            columns = schema(conn, uris)
            check_hive_fields(conn, uris, leaf, columns)
            available = {c["name"].lower(): c["name"] for c in columns}
            if not {"hostname", "pidhash", "pid", "activitytype", "eventtime"} <= available.keys():
                raise CompactionError("Process evidence missing required identity/lifecycle columns")
            projections = []
            for name in list(SOURCE_COLUMNS):
                dtype = types.get(name, "VARCHAR")
                actual = available.get(name)
                if name == "processargs" and actual is None:
                    actual = available.get("commandline")
                expr = '"' + actual.replace('"', '""') + '"' if actual else "NULL"
                projections.append(f"CAST({expr} AS {dtype})")
            conn.execute("INSERT INTO entity_input SELECT DISTINCT " + ",".join(projections) +
                         ", ? FROM read_parquet(?, union_by_name=true, hive_partitioning=false)",
                         [leaf.day + leaf.hour, uris])
    identity_where = (
        "hostname IS NULL OR trim(hostname) = '' OR "
        "instr(hostname, chr(10)) > 0 OR instr(hostname, chr(13)) > 0 OR "
        "pidhash IS NULL OR upper(trim(pidhash)) IN "
        "('', 'UNKNOWN', 'NA', '-1', '0', '00000000000000000000000000000000') OR "
        "upper(trim(pidhash)) = upper(md5('N`Process`' || upper(md5("
        "'Process_ID`llnl`0`' || upper(trim(hostname)) || '`-1`')) || '`')) OR "
        "pid IS NULL OR pid < 0"
    )
    invalid = []
    by_hour = defaultdict(list)
    by_reason = Counter()
    by_host = Counter()
    for row in rows(conn, f"SELECT * FROM entity_input WHERE {identity_where}"):
        reasons = identity_reasons(row)
        if not reasons:
            continue
        record = {
            "hostname": row["hostname"],
            "pidhash": row["pidhash"],
            "pid": row["pid"],
            "parentpidhash": row["parentpidhash"],
            "parentpid": row["parentpid"],
            "activitytype": row["activitytype"],
            "eventtime": row["eventtime"],
            "partition": row["partition"],
            "reasons": reasons,
        }
        invalid.append(record)
        for reason in reasons:
            by_reason[reason] += 1
        host = (
            host_name(row["hostname"])
            if isinstance(row["hostname"], str) and row["hostname"].strip()
            else "<invalid-hostname>"
        )
        by_host[host] += 1
        by_hour[row["partition"]].append(record)

    def fingerprint(records):
        return hashlib.sha256(
            "\n".join(canonical(record) for record in sorted(records, key=canonical)).encode()
        ).hexdigest() if records else ""

    hourly = {}
    for partition, records in sorted(by_hour.items()):
        hourly[partition] = {
            "rows": len(records),
            "fingerprint": fingerprint(records),
            "reasons": dict(sorted(Counter(r for record in records for r in record["reasons"]).items())),
            "hosts": dict(sorted(Counter(
                (
                    host_name(record["hostname"])
                    if isinstance(record["hostname"], str) and record["hostname"].strip()
                    else "<invalid-hostname>"
                )
                for record in records
            ).items())),
        }
    exclusion_report = {
        "policy": identity_policy,
        "rows": len(invalid),
        "fingerprint": fingerprint(invalid),
        "reasons": dict(sorted(by_reason.items())),
        "hosts": dict(sorted(by_host.items())),
        "hours": hourly,
    }
    if invalid:
        conn.execute(
            "CREATE TEMP TABLE identity_exclusions AS "
            f"SELECT * FROM entity_input WHERE {identity_where}"
        )
        if identity_policy == "reject":
            raise CompactionError(
                "Unknown process instance identity; source evidence retained; "
                f"rows={len(invalid)} fingerprint={exclusion_report['fingerprint']}"
            )
        conn.execute(
            f"DELETE FROM entity_input WHERE {identity_where}"
        )
    conn.execute("UPDATE entity_input SET hostname=upper(trim(hostname))")
    return exclusion_report


SOURCE_COLUMNS = (
    "hostname", "pidhash", "pid", "parentpidhash", "parentpid", "activitytype", "eventtime",
    "processname", "processpath", "username", "processargs", "agentid",
)
