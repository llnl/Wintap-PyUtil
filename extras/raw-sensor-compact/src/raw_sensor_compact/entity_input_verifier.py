"""SQL-owned entity input validation with streaming, compatible exclusion hashes.

No collection of invalid process records is retained in Python. SQL owns projection,
sorting, counts and reductions; one bounded diagnostic record is canonicalized at
a time. Only bounded per-hour/host/reason aggregates leave the connection. The
existing writer's loader and public compact validators are intentionally unchanged.
"""

import hashlib

from .config import CompactionError
from .engine import check_hive_fields, schema
from .entities import MAX_ENTITY_HOSTS, SOURCE_COLUMNS, identity_reasons
from .phase_evidence import canonical

MAX_DIAGNOSTIC_BYTES = 64 * 1024
MAX_RECORD_BYTES = 64 * 1024
IDENTITY_WHERE = (
    "hostname IS NULL OR trim(hostname) = '' OR "
    "instr(hostname, chr(0)) > 0 OR instr(hostname, chr(10)) > 0 OR "
    "instr(hostname, chr(13)) > 0 OR "
    "pidhash IS NULL OR upper(trim(pidhash)) IN "
    "('', 'UNKNOWN', 'NA', '-1', '0', '00000000000000000000000000000000') OR "
    "upper(trim(pidhash)) = upper(md5('N`Process`' || upper(md5("
    "'Process_ID`llnl`0`' || upper(trim(hostname)) || '`-1`')) || '`')) OR "
    "pid IS NULL OR pid < 0"
)


def _record(
    hostname, pidhash, pid, parentpidhash, parentpid, activitytype, eventtime, partition
):
    record = dict(
        hostname=hostname,
        pidhash=pidhash,
        pid=pid,
        parentpidhash=parentpidhash,
        parentpid=parentpid,
        activitytype=activitytype,
        eventtime=eventtime,
        partition=partition,
    )
    record["reasons"] = identity_reasons(record)
    if not record["reasons"]:
        return None
    encoded = canonical(record)
    if len(encoded.encode()) > MAX_RECORD_BYTES:
        raise CompactionError("Entity exclusion record exceeds byte budget")
    return encoded


def _groups(conn, query, params, ceiling, deadline):
    cursor = conn.execute(query, params)
    result = {}
    while row := cursor.fetchone():
        deadline()
        if len(result) >= ceiling:
            raise CompactionError("Entity exclusion aggregate exceeds scope budget")
        result[row[0]] = row[1]
    return result


def _summary(conn, partition, *, hosts, deadline):
    where = " WHERE partition=?" if partition is not None else ""
    params = [partition] if partition is not None else []
    count = conn.execute(
        "SELECT count(*) FROM verifier_exclusions" + where, params
    ).fetchone()[0]
    host_counts = _groups(
        conn,
        "SELECT observed_host, count(*) FROM verifier_exclusions"
        + where
        + " GROUP BY observed_host ORDER BY observed_host",
        params,
        MAX_ENTITY_HOSTS,
        deadline,
    )
    reasons = _groups(
        conn,
        "SELECT json_extract_string(reason.value, '$'), count(*) FROM "
        "verifier_exclusions e, json_each(e.encoded, '$.reasons') reason"
        + where
        + " GROUP BY 1 ORDER BY 1",
        params,
        4,
        deadline,
    )
    cursor = conn.execute(
        "SELECT encoded FROM verifier_exclusions" + where + " ORDER BY encoded",
        params,
    )
    digest, observed = hashlib.sha256(), 0
    while row := cursor.fetchone():
        deadline()
        payload = row[0].encode()
        if len(payload) > MAX_RECORD_BYTES:
            raise CompactionError("Entity exclusion record exceeds byte budget")
        if observed:
            digest.update(b"\n")
        digest.update(payload)
        observed += 1
    if observed != count:
        raise CompactionError("Entity exclusion aggregate count changed")
    return dict(
        rows=count,
        fingerprint=digest.hexdigest() if count else "",
        reasons=reasons,
        hosts=host_counts,
    )


def load_process_inputs(conn, inventories, args, hours, *, deadline):
    """Preserve physical schema/partition projection and quarantine fingerprints."""
    for table in ("entity_input", "verifier_exclusions", "verifier_exclusion_input"):
        conn.execute("DROP TABLE IF EXISTS " + table)
    conn.execute(
        "CREATE TEMP TABLE entity_input ("
        "hostname VARCHAR, pidhash VARCHAR, pid BIGINT, "
        "parentpidhash VARCHAR, parentpid BIGINT, activitytype VARCHAR, "
        "eventtime BIGINT, processname VARCHAR, processpath VARCHAR, "
        "username VARCHAR, processargs VARCHAR, agentid VARCHAR, partition VARCHAR)"
    )
    types = {"pid": "BIGINT", "parentpid": "BIGINT", "eventtime": "BIGINT"}
    for inventory in inventories:
        for leaf, files in sorted(inventory.groups.items()):
            deadline()
            uris = [file.uri for file in files]
            columns = schema(conn, uris)
            check_hive_fields(conn, uris, leaf, columns)
            available = {c["name"].lower(): c["name"] for c in columns}
            if not {"hostname", "pidhash", "pid", "activitytype", "eventtime"} <= (
                available.keys()
            ):
                raise CompactionError("Missing required process input columns")
            projections = []
            for name in SOURCE_COLUMNS:
                actual = available.get(name)
                if name == "processargs" and actual is None:
                    actual = available.get("commandline")
                expr = '"' + actual.replace('"', '""') + '"' if actual else "NULL"
                projections.append(f"CAST({expr} AS {types.get(name, 'VARCHAR')})")
            conn.execute(
                "INSERT INTO entity_input SELECT DISTINCT "
                + ",".join(projections)
                + ", ? FROM read_parquet(?, union_by_name=true, "
                "hive_partitioning=false)",
                [leaf.day + leaf.hour, uris],
            )
    if conn.execute(
        "SELECT count(*) FROM entity_input WHERE "
        "instr(hostname, chr(0)) > 0 OR instr(hostname, chr(10)) > 0 OR "
        "instr(hostname, chr(13)) > 0 OR "
        "activitytype IS NULL OR upper(activitytype) NOT IN ('START','STOP','REFRESH') "
        "OR ("
        + IDENTITY_WHERE
        + ") AND coalesce(trim(hostname), '') <> '' AND upper(trim(hostname)) "
        "NOT IN (SELECT unnest(?::VARCHAR[]))",
        [args.entity_host],
    ).fetchone()[0]:
        raise CompactionError("Invalid process lifecycle or unexpected host")
    if conn.execute(
        "SELECT count(DISTINCT upper(trim(hostname))) FROM entity_input"
    ).fetchone()[0] > MAX_ENTITY_HOSTS:
        raise CompactionError("Observed entity host count exceeds scope budget")
    input_counts = _groups(
        conn,
        "SELECT partition, count(*) FROM entity_input GROUP BY partition "
        "ORDER BY partition",
        [],
        len(hours),
        deadline,
    )
    # Conservative character cap bounds ASCII escaping before Python serialization.
    length = "+".join(
        f"length(coalesce({key},''))"
        for key in ("hostname", "pidhash", "parentpidhash", "activitytype", "partition")
    )
    if conn.execute(
        f"SELECT count(*) FROM entity_input WHERE ({IDENTITY_WHERE}) "
        f"AND ({length}) > ?",
        [(MAX_RECORD_BYTES - 1024) // 6],
    ).fetchone()[0]:
        raise CompactionError("Entity exclusion record exceeds byte budget")
    # A database-local staging table is visible to a separate streaming cursor.
    # Both connections belong to the caller's ephemeral in-memory database; no
    # dataset write or Python UDF (and no optional NumPy dependency) is involved.
    conn.execute(
        "CREATE TABLE verifier_exclusion_input AS SELECT "
        "hostname,pidhash,pid,parentpidhash,parentpid,activitytype,eventtime,partition "
        "FROM entity_input WHERE " + IDENTITY_WHERE
    )
    conn.execute(
        "CREATE TEMP TABLE verifier_exclusions "
        "(partition VARCHAR, observed_host VARCHAR, encoded VARCHAR)"
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM verifier_exclusion_input")
            while row := cursor.fetchone():
                deadline()
                encoded = _record(*row)
                if encoded is None:
                    continue
                host = (
                    row[0].strip().upper()
                    if row[0] and row[0].strip()
                    else "<invalid-hostname>"
                )
                conn.execute(
                    "INSERT INTO verifier_exclusions VALUES (?,?,?)",
                    [row[-1], host, encoded],
                )
    finally:
        conn.execute("DROP TABLE verifier_exclusion_input")
    summary = _summary(conn, None, hosts=args.entity_host, deadline=deadline)
    hourly = {}
    for partition, count in input_counts.items():
        details = _summary(conn, partition, hosts=args.entity_host, deadline=deadline)
        hourly[partition] = {
            "input_rows": count,
            **details,
            "ratio": details["rows"] / count,
            "max_ratio": args.unknown_identity_max_ratio,
        }
    count = sum(input_counts.values())
    result = {
        "policy": args.unknown_identity_policy,
        "input_rows": count,
        **summary,
        "ratio": summary["rows"] / count if count else 0.0,
        "max_ratio": args.unknown_identity_max_ratio,
        "hours": hourly,
    }
    if len(canonical(result).encode()) > MAX_DIAGNOSTIC_BYTES:
        raise CompactionError("Entity exclusion report exceeds byte budget; no pruning")
    if summary["rows"] and (
        args.unknown_identity_policy == "reject"
        or any(d["ratio"] > args.unknown_identity_max_ratio for d in hourly.values())
    ):
        raise CompactionError("Unknown process identity violates the bound policy")
    conn.execute(f"DELETE FROM entity_input WHERE {IDENTITY_WHERE}")
    conn.execute("UPDATE entity_input SET hostname=upper(trim(hostname))")
    deadline()
    return result
