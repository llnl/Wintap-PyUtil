# ruff: noqa: E501

"""DuckDB-backed process entity reduction.

The entity snapshot remains Parquet for interoperability, but the working state
is kept in DuckDB relations.  This module deliberately does not materialize a
Python dictionary containing one Evidence object per process instance.
"""

from pathlib import Path

from .entities import FIELDS, SCHEMA, host_name
from .storage import quote


def _current_values(conn):
    conn.execute(
        """
        CREATE TEMP TABLE entity_values AS
        SELECT DISTINCT hostname, pidhash, field, value
        FROM (
            SELECT hostname, pidhash, 'pids' AS field,
                   to_json(pid)::VARCHAR AS value
            FROM entity_input WHERE pid IS NOT NULL
            UNION ALL
            SELECT hostname, pidhash, 'parents',
                   json_array(parentpidhash, parentpid, upper(activitytype))::VARCHAR
            FROM entity_input
            UNION ALL
            SELECT hostname, pidhash, 'births', to_json(eventtime)::VARCHAR
            FROM entity_input
            WHERE eventtime > 0 AND upper(activitytype) <> 'STOP'
            UNION ALL
            SELECT hostname, pidhash, 'stops', to_json(eventtime)::VARCHAR
            FROM entity_input
            WHERE eventtime > 0 AND upper(activitytype) = 'STOP'
            UNION ALL
            SELECT hostname, pidhash, 'names', to_json(processname)::VARCHAR
            FROM entity_input
            WHERE processname IS NOT NULL AND processname <> ''
            UNION ALL
            SELECT hostname, pidhash, 'paths', to_json(processpath)::VARCHAR
            FROM entity_input
            WHERE processpath IS NOT NULL AND processpath <> ''
            UNION ALL
            SELECT hostname, pidhash, 'users', to_json(username)::VARCHAR
            FROM entity_input
            WHERE username IS NOT NULL AND username <> '' AND lower(username) <> 'na'
            UNION ALL
            SELECT hostname, pidhash, 'args', to_json(processargs)::VARCHAR
            FROM entity_input
            WHERE processargs IS NOT NULL AND processargs <> ''
            UNION ALL
            SELECT hostname, pidhash, 'agents', to_json(agentid)::VARCHAR
            FROM entity_input
            WHERE agentid IS NOT NULL AND agentid <> ''
            UNION ALL
            SELECT hostname, pidhash, 'activities', to_json(upper(activitytype))::VARCHAR
            FROM entity_input
        ) valueset
        """
    )
    conn.execute(
        """
        CREATE TEMP TABLE entity_bounds_raw AS
        SELECT hostname, pidhash, min(partition) AS first_partition,
               max(partition) AS last_partition
        FROM entity_input
        GROUP BY hostname, pidhash
        """
    )


def _merge_snapshot(conn, path):
    path = str(path)
    conn.execute(
        """
        INSERT INTO entity_values
        SELECT e.hostname, e.pid_hash, fields.key,
               values.value::VARCHAR
        FROM read_parquet(?, hive_partitioning=false) e,
             json_each(e.evidence_json) fields,
             json_each(fields.value) values
        """,
        [path],
    )
    conn.execute(
        """
        INSERT INTO entity_bounds_raw
        SELECT hostname, pid_hash, first_partition, last_partition
        FROM read_parquet(?, hive_partitioning=false)
        """,
        [path],
    )


def build_state(conn, old_paths=()):
    """Build SQL entity state from current inputs and old snapshots.

    The relation is temporary in the command's DuckDB database, which may spill
    to the configured temporary directory. Durability across commands remains
    the validated Parquet snapshot; no Python entity-state cache is maintained.
    """
    for name in ("entity_state", "entity_bounds", "entity_values", "entity_bounds_raw"):
        conn.execute(f"DROP TABLE IF EXISTS {name}")
    _current_values(conn)
    for path in old_paths:
        _merge_snapshot(conn, path)
    conn.execute(
        """
        CREATE TEMP TABLE entity_bounds AS
        SELECT hostname, pidhash, min(first_partition) AS first_partition,
               max(last_partition) AS last_partition
        FROM entity_bounds_raw
        GROUP BY hostname, pidhash
        """
    )
    _reduce_state(conn)


def _reduce_state(
    conn, values="entity_values", bounds="entity_bounds", target="entity_state"
):
    """One SQL derivation for reduction and evidence/scalar verification.

    Relation names are private constants, never caller-controlled SQL identifiers.
    Validation uses separate spillable relations so it cannot alter update state.
    """
    conn.execute(
        f"""
        CREATE TEMP TABLE {target} AS
        WITH grouped AS (
            SELECT hostname, pidhash,
                list_sort(list_distinct(list(value) FILTER (WHERE field='pids'))) pids,
                list_sort(list_distinct(list(value) FILTER (WHERE field='parents'))) parents,
                list_sort(list_distinct(list(value) FILTER (WHERE field='births'))) births,
                list_sort(list_distinct(list(value) FILTER (WHERE field='stops'))) stops,
                list_sort(list_distinct(list(value) FILTER (WHERE field='names'))) names,
                list_sort(list_distinct(list(value) FILTER (WHERE field='paths'))) paths,
                list_sort(list_distinct(list(value) FILTER (WHERE field='users'))) users,
                list_sort(list_distinct(list(value) FILTER (WHERE field='args'))) args_values,
                list_sort(list_distinct(list(value) FILTER (WHERE field='agents'))) agents,
                list_sort(list_distinct(list(value) FILTER (WHERE field='activities'))) activities
            FROM {values}
            GROUP BY hostname, pidhash
        ), arrays AS (
            SELECT *,
                coalesce(to_json(list_transform(pids, lambda x: json(x))), '[]') pids_json,
                coalesce(to_json(list_transform(parents, lambda x: json(x))), '[]') parents_json,
                coalesce(to_json(list_transform(births, lambda x: json(x))), '[]') births_json,
                coalesce(to_json(list_transform(stops, lambda x: json(x))), '[]') stops_json,
                coalesce(to_json(list_transform(names, lambda x: json(x))), '[]') names_json,
                coalesce(to_json(list_transform(paths, lambda x: json(x))), '[]') paths_json,
                coalesce(to_json(list_transform(users, lambda x: json(x))), '[]') users_json,
                coalesce(to_json(list_transform(args_values, lambda x: json(x))), '[]') args_json,
                coalesce(to_json(list_transform(agents, lambda x: json(x))), '[]') agents_json,
                coalesce(to_json(list_transform(activities, lambda x: json(x))), '[]') activities_json
            FROM grouped
        ), scalar AS (
            SELECT *,
                CASE WHEN coalesce(len(pids), 0) = 1
                     THEN try_cast(json_extract_string(pids[1], '$') AS BIGINT) END os_pid,
                CASE WHEN coalesce(len(names), 0) > 0
                     THEN json_extract_string(names[1], '$') END process_name,
                CASE WHEN coalesce(len(paths), 0) > 0
                     THEN json_extract_string(paths[1], '$') END process_path,
                CASE WHEN coalesce(len(users), 0) > 0
                     THEN json_extract_string(users[1], '$') END user_name,
                CASE WHEN coalesce(len(args_values), 0) > 0
                     THEN json_extract_string(args_values[1], '$') END args,
                CASE WHEN coalesce(len(agents), 0) > 0
                     THEN json_extract_string(agents[1], '$') END agent_id,
                CASE WHEN coalesce(len(births), 0) = 1
                     THEN try_cast(json_extract_string(births[1], '$') AS BIGINT) END birth,
                CASE WHEN coalesce(len(stops), 0) = 1
                     THEN try_cast(json_extract_string(stops[1], '$') AS BIGINT) END stop
            FROM arrays
        ), parent_candidates AS (
            SELECT s.hostname, s.pidhash,
                   json_extract_string(j.value, '$[0]') parent_hash,
                   try_cast(json_extract_string(j.value, '$[1]') AS BIGINT) parent_pid
            FROM scalar s, json_each(s.parents_json) j
        ), parent_classified AS (
            SELECT *,
                parent_hash IS NOT NULL
                AND upper(trim(parent_hash)) NOT IN
                    ('', 'UNKNOWN', 'NA', '-1', '0',
                     '00000000000000000000000000000000')
                AND upper(trim(parent_hash)) <> upper(md5(
                    'N`Process`' || upper(md5(
                        'Process_ID`llnl`0`' || upper(trim(hostname)) || '`-1`'
                    )) || '`'
                ))
                AND (parent_pid IS NULL OR parent_pid >= 0) AS real_parent,
                parent_hash IS NOT NULL OR parent_pid < 0 AS sentinel_parent
            FROM parent_candidates
        ), parent_stats AS (
            SELECT hostname, pidhash,
                count(*) FILTER (WHERE real_parent) real_count,
                count(DISTINCT parent_hash) FILTER (WHERE real_parent) real_hashes,
                count(DISTINCT parent_pid) FILTER (
                    WHERE real_parent AND parent_pid IS NOT NULL
                ) real_pids,
                min(parent_hash) FILTER (WHERE real_parent) parent_hash,
                min(parent_pid) FILTER (
                    WHERE real_parent AND parent_pid IS NOT NULL
                ) parent_pid,
                count(*) FILTER (WHERE sentinel_parent) sentinel_count
            FROM parent_classified
            GROUP BY hostname, pidhash
        )
        SELECT s.hostname, s.pidhash AS pid_hash, s.os_pid,
            CASE WHEN p.real_hashes > 1 OR p.real_pids > 1
                 THEN NULL WHEN p.real_count > 0 THEN p.parent_hash END parent_pid_hash,
            CASE WHEN p.real_hashes > 1 OR p.real_pids > 1
                 THEN NULL WHEN p.real_count > 0 THEN p.parent_pid END parent_os_pid,
            CASE WHEN p.real_hashes > 1 OR p.real_pids > 1 THEN 'conflicted'
                 WHEN p.real_count > 0 THEN 'resolved'
                 WHEN p.sentinel_count > 0 THEN 'sentinel' ELSE 'missing' END parent_status,
            CASE WHEN coalesce(len(s.pids), 0) > 1 THEN 'conflicted'
                 ELSE 'recorded' END identity_status,
            s.process_name, s.process_path, s.user_name, s.args, s.agent_id,
            CASE WHEN coalesce(len(s.births), 0) > 1
                       OR coalesce(len(s.stops), 0) > 1
                       OR (s.birth IS NOT NULL AND s.stop IS NOT NULL AND s.stop < s.birth)
                 THEN NULL ELSE s.birth END birth_filetime,
            CASE WHEN coalesce(len(s.births), 0) > 1
                       OR coalesce(len(s.stops), 0) > 1
                       OR (s.birth IS NOT NULL AND s.stop IS NOT NULL AND s.stop < s.birth)
                 THEN NULL ELSE s.stop END stop_filetime,
            CASE WHEN coalesce(len(s.births), 0) > 1
                       OR coalesce(len(s.stops), 0) > 1
                       OR (s.birth IS NOT NULL AND s.stop IS NOT NULL AND s.stop < s.birth)
                 THEN 'conflicted'
                 WHEN list_contains(s.activities, '"STOP"') THEN 'exited'
                 ELSE 'observed' END lifetime_status,
            list_contains(s.activities, '"START"') seen_start,
            list_contains(s.activities, '"REFRESH"') seen_refresh,
            list_contains(s.activities, '"STOP"') seen_stop,
            json_object(
                'activities', s.activities_json, 'agents', s.agents_json,
                'args', s.args_json, 'births', s.births_json,
                'names', s.names_json, 'parents', s.parents_json,
                'paths', s.paths_json, 'pids', s.pids_json,
                'stops', s.stops_json, 'users', s.users_json
            )::VARCHAR evidence_json,
            b.first_partition, b.last_partition
        FROM scalar s
        LEFT JOIN parent_stats p USING (hostname, pidhash)
        JOIN {bounds} b USING (hostname, pidhash)
        """
    )


def state_count(conn, host):
    return int(
        conn.execute(
            "SELECT count(*) FROM entity_state WHERE hostname=?", [host]
        ).fetchone()[0]
    )


def state_hash(conn, host, path=None):
    relation = "entity_state" if path is None else "read_parquet(?)"
    params = [] if path is None else [str(path)]
    where = "WHERE hostname=?"
    params.append(host)
    columns = ",".join(f"{name} := {name}" for name in SCHEMA)
    return conn.execute(
        f"WITH hashed AS (SELECT row_number() OVER (ORDER BY pid_hash)-1 ordinal, "
        f"sha256(to_json(struct_pack({columns}))) row_hash FROM {relation} {where}), "
        "chunks AS (SELECT ordinal//1024 chunk, "
        "sha256(string_agg(row_hash, '\\n' ORDER BY ordinal)) chunk_hash "
        "FROM hashed GROUP BY chunk) "
        "SELECT sha256(coalesce(string_agg(chunk_hash, '\\n' ORDER BY chunk), '')) FROM chunks",
        params,
    ).fetchone()[0]


def write_state(conn, path, host, metadata, limits):
    count = state_count(conn, host)
    if count > limits.max_rows:
        raise ValueError("Entity snapshot exceeds row budget")
    values = ",".join(quote(k) + ": " + quote(v) for k, v in metadata.items())
    columns = ",".join(SCHEMA)
    conn.execute(
        f"COPY (SELECT {columns} FROM entity_state WHERE hostname=? ORDER BY pid_hash) "
        f"TO ? (FORMAT PARQUET, COMPRESSION ZSTD, KV_METADATA {{{values}}})",
        [host, str(path)],
    )
    if Path(path).stat().st_size > limits.max_bytes:
        raise ValueError("Entity snapshot exceeds byte budget")
    validate_state(conn, path, host, metadata, limits)
    return count


def validate_state(conn, path, host, metadata, limits):
    actual = {
        row[0]: row[1]
        for row in conn.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)",
            [str(path)],
        ).fetchall()
    }
    if actual != SCHEMA:
        raise ValueError("Entity snapshot schema/version mismatch")
    stored = dict(
        conn.execute(
            "SELECT decode(key), decode(value) FROM parquet_kv_metadata(?)",
            [str(path)],
        ).fetchall()
    )
    if stored != metadata:
        raise ValueError("Entity host/namespace/coverage metadata mismatch")
    count = int(
        conn.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0]
    )
    if count > limits.max_rows or Path(path).stat().st_size > limits.max_bytes:
        raise ValueError("Entity snapshot exceeds row/byte budget")
    # Read-only view, not Python rows or decoded Evidence instances. Expansion and
    # derivation below are DuckDB-owned and may spill under the command's limits.
    conn.execute(
        "CREATE OR REPLACE TEMP VIEW entity_validation_stored AS "
        f"SELECT * FROM read_parquet({quote(str(path))}, hive_partitioning=false)"
    )
    try:
        _validate_evidence(conn, host_name(host))
    finally:
        for name in ("expected", "values", "bounds"):
            conn.execute(f"DROP TABLE IF EXISTS entity_validation_{name}")
        conn.execute("DROP VIEW entity_validation_stored")


def _validate_evidence(conn, host):
    invalid, duplicates = conn.execute(
        """
        SELECT count(*) FILTER (WHERE
            hostname IS DISTINCT FROM ? OR pid_hash IS NULL
            OR upper(trim(pid_hash)) IN ('', 'UNKNOWN', 'NA', '-1', '0',
                '00000000000000000000000000000000')
            OR upper(trim(pid_hash)) = upper(md5('N`Process`' || upper(md5(
                'Process_ID`llnl`0`' || ? || '`-1`')) || '`'))),
            count(*) - count(DISTINCT pid_hash)
        FROM entity_validation_stored
        """,
        [host, host],
    ).fetchone()
    if invalid or duplicates:
        raise ValueError("Invalid/duplicate entity instance key")
    if conn.execute(
        """
        SELECT count(*) FROM entity_validation_stored
        WHERE first_partition IS NULL OR last_partition IS NULL
           OR NOT regexp_full_match(first_partition, '[0-9]{10}')
           OR NOT regexp_full_match(last_partition, '[0-9]{10}')
           OR try_strptime(first_partition, '%Y%m%d%H') IS NULL
           OR try_strptime(last_partition, '%Y%m%d%H') IS NULL
           OR first_partition > last_partition
        """
    ).fetchone()[0]:
        raise ValueError("Invalid entity publication bounds")
    if conn.execute(
        "SELECT count(*) FROM entity_validation_stored "
        "WHERE NOT coalesce(json_valid(evidence_json), false)"
    ).fetchone()[0]:
        raise ValueError("Invalid process entity evidence")
    fields = ",".join(quote(field) for field in FIELDS)
    if conn.execute(
        f"""
        SELECT count(*) FROM entity_validation_stored s
        WHERE json_type(evidence_json) <> 'OBJECT'
           OR (SELECT count(*) FROM json_each(s.evidence_json)) <> {len(FIELDS)}
           OR (SELECT count(DISTINCT key) FROM json_each(s.evidence_json)) <> {len(FIELDS)}
           OR EXISTS (SELECT 1 FROM json_each(s.evidence_json) f
                      WHERE f.key NOT IN ({fields}) OR f.type <> 'ARRAY')
           OR json_array_length(evidence_json, '$.pids') = 0
           OR json_array_length(evidence_json, '$.activities') = 0
        """
    ).fetchone()[0]:
        raise ValueError("Invalid process entity evidence structure")
    conn.execute(
        """
        CREATE TEMP TABLE entity_validation_values AS
        SELECT s.hostname, s.pid_hash AS pidhash, f.key AS field,
               json(v.value)::VARCHAR AS value, v.type
        FROM entity_validation_stored s, json_each(s.evidence_json) f,
             json_each(f.value) v
        """
    )
    if conn.execute(
        """
        SELECT count(*) FROM entity_validation_values
        WHERE (field IN ('pids', 'births', 'stops') AND (
                  type NOT IN ('BIGINT', 'UBIGINT')
                  OR try_cast(value AS BIGINT) IS NULL OR try_cast(value AS BIGINT) < 0))
           OR (field IN ('names', 'paths', 'users', 'args', 'agents', 'activities')
               AND type <> 'VARCHAR')
           OR (field = 'activities' AND value NOT IN ('"START"', '"REFRESH"', '"STOP"'))
           OR (field = 'parents' AND (
                  type <> 'ARRAY' OR json_array_length(value) <> 3
                  OR json_type(value, '$[0]') NOT IN ('NULL', 'VARCHAR')
                  OR json_type(value, '$[1]') NOT IN ('NULL', 'BIGINT', 'UBIGINT')
                  OR (json_type(value, '$[1]') <> 'NULL'
                      AND try_cast(json_extract_string(value, '$[1]') AS BIGINT) IS NULL)
                  OR coalesce(json_extract_string(value, '$[2]'), '')
                     NOT IN ('START', 'REFRESH', 'STOP')))
        """
    ).fetchone()[0]:
        raise ValueError("Invalid process entity evidence types")
    if conn.execute(
        """
        SELECT count(*) FROM (
            SELECT hostname, pidhash, field FROM entity_validation_values
            GROUP BY hostname, pidhash, field
            HAVING count(*) <> count(DISTINCT value)
        )
        """
    ).fetchone()[0]:
        raise ValueError("Invalid duplicate process entity evidence")
    conn.execute(
        """
        CREATE TEMP TABLE entity_validation_bounds AS
        SELECT hostname, pid_hash AS pidhash, first_partition, last_partition
        FROM entity_validation_stored
        """
    )
    _reduce_state(
        conn,
        "entity_validation_values",
        "entity_validation_bounds",
        "entity_validation_expected",
    )
    columns = [field for field in SCHEMA if field != "evidence_json"]
    stored = ",".join("s." + field for field in columns)
    expected = ",".join("e." + field for field in columns)
    if conn.execute(
        "SELECT count(*) FROM entity_validation_stored s "
        "FULL JOIN entity_validation_expected e USING (hostname, pid_hash) "
        f"WHERE row({stored}) IS DISTINCT FROM row({expected})"
    ).fetchone()[0]:
        raise ValueError("Entity scalar fields disagree with evidence")
