"""SQL-owned Parquet generation journal; requires external dataset exclusion.

The last row is the current authority (or an unresolved publication intent).
Historical receipt rows are never pruned or decoded into Python entity state.
"""

import json
from datetime import timedelta
from pathlib import Path

from .config import CompactionError

RELATIVE = "_authority/authority.parquet"
SCHEMA = {
    "authority_version": "VARCHAR",
    "generation": "BIGINT",
    "generation_id": "VARCHAR",
    "parent_id": "VARCHAR",
    "status": "VARCHAR",
    "binding_json": "VARCHAR",
    "bootstrap_starts": "VARCHAR",
    "before_hashes": "VARCHAR",
    "snapshot_hashes": "VARCHAR",
    "semantic_hashes": "VARCHAR",
    "window_start": "VARCHAR",
    "window_end": "VARCHAR",
    "input_fingerprint": "VARCHAR",
}
IDENTITY_FIELDS = tuple(k for k in SCHEMA if k not in {"status", "generation_id"})


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def binding(source, compacted, entities, hosts, policy, max_ratio, metrics_root=None):
    return canonical(
        {
            "raw_root": source.dataset.root,
            "compacted_root": compacted.dataset.root,
            "entity_root": entities.store.dataset.root,
            "metrics_root": metrics_root
            or source.dataset.sibling("processing_metrics").root,
            "namespace": entities.namespace,
            "hosts": sorted(set(hosts)),
            "identity_policy": policy,
            "identity_max_ratio": float(max_ratio),
        }
    )


class Authority:
    def __init__(self, conn, entities, directory, expected_binding):
        self.conn = conn
        self.entities = entities
        self.directory = Path(directory)
        self.binding = expected_binding
        self.path = self.directory / "authority-old.parquet"
        self.identity = entities.read_object(RELATIVE, self.path)
        conn.execute("DROP TABLE IF EXISTS entity_authority")
        if self.identity is None:
            conn.execute(
                "CREATE TEMP TABLE entity_authority ("
                + ",".join(k + " " + v for k, v in SCHEMA.items())
                + ")"
            )
            self.latest = None
        else:
            actual = dict(
                (r[0], r[1])
                for r in conn.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)",
                    [str(self.path)],
                ).fetchall()
            )
            if actual != SCHEMA:
                raise CompactionError(
                    "Entity authority schema mismatch; explicit rebuild required"
                )
            conn.execute(
                "CREATE TEMP TABLE entity_authority AS SELECT * FROM "
                "read_parquet(?, hive_partitioning=false)",
                [str(self.path)],
            )
            self._validate()
            cursor = conn.execute(
                "SELECT * FROM entity_authority ORDER BY generation DESC LIMIT 1"
            )
            self.latest = dict(zip(SCHEMA, cursor.fetchone(), strict=True))

    def _validate(self):
        conn = self.conn
        count, invalid = conn.execute(
            """
            WITH chain AS (
                SELECT *, row_number() OVER (ORDER BY generation) ordinal,
                    lag(generation_id) OVER (ORDER BY generation) previous_id,
                    lag(snapshot_hashes) OVER (ORDER BY generation) previous_hashes,
                    lag(bootstrap_starts) OVER (ORDER BY generation) previous_bootstrap,
                    max(generation) OVER () last_generation
                FROM entity_authority
            )
            SELECT count(*), count(*) FILTER (WHERE
                authority_version IS DISTINCT FROM '1'
                OR generation IS DISTINCT FROM ordinal
                OR NOT coalesce(regexp_full_match(generation_id, '[a-f0-9]{64}'), false)
                OR parent_id IS DISTINCT FROM coalesce(previous_id, '')
                OR binding_json IS DISTINCT FROM ?
                OR status IS NULL OR status NOT IN ('pending', 'committed')
                OR (status='pending' AND generation<>last_generation)
                OR (generation>1 AND (before_hashes IS DISTINCT FROM previous_hashes
                    OR bootstrap_starts IS DISTINCT FROM previous_bootstrap))
                OR NOT coalesce(
                    regexp_full_match(input_fingerprint, '[a-f0-9]{64}'), false)
                OR try_strptime(window_start, '%Y-%m-%dT%H:00:00Z') IS NULL
                OR try_strptime(window_end, '%Y-%m-%dT%H:00:00Z') IS NULL
                OR window_start >= window_end)
            FROM chain
        """,
            [self.binding],
        ).fetchone()
        if (
            not count
            or invalid
            or conn.execute(
                "SELECT count(*)-count(DISTINCT generation_id) FROM entity_authority"
            ).fetchone()[0]
        ):
            raise CompactionError("Entity authority binding/lineage is invalid")
        identity = ",".join(f"{field} := {field}" for field in IDENTITY_FIELDS)
        if conn.execute(
            "SELECT count(*) FROM entity_authority WHERE generation_id IS DISTINCT "
            f"FROM sha256(to_json(struct_pack({identity})))"
        ).fetchone()[0]:
            raise CompactionError(
                "Entity authority generation receipt identity mismatch"
            )
        hosts = json.loads(self.binding)["hosts"]
        for field in (
            "bootstrap_starts",
            "before_hashes",
            "snapshot_hashes",
            "semantic_hashes",
        ):
            if conn.execute(
                "SELECT count(*) FROM entity_authority "
                f"WHERE NOT coalesce(json_valid({field}), false)"
            ).fetchone()[0]:
                raise CompactionError("Entity authority evidence is invalid")
            if field == "bootstrap_starts":
                invalid_value = (
                    "j.type <> 'VARCHAR' OR try_strptime("
                    "json_extract_string(j.value, '$'), '%Y-%m-%dT%H:00:00Z') IS NULL"
                )
            else:
                invalid_value = (
                    "NOT (j.type='VARCHAR' AND regexp_full_match("
                    "json_extract_string(j.value, '$'), '[a-f0-9]{64}'))"
                )
                if field == "before_hashes":
                    invalid_value += " AND NOT (a.generation=1 AND j.type='NULL')"
            invalid = conn.execute(
                f"""
                SELECT count(*) FROM entity_authority a
                WHERE json_type({field}) <> 'OBJECT'
                    OR (SELECT count(*) FROM json_each(a.{field})) <> ?
                    OR (SELECT count(DISTINCT key) FROM json_each(a.{field})) <> ?
                    OR EXISTS (SELECT 1 FROM json_each(a.{field}) j WHERE
                        j.key NOT IN (SELECT unnest(?::VARCHAR[])) OR
                        ({invalid_value}))
            """,
                [len(hosts), len(hosts), hosts],
            ).fetchone()[0]
            if invalid:
                raise CompactionError("Entity authority host evidence is invalid")
        if conn.execute(
            "SELECT count(*) FROM entity_authority a, json_each(a.before_hashes) j "
            "WHERE generation=1 AND j.type<>'NULL'"
        ).fetchone()[0]:
            raise CompactionError(
                "Entity authority genesis must start with absent snapshots"
            )

    def check(self, identities, bootstrap, *, pending=False):
        if self.latest is None:
            if any(value is not None for value in identities.values()):
                raise CompactionError(
                    "Entity authority missing; rebuild into a fresh root explicitly"
                )
            return
        row = self.latest
        if row["status"] == "pending" and not pending:
            raise CompactionError(
                "Entity authority has unresolved publication intent; "
                "exclusive recovery required"
            )
        before, after = (
            json.loads(row["before_hashes"]),
            json.loads(row["snapshot_hashes"]),
        )
        expected_bootstrap = json.loads(row["bootstrap_starts"])
        for host, identity in identities.items():
            allowed = (
                (before[host], after[host])
                if row["status"] == "pending"
                else (after[host],)
            )
            if identity not in allowed or (
                identity is not None and bootstrap[host] != expected_bootstrap[host]
            ):
                raise CompactionError(
                    "Entity authority does not match current snapshot/provenance"
                )

    def prepare(self, report, hours, identities, after_hashes):
        row = {
            "authority_version": "1",
            "generation": self.latest["generation"] + 1 if self.latest else 1,
            "generation_id": "",
            "parent_id": self.latest["generation_id"] if self.latest else "",
            "status": "pending",
            "binding_json": self.binding,
            "bootstrap_starts": canonical(
                {h["hostname"]: h["bootstrap_start"] for h in report["hosts"]}
            ),
            "before_hashes": canonical(identities),
            "snapshot_hashes": canonical(after_hashes),
            "semantic_hashes": canonical(
                {h["hostname"]: h["semantic_sha256"] for h in report["hosts"]}
            ),
            "window_start": hours[0].strftime("%Y-%m-%dT%H:00:00Z"),
            "window_end": (hours[-1] + timedelta(hours=1)).strftime(
                "%Y-%m-%dT%H:00:00Z"
            ),
            "input_fingerprint": report["process_input_fingerprint"],
        }
        if self.latest and self.latest["status"] == "pending":
            for field in (
                "binding_json",
                "bootstrap_starts",
                "snapshot_hashes",
                "semantic_hashes",
                "window_start",
                "window_end",
                "input_fingerprint",
            ):
                if row[field] != self.latest[field]:
                    raise CompactionError(
                        "Entity authority intent candidates/input changed; "
                        "explicit recovery required"
                    )
            row = self.latest
        else:
            identity = ",".join(
                f"{field} := ?::{SCHEMA[field]}" for field in IDENTITY_FIELDS
            )
            row["generation_id"] = self.conn.execute(
                f"SELECT sha256(to_json(struct_pack({identity})))",
                [row[field] for field in IDENTITY_FIELDS],
            ).fetchone()[0]
            self.conn.execute(
                "INSERT INTO entity_authority VALUES ("
                + ",".join("?" for _ in SCHEMA)
                + ")",
                [row[k] for k in SCHEMA],
            )
        self.latest = row
        self._validate()
        return row["generation_id"]

    def publish(self, *, commit=False):
        if commit:
            self.conn.execute(
                "UPDATE entity_authority SET status='committed' WHERE generation=?",
                [self.latest["generation"]],
            )
        self._validate()
        candidate = self.directory / "authority-new.parquet"
        self.conn.execute(
            "COPY (SELECT * FROM entity_authority ORDER BY generation) "
            "TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(candidate)],
        )
        if candidate.stat().st_size > self.entities.limits.max_bytes:
            raise CompactionError(
                "Entity authority exceeds byte budget; history not truncated"
            )
        check = self.directory / "authority-check.parquet"
        if self.entities.read_object(RELATIVE, check) != self.identity:
            raise CompactionError("Entity authority changed during publication")
        try:
            self.entities.publish_object(RELATIVE, candidate)
            from .entity_store import digest

            identity = digest(candidate)
            if self.entities.read_object(RELATIVE, check) != identity:
                raise CompactionError("Entity authority content verification failed")
            self.identity = identity
        except Exception:
            raise CompactionError(
                "Entity authority publication uncertain; no source deletion; "
                "exclusive recovery required"
            ) from None
