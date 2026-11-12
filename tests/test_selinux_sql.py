"""Execute shipping SELinux SQL with synthetic data, without operator datasets."""

from pathlib import Path

import duckdb
import jinja2
import pytest

DBT = Path(__file__).resolve().parents[1] / "wintap_dbt"
FAMILIES = ("avc", "interaction", "transition")


def render(path):
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    macros = env.from_string((DBT / "macros/time.sql").read_text()).module
    env.globals.update(
        {name: getattr(macros, name) for name in dir(macros) if not name.startswith("_")}
    )
    env.globals.update(ref=lambda name: name, first_existing_raw_event=lambda names: None)
    return env.from_string(path.read_text()).render()


@pytest.fixture
def connection():
    with duckdb.connect() as conn:
        conn.execute("set TimeZone = 'UTC'")
        for family in FAMILIES:
            bronze = f"stg_raw_selinux{family}"
            conn.execute(
                f"create table {bronze} as "
                + render(DBT / f"models/bronze/{bronze}.sql")
            )
            conn.execute(
                f"create view selinux_{family} as "
                + render(DBT / f"models/silver/selinux_{family}.sql")
            )
        conn.execute("""
            create table process (
              hostname varchar, pid_hash varchar, process_name varchar, os_pid integer,
              process_started timestamp, process_term timestamp,
              first_seen timestamp, last_seen timestamp
            )
        """)
        conn.execute(
            "create view selinux_attribution as "
            + render(DBT / "models/silver/selinux_attribution.sql")
        )
        yield conn


def insert_event(conn, family="avc", pid=42, pid_hash="FIXMEPID", hostname="test-host"):
    conn.execute(
        f"""
        insert into stg_raw_selinux{family}
          (MessageType, PID, PidHash, ProcessName, AgentId, Hostname,
           EventTime, FirstSeen, LastSeen, EventCount, PolicyEpoch, dayPK, hourPK, num_dups)
        values (?, ?, ?, 'worker', 'test-agent', ?, 1735689600,
                133801632000000000, 133801632005000000, 7, 1, 20250101, '00', 2)
        """,
        [f"SELINUX_{family.upper()}", pid, pid_hash, hostname],
    )


def insert_process(conn, pid_hash, pid=42, hostname="test-host"):
    conn.execute("""
        insert into process values (?, ?, 'worker', ?,
          timestamp '2024-12-31 23:59:59', timestamp '2025-01-01 00:00:01',
          timestamp '2024-12-31 23:59:59', timestamp '2025-01-01 00:00:01')
    """, [hostname, pid_hash, pid])


def test_optional_sources_build_empty_gold(connection):
    for name in (
        "selinux_event_detail", "selinux_avc_summary", "selinux_interaction_summary",
        "selinux_transition_summary", "selinux_context", "selinux_process_summary",
    ):
        connection.execute(
            f"create view {name} as " + render(DBT / f"models/gold/{name}.sql")
        )
        assert connection.execute(f"select count(*) from {name}").fetchone() == (0,)


@pytest.mark.parametrize("family", FAMILIES)
def test_normalization_preserves_units_counts_and_identity(connection, family):
    insert_event(connection, family)
    row = connection.execute(f"""
        select event_time = timestamp '2025-01-01 00:00:00',
               first_seen = timestamp '2025-01-01 00:00:00',
               last_seen = timestamp '2025-01-01 00:00:00.5',
               event_count, num_raw_rows, event_id
        from selinux_{family}
    """).fetchone()
    assert row[:5] == (True, True, True, 7, 2)
    connection.execute(f"update stg_raw_selinux{family} set num_dups = 3")
    assert connection.execute(f"select event_id from selinux_{family}").fetchone()[0] == row[5]


@pytest.mark.parametrize(
    "raw_hash,candidates,status,attributed",
    [
        ("known", ["known"], "exact", "known"),
        ("FIXMEPID", ["candidate"], "unique_temporal", "candidate"),
        ("FIXMEPID", ["first", "second"], "ambiguous", None),
        ("FIXMEPID", [], "unmatched", None),
    ],
)
def test_attribution_outcomes_are_conservative(
    connection, raw_hash, candidates, status, attributed
):
    insert_event(connection, pid_hash=raw_hash)
    for candidate in candidates:
        insert_process(connection, candidate)
    # An otherwise matching PID on another host must never become a candidate.
    insert_process(connection, "foreign", hostname="other-host")
    assert connection.execute("""
        select attribution_status, attributed_pid_hash, candidate_count
        from selinux_attribution
    """).fetchone() == (status, attributed, len(candidates))


def test_gold_preserves_family_rows_and_producer_counts(connection):
    for family in FAMILIES:
        insert_event(connection, family)
    connection.execute(
        "create view selinux_event_detail as "
        + render(DBT / "models/gold/selinux_event_detail.sql")
    )
    assert connection.execute("""
        select event_family, count(*), sum(event_count), sum(num_raw_rows)
        from selinux_event_detail group by event_family order by event_family
    """).fetchall() == [(f"selinux_{family}", 1, 7, 2) for family in FAMILIES]
