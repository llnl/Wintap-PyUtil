"""Integrity regressions exercised through the operational SQL entry points."""

import json
from pathlib import Path

import pytest
from test_entities import HOURS, observation, seed
from test_entities import stores as stores_fixture

from raw_sensor_compact.cli import main
from raw_sensor_compact.config import CompactionError, hour_window
from raw_sensor_compact.entities import Evidence, unknown_hash
from raw_sensor_compact.entity_store import update_entities
from raw_sensor_compact.storage import quote

stores = stores_fixture


def rewrite(conn, entities, rows, metadata=None):
    target = Path(entities.store.dataset.root) / entities.relative("HOST")
    metadata = metadata or entities.sql_metadata(conn, target)
    conn.execute(
        "create or replace temp table corrupt as "
        "select * from read_parquet(?, hive_partitioning=false)",
        [str(target)],
    )
    conn.execute(rows)
    kv = ",".join(quote(k) + ": " + quote(v) for k, v in metadata.items())
    conn.execute(
        f"copy corrupt to ? (format parquet, kv_metadata {{{kv}}})", [str(target)]
    )
    return target


def verify(
    source,
    compacted,
    entities,
    *,
    command="entities-verify",
    start="2026-01-01T00:00:00Z",
    end="2026-01-01T02:00:00Z",
):
    return main(
        [
            command,
            "--source-root",
            source.dataset.root,
            "--destination-root",
            compacted.dataset.root,
            "--process-entity-root",
            entities.store.dataset.root,
            "--entity-host",
            "host",
            "--entity-namespace",
            "pid-v2",
            "--start-hour",
            start,
            "--end-hour",
            end,
        ]
    )


CORRUPTIONS = [
    "insert into corrupt select * from corrupt",
    "update corrupt set pid_hash=null",
    "update corrupt set pid_hash='unknown'",
    "update corrupt set hostname=null",
    "update corrupt set hostname='host'",
    "update corrupt set first_partition=null",
    "update corrupt set first_partition='2026023000'",
    "update corrupt set last_partition='2026010124'",
    "update corrupt set first_partition='2026010102'",
    "update corrupt set os_pid=999",
    "update corrupt set identity_status='conflicted'",
    "update corrupt set process_name='wrong'",
    "update corrupt set process_path='wrong'",
    "update corrupt set user_name='wrong'",
    "update corrupt set args='wrong'",
    "update corrupt set agent_id='wrong'",
    "update corrupt set birth_filetime=999",
    "update corrupt set stop_filetime=999",
    "update corrupt set lifetime_status='exited'",
    "update corrupt set seen_start=false",
    "update corrupt set seen_refresh=null",
    "update corrupt set seen_stop=true",
    "update corrupt set evidence_json=null",
    "update corrupt set evidence_json='[]'",
    *[
        "update corrupt set evidence_json=json_merge_patch(evidence_json, "
        + quote(patch)
        + ")"
        for patch in (
            '{"extra":[]}',
            '{"names":null}',
            '{"names":42}',
            '{"pids":[]}',
            '{"pids":[true]}',
            '{"pids":[10.0]}',
            '{"pids":[-1]}',
            '{"births":[9223372036854775808]}',
            '{"stops":["100"]}',
            '{"names":[null]}',
            '{"activities":[]}',
            '{"activities":["BOGUS"]}',
            '{"parents":[["parent",9]]}',
            '{"parents":[[1,9,"START"]]}',
            '{"parents":[["parent",9.0,"START"]]}',
            '{"parents":[["parent",9,"BOGUS"]]}',
        )
    ],
]


@pytest.mark.parametrize("corruption", CORRUPTIONS)
@pytest.mark.parametrize("entrypoint", ["verify", "update"])
def test_corrupt_snapshot_rejected_by_verify_and_update(
    conn, stores, capsys, corruption, entrypoint, monkeypatch
):
    source, compacted, entities = stores
    raw = seed(conn, source)
    update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    target = rewrite(conn, entities, corruption)
    before = target.read_bytes()
    # Neither operational path may fall back to decoded per-process Python state.
    monkeypatch.setattr(
        Evidence, "restore", lambda *a: pytest.fail("Python entity decoding")
    )
    if entrypoint == "verify":
        assert verify(source, compacted, entities) == 2
        assert json.loads(capsys.readouterr().err)["status"] == "error"
    else:
        with pytest.raises(CompactionError):
            update_entities(conn, source, compacted, entities, HOURS, ["HOST"])
    assert raw.exists() and target.read_bytes() == before


def test_producer_unknown_key_rejected(conn, stores, capsys):
    source, compacted, entities = stores
    seed(conn, source)
    update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    rewrite(conn, entities, f"update corrupt set pid_hash='{unknown_hash('HOST')}'")
    assert verify(source, compacted, entities) == 2


@pytest.mark.parametrize("mode", ["changed", "unchanged", "replay", "resume"])
def test_bootstrap_provenance_survives_later_update(conn, stores, mode, capsys):
    source, compacted, entities = stores
    seed(conn, source)
    first = hour_window("2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z")
    later = hour_window("2026-01-01T01:00:00Z", "2026-01-01T02:00:00Z")
    update_entities(conn, source, compacted, entities, first, ["HOST"], initialize=True)
    target = Path(entities.store.dataset.root) / entities.relative("HOST")
    before = target.read_bytes()
    if mode in {"changed", "replay", "resume"}:
        seed(conn, source, observation(activitytype="STOP", eventtime=200), hour="01")
    options = {"initialize": True, "resume_init": True} if mode == "resume" else {}
    report, _ = update_entities(
        conn, source, compacted, entities, later, ["HOST"], **options
    )
    assert report["hosts"][0]["bootstrap_start"] == "2026-01-01T00:00:00Z"
    bootstrap = "2026-01-01T00:00:00Z"
    assert entities.sql_metadata(conn, target)["bootstrap_start"] == bootstrap
    if mode == "unchanged":
        assert not report["hosts"][0]["changed"] and before == target.read_bytes()
    if mode == "replay":
        for window in (later, HOURS, later):
            report, _ = update_entities(
                conn, source, compacted, entities, window, ["HOST"]
            )
            assert not report["hosts"][0]["changed"]
            assert report["hosts"][0]["bootstrap_start"] == "2026-01-01T00:00:00Z"
            assert entities.sql_metadata(conn, target)["bootstrap_start"] == bootstrap
        assert (
            verify(
                source,
                compacted,
                entities,
                command="entities-replay",
                start="2026-01-01T01:00:00Z",
            )
            == 0
        )
        replay_report = json.loads(capsys.readouterr().out)
        assert not replay_report["hosts"][0]["changed"]
        assert replay_report["hosts"][0]["bootstrap_start"] == bootstrap
        metric = (
            Path(source.dataset.root).parent
            / "processing_metrics/dayPK=20260101/hourPK=01/metrics.parquet"
        )
        assert (
            conn.execute(
                "select entity_bootstrap_start from read_parquet(?)", [str(metric)]
            ).fetchone()[0]
            == bootstrap
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"pid": 11, "eventtime": 101, "processname": "aaa"},
        {"activitytype": "STOP", "eventtime": 50},
        {"parentpidhash": "other", "parentpid": 8},
        {"parentpid": None},
        {"parentpidhash": None, "parentpid": None},
        {"parentpidhash": "unknown", "parentpid": -1},
        {"pid": 2**63 - 1, "eventtime": 2**63 - 1, "parentpid": -(2**63)},
    ],
)
def test_valid_evidence_and_replay_stay_sql_backed(
    conn, stores, changes, monkeypatch, capsys
):
    source, compacted, entities = stores
    original = observation()
    other = observation(**changes)
    evidence = Evidence()
    for row in (original, other):
        evidence.observe(row)
    expected = evidence.row("HOST", "child")
    seed(conn, source, other)
    seed(conn, source, original, filename="second.parquet")
    monkeypatch.setattr(
        Evidence, "restore", lambda *a: pytest.fail("Python entity decoding")
    )
    update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    target = Path(entities.store.dataset.root) / entities.relative("HOST")
    cursor = conn.execute(
        "select * from read_parquet(?, hive_partitioning=false)", [str(target)]
    )
    actual = dict(
        zip([field[0] for field in cursor.description], cursor.fetchone(), strict=True)
    )
    assert actual == expected
    assert verify(source, compacted, entities) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "passed"
    report, _ = update_entities(conn, source, compacted, entities, HOURS, ["HOST"])
    assert not report["hosts"][0]["changed"]
    assert (
        conn.execute(
            "select count(*) from duckdb_tables() "
            "where table_name like 'entity_validation_%'"
        ).fetchone()[0]
        == 0
    )


def test_empty_initialized_host_stays_sql_backed(conn, stores, monkeypatch, capsys):
    source, compacted, entities = stores
    monkeypatch.setattr(
        Evidence, "restore", lambda *a: pytest.fail("Python entity decoding")
    )
    update_entities(
        conn,
        source,
        compacted,
        entities,
        HOURS,
        ["HOST"],
        initialize=True,
        allow_empty=True,
    )
    assert verify(source, compacted, entities) == 0
    assert json.loads(capsys.readouterr().out)["hosts"][0]["rows"] == 0
    report, _ = update_entities(conn, source, compacted, entities, HOURS, ["HOST"])
    assert not report["hosts"][0]["changed"]


def test_empty_parent_evidence_is_missing_not_pruned(conn, stores, capsys, monkeypatch):
    source, compacted, entities = stores
    seed(conn, source)
    build = entities.build_sql_state

    def empty_parent_candidate(conn, paths=()):
        build(conn, paths)
        payload = json.loads(
            conn.execute("select evidence_json from entity_state").fetchone()[0]
        )
        payload["parents"] = []
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        conn.execute(
            "update entity_state set parent_pid_hash=null, parent_os_pid=null, "
            "parent_status='missing', evidence_json=?",
            [encoded],
        )

    # Construct a valid publication candidate, rather than bypassing current-state
    # authority by manually replacing an already committed live snapshot.
    monkeypatch.setattr(entities, "build_sql_state", empty_parent_candidate)
    update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    monkeypatch.setattr(entities, "build_sql_state", build)
    assert verify(source, compacted, entities) == 0
    assert json.loads(capsys.readouterr().out)["hosts"][0]["rows"] == 1
    later = hour_window("2026-01-01T01:00:00Z", "2026-01-01T02:00:00Z")
    report, _ = update_entities(conn, source, compacted, entities, later, ["HOST"])
    assert report["hosts"][0]["rows"] == 1
    assert not report["hosts"][0]["changed"]
