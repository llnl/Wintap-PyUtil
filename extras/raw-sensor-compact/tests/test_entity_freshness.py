"""Freshness is independent of scalar/evidence integrity; use active entrypoints."""

from pathlib import Path

import pytest
from test_entities import HOURS, observation, seed
from test_entities import stores as stores_fixture
from test_entity_integrity import rewrite, verify

from raw_sensor_compact.config import CompactionError, hour_window
from raw_sensor_compact.entity_store import update_entities

stores = stores_fixture
AUTHORITY = "_authority/authority.parquet"
LATER = hour_window("2026-01-01T01:00:00Z", "2026-01-01T02:00:00Z")


@pytest.mark.parametrize("entrypoint", ["verify", "update"])
@pytest.mark.parametrize(
    "damage", ["rollback", "replacement", "bootstrap", "missing_authority"]
)
def test_current_state_authority_rejects_valid_but_untrusted_state(
    conn, stores, entrypoint, damage, capsys
):
    source, compacted, entities = stores
    raw = seed(conn, source)
    update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    target = Path(entities.store.dataset.root) / entities.relative("HOST")
    original = target.read_bytes()
    seed(conn, source, observation(activitytype="STOP", eventtime=200), hour="01")
    update_entities(conn, source, compacted, entities, LATER, ["HOST"])
    if damage == "rollback":
        target.write_bytes(original)
    elif damage == "replacement":
        rewrite(conn, entities, "delete from corrupt")  # Valid empty snapshot.
    elif damage == "bootstrap":
        metadata = entities.sql_metadata(conn, target)
        metadata["bootstrap_start"] = "2025-12-31T00:00:00Z"
        rewrite(conn, entities, "select 1", metadata)
    else:
        (Path(entities.store.dataset.root) / AUTHORITY).unlink(missing_ok=True)
    entities.validate_sql(conn, target, "HOST", None)  # All four pass integrity.
    before = target.read_bytes()
    if entrypoint == "verify":
        assert verify(source, compacted, entities) == 2
        assert "authority" in capsys.readouterr().err.lower()
    else:
        with pytest.raises(CompactionError, match="authority"):
            update_entities(conn, source, compacted, entities, LATER, ["HOST"])
    assert raw.exists() and target.read_bytes() == before


def test_forward_evolution_and_out_of_order_replay_remain_valid(conn, stores, capsys):
    source, compacted, entities = stores
    seed(conn, source)
    first, _ = update_entities(
        conn, source, compacted, entities, HOURS, ["HOST"], initialize=True
    )
    seed(conn, source, observation(activitytype="STOP", eventtime=200), hour="01")
    later, _ = update_entities(conn, source, compacted, entities, LATER, ["HOST"])
    assert first["hosts"][0]["semantic_sha256"] != later["hosts"][0]["semantic_sha256"]
    assert verify(source, compacted, entities) == 0
    capsys.readouterr()
    replay, _ = update_entities(conn, source, compacted, entities, HOURS, ["HOST"])
    assert not replay["hosts"][0]["changed"]
    assert replay["hosts"][0]["bootstrap_start"] == first["hosts"][0]["bootstrap_start"]


@pytest.mark.parametrize("entrypoint", ["verify", "update"])
def test_expected_host_subset_cannot_rebind_authority(conn, stores, entrypoint):
    source, compacted, entities = stores
    seed(conn, source)
    seed(conn, source, observation(hostname="OTHER"), filename="other.parquet")
    update_entities(
        conn, source, compacted, entities, HOURS, ["HOST", "OTHER"], initialize=True
    )
    if entrypoint == "verify":
        assert verify(source, compacted, entities) == 2
    else:
        with pytest.raises(CompactionError, match="authority"):
            update_entities(conn, source, compacted, entities, LATER, ["HOST"])


@pytest.mark.parametrize(
    "phase", ["intent_before", "intent_after", "commit_before", "commit_after"]
)
def test_authority_put_crashes_only_recover_recorded_candidates(
    conn, stores, monkeypatch, phase, capsys
):
    source, compacted, entities = stores
    raw = seed(conn, source)
    update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    seed(conn, source, observation(activitytype="STOP", eventtime=200), hour="01")
    publish = entities.publish_object

    def crash(relative, candidate):
        if relative == AUTHORITY:
            status = conn.execute(
                "select status from read_parquet(?) order by generation desc limit 1",
                [str(candidate)],
            ).fetchone()[0]
            target = "pending" if phase.startswith("intent") else "committed"
            if status == target:
                if phase.endswith("after"):
                    publish(relative, candidate)
                raise OSError("injected authority PUT/ack failure")
        publish(relative, candidate)

    monkeypatch.setattr(entities, "publish_object", crash)
    with pytest.raises(CompactionError, match="authority publication uncertain"):
        update_entities(conn, source, compacted, entities, LATER, ["HOST"])
    assert raw.exists()
    expected_verify = 0 if phase in {"intent_before", "commit_after"} else 2
    assert verify(source, compacted, entities) == expected_verify
    capsys.readouterr()
    monkeypatch.setattr(entities, "publish_object", publish)
    report, _ = update_entities(conn, source, compacted, entities, LATER, ["HOST"])
    assert report["hosts"][0]["changed"] == phase.startswith("intent")
    assert verify(source, compacted, entities) == 0


@pytest.mark.parametrize("damage", ["replacement", "changed_input", "changed_window"])
def test_pending_intent_cannot_bless_new_snapshot_or_inputs(
    conn, stores, monkeypatch, damage
):
    source, compacted, entities = stores
    seed(conn, source)
    update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    seed(conn, source, observation(activitytype="STOP", eventtime=200), hour="01")
    publish = entities.publish
    monkeypatch.setattr(
        entities,
        "publish",
        lambda *args: (_ for _ in ()).throw(OSError("before host PUT")),
    )
    with pytest.raises(CompactionError):
        update_entities(conn, source, compacted, entities, LATER, ["HOST"])
    monkeypatch.setattr(entities, "publish", publish)
    if damage == "replacement":
        rewrite(conn, entities, "delete from corrupt")
    elif damage == "changed_input":
        seed(
            conn, source, observation(pidhash="new"), hour="01", filename="new.parquet"
        )
    with pytest.raises(CompactionError, match="authority"):
        update_entities(
            conn,
            source,
            compacted,
            entities,
            HOURS if damage == "changed_window" else LATER,
            ["HOST"],
        )


@pytest.mark.parametrize(
    "change",
    [
        "delete from changed where generation=1",
        "insert into changed select * from changed where generation=1",
        "update changed set parent_id='broken' where generation=2",
        "update changed set input_fingerprint=repeat('a',64) where generation=1",
        "update changed set generation_id=repeat('a',64) where generation=2",
        "update changed set snapshot_hashes='{}' where generation=2",
        "update changed set bootstrap_starts=null where generation=2",
    ],
)
@pytest.mark.parametrize("entrypoint", ["verify", "update"])
def test_authority_receipt_corruption_rejected(conn, stores, change, entrypoint):
    source, compacted, entities = stores
    seed(conn, source)
    update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    update_entities(conn, source, compacted, entities, LATER, ["HOST"])
    target = Path(entities.store.dataset.root) / AUTHORITY
    conn.execute(
        "create temp table changed as select * from "
        "read_parquet(?, hive_partitioning=false)",
        [str(target)],
    )
    conn.execute(change)
    conn.execute("copy changed to ? (format parquet)", [str(target)])
    if entrypoint == "verify":
        assert verify(source, compacted, entities) == 2
    else:
        with pytest.raises(CompactionError, match="authority"):
            update_entities(conn, source, compacted, entities, LATER, ["HOST"])
