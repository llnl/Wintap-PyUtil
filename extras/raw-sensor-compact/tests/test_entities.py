import io
import itertools
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from raw_sensor_compact.compaction import process_hours
from raw_sensor_compact.config import CompactionError, Dataset, Limits, hour_window
from raw_sensor_compact.entities import EntityLimits, Evidence, unknown_hash
from raw_sensor_compact.entity_store import EntityStore, update_entities
from raw_sensor_compact.inventory import Store

HOURS = hour_window("2026-01-01T00:00:00Z", "2026-01-01T02:00:00Z")


def observation(**changes):
    row = dict(hostname="HOST", pidhash="child", pid=10, parentpidhash="parent", parentpid=9,
               activitytype="START", eventtime=100, processname="child", processpath="/child",
               username="u", processargs="", agentid="a", partition="2026010100")
    return {**row, **changes}


def reduce(observations):
    state = Evidence()
    for row in observations:
        state.observe(row)
    return state.row("HOST", "child")


def test_permutations_duplicates_and_stale_refresh():
    inputs = [observation(), observation(activitytype="STOP", eventtime=200),
              observation(activitytype="REFRESH", eventtime=100, partition="2026010101", agentid="restart")]
    expected = reduce(inputs)
    for order in itertools.permutations(inputs):
        assert reduce(order) == expected
        assert reduce([*order, *order]) == expected
    assert expected["seen_stop"] and expected["lifetime_status"] == "exited"
    assert expected["stop_filetime"] == 200 and expected["birth_filetime"] == 100


@pytest.mark.parametrize("changes,status,parent,pid", [
    ({"parentpidhash": None, "parentpid": None}, "missing", None, None),
    ({"parentpidhash": unknown_hash("HOST"), "parentpid": 9}, "sentinel", None, None),
    ({"parentpidhash": "unknown", "parentpid": -1}, "sentinel", None, None),
    ({"parentpidhash": "parent", "parentpid": None}, "resolved", "parent", None),
])
def test_parent_classes(changes, status, parent, pid):
    row = reduce([observation(**changes)])
    assert (row["parent_status"], row["parent_pid_hash"], row["parent_os_pid"]) == (status, parent, pid)


def test_parent_pair_conflicts_and_missing_completion():
    assert reduce([observation(parentpid=None), observation()])["parent_os_pid"] == 9
    assert reduce([observation(parentpidhash="unknown"), observation()])["parent_pid_hash"] == "parent"
    for other in (observation(parentpidhash="other"), observation(parentpid=8)):
        row = reduce([observation(), other])
        assert row["parent_status"] == "conflicted"
        assert row["parent_pid_hash"] is None and row["parent_os_pid"] is None


def test_identity_lifetime_and_display_conflicts():
    row = reduce([observation(), observation(pid=11, eventtime=101, processname="aaa")])
    assert row["identity_status"] == "conflicted" and row["os_pid"] is None
    assert row["lifetime_status"] == "conflicted" and row["birth_filetime"] is None
    assert row["process_name"] == "aaa"  # Deterministic, evidence preserves both.
    assert reduce([observation(), observation(activitytype="STOP", eventtime=50)])["lifetime_status"] == "conflicted"


@pytest.fixture
def stores(tmp_path):
    result = []
    for name in ("raw_sensor", "compacted", "entities"):
        path = tmp_path / name
        path.mkdir()
        result.append(Store(Dataset.parse(str(path), event_root=name != "entities")))
    return result[0], result[1], EntityStore(result[2], "pid-v2")


def seed(conn, source, row=None, *, hour="00", filename="part.parquet"):
    row = row or observation()
    path = Path(source.dataset.root) / f"raw_process/dayPK=20260101/hourPK={hour}/{filename}"
    path.parent.mkdir(parents=True, exist_ok=True)
    types = {"pid": "BIGINT", "parentpid": "BIGINT", "eventtime": "BIGINT"}
    fields = [name for name in row if name != "partition"]
    query = "SELECT " + ",".join(f"?::{types.get(k, 'VARCHAR')} {k}" for k in fields)
    conn.execute("COPY (" + query + ") TO ? (FORMAT PARQUET)", [*[row[k] for k in fields], str(path)])
    return path


def state(conn, entities, tmp_path, host="HOST"):
    path = tmp_path / "read.parquet"
    assert entities.read(host, path)
    return entities.validate(conn, path, host)[0]


def test_init_replay_noop_restart_compacted_history(conn, stores, tmp_path):
    source, compacted, entities = stores
    path = seed(conn, source)
    report, _ = update_entities(conn, source, compacted, entities, HOURS, ["host"], initialize=True)
    assert report["hosts"][0]["rows"] == 1
    target = Path(entities.store.dataset.root) / entities.relative("HOST")
    before = target.read_bytes()
    # A fresh EntityStore has no in-memory persistence.
    restarted = EntityStore(entities.store, "pid-v2")
    report, _ = update_entities(conn, source, compacted, restarted, HOURS, ["HOST"])
    assert not report["hosts"][0]["changed"] and target.read_bytes() == before
    dest = Path(compacted.dataset.root) / path.relative_to(source.dataset.root)
    dest.parent.mkdir(parents=True)
    dest.write_bytes(path.read_bytes())
    report, _ = update_entities(conn, source, compacted, restarted, HOURS, ["HOST"])
    assert not report["hosts"][0]["changed"]  # raw/compacted overlap adds no vote.
    path.unlink()
    seed(conn, source, observation(activitytype="STOP", eventtime=200), hour="01")
    update_entities(conn, source, compacted, restarted, HOURS, ["HOST"])
    assert state(conn, entities, tmp_path)["child"].row("HOST", "child")["seen_stop"]


def test_pid_reuse_and_host_scope(conn, stores, tmp_path):
    source, compacted, entities = stores
    seed(conn, source)
    seed(conn, source, observation(pidhash="reused", eventtime=300), filename="reuse.parquet")
    seed(conn, source, observation(hostname="OTHER"), filename="other.parquet")
    update_entities(conn, source, compacted, entities, HOURS, ["HOST", "OTHER"], initialize=True)
    assert len(state(conn, entities, tmp_path)) == 2
    assert len(state(conn, entities, tmp_path, "OTHER")) == 1


def test_empty_metadata_and_missing_expected_snapshot(conn, stores, tmp_path):
    source, compacted, entities = stores
    with pytest.raises(CompactionError, match="allow-empty"):
        update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True, allow_empty=True)
    assert not state(conn, entities, tmp_path)
    with pytest.raises(CompactionError, match="absent"):
        update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True, allow_empty=True)
    target = Path(entities.store.dataset.root) / entities.relative("HOST")
    target.unlink()
    with pytest.raises(CompactionError, match="missing"):
        update_entities(conn, source, compacted, entities, HOURS, ["HOST"])


@pytest.mark.parametrize("changes", [{"pid": -1}, {"pidhash": "unknown"}, {"hostname": ""}])
def test_unknown_identity_fails_closed(conn, stores, changes):
    source, compacted, entities = stores
    path = seed(conn, source, observation(**changes))
    with pytest.raises(CompactionError):
        update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    assert path.exists() and not list(Path(entities.store.dataset.root).glob("**/*.parquet"))


def test_unknown_identity_quarantine_is_explicit_and_not_published(conn, stores, tmp_path):
    source, compacted, entities = stores
    seed(conn, source)
    excluded = seed(
        conn, source, observation(pidhash="unknown"), filename="excluded.parquet"
    )
    report, _ = update_entities(
        conn,
        source,
        compacted,
        entities,
        HOURS,
        ["HOST"],
        initialize=True,
        identity_policy="quarantine",
    )
    exclusions = report["identity_exclusions"]
    assert exclusions["policy"] == "quarantine"
    assert exclusions["rows"] == 1
    assert exclusions["reasons"] == {"unknown_pid_hash": 1}
    assert exclusions["fingerprint"]
    assert excluded.exists()
    assert set(state(conn, entities, tmp_path)) == {"child"}


def test_plan_read_only_and_resource_failure(conn, stores):
    source, compacted, entities = stores
    seed(conn, source)
    update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True, plan=True)
    assert not list(Path(entities.store.dataset.root).glob("**/*.parquet"))
    seed(conn, source, observation(pidhash="second"), filename="second.parquet")
    entities.limits = EntityLimits(max_rows=1)
    with pytest.raises(CompactionError, match="row budget"):
        update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    assert not list(Path(entities.store.dataset.root).glob("**/*.parquet"))


@pytest.mark.parametrize("drift", ["new", "changed", "removed"])
def test_cross_phase_object_drift_blocks_delete(conn, stores, drift):
    source, compacted, entities = stores
    original = seed(conn, source)
    retained = seed(conn, source, filename="retained.parquet")
    _, inventory = update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    if drift == "new":
        seed(conn, source, observation(pidhash="new"), filename="new.parquet")
    elif drift == "changed":
        seed(conn, source, observation(pidhash="changed"))
    else:
        original.unlink()
    with pytest.raises(CompactionError, match="published entity"):
        process_hours(conn, source, compacted, HOURS, ("raw_process",), Limits(),
                      target_size_bytes=100000, delete_source=True, process_inventory=inventory)
    assert retained.exists()


def test_combined_delete_then_replay_skipped_hour(conn, stores, tmp_path):
    source, compacted, entities = stores
    original = seed(conn, source)
    _, inventory = update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    report = process_hours(conn, source, compacted, HOURS, ("raw_process",), Limits(),
                           target_size_bytes=100000, delete_source=True, process_inventory=inventory)
    assert report["status"] == "completed" and not original.exists()
    late = seed(conn, source, observation(pidhash="late"), filename="late.parquet")
    _, inventory = update_entities(conn, source, compacted, entities, HOURS, ["HOST"])
    assert len(state(conn, entities, tmp_path)) == 2
    report = process_hours(conn, source, compacted, HOURS, ("raw_process",), Limits(),
                           target_size_bytes=100000, delete_source=True, process_inventory=inventory)
    assert report["status"] == "skipped" and late.exists()


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.fail = None

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        data = self.objects[Key]
        return {"Body": io.BytesIO(data), "ContentLength": len(data)}

    def put_object(self, Bucket, Key, Body):
        data = Body.read()
        self.objects[Key] = data if self.fail != "corrupt" else b"broken"
        if self.fail == "ack":
            raise OSError("lost acknowledgment")


@pytest.mark.parametrize("failure", ["ack", "corrupt"])
def test_s3_verification_lost_ack_retains_source(conn, stores, tmp_path, failure):
    source, compacted, _ = stores
    source_path = seed(conn, source)
    client = FakeS3()
    entities = EntityStore(Store(Dataset.parse("s3://test/entities"), client), "pid-v2")
    client.fail = failure
    with pytest.raises(CompactionError, match="no source deletion"):
        update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    assert source_path.exists()
    client.fail = None
    if failure == "ack":
        report, _ = update_entities(conn, source, compacted, entities, HOURS, ["HOST"])
        assert not report["hosts"][0]["changed"]
    else:
        with pytest.raises(Exception):
            update_entities(conn, source, compacted, entities, HOURS, ["HOST"])


def test_partial_host_publication_replay(conn, stores, monkeypatch, tmp_path):
    source, compacted, entities = stores
    seed(conn, source)
    seed(conn, source, observation(hostname="OTHER"), filename="other.parquet")
    update_entities(conn, source, compacted, entities, HOURS, ["HOST", "OTHER"], initialize=True)
    seed(conn, source, observation(activitytype="STOP", eventtime=200), hour="01")
    seed(conn, source, observation(hostname="OTHER", activitytype="STOP", eventtime=200), hour="01", filename="other.parquet")
    publish = entities.publish
    def fail(host, path):
        if host == "OTHER":
            raise OSError("failure")
        publish(host, path)
    monkeypatch.setattr(entities, "publish", fail)
    with pytest.raises(CompactionError, match="advanced hosts: HOST"):
        update_entities(conn, source, compacted, entities, HOURS, ["HOST", "OTHER"])
    monkeypatch.setattr(entities, "publish", publish)
    report, _ = update_entities(conn, source, compacted, entities, HOURS, ["HOST", "OTHER"])
    assert [h["changed"] for h in report["hosts"]] == [False, True]


def test_partial_init_can_explicitly_resume(conn, stores, monkeypatch):
    source, compacted, entities = stores
    seed(conn, source)
    seed(conn, source, observation(hostname="OTHER"), filename="other.parquet")
    original = entities.publish
    def fail(host, candidate):
        if host == "OTHER":
            raise OSError("partial initialization")
        original(host, candidate)
    monkeypatch.setattr(entities, "publish", fail)
    with pytest.raises(CompactionError, match="advanced hosts: HOST"):
        update_entities(conn, source, compacted, entities, HOURS, ["HOST", "OTHER"], initialize=True)
    monkeypatch.setattr(entities, "publish", original)
    report, _ = update_entities(conn, source, compacted, entities, HOURS, ["HOST", "OTHER"],
                                initialize=True, resume_init=True)
    assert [h["changed"] for h in report["hosts"]] == [False, True]


def test_missing_archive_root_and_state_corruption(conn, stores, tmp_path):
    source, compacted, entities = stores
    Path(compacted.dataset.root).rmdir()
    seed(conn, source)
    update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    target = Path(entities.store.dataset.root) / entities.relative("HOST")
    conn.execute("create table corrupt as select * replace('fake-parent' as parent_pid_hash) from read_parquet(?, hive_partitioning=false)", [str(target)])
    metadata = entities.metadata("HOST", "2026-01-01T00:00:00Z")
    from raw_sensor_compact.storage import quote
    kv = ','.join(quote(k) + ': ' + quote(v) for k, v in metadata.items())
    conn.execute(f"copy corrupt to ? (format parquet, kv_metadata {{{kv}}})", [str(target)])
    with pytest.raises(CompactionError, match="disagree"):
        update_entities(conn, source, compacted, entities, HOURS, ["HOST"])


def test_byte_budget_failure_before_publication(conn, stores):
    source, compacted, entities = stores
    seed(conn, source, observation(processargs="x" * 10000))
    entities.limits = EntityLimits(max_bytes=4096)
    with pytest.raises(CompactionError, match="byte budget"):
        update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    assert not list(Path(entities.store.dataset.root).glob("**/*.parquet"))


def test_raw_failure_after_entity_publication_retains_evidence(conn, stores, tmp_path, monkeypatch):
    source, compacted, entities = stores
    path = seed(conn, source)
    _, inventory = update_entities(conn, source, compacted, entities, HOURS, ["HOST"], initialize=True)
    def fail(*args, **kwargs):
        raise CompactionError("Injected raw COPY failure")
    monkeypatch.setattr("raw_sensor_compact.compaction.copy_parquet", fail)
    with pytest.raises(CompactionError, match="Injected raw COPY"):
        process_hours(conn, source, compacted, HOURS, ("raw_process",), Limits(),
                      target_size_bytes=100000, delete_source=True, process_inventory=inventory)
    assert path.exists()
    assert state(conn, entities, tmp_path)["child"].row("HOST", "child")["parent_pid_hash"] == "parent"
