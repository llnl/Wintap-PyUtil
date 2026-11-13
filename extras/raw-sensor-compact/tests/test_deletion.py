from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

from raw_sensor_compact.compaction import process_hours
from raw_sensor_compact.config import CompactionError, Dataset, Leaf, Limits
from raw_sensor_compact.deletion import delete_hour
from raw_sensor_compact.inventory import Store

HOUR = datetime(2026, 1, 1, tzinfo=timezone.utc)
PARENT = Leaf("network", "20260101", "00")


def run(conn, source, dest, **kwargs):
    return process_hours(
        conn,
        Store(source),
        Store(dest),
        (HOUR,),
        ("network",),
        Limits(),
        target_size_bytes=1_000_000,
        delete_source=True,
        **kwargs,
    )


@pytest.fixture
def corpus(tmp_path, parquet):
    source = Dataset.parse(str(tmp_path / "raw_sensor"), event_root=True)
    dest = Dataset.parse(str(tmp_path / "compacted"), event_root=True)
    for proto in ("tcp", "udp"):
        leaf = Leaf("network", "20260101", "00", proto)
        for i in range(2):
            parquet(
                source.file_uri(leaf.relative + f"host{i}.parquet"),
                "select 1 value from range(3)",
            )
    # Entire hour prefix includes non-Parquet markers, but adjacent hours survive.
    Path(source.file_uri(PARENT.relative + "marker")).write_text("marker")
    neighbor = Path(
        source.file_uri(Leaf("network", "20260101", "01").relative + "keep")
    )
    neighbor.parent.mkdir(parents=True)
    neighbor.write_text("keep")
    return source, dest, neighbor


def test_validated_all_protocols_then_prefix_deleted(conn, corpus):
    source, dest, neighbor = corpus
    result = run(conn, source, dest)
    assert len(result["partitions"]) == 2
    assert all(p["source_rows"] == p["merged_rows"] == 6 for p in result["partitions"])
    assert result["hours"][0]["source_deleted_objects"] == 5
    assert not Store(source).exists(PARENT.relative)
    assert neighbor.read_text() == "keep"
    assert run(conn, source, dest)["status"] == "skipped"


@pytest.mark.parametrize("failure", ["copy", "validation", "schema"])
def test_merge_failures_never_delete(conn, corpus, monkeypatch, failure):
    source, dest, _ = corpus
    before = {p: p.read_bytes() for p in Path(source.root).rglob("*") if p.is_file()}
    symbol = {"copy": "copy_parquet", "validation": "finish_leaf", "schema": "schema"}[
        failure
    ]
    monkeypatch.setattr(
        "raw_sensor_compact.compaction." + symbol,
        Mock(side_effect=CompactionError("injected merge failure")),
    )
    deletion = Mock(side_effect=AssertionError("must not delete"))
    monkeypatch.setattr("raw_sensor_compact.compaction.delete_hour", deletion)
    with pytest.raises(CompactionError, match="injected"):
        run(conn, source, dest)
    deletion.assert_not_called()
    assert before == {p: p.read_bytes() for p in before}


def test_partial_deletion_stops_and_rerun_does_not_delete(conn, corpus, monkeypatch):
    source, dest, _ = corpus

    def fail(store, parent):
        next(Path(store.dataset.file_uri(parent.relative)).rglob("*.parquet")).unlink()
        raise CompactionError("injected partial deletion")

    monkeypatch.setattr("raw_sensor_compact.compaction.delete_hour", fail)
    with pytest.raises(CompactionError, match="partial deletion") as error:
        run(conn, source, dest)
    assert error.value.phase == "delete_source"
    assert error.value.source_hour == source.file_uri(PARENT.relative)
    assert len(list(Path(source.root).rglob("*.parquet"))) == 3
    assert run(conn, source, dest)["status"] == "skipped"


def test_second_protocol_failure_preserves_whole_source_hour(conn, corpus, monkeypatch):
    import raw_sensor_compact.compaction as compaction

    source, dest, _ = corpus
    original = compaction.finish_leaf
    calls = 0

    def fail_second(*args):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise CompactionError("injected second protocol failure")
        return original(*args)

    monkeypatch.setattr(compaction, "finish_leaf", fail_second)
    with pytest.raises(CompactionError, match="second protocol"):
        run(conn, source, dest)
    assert len(list(Path(source.root).rglob("*.parquet"))) == 4
    assert len(list(Path(dest.root).rglob("*.parquet"))) == 2


@pytest.mark.parametrize("minutes", [0, 9])
def test_grace_blocks_before_writing(conn, corpus, minutes):
    source, dest, _ = corpus
    with pytest.raises(CompactionError, match="grace"):
        run(conn, source, dest, now=HOUR + timedelta(hours=1, minutes=minutes))
    assert not Path(dest.root).exists()
    assert len(list(Path(source.root).rglob("*.parquet"))) == 4


def test_grace_boundary_allowed(conn, corpus):
    source, dest, _ = corpus
    assert (
        run(conn, source, dest, now=HOUR + timedelta(hours=1, minutes=10))["status"]
        == "completed"
    )


def test_symlink_deletion_rejected_before_unlinking(tmp_path):
    root = Dataset.parse(str(tmp_path / "raw_sensor"), event_root=True)
    directory = Path(root.file_uri(PARENT.relative))
    directory.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.write_text("keep")
    (directory / "link").symlink_to(outside)
    with pytest.raises(CompactionError, match="Symlink"):
        delete_hour(Store(root), PARENT)
    assert outside.read_text() == "keep"


def s3_store():
    client = Mock()
    root = Dataset.parse("s3://test/isolated/raw_sensor", event_root=True)
    return Store(root, client), root.object_key(PARENT.relative)


def test_s3_paginated_prefix_delete_batches_and_markers():
    store, prefix = s3_store()
    keys = [prefix + f"{i}.parquet" for i in range(1001)] + [prefix]
    store.client.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": key} for key in keys[:900]]},
        {"Contents": [{"Key": key} for key in keys[900:]]},
    ]
    store.client.delete_objects.return_value = {}
    store.client.list_objects_v2.return_value = {}
    assert delete_hour(store, PARENT) == 1002
    assert store.client.delete_objects.call_count == 2
    deleted = {
        v["Key"]
        for call in store.client.delete_objects.call_args_list
        for v in call.kwargs["Delete"]["Objects"]
    }
    assert deleted == set(keys)


@pytest.mark.parametrize(
    "failure", ["per-object", "transport", "remaining", "out-of-scope"]
)
def test_s3_deletion_errors_are_not_success(failure):
    store, prefix = s3_store()
    key = "other/hour/keep" if failure == "out-of-scope" else prefix + "one.parquet"
    store.client.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": key}]}
    ]
    store.client.delete_objects.return_value = (
        {"Errors": [{"Key": key}]} if failure == "per-object" else {}
    )
    store.client.list_objects_v2.return_value = (
        {"Contents": [{"Key": key}]} if failure == "remaining" else {}
    )
    if failure == "transport":
        store.client.delete_objects.side_effect = RuntimeError("secret backend text")
    with pytest.raises(CompactionError):
        delete_hour(store, PARENT)
    if failure == "out-of-scope":
        store.client.delete_objects.assert_not_called()
