import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from raw_sensor_compact.cli import main
from raw_sensor_compact.compaction import process_hours
from raw_sensor_compact.config import CompactionError, Dataset, Leaf, Limits
from raw_sensor_compact.inventory import Store

HOURS = (datetime(2026, 1, 1, tzinfo=timezone.utc),)
LEAF = Leaf("pidstat", "20260101", "00")


@pytest.fixture
def corpus(tmp_path, parquet):
    source = Dataset.parse(str(tmp_path / "lintap/raw_sensor"), event_root=True)
    destination = Dataset.parse(str(tmp_path / "lintap/compacted"), event_root=True)
    files = [
        parquet(
            source.file_uri(LEAF.relative + f"host{i}.parquet"),
            "SELECT 1 AS value, NULL::VARCHAR AS optional FROM range(2)",
        )
        for i in range(3)
    ]
    return SimpleNamespace(
        source=source,
        destination=destination,
        files=files,
        limits=Limits(),
        tmp_path=tmp_path,
    )


def process(conn, corpus, *, plan=False, hours=HOURS):
    return process_hours(
        conn,
        Store(corpus.source),
        Store(corpus.destination),
        hours,
        (),
        corpus.limits,
        target_size_bytes=1_000_000,
        plan=plan,
    )


def test_exact_layout_and_rerun_skip(conn, corpus):
    before = {p: hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in corpus.files}
    first = process(conn, corpus)
    assert first["status"] == "completed"
    result = first["partitions"][0]
    assert result["source_rows"] == result["merged_rows"] == 6
    assert result["merged_files"] == 1
    relative = Path(result["files"][0]).relative_to(corpus.destination.root).as_posix()
    assert relative.startswith("pidstat/dayPK=20260101/hourPK=00/")
    assert "raw_sensor/" not in relative and "runs/" not in relative
    output_before = Path(result["files"][0]).read_bytes()
    second = process(conn, corpus)
    assert second["status"] == "skipped"
    assert second["hours"][0]["status"] == "skipped_existing"
    assert not second["partitions"]
    assert Path(result["files"][0]).read_bytes() == output_before
    assert before == {
        p: hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in before
    }
    assert not list(Path(corpus.destination.root).rglob("*.json"))


def test_existing_hour_is_skipped_without_reading_source(conn, corpus, monkeypatch):
    folder = Path(corpus.destination.file_uri(LEAF.relative))
    folder.mkdir(parents=True)
    (folder / "incomplete.txt").write_text("manual cleanup needed")
    for file in corpus.files:
        Path(file).write_bytes(b"not parquet")
    monkeypatch.setattr(
        "raw_sensor_compact.compaction.discover",
        Mock(side_effect=AssertionError("Skipped source must not be scanned")),
    )
    assert process(conn, corpus)["status"] == "skipped"
    assert process(conn, corpus, plan=True)["status"] == "skipped"


def test_empty_local_directory_also_means_skip(conn, corpus):
    Path(corpus.destination.file_uri(LEAF.relative)).mkdir(parents=True)
    assert process(conn, corpus)["hours"][0]["status"] == "skipped_existing"


def test_skip_whole_event_hour_when_one_protocol_exists(conn, tmp_path, parquet):
    source = Dataset.parse(str(tmp_path / "raw_sensor"), event_root=True)
    destination = Dataset.parse(str(tmp_path / "compacted"), event_root=True)
    for proto in ("TCP", "UDP"):
        leaf = Leaf("network", "20260101", "00", proto)
        parquet(source.file_uri(leaf.relative + "host.parquet"), "SELECT 1 AS value")
    tcp = Leaf("network", "20260101", "00", "TCP")
    parquet(destination.file_uri(tcp.relative + "partial.parquet"), "SELECT 1 AS value")
    report = process_hours(
        conn,
        Store(source),
        Store(destination),
        HOURS,
        (),
        Limits(),
        target_size_bytes=1_000_000,
    )
    assert report["status"] == "skipped"
    assert not Path(
        destination.file_uri(Leaf("network", "20260101", "00", "UDP").relative)
    ).exists()


def test_protocol_leaves_are_merged_separately(conn, tmp_path, parquet):
    source = Dataset.parse(str(tmp_path / "raw_sensor"), event_root=True)
    destination = Dataset.parse(str(tmp_path / "compacted"), event_root=True)
    for proto in ("TCP", "UDP"):
        leaf = Leaf("network", "20260101", "00", proto)
        for i in range(2):
            parquet(
                source.file_uri(leaf.relative + f"host{i}.parquet"),
                f"SELECT '{proto}' AS payload FROM range(2)",
            )
    report = process_hours(
        conn,
        Store(source),
        Store(destination),
        HOURS,
        (),
        Limits(),
        target_size_bytes=1_000_000,
    )
    assert len(report["hours"]) == 1
    assert {p["protoPK"] for p in report["partitions"]} == {"TCP", "UDP"}
    assert all(p["merged_rows"] == p["source_rows"] == 4 for p in report["partitions"])


def test_failed_merge_leaves_output_and_rerun_skips(conn, corpus, monkeypatch):
    original = process(conn, corpus, plan=True)
    assert original["status"] == "planned"
    assert not Path(corpus.destination.root).exists()
    original_finish = __import__(
        "raw_sensor_compact.compaction", fromlist=["finish_leaf"]
    ).finish_leaf
    monkeypatch.setattr(
        "raw_sensor_compact.compaction.finish_leaf",
        Mock(side_effect=CompactionError("Injected validation failure")),
    )
    with pytest.raises(CompactionError, match="Injected") as error:
        process(conn, corpus)
    assert error.value.destination_hour == corpus.destination.file_uri(LEAF.relative)
    assert list(Path(error.value.destination_hour).glob("*.parquet"))
    assert process(conn, corpus)["status"] == "skipped"
    monkeypatch.setattr("raw_sensor_compact.compaction.finish_leaf", original_finish)
    # Simulate human removal of this test's failed hour only.
    for p in Path(error.value.destination_hour).glob("*.parquet"):
        p.unlink()
    Path(error.value.destination_hour).rmdir()
    assert process(conn, corpus)["status"] == "completed"


def test_schema_failure_does_not_create_destination(conn, corpus, parquet):
    parquet(
        corpus.source.file_uri(LEAF.relative + "bad.parquet"), "SELECT 'bad' AS value"
    )
    with pytest.raises(CompactionError, match="Incompatible"):
        process(conn, corpus)
    assert not Path(corpus.destination.root).exists()


def test_existing_hour_does_not_prevent_next_hour(conn, corpus, parquet):
    Path(corpus.destination.file_uri(LEAF.relative)).mkdir(parents=True)
    next_leaf = Leaf("pidstat", "20260101", "01")
    for i in range(2):
        parquet(
            corpus.source.file_uri(next_leaf.relative + f"host{i}.parquet"),
            "SELECT 2 AS value",
        )
    hours = HOURS + (datetime(2026, 1, 1, 1, tzinfo=timezone.utc),)
    report = process(conn, corpus, hours=hours)
    assert [h["status"] for h in report["hours"]] == ["skipped_existing", "merged"]
    assert report["partitions"][0]["hourPK"] == "01"


def test_cli_fixed_root_merge_verify_and_skip(corpus, capsys):
    flags = [
        "--source-root",
        corpus.source.root,
        "--destination-root",
        corpus.destination.root,
        "--start-hour",
        "2026-01-01T00:00:00Z",
        "--end-hour",
        "2026-01-01T01:00:00Z",
    ]
    assert main(["compact", *flags, "--target-size-bytes", "1000000"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "completed"
    assert main(["verify", *flags]) == 0
    assert json.loads(capsys.readouterr().out)["partitions"][0]["row_delta"] == 0
    assert main(["compact", *flags, "--target-size-bytes", "1000000"]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "skipped"


def test_s3_exists_uses_any_object_and_exact_hour_prefix():
    client = Mock()
    client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "lintap/compacted/pidstat/dayPK=20260101/hourPK=00/marker"}
        ]
    }
    store = Store(Dataset.parse("s3://test/lintap/compacted", event_root=True), client)
    assert store.exists(LEAF.relative)
    client.list_objects_v2.assert_called_once_with(
        Bucket="test",
        Prefix="lintap/compacted/pidstat/dayPK=20260101/hourPK=00/",
        MaxKeys=1,
    )
    client.put_object.assert_not_called()


def test_source_destination_actual_sibling_roots():
    from raw_sensor_compact.config import separate_roots

    source = Dataset.parse("s3://bucket/lintap/raw_sensor", event_root=True)
    destination = Dataset.parse("s3://bucket/lintap/compacted", event_root=True)
    separate_roots(source, destination)
    assert source.file_uri(LEAF.relative + "input.parquet") == (
        "s3://bucket/lintap/raw_sensor/pidstat/dayPK=20260101/hourPK=00/input.parquet"
    )
    assert destination.file_uri(LEAF.relative + "merged.parquet") == (
        "s3://bucket/lintap/compacted/pidstat/dayPK=20260101/hourPK=00/merged.parquet"
    )
