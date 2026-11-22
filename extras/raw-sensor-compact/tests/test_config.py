from datetime import datetime, timezone

import pytest

from raw_sensor_compact.config import (
    CompactionError,
    Dataset,
    Leaf,
    Limits,
    hour_window,
    separate_roots,
)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "s3://bucket/raw_sensor",
        "s3://bucket/a/../b",
        "s3://user@bucket/data",
        "s3://bucket/a//b",
        "s3://bucket/data?secret=x",
        "s3://bucket/a#fragment",
        "s3://bucket/%2e%2e",
        "s3://bucket/a*",
        "https://example/data",
    ],
)
def test_invalid_roots(value):
    with pytest.raises(CompactionError):
        Dataset.parse(value)


def test_dataset_paths_and_overlap(tmp_path):
    a = Dataset.parse("s3://bucket/a/")
    assert a.object_key("raw_sensor/pidstat/") == "a/raw_sensor/pidstat/"
    assert a.file_uri("raw_sensor/pidstat/x.parquet") == (
        "s3://bucket/a/raw_sensor/pidstat/x.parquet"
    )
    for b in [a, Dataset.parse("s3://bucket/a/child"), Dataset.parse("s3://bucket")]:
        with pytest.raises(CompactionError, match="overlap"):
            separate_roots(a, b)
    separate_roots(a, Dataset.parse("s3://bucket/abc"))
    separate_roots(a, Dataset.parse("s3://other/a"))
    local = Dataset.parse(str(tmp_path / "source"))
    with pytest.raises(CompactionError):
        separate_roots(local, Dataset.parse(str(tmp_path / "source" / "derived")))
    separate_roots(local, a)


def test_hour_window_crosses_year():
    hours = hour_window(
        "2025-12-31T23:00:00Z",
        "2026-01-01T02:00:00Z",
        now=datetime(2026, 1, 1, 2, 12, tzinfo=timezone.utc),
    )
    assert [(h.strftime("%Y%m%d"), h.strftime("%H")) for h in hours] == [
        ("20251231", "23"),
        ("20260101", "00"),
        ("20260101", "01"),
    ]


@pytest.mark.parametrize(
    "start,end",
    [
        ("2026-01-01T00:00:00", "2026-01-01T01:00:00Z"),
        ("2026-01-01T00:30:00Z", "2026-01-01T01:00:00Z"),
        ("2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        ("2026-01-01T02:00:00Z", "2026-01-01T01:00:00Z"),
        ("2026-01-01T01:00:00Z", "2026-01-01T03:00:00Z"),
        ("2025-12-01T00:00:00Z", "2026-01-01T00:00:00Z"),
        ("2026-02-30T00:00:00Z", "2026-02-30T01:00:00Z"),
    ],
)
def test_invalid_hours(start, end):
    with pytest.raises(CompactionError):
        hour_window(start, end, now=datetime(2026, 1, 1, 2, tzinfo=timezone.utc))


@pytest.mark.parametrize(
    "relative",
    [
        "raw_sensor/pidstat/dayPK=20260230/hourPK=00/a.parquet",
        "raw_sensor/pidstat/dayPK=20260101/hourPK=24/a.parquet",
        "raw_sensor/pidstat/dayPK=20260101/hourPK=1/a.parquet",
        "raw_sensor/pidstat/dayPK=20260101/hourPK=00/host=x/a.parquet",
        "raw_sensor/pidstat/dayPK=20260101/hourPK=00/protoPK=../a.parquet",
        "raw_sensor/pidstat/dayPK=20260101/hourPK=00/a*.parquet",
    ],
)
def test_invalid_leaf(relative):
    with pytest.raises(CompactionError):
        Leaf.from_relative(relative)


def test_leaf_mirrors_protocol():
    leaf = Leaf.from_relative(
        "raw_sensor/raw_process_conn/dayPK=20260101/hourPK=00/protoPK=TCP/a.parquet"
    )
    assert leaf.protocol == "TCP"
    assert leaf.relative.endswith("hourPK=00/protoPK=TCP/")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"threads": 0},
        {"max_files": 0},
        {"timeout": 0},
        {"timeout": float("nan")},
        {"max_hours": 0},
        {"memory_limit": "1GB'; SELECT 1;--"},
    ],
)
def test_limits_fail_closed(kwargs):
    with pytest.raises(CompactionError):
        Limits(**kwargs)
