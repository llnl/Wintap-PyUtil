from datetime import datetime, timezone

import pytest

from raw_sensor_compact.config import CompactionError, Dataset, Leaf
from raw_sensor_compact.inventory import Store, discover, recheck

HOURS = (datetime(2026, 1, 1, 0, tzinfo=timezone.utc),)
PREFIX = "a/raw_sensor/pidstat/dayPK=20260101/hourPK=00/"


class Paginator:
    def __init__(self, client):
        self.client = client

    def paginate(self, **kwargs):
        self.client.calls.append(kwargs)
        if "Delimiter" in kwargs:
            return iter([{"CommonPrefixes": [{"Prefix": "a/raw_sensor/pidstat/"}]}])
        return iter(self.client.pages)


class FakeClient:
    def __init__(self, pages):
        self.pages, self.calls = pages, []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return Paginator(self)


def item(name, **changes):
    return {"Key": PREFIX + name, "Size": 10, "ETag": "etag", **changes}


def test_paginated_listing_is_exact_and_deduplicated():
    client = FakeClient(
        [
            {"Contents": [item("one.parquet"), item("ignore.json")]},
            {"Contents": [item("one.parquet"), item("two.parquet")]},
        ]
    )
    inventory = discover(Store(Dataset.parse("s3://bucket/a"), client), HOURS)
    assert client.calls == [
        {"Bucket": "bucket", "Prefix": "a/raw_sensor/", "Delimiter": "/"},
        {"Bucket": "bucket", "Prefix": PREFIX},
    ]
    files = inventory.groups[Leaf("pidstat", "20260101", "00")]
    assert len(files) == 2
    assert sum(file.size for file in files) == 20


@pytest.mark.parametrize(
    "pages,limit",
    [
        ([{"Contents": [item("one.parquet"), item("two.parquet")]}], 1),
        (
            [{"Contents": [item("one.parquet"), item("one.parquet", ETag="changed")]}],
            10,
        ),
        (
            [
                {
                    "Contents": [
                        item(
                            "one.parquet",
                            Key="a/raw_sensor/pidstat/dayPK=20260101/hourPK=01/x.parquet",
                        )
                    ]
                }
            ],
            10,
        ),
        ([{"Contents": [item("host=x/one.parquet")]}], 10),
    ],
)
def test_no_truncation_or_out_of_scope_objects(pages, limit):
    client = FakeClient(pages)
    with pytest.raises(CompactionError):
        discover(
            Store(Dataset.parse("s3://bucket/a"), client),
            HOURS,
            ("pidstat",),
            max_files=limit,
        )


def test_protocols_and_adjacent_hours_stay_separate(tmp_path, parquet):
    root = Dataset.parse(str(tmp_path / "source"))
    for proto in ("TCP", "UDP"):
        leaf = Leaf("network", "20260101", "00", proto)
        parquet(root.file_uri(leaf.relative + "host1.parquet"), "SELECT 1 AS value")
    other = Leaf("network", "20260101", "01", "TCP")
    parquet(root.file_uri(other.relative + "host1.parquet"), "SELECT 2 AS value")
    inventory = discover(Store(root), HOURS)
    assert len(inventory.groups) == 2
    assert {leaf.protocol for leaf in inventory.groups} == {"TCP", "UDP"}
    assert all(leaf.hour == "00" for leaf in inventory.groups)


def test_empty_and_drift_recheck(tmp_path, parquet):
    root = Dataset.parse(str(tmp_path / "source"))
    (tmp_path / "source").mkdir()
    store = Store(root)
    inventory = discover(store, HOURS, ("pidstat",))
    assert not inventory.groups
    assert inventory.empty_hours == ("raw_sensor/pidstat/dayPK=20260101/hourPK=00/",)
    recheck(store, inventory, HOURS, max_files=100)
    parquet(root.file_uri(inventory.empty_hours[0] + "new.parquet"), "SELECT 1 AS x")
    with pytest.raises(CompactionError, match="changed"):
        recheck(store, inventory, HOURS, max_files=100)


def test_auto_family_recheck_finds_new_family(tmp_path, parquet):
    root = Dataset.parse(str(tmp_path / "source"))
    (tmp_path / "source").mkdir()
    store = Store(root)
    inventory = discover(store, HOURS)
    parquet(
        root.file_uri(Leaf("new_family", "20260101", "00").relative + "new.parquet"),
        "SELECT 1 AS x",
    )
    with pytest.raises(CompactionError, match="changed"):
        recheck(store, inventory, HOURS, max_files=100)


def test_symlinks_are_not_followed(tmp_path, parquet):
    root = tmp_path / "source"
    leaf = Leaf("pidstat", "20260101", "00")
    real = parquet(tmp_path / "secret.parquet", "SELECT 1 AS x")
    folder = root / leaf.relative
    folder.mkdir(parents=True)
    (folder / "link.parquet").symlink_to(real)
    with pytest.raises(CompactionError, match="Symlink"):
        discover(Store(Dataset.parse(str(root))), HOURS, ("pidstat",))


def test_expired_deadline_stops_listing_before_sdk_call():
    client = FakeClient([])
    store = Store(Dataset.parse("s3://bucket/a"), client, deadline=0)
    with pytest.raises(CompactionError, match="timeout"):
        discover(store, HOURS, ("pidstat",))
    assert not client.calls


@pytest.mark.parametrize("mutation", ["change", "remove"])
def test_existing_source_identity_changes_fail(tmp_path, parquet, mutation):
    root = Dataset.parse(str(tmp_path / "source"))
    path = root.file_uri(Leaf("pidstat", "20260101", "00").relative + "a.parquet")
    parquet(path, "SELECT 1 AS value")
    store = Store(root)
    inventory = discover(store, HOURS)
    if mutation == "change":
        parquet(path, "SELECT 1 AS value FROM range(10)")
    else:
        from pathlib import Path

        Path(path).unlink()
    with pytest.raises(CompactionError, match="changed"):
        recheck(store, inventory, HOURS, max_files=100)
