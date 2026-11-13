"""Explicitly opted-in, bounded real S3 tests; never delete source or output."""

import contextlib
import hashlib
import io
import json
import os

import pytest

from raw_sensor_compact.cli import main
from raw_sensor_compact.config import Dataset, Leaf, hour_window, separate_roots
from raw_sensor_compact.engine import connection
from raw_sensor_compact.inventory import Store, discover
from raw_sensor_compact.storage import S3Settings, open_s3


def fixture_values():
    names = (
        "SOURCE",
        "DESTINATION_ROOT",
        "START_HOUR",
        "END_HOUR",
        "EVENT_TYPES",
        "EXPECTED_ROWS",
    )
    values = {name: os.environ.get(f"RAW_COMPACT_S3_TEST_{name}") for name in names}
    assert all(values.values()), "Every documented fixture variable is required"
    assert values["SOURCE"].startswith("s3://")
    assert values["DESTINATION_ROOT"].startswith("s3://")
    assert int(values["EXPECTED_ROWS"]) > 0
    return values


def flags(values):
    return [
        "--source-root",
        values["SOURCE"],
        "--destination-root",
        values["DESTINATION_ROOT"],
        "--start-hour",
        values["START_HOUR"],
        "--end-hour",
        values["END_HOUR"],
        "--max-hours",
        "1",
        "--max-files",
        "100",
        "--timeout",
        "180",
        *[a for e in values["EVENT_TYPES"].split(",") for a in ("--event-type", e)],
    ]


def invoke(command, values, *extra):
    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        code = main([command, *flags(values), *extra])
    assert code == 0, err.getvalue()
    return json.loads(out.getvalue())


@pytest.mark.s3
def test_seeded_fixture_counts():
    if os.environ.get("RAW_COMPACT_S3_TEST_APPROVED") != "isolated-read-only":
        pytest.skip("Isolated S3 read-only access not explicitly approved")
    values = fixture_values()
    report = invoke("verify", values)
    assert report["status"] == "passed"
    assert sum(p["source_rows"] for p in report["partitions"]) == int(
        values["EXPECTED_ROWS"]
    )
    assert all(p["row_delta"] == 0 for p in report["partitions"])


@pytest.mark.s3
def test_fixed_destination_s3_merge_and_skip():
    if os.environ.get("RAW_COMPACT_S3_TEST_APPROVED") != "isolated-write":
        pytest.skip("Isolated S3 destination writes not explicitly approved")
    values = fixture_values()
    source = Dataset.parse(values["SOURCE"], event_root=True)
    destination = Dataset.parse(values["DESTINATION_ROOT"], event_root=True)
    separate_roots(source, destination)
    hours = hour_window(values["START_HOUR"], values["END_HOUR"], max_hours=1)
    events = tuple(values["EVENT_TYPES"].split(","))
    with connection() as conn:
        client = open_s3(conn, S3Settings())
        inputs = discover(Store(source, client), hours, events, max_files=100)
        assert inputs.groups
        for event in events:
            for hour in hours:
                parent = Leaf(event, hour.strftime("%Y%m%d"), hour.strftime("%H"))
                assert not Store(destination, client).exists(parent.relative), (
                    "Write fixture must start with absent destination hours"
                )

        def hashes(dataset, inventory):
            result = {}
            for files in inventory.groups.values():
                for file in files:
                    response = client.get_object(
                        Bucket=dataset.bucket, Key=dataset.object_key(file.relative)
                    )
                    digest = hashlib.sha256()
                    with response["Body"] as stream:
                        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                            digest.update(chunk)
                    result[file.relative] = digest.hexdigest()
            return result

        before = hashes(source, inputs)
        report = invoke("compact", values, "--target-size-bytes", "134217728")
        assert report["status"] == "completed"
        assert sum(p["source_rows"] for p in report["partitions"]) == int(
            values["EXPECTED_ROWS"]
        )
        assert all(p["source_rows"] == p["merged_rows"] for p in report["partitions"])
        assert all(
            "/runs/" not in f and "/compacted/raw_sensor/" not in f
            for p in report["partitions"]
            for f in p["files"]
        )
        assert invoke("verify", values)["status"] == "passed"
        output = discover(Store(destination, client), hours, events, max_files=100)
        merged_before = hashes(destination, output)
        skipped = invoke("compact", values, "--target-size-bytes", "134217728")
        assert skipped["status"] == "skipped"
        assert all(h["status"] == "skipped_existing" for h in skipped["hours"])
        assert merged_before == hashes(destination, output)
        assert before == hashes(source, inputs)
    print(
        json.dumps(
            {
                "destination_root": destination.root,
                "source_sha256_unchanged": len(before),
                "rerun_status": skipped["status"],
                "partitions": [
                    {k: v for k, v in p.items() if k not in {"files", "schema"}}
                    for p in report["partitions"]
                ],
            }
        )
    )
