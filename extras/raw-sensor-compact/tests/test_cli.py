import json

import pytest

from raw_sensor_compact.cli import main
from raw_sensor_compact.config import Leaf


def args(command, source, merged, *events):
    result = [
        command,
        "--source-root",
        str(source).rstrip("/") + "/raw_sensor",
        "--destination-root",
        str(merged).rstrip("/") + "/raw_sensor",
        "--start-hour",
        "2026-01-01T00:00:00Z",
        "--end-hour",
        "2026-01-01T01:00:00Z",
    ]
    for event in events:
        result.extend(["--event-type", event])
    return result


def test_plan_reads_selected_hour_and_never_creates_destination(
    tmp_path, parquet, capsys
):
    source, merged = tmp_path / "source", tmp_path / "destination"
    leaf = Leaf("pidstat", "20260101", "00")
    parquet(source / leaf.relative / "a.parquet", "SELECT 1 AS pid")
    other = Leaf("pidstat", "20260101", "01")
    # If this adjacent-hour file were opened the plan would fail.
    invalid = source / other.relative / "bad.parquet"
    invalid.parent.mkdir(parents=True)
    invalid.write_bytes(b"not parquet")
    assert main(args("plan", source, merged, "pidstat")) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "planned"
    assert report["partitions"][0]["schema"] == [{"name": "pid", "type": "INTEGER"}]
    assert not merged.exists()


@pytest.mark.parametrize("count,exit_code", [(5, 0), (4, 1), (6, 1)])
def test_operational_verify_count_status(tmp_path, parquet, capsys, count, exit_code):
    source, merged = tmp_path / "source", tmp_path / "merged"
    leaf = Leaf("pidstat", "20260101", "00")
    parquet(source / leaf.relative / "a.parquet", "SELECT 1 AS value FROM range(3)")
    parquet(
        source / leaf.relative / "b.parquet",
        "SELECT NULL::INTEGER AS value FROM range(2)",
    )
    parquet(
        merged / leaf.relative / "compact.parquet",
        f"SELECT 1 AS value FROM range({count})",
    )
    assert main(args("verify", source, merged)) == exit_code
    report = json.loads(capsys.readouterr().out)
    check = report["partitions"][0]
    assert check["source_rows"] == 5
    assert check["merged_rows"] == count
    assert check["row_delta"] == count - 5


def test_deltas_cannot_cancel_between_protocols(tmp_path, parquet, capsys):
    source, merged = tmp_path / "source", tmp_path / "merged"
    for proto, source_count, merged_count in [("TCP", 5, 4), ("UDP", 4, 5)]:
        leaf = Leaf("network", "20260101", "00", proto)
        parquet(
            source / leaf.relative / "a.parquet",
            f"SELECT 1 AS value FROM range({source_count})",
        )
        parquet(
            merged / leaf.relative / "out.parquet",
            f"SELECT 1 AS value FROM range({merged_count})",
        )
    assert main(args("verify", source, merged)) == 1
    report = json.loads(capsys.readouterr().out)
    assert len(report["partitions"]) == 2
    assert {part["row_delta"] for part in report["partitions"]} == {-1, 1}


def test_extra_output_family_is_checked(tmp_path, parquet, capsys):
    source, merged = tmp_path / "source", tmp_path / "merged"
    (source / "raw_sensor").mkdir(parents=True)
    parquet(
        merged / Leaf("extra", "20260101", "00").relative / "out.parquet",
        "SELECT 1 AS x",
    )
    assert main(args("verify", source, merged)) == 1
    check = json.loads(capsys.readouterr().out)["partitions"][0]
    assert check["source_rows"] == 0
    assert check["merged_rows"] == 1


def test_empty_selection_is_explicit(tmp_path, capsys):
    source, merged = tmp_path / "source", tmp_path / "merged"
    (source / "raw_sensor").mkdir(parents=True)
    (merged / "raw_sensor").mkdir(parents=True)
    assert main(args("verify", source, merged, "absent")) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "empty"


def test_invalid_parameters_fail_without_backend_details(tmp_path, capsys):
    assert main(args("plan", tmp_path, tmp_path)) == 2
    assert "overlap" in json.loads(capsys.readouterr().err)["message"]


def test_backend_errors_do_not_leak_credentials(tmp_path, capsys, monkeypatch):
    def fail(*args, **kwargs):
        raise RuntimeError("signed_url=secret TOKEN=supersecret")

    monkeypatch.setattr("raw_sensor_compact.cli.open_s3", fail)
    assert main(args("plan", "s3://bucket/source", "s3://bucket/merged")) == 2
    output = capsys.readouterr().err
    assert "supersecret" not in output
    assert "suppressed" in output
