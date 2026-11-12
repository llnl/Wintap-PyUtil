"""Offline configuration and real DBT/DuckDB source-preflight regression tests."""

import importlib.util
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "check_dbt_config", ROOT / "scripts/check_dbt_config.py"
)
CONFIG = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CONFIG)


@pytest.fixture
def env(tmp_path):
    values = {
        k: v for k, v in os.environ.items() if not k.startswith(("WINTAP_", "AWS_", "DUCKDB_"))
    }
    values.update(
        WINTAP_DATA_ROOT=str(tmp_path / "output"),
        WINTAP_DBT_DATASET=str(tmp_path / "output"),
        WINTAP_DBT_DATABASE=str(tmp_path / "output/duckdb/wintap.duckdb"),
        WINTAP_DBT_RAW_SENSOR_DATASET=str(tmp_path / "input"),
        WINTAP_DBT_START_DAY="20261031",
        WINTAP_DBT_END_DAY="20261103",
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
    )
    (tmp_path / "input/raw_sensor").mkdir(parents=True)
    return values


def parquet_file(tmp_path):
    path = tmp_path / "input/raw_sensor/pidstat/dayPK=20261031/hourPK=09/sample.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect() as conn:
        conn.execute("copy (select 1 as pid) to ? (format parquet)", [str(path)])
    return path


def operation(env, tmp_path):
    CONFIG.validate(env)
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "--log-path",
            str(tmp_path / "logs"),
            "run-operation",
            "validate_raw_sensor_source",
            "--project-dir",
            str(ROOT / "wintap_dbt"),
            "--profiles-dir",
            str(ROOT / "wintap_dbt"),
            "--target-path",
            str(tmp_path / "target"),
        ],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=90,
    )


def test_local_split_input(env):
    CONFIG.validate(env)
    assert not (Path(env["WINTAP_DBT_DATASET"]) / "raw_sensor").exists()
    assert Path(env["WINTAP_DBT_DATABASE"]).parent.is_dir()


def test_local_fallback(env):
    env["WINTAP_DBT_DATASET"] = env.pop("WINTAP_DBT_RAW_SENSOR_DATASET")
    CONFIG.validate(env)


def test_missing_local_source(env):
    env["WINTAP_DBT_RAW_SENSOR_DATASET"] += "-missing"
    with pytest.raises(ValueError, match="Local raw source directory"):
        CONFIG.validate(env)


def test_s3_without_placeholder(env):
    env["WINTAP_DBT_RAW_SENSOR_DATASET"] = "s3://lintap-alpha/lintap"
    CONFIG.validate(env)
    assert not (Path(env["WINTAP_DBT_DATASET"]) / "raw_sensor").exists()


@pytest.mark.parametrize(
    "source",
    [
        "s3://",
        "s3:///prefix",
        "s3://user:secret@bucket/prefix",
        "s3://bucket:9000/prefix",
        "s3://bucket/prefix?x=1",
        "s3://bucket/*",
        "https://bucket/prefix",
        "file:///data",
    ],
)
def test_bad_source(env, source):
    env["WINTAP_DBT_RAW_SENSOR_DATASET"] = source
    with pytest.raises(ValueError):
        CONFIG.validate(env)


@pytest.mark.parametrize(
    "name,value",
    [
        ("START_DAY", ""),
        ("START_DAY", "20260230"),
        ("START_DAY", "20261104"),
        ("END_DAY", "2026-11-03"),
        ("START_HOUR", "abc"),
        ("START_HOUR", "24"),
        ("END_HOUR", "-1"),
    ],
)
def test_invalid_window(env, name, value):
    env[f"WINTAP_DBT_{name}"] = value
    with pytest.raises(ValueError):
        CONFIG.validate(env)


def test_reversed_hours(env):
    env.update(WINTAP_DBT_END_DAY="20261031", WINTAP_DBT_START_HOUR="12", WINTAP_DBT_END_HOUR="09")
    with pytest.raises(ValueError, match="End day/hour"):
        CONFIG.validate(env)


def test_output_only_is_independent_of_source_and_dates(env):
    path = Path(env["WINTAP_DBT_DATABASE"])
    path.parent.mkdir(parents=True)
    with duckdb.connect(str(path)):
        pass
    env.update(WINTAP_DBT_RAW_SENSOR_DATASET="s3://", WINTAP_DBT_START_DAY="")
    CONFIG.validate(env, output_only=True)


def test_missing_output(env):
    with pytest.raises(ValueError, match="run make dbt-build first"):
        CONFIG.validate(env, output_only=True)


def test_remote_database_rejected(env):
    env["WINTAP_DBT_DATABASE"] = "s3://bucket/output.duckdb"
    with pytest.raises(ValueError, match="local filesystem"):
        CONFIG.validate(env)


def test_make_local_autodiscovery(env, tmp_path):
    parquet_file(tmp_path)
    env.pop("WINTAP_DBT_START_DAY")
    env.pop("WINTAP_DBT_END_DAY")
    result = subprocess.run(
        ["make", "print-dbt-config"], cwd=ROOT, env=env, capture_output=True, text=True, timeout=90
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "WINTAP_DBT_START_DAY=20261031" in result.stdout
    assert "RAW_SOURCE_TYPE=local" in result.stdout


def test_make_s3_does_not_discover_local_dates(env, tmp_path):
    parquet_file(tmp_path)
    env["WINTAP_DBT_DATASET"] = env["WINTAP_DBT_RAW_SENSOR_DATASET"]
    env["WINTAP_DBT_RAW_SENSOR_DATASET"] = "s3://lintap-alpha/lintap"
    env.pop("WINTAP_DBT_START_DAY")
    env.pop("WINTAP_DBT_END_DAY")
    result = subprocess.run(
        ["make", "print-dbt-config"], cwd=ROOT, env=env, capture_output=True, text=True, timeout=90
    )
    assert result.returncode != 0
    assert "S3 requires explicit partition dates" in result.stderr


def test_make_s3_print_and_target_dependencies(env):
    env["WINTAP_DBT_RAW_SENSOR_DATASET"] = "s3://lintap-alpha/lintap"
    result = subprocess.run(
        ["make", "print-dbt-config"], cwd=ROOT, env=env, capture_output=True, text=True, timeout=90
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "RAW_SOURCE_TYPE=s3" in result.stdout
    assert "WINTAP_DBT_RAW_SENSOR_DATASET=s3://lintap-alpha/lintap" in result.stdout
    assert not (Path(env["WINTAP_DBT_DATASET"]) / "raw_sensor").exists()
    build = subprocess.run(
        ["make", "-n", "dbt-build"], cwd=ROOT, env=env, capture_output=True, text=True, timeout=10
    )
    assert build.returncode == 0, build.stderr
    assert build.stdout.index("run-operation validate_raw_sensor_source") < build.stdout.index(
        "dbt build"
    )
    dashboard = subprocess.run(
        ["make", "-n", "qa-dashboard"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert "--output-only" in dashboard.stdout
    assert "run-operation" not in dashboard.stdout


def test_unreadable_local_input(env, monkeypatch):
    original = os.access
    monkeypatch.setattr(
        CONFIG.os,
        "access",
        lambda path, mode: False if Path(path).name == "raw_sensor" else original(path, mode),
    )
    with pytest.raises(ValueError, match="not readable"):
        CONFIG.validate(env)


@pytest.mark.parametrize(
    "scenario", ["valid", "alias", "nested-proto", "empty", "outside-hour", "corrupt"]
)
def test_local_dbt_preflight(env, tmp_path, scenario):
    if scenario != "empty":
        path = parquet_file(tmp_path)
        if scenario == "corrupt":
            path.write_bytes(b"not parquet")
        elif scenario == "alias":
            dest = (
                tmp_path / "input/raw_sensor/raw_image_load/dayPK=20261031/hourPK=09/sample.parquet"
            )
            dest.parent.mkdir(parents=True)
            path.rename(dest)
        elif scenario == "nested-proto":
            dest = (
                tmp_path
                / "input/raw_sensor/raw_process_conn_incr/dayPK=20261031/hourPK=09/protoPK=tcp/sample.parquet"
            )
            dest.parent.mkdir(parents=True)
            path.rename(dest)
    if scenario == "outside-hour":
        env.update(
            WINTAP_DBT_START_HOUR="10", WINTAP_DBT_END_DAY="20261031", WINTAP_DBT_END_HOUR="11"
        )
    result = operation(env, tmp_path)
    output = result.stdout + result.stderr
    if scenario in ("valid", "alias", "nested-proto"):
        assert result.returncode == 0, output
        assert "Raw source check passed" in output
    else:
        assert result.returncode != 0, output
        assert ("No input found" if scenario != "corrupt" else "Parquet") in output


@pytest.mark.parametrize(
    "scenario",
    ["valid", "hour-window", "profile", "empty", "list-denied", "read-denied", "tls-error"],
)
def test_s3_dbt_preflight(env, tmp_path, scenario):
    """Real DuckDB httpfs against a private loopback S3 protocol fixture, not live S3."""
    data = parquet_file(tmp_path).read_bytes()
    key = "lintap/raw_sensor/pidstat/dayPK=20261031/hourPK=09/sample.parquet"
    requests = []
    authorization_headers = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def respond(self, status, body=b"", content_type="application/octet-stream"):
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Content-Type", content_type)
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def do_HEAD(self):
            requests.append((self.command, self.path))
            authorization_headers.append(self.headers.get("Authorization", ""))
            self.respond(403 if scenario == "read-denied" else 200, data)

        def do_GET(self):
            requests.append((self.command, self.path))
            authorization_headers.append(self.headers.get("Authorization", ""))
            query = parse_qs(urlsplit(self.path).query)
            if "list-type" in query:
                if scenario == "list-denied":
                    self.respond(
                        403, b"<Error><Code>AccessDenied</Code></Error>", "application/xml"
                    )
                    return
                prefix = query.get("prefix", [""])[0]
                contents = ""
                if scenario != "empty" and key.startswith(prefix):
                    contents = f"<Contents><Key>{key}</Key><Size>{len(data)}</Size><ETag>fixture</ETag></Contents>"
                body = f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>lintap-alpha</Name><Prefix>{prefix}</Prefix><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>'
                self.respond(200, body.encode(), "application/xml")
            elif scenario == "read-denied":
                self.respond(403, b"<Error><Code>AccessDenied</Code></Error>")
            else:
                byte_range = self.headers.get("Range")
                if byte_range:
                    start, end = byte_range.removeprefix("bytes=").split("-")
                    start, end = int(start), int(end) if end else len(data) - 1
                    chunk = data[start : end + 1]
                    self.send_response(206)
                    self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
                    self.send_header("Content-Length", str(len(chunk)))
                    self.end_headers()
                    self.wfile.write(chunk)
                else:
                    self.respond(200, data)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    env.update(
        WINTAP_DBT_RAW_SENSOR_DATASET="s3://lintap-alpha/lintap",
        AWS_ENDPOINT_URL=f"http://127.0.0.1:{server.server_port}",
        AWS_DEFAULT_REGION="garage",
        DUCKDB_USE_SSL="false",
        AWS_EC2_METADATA_DISABLED="true",
        AWS_CONFIG_FILE=str(tmp_path / "no-aws-config"),
        AWS_SHARED_CREDENTIALS_FILE=str(tmp_path / "no-aws-credentials"),
    )
    if scenario == "profile":
        config_file = tmp_path / "aws-config"
        config_file.write_text("[profile lintap-alpha]\nregion = garage\n")
        credentials_file = tmp_path / "aws-credentials"
        credentials_file.write_text(
            "[default]\n"
            "aws_access_key_id = wrong-default-key\n"
            "aws_secret_access_key = wrong-default-secret\n\n"
            "[lintap-alpha]\n"
            "aws_access_key_id = profile-key\n"
            "aws_secret_access_key = profile-secret\n"
        )
        env.update(
            AWS_PROFILE="lintap-alpha",
            AWS_CONFIG_FILE=str(config_file),
            AWS_SHARED_CREDENTIALS_FILE=str(credentials_file),
        )
    else:
        env.update(AWS_ACCESS_KEY_ID="fixture-key", AWS_SECRET_ACCESS_KEY="fixture-secret")
    if scenario == "hour-window":
        env.update(
            WINTAP_DBT_START_HOUR="09", WINTAP_DBT_END_DAY="20261031", WINTAP_DBT_END_HOUR="09"
        )
    elif scenario == "tls-error":
        env["DUCKDB_USE_SSL"] = "true"
    try:
        result = operation(env, tmp_path)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    output = result.stdout + result.stderr
    if scenario == "tls-error":
        assert result.returncode != 0, output
        assert "No input found" not in output
        assert "SSL" in output or "HTTP" in output, output
        return
    assert requests, output
    for _, path in requests:
        query = parse_qs(urlsplit(path).query)
        if "list-type" in query:
            assert "/dayPK=" in query["prefix"][0]
            if scenario == "hour-window":
                assert "/hourPK=09/" in query["prefix"][0]
    if scenario in ("valid", "hour-window", "profile"):
        assert result.returncode == 0, output
        assert "Raw source check passed" in output
        assert any("/lintap-alpha/" in path for _, path in requests)
        if scenario == "profile":
            assert any("Credential=profile-key/" in header for header in authorization_headers)
    else:
        assert result.returncode != 0, output
        assert ("No input found" if scenario == "empty" else "403") in output
        if scenario != "empty":
            assert "No input found" not in output
