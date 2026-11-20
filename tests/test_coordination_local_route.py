"""Ordinary local DBT actions do not even import coordination runtime dependencies."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "action,calls",
    [
        ("dbt-build", 2),
        ("dbt-source", 1),
        ("dbt-test", 1),
        ("dbt-docs", 1),
        ("dbt-debug", 1),
    ],
)
def test_local_action_without_sdk_ledger_or_compactor_import(tmp_path, action, calls):
    fake_dbt = tmp_path / "ordinary-dbt.py"
    output = tmp_path / "calls.jsonl"
    fake_dbt.write_text(
        "import json,pathlib,sys\nwith pathlib.Path(sys.argv[1]).open('a') as f: f.write(json.dumps(sys.argv[2:])+'\\n')\n"
    )
    script = """
import importlib.abc,runpy,sys
class OfflineOnly(importlib.abc.MetaPathFinder):
    def find_spec(self,fullname,path=None,target=None):
        if fullname.split('.')[0] in {'boto3','duckdb','raw_sensor_compact'} or fullname in {'wintappy.coordination.ledger','wintappy.coordination.execution','wintappy.coordination.service'}:
            raise AssertionError('Local route imported an inactive runtime dependency')
sys.meta_path.insert(0,OfflineOnly())
sys.argv=['client',*sys.argv[1:]]
runpy.run_module('wintappy.coordination.client',run_name='__main__')
"""
    env = {k: v for k, v in os.environ.items() if not k.startswith(("WINTAP_", "AWS_", "DUCKDB_"))}
    env.update(
        WINTAP_DBT_DATASET=str(tmp_path / "input"),
        WINTAP_DBT_PROCESSING_METRICS_ROOT="s3://synthetic-bucket/inactive",
        WINTAP_COORDINATION_ROOT="invalid-unused",
        WINTAP_COORDINATION_CLIENT_STATE=str(tmp_path / "never-created"),
        AWS_PROFILE="missing-synthetic-profile",
    )
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            script,
            action,
            "--dbt-command",
            f"{sys.executable} {fake_dbt} {output}",
        ],
        check=False,
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    recorded = [json.loads(line) for line in output.read_text().splitlines()]
    assert len(recorded) == calls
    if action == "dbt-build":
        assert recorded[0][:2] == ["run-operation", "validate_raw_sensor_source"]
        assert recorded[1][0] == "build"
    assert not (tmp_path / "never-created").exists()


def test_explicit_direct_s3_action_skips_coordination(tmp_path):
    fake_dbt = tmp_path / "ordinary-dbt.py"
    output = tmp_path / "calls.jsonl"
    fake_dbt.write_text(
        "import json,pathlib,sys\nwith pathlib.Path(sys.argv[1]).open('a') as f: f.write(json.dumps(sys.argv[2:])+'\\n')\n"
    )
    env = {k: v for k, v in os.environ.items() if not k.startswith(("WINTAP_", "AWS_", "DUCKDB_"))}
    env.update(
        WINTAP_DBT_RAW_SENSOR_DATASET="s3://synthetic-bucket/dataset",
        WINTAP_DBT_INCLUDE_COMPACTED="false",
        WINTAP_DBT_COORDINATION_MODE="direct",
        WINTAP_DBT_START_DAY="20261031",
        WINTAP_DBT_END_DAY="20261031",
        WINTAP_COORDINATION_ROOT="s3://synthetic-bucket/never-created",
        WINTAP_COORDINATION_CLIENT_STATE=str(tmp_path / "never-created"),
        AWS_PROFILE="missing-synthetic-profile",
    )
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            "import runpy,sys; sys.argv=['client',*sys.argv[1:]]; runpy.run_module('wintappy.coordination.client',run_name='__main__')",
            "dbt-build",
            "--dbt-command",
            f"{sys.executable} {fake_dbt} {output}",
        ],
        check=False,
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    recorded = [json.loads(line) for line in output.read_text().splitlines()]
    assert len(recorded) == 2
    assert recorded[0][:2] == ["run-operation", "validate_raw_sensor_source"]
    assert recorded[1][0] == "build"
    assert not (tmp_path / "never-created").exists()
