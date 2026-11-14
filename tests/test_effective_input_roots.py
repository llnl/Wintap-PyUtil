"""Real DBT routing consults only roots active in the selected entity mode."""

import subprocess
import sys
from pathlib import Path

import pytest

from .test_compacted_inputs import seed_fixture
from .test_dbt_source_validation import CONFIG, ROOT, operation, parquet_file
from .test_dbt_source_validation import env as env_fixture

env = env_fixture


def offline_credentials(env, tmp_path):
    env.update(
        AWS_PROFILE="missing-synthetic-profile",
        AWS_CONFIG_FILE=str(tmp_path / "absent-config"),
        AWS_SHARED_CREDENTIALS_FILE=str(tmp_path / "absent-credentials"),
        AWS_EC2_METADATA_DISABLED="true",
        DBT_TARGET_PATH=str(tmp_path / "target"),
        DBT_LOG_PATH=str(tmp_path / "logs"),
    )


def test_unused_remote_metrics_override_is_local_preflight(env, tmp_path):
    parquet_file(tmp_path)
    offline_credentials(env, tmp_path)
    env["WINTAP_DBT_PROCESSING_METRICS_ROOT"] = "s3://synthetic-bucket/unused-metrics"
    result = operation(env, tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Raw source check passed" in result.stdout


def test_make_full_local_build_ignores_unused_remote_metrics(env, tmp_path):
    seed_fixture(tmp_path / "input/raw_sensor")
    env.update(WINTAP_DBT_START_DAY="20260101", WINTAP_DBT_END_DAY="20260102")
    offline_credentials(env, tmp_path)
    env["WINTAP_DBT_PROCESSING_METRICS_ROOT"] = "s3://synthetic-bucket/unused-metrics"
    result = subprocess.run(
        ["make", "dbt-build", f"DBT={sys.executable} -m dbt.cli.main"],
        cwd=ROOT,
        check=False,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert Path(env["WINTAP_DBT_DATABASE"]).is_file()
    assert not (tmp_path / "input/compacted").exists()
    assert not (tmp_path / "input/process_entities").exists()
    assert "raw-sensor-compact" not in result.stdout
    assert "coordination" not in result.stdout.lower()


@pytest.mark.parametrize("remote", ["raw", "compacted", "entity", "metrics"])
def test_each_active_remote_root_selects_s3_configuration(env, tmp_path, remote):
    offline_credentials(env, tmp_path)
    Path(env["WINTAP_DBT_DATABASE"]).parent.mkdir(parents=True)
    if remote in {"entity", "metrics"}:
        env.update(
            WINTAP_DBT_PROCESS_ENTITY_ROOT=str(tmp_path / "entities"),
            WINTAP_DBT_PROCESS_ENTITY_HOSTS="synthetic-host",
            WINTAP_DBT_PROCESS_ENTITY_NAMESPACE="synthetic-pid-v2",
        )
    variable = {
        "raw": "WINTAP_DBT_RAW_SENSOR_DATASET",
        "compacted": "WINTAP_DBT_COMPACTED_ROOT",
        "entity": "WINTAP_DBT_PROCESS_ENTITY_ROOT",
        "metrics": "WINTAP_DBT_PROCESSING_METRICS_ROOT",
    }[remote]
    env[variable] = f"s3://synthetic-bucket/{remote}"
    # This operation deliberately needs no inventory/entity bootstrap: it exercises
    # the active secret-configuration path and fails locally at missing credentials.
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "dbt.cli.main",
            "run-operation",
            "configure_raw_sensor_access",
            "--project-dir",
            str(ROOT / "wintap_dbt"),
            "--profiles-dir",
            str(ROOT / "wintap_dbt"),
        ],
        cwd=ROOT,
        check=False,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0
    assert "secret" in (result.stdout + result.stderr).lower()


def test_active_roots_resolver_ignores_disabled_metrics_and_binds_defaults(env):
    env["WINTAP_DBT_PROCESSING_METRICS_ROOT"] = "s3://synthetic-bucket/unused"
    roots = CONFIG.effective_input_roots(env)
    assert roots == {
        "raw": env["WINTAP_DBT_RAW_SENSOR_DATASET"] + "/raw_sensor",
        "compacted": env["WINTAP_DBT_RAW_SENSOR_DATASET"] + "/compacted",
    }
    env["WINTAP_DBT_PROCESS_ENTITY_ROOT"] = "/synthetic/entities"
    assert (
        CONFIG.effective_input_roots(env)["metrics"]
        == env["WINTAP_DBT_PROCESSING_METRICS_ROOT"]
    )
    env["WINTAP_DBT_PROCESSING_METRICS_ROOT"] = ""
    assert (
        CONFIG.effective_input_roots(env)["metrics"]
        == env["WINTAP_DBT_RAW_SENSOR_DATASET"] + "/processing_metrics"
    )
