"""Bound local CLI invocation receipts; successful execution still cannot reopen."""

import json
import sys
from pathlib import Path

import pytest

from .test_coordination_ledger import HOUR, NOW

PROJECT = Path(__file__).resolve().parents[1] / "extras/raw-sensor-compact"


def binding(tmp_path):
    from wintappy.coordination.protocol import dataset_binding

    return dataset_binding(
        "synthetic-dataset",
        {
            "raw": str(tmp_path / "raw_sensor"),
            "compacted": str(tmp_path / "compacted"),
        },
    )


def initialize(tmp_path):
    from wintappy.coordination.ledger import Ledger

    Ledger.initialize(tmp_path / "state", binding(tmp_path), HOUR)


def config(tmp_path):
    return {
        "executable": str(PROJECT / ".venv/bin/python"),
        "project": str(PROJECT),
        "event_types": ["raw_process"],
        "target_size_bytes": 134217728,
        "temp_directory": str(tmp_path / "spill"),
        "run_evidence_directory": str(tmp_path / "phase-evidence"),
        "memory_limit": "128MB",
        "max_temp_size": "256MB",
        "timeout": 10,
        "supervision_timeout_seconds": 20,
        "stop_grace_seconds": 0.1,
        "shutdown_seconds": 2,
    }


def prepare(tmp_path, **overrides):
    from wintappy.coordination.execution import prepare_execution

    initialize(tmp_path)
    values = config(tmp_path)
    values.update(overrides)
    return prepare_execution(tmp_path / "state", binding(tmp_path), HOUR, NOW, values)


def test_resolved_receipt_binds_explicit_cli_defaults_and_inactive_storage(tmp_path):
    from wintappy.coordination.execution import load_execution

    prepared = prepare(tmp_path)
    receipt = load_execution(prepared.path, prepared.sha256)
    assert receipt == prepared.receipt
    assert receipt["binding"] == binding(tmp_path)
    assert receipt["runtime"]["duckdb_version"] == "2.0.0"
    assert receipt["runtime"]["executable"] == str(PROJECT / ".venv/bin/python")
    assert receipt["settings"]["max_hours"] == 1
    assert receipt["settings"]["threads"] == 2
    assert receipt["settings"]["delete_source"] is True
    assert receipt["settings"]["entity"] is None
    assert receipt["settings"]["storage"] is None
    assert "--delete-source" in receipt["command"]
    assert "--process-entity-root" not in receipt["command"]
    assert receipt["run_id"] != receipt["cli_run_id"]
    assert receipt["hour_end"] == "2026-01-02T00:00:00Z"


@pytest.mark.parametrize(
    "override",
    [
        {"extra": "synthetic-secret"},
        {"event_types": []},
        {"event_types": ["../wrong"]},
        {"threads": True},
        {"target_size_bytes": 0},
        {"memory_limit": "unbounded"},
        {"timeout": float("nan")},
        {"max_log_bytes": True},
        {"max_report_bytes": 100 * 1024 * 1024},
        {"supervision_timeout_seconds": 5},
        {"executable": sys.executable},
    ],
)
def test_invalid_effective_configuration_cannot_create_intent(tmp_path, override):
    from wintappy.coordination.execution import prepare_execution
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError

    initialize(tmp_path)
    with pytest.raises(CoordinationError):
        prepare_execution(
            tmp_path / "state",
            binding(tmp_path),
            HOUR,
            NOW,
            {**config(tmp_path), **override},
        )
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.snapshot()["mode"] == "OPEN"
        assert (
            ledger._connection.execute(
                "select count(*) from compaction_runs"
            ).fetchone()[0]
            == 0
        )


@pytest.mark.parametrize("damage", ["missing", "changed", "oversized", "duplicate"])
def test_receipt_damage_refuses_launch_before_writer_intent(
    tmp_path, damage, monkeypatch
):
    from wintappy.coordination.execution import MAX_EXECUTION_BYTES
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError
    from wintappy.coordination.writer import execute_prepared

    prepared = prepare(tmp_path)
    if damage == "missing":
        prepared.path.unlink()
    elif damage == "changed":
        prepared.path.write_bytes(prepared.path.read_bytes() + b" ")
    elif damage == "oversized":
        prepared.path.write_bytes(b" " * (MAX_EXECUTION_BYTES + 1))
    else:
        original = prepared.path.read_text()
        prepared.path.write_text('{"version":1,' + original[1:])
    monkeypatch.setattr(
        "wintappy.coordination.writer.run_supervised",
        lambda *a, **kw: pytest.fail("invalid receipt launched"),
    )
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        with pytest.raises(CoordinationError):
            execute_prepared(ledger, prepared, NOW)
        assert ledger.snapshot()["mode"] == "DRAINING"
        assert (
            ledger._connection.execute(
                "select count(*) from compaction_runs"
            ).fetchone()[0]
            == 0
        )


@pytest.mark.parametrize("mutation", ["command", "binding", "runtime"])
def test_rechecks_actual_invocation_and_qualified_runtime_before_launch(
    tmp_path, monkeypatch, mutation
):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError
    from wintappy.coordination.writer import execute_prepared

    prepared = prepare(tmp_path)
    if mutation == "runtime":
        monkeypatch.setattr(
            "wintappy.coordination.execution.runtime_identity",
            lambda *a: {"duckdb_version": "wrong"},
        )
    elif mutation == "command":
        prepared = prepared._replace(command=(sys.executable, "-c", "pass"))
    else:
        prepared = prepared._replace(
            binding={**binding(tmp_path), "dataset_id": "other"}
        )
    monkeypatch.setattr(
        "wintappy.coordination.writer.run_supervised",
        lambda *a, **kw: pytest.fail("mismatched invocation launched"),
    )
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        with pytest.raises(CoordinationError):
            execute_prepared(ledger, prepared, NOW)
        assert ledger.snapshot()["mode"] == "DRAINING"


@pytest.mark.skipif(sys.platform != "linux", reason="Linux writer subreaper")
def test_real_bound_empty_compactor_has_receipt_before_launch_but_still_blocks(
    tmp_path,
):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.writer import execute_prepared

    prepared = prepare(tmp_path)
    roots = binding(tmp_path)["roots"]
    Path(roots["raw"]).mkdir(parents=True)
    Path(roots["compacted"]).mkdir(parents=True)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        result = execute_prepared(ledger, prepared, NOW)
        assert result["status"] == "empty"
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.snapshot()["next_hour"] == HOUR
        intent = json.loads(
            ledger._connection.execute(
                "select intent_json from compaction_runs"
            ).fetchone()[0]
        )
        assert intent["execution"]["sha256"] == prepared.sha256
        assert intent["execution"]["receipt"] == prepared.receipt
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.snapshot()["mode"] == "BLOCKED"


@pytest.mark.parametrize("phase", ["after_intent", "after_launch"])
def test_post_intent_receipt_uncertainty_blocks_without_cursor_advance(
    tmp_path, monkeypatch, phase
):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError
    from wintappy.coordination.writer import execute_prepared

    prepared = prepare(tmp_path)
    if phase == "after_intent":
        begin = Ledger.begin_writer

        def changed(self, *args, **kwargs):
            run = begin(self, *args, **kwargs)
            prepared.path.unlink()
            return run

        monkeypatch.setattr(Ledger, "begin_writer", changed)
        monkeypatch.setattr(
            "wintappy.coordination.writer.run_supervised",
            lambda *a, **kw: pytest.fail("changed post-intent receipt launched"),
        )
    else:

        def changed(*args, **kwargs):
            prepared.path.unlink()
            return {"status": "exited", "exit_code": 0, "children_stopped": True}

        monkeypatch.setattr("wintappy.coordination.writer.run_supervised", changed)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        with pytest.raises(CoordinationError):
            execute_prepared(ledger, prepared, NOW)
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.snapshot()["next_hour"] == HOUR


def test_synthetic_environment_secrets_are_not_persisted_or_scope_inputs(
    tmp_path, monkeypatch
):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.writer import execute_prepared

    secret = "SYNTHETIC-SECRET-DO-NOT-PERSIST"
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", secret)
    monkeypatch.setenv("WINTAP_DBT_RAW_SENSOR_DATASET", secret)
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://synthetic.invalid")
    prepared = prepare(tmp_path)
    assert secret not in prepared.path.read_text()
    assert secret not in repr(prepared)
    assert prepared.receipt["settings"]["storage"] is None
    seen = []

    def observed(command, directory, **kwargs):
        seen.append(kwargs["env"])
        assert secret not in command
        return {"status": "exited", "exit_code": 0, "children_stopped": True}

    monkeypatch.setattr("wintappy.coordination.writer.run_supervised", observed)
    monkeypatch.setattr(
        "wintappy.coordination.writer.read_report", lambda *a, **kw: {"status": "empty"}
    )
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        execute_prepared(ledger, prepared, NOW)
        rows = ledger._connection.execute(
            "select intent_json from compaction_runs union all select details_json from events"
        ).fetchall()
        assert all(secret not in row[0] for row in rows)
    assert len(seen) == 1
    assert "WINTAP_DBT_RAW_SENSOR_DATASET" not in seen[0]
    assert "AWS_ENDPOINT_URL" not in seen[0]


@pytest.mark.parametrize(
    "override",
    [
        {"memory_limit": "33GB"},
        {"max_temp_size": "65GB"},
        {"run_evidence_directory": "state"},
    ],
)
def test_storage_ceiling_and_archive_ledger_separation_refuse_configuration(
    tmp_path, override
):
    from wintappy.coordination.protocol import CoordinationError

    if override.get("run_evidence_directory") == "state":
        override = {"run_evidence_directory": str(tmp_path / "state")}
    with pytest.raises(CoordinationError):
        prepare(tmp_path, **override)


def test_lost_writer_intent_commit_acknowledgment_is_blocked_in_same_session(
    tmp_path, monkeypatch
):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError
    from wintappy.coordination.writer import execute_prepared

    prepared = prepare(tmp_path)
    monkeypatch.setattr(
        "wintappy.coordination.writer.run_supervised",
        lambda *a, **kw: pytest.fail("uncertain COMMIT launched"),
    )
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        observed = []

        def failure(phase):
            if phase == "after_commit" and not observed:
                observed.append(phase)
                raise OSError("synthetic lost commit acknowledgment")

        ledger.failure_hook = failure
        with pytest.raises(CoordinationError):
            execute_prepared(ledger, prepared, NOW)
        assert observed == ["after_commit"]
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.snapshot()["next_hour"] == HOUR


def test_launch_exception_does_not_disclose_ephemeral_synthetic_secret(
    tmp_path, monkeypatch
):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.writer import execute_prepared

    prepared = prepare(tmp_path)
    secret = "SYNTHETIC-LAUNCH-SECRET-NEVER-DISCLOSE"

    def fail(*args, **kwargs):
        raise OSError(secret)

    monkeypatch.setattr("wintappy.coordination.writer.run_supervised", fail)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        with pytest.raises(Exception) as error:
            execute_prepared(ledger, prepared, NOW)
        assert secret not in str(error.value)
        assert ledger.snapshot()["mode"] == "BLOCKED"
        rows = ledger._connection.execute("select details_json from events").fetchall()
        assert all(secret not in row[0] for row in rows)


@pytest.mark.parametrize("source", ["override", "environment", "profile"])
def test_remote_scope_is_sdk_resolved_once_without_endpoint_access(
    tmp_path, monkeypatch, source
):
    from types import SimpleNamespace

    from wintappy.coordination.execution import resolve_storage
    from wintappy.coordination.protocol import dataset_binding

    secret = "SYNTHETIC-FROZEN-CREDENTIAL"
    config = {}
    if source == "override":
        config = {
            "profile": "synthetic-profile",
            "region": "synthetic-region",
            "endpoint_url": "https://override.invalid",
        }
    elif source == "environment":
        monkeypatch.setenv("AWS_PROFILE", "synthetic-profile")
        monkeypatch.setenv("AWS_REGION", "synthetic-region")
        monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "https://environment.invalid")
        monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    else:
        for key in (
            "AWS_PROFILE",
            "AWS_DEFAULT_PROFILE",
            "AWS_REGION",
            "AWS_DEFAULT_REGION",
            "AWS_ENDPOINT_URL",
            "AWS_ENDPOINT_URL_S3",
        ):
            monkeypatch.delenv(key, raising=False)
    calls = []

    class Session:
        region_name = "profile-region"

        def __init__(self, **kwargs):
            calls.append(kwargs)

        def get_credentials(self):
            return SimpleNamespace(
                get_frozen_credentials=lambda: SimpleNamespace(
                    access_key="synthetic-key",
                    secret_key=secret,
                    token="synthetic-token",
                )
            )

        def client(self, name, **kwargs):
            assert name == "s3" and kwargs["aws_secret_access_key"] == secret
            endpoint = kwargs["endpoint_url"] or "https://profile.invalid"
            return SimpleNamespace(meta=SimpleNamespace(endpoint_url=endpoint))

    remote = dataset_binding(
        "synthetic-remote",
        {
            "raw": "s3://synthetic-bucket/raw_sensor",
            "compacted": str(tmp_path / "compacted"),
        },
    )
    settings, credentials = resolve_storage(remote, config, session_factory=Session)
    assert len(calls) == 1
    assert settings["endpoint_url"] == "https://" + source + ".invalid"
    assert settings["region"] == (
        "profile-region" if source == "profile" else "synthetic-region"
    )
    assert secret not in json.dumps(settings)
    assert credentials["AWS_SECRET_ACCESS_KEY"] == secret


@pytest.mark.parametrize("kind", ["fifo", "hardlink"])
def test_receipt_requires_one_regular_file_without_blocking_io(
    tmp_path, monkeypatch, kind
):
    import os

    from wintappy.coordination import execution
    from wintappy.coordination.protocol import CoordinationError

    prepared = prepare(tmp_path)
    if kind == "fifo":
        prepared.path.unlink()
        os.mkfifo(prepared.path)
    else:
        os.link(prepared.path, prepared.path.with_name("alias.json"))
    monkeypatch.setattr(
        execution, "_read_bytes", lambda *a: pytest.fail("unsafe receipt opened")
    )
    with pytest.raises(CoordinationError):
        execution.load_execution(prepared.path, prepared.sha256)


def test_compiled_invocation_arg_budget_is_checked_before_receipt_publication(tmp_path):
    from wintappy.coordination.protocol import CoordinationError

    with pytest.raises(CoordinationError):
        prepare(tmp_path, event_types=[f"event_{index:03d}" for index in range(128)])


def test_entity_receipt_binds_all_four_roots_and_active_identity_policy(tmp_path):
    from wintappy.coordination.execution import prepare_execution
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import dataset_binding

    bound = dataset_binding(
        "synthetic-entities",
        {
            **binding(tmp_path)["roots"],
            "entity": str(tmp_path / "entities"),
            "metrics": str(tmp_path / "metrics"),
        },
        {
            "namespace": "synthetic-namespace",
            "hosts": ["SYNTHETIC-HOST"],
            "identity_policy": "quarantine",
            "identity_max_ratio": 0.025,
        },
    )
    Ledger.initialize(tmp_path / "state", bound, HOUR)
    prepared = prepare_execution(tmp_path / "state", bound, HOUR, NOW, config(tmp_path))
    receipt = prepared.receipt
    command = receipt["command"]
    for key, root in (
        ("process-entity-root", "entity"),
        ("processing-metrics-root", "metrics"),
    ):
        assert command[command.index("--" + key) + 1] == bound["roots"][root]
    assert command[command.index("--unknown-identity-policy") + 1] == "quarantine"
    assert command[command.index("--unknown-identity-max-ratio") + 1] == "0.025"
    assert receipt["settings"]["entity"] == bound["entity"]


@pytest.mark.skipif(sys.platform != "linux", reason="Linux writer subreaper")
def test_real_local_migration_keeps_original_phase_evidence_and_blocks(tmp_path):
    import subprocess

    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.writer import execute_prepared

    prepared = prepare(tmp_path)
    raw = (
        Path(binding(tmp_path)["roots"]["raw"])
        / "raw_process/dayPK=20260101/hourPK=23/part.parquet"
    )
    raw.parent.mkdir(parents=True)
    Path(binding(tmp_path)["roots"]["compacted"]).mkdir()
    subprocess.run(
        [
            str(PROJECT / ".venv/bin/python"),
            "-I",
            "-B",
            "-c",
            'import duckdb,sys; c=duckdb.connect(); c.execute("copy (select 1 as synthetic_value) to ? (format parquet)",[sys.argv[1]])',
            str(raw),
        ],
        check=True,
        env={},
        timeout=20,
    )
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        report = execute_prepared(ledger, prepared, NOW)
        assert report["status"] == "completed"
        assert report["phase_evidence"]["migration_complete"] is True
        assert not raw.exists()
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.snapshot()["next_hour"] == HOUR
    assert list((tmp_path / "compacted").glob("**/*.parquet"))
    assert list(
        (tmp_path / "phase-evidence" / prepared.receipt["cli_run_id"]).glob("*.json")
    )


def test_credential_env_injection_cannot_change_scope_or_create_intent(
    tmp_path, monkeypatch
):
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import CoordinationError
    from wintappy.coordination.writer import execute_prepared

    prepared = prepare(tmp_path)._replace(
        credentials={"PYTHONPATH": "synthetic-injection"}
    )
    monkeypatch.setattr(
        "wintappy.coordination.writer.run_supervised",
        lambda *a, **kw: pytest.fail("environment injection launched"),
    )
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        with pytest.raises(CoordinationError):
            execute_prepared(ledger, prepared, NOW)
        assert ledger.snapshot()["mode"] == "DRAINING"


@pytest.mark.parametrize(
    "damage",
    [
        "duplicate",
        "nonfinite",
        "noncanonical",
        "unknown",
        "boolean-version",
        "boolean-budget",
    ],
)
def test_receipt_decoder_rejects_invalid_bytes_even_with_matching_checksum(
    tmp_path, damage
):
    import hashlib

    from wintappy.coordination.execution import load_execution
    from wintappy.coordination.protocol import CoordinationError, canonical

    prepared = prepare(tmp_path)
    receipt = json.loads(prepared.path.read_bytes())
    if damage == "duplicate":
        payload = ('{"version":1,' + canonical(receipt)[1:]).encode()
    elif damage == "nonfinite":
        receipt["settings"]["timeout"] = float("nan")
        payload = json.dumps(receipt).encode()
    elif damage == "noncanonical":
        payload = json.dumps(receipt, indent=2).encode()
    else:
        if damage == "unknown":
            receipt["unknown"] = "synthetic-unused"
        elif damage == "boolean-version":
            receipt["version"] = True
        else:
            receipt["settings"]["threads"] = True
        payload = canonical(receipt).encode()
    prepared.path.write_bytes(payload)
    with pytest.raises(CoordinationError):
        load_execution(prepared.path, hashlib.sha256(payload).hexdigest())


def test_remote_prepared_scope_and_frozen_credentials_survive_environment_change(
    tmp_path, monkeypatch
):
    import hashlib
    from types import SimpleNamespace

    from wintappy.coordination import execution
    from wintappy.coordination.ledger import Ledger
    from wintappy.coordination.protocol import dataset_binding
    from wintappy.coordination.writer import execute_prepared

    secret = "SYNTHETIC-REMOTE-SECRET-DO-NOT-CAPTURE"

    class Session:
        region_name = "synthetic-region"

        def __init__(self, **kwargs):
            pass

        def get_credentials(self):
            return SimpleNamespace(
                get_frozen_credentials=lambda: SimpleNamespace(
                    access_key="synthetic-key",
                    secret_key=secret,
                    token="synthetic-token",
                )
            )

        def client(self, *args, **kwargs):
            return SimpleNamespace(
                meta=SimpleNamespace(endpoint_url="https://resolved.invalid")
            )

    for key in (
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
    ):
        monkeypatch.delenv(key, raising=False)
    bound = dataset_binding(
        "synthetic-remote",
        {
            "raw": "s3://synthetic-bucket/raw_sensor",
            "compacted": str(tmp_path / "compacted"),
        },
    )
    Ledger.initialize(tmp_path / "state", bound, HOUR)
    monkeypatch.setattr(execution.boto3, "Session", Session)
    prepared = execution.prepare_execution(
        tmp_path / "state", bound, HOUR, NOW, config(tmp_path)
    )
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://changed.invalid")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "changed-region")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "SYNTHETIC-CHANGED-SECRET")
    monkeypatch.setenv("PYTHONPATH", "synthetic-injection")
    seen = []

    def observed(command, directory, **kwargs):
        seen.append(command)
        assert (
            command[command.index("--endpoint-url") + 1] == "https://resolved.invalid"
        )
        assert command[command.index("--region") + 1] == "synthetic-region"
        assert "--profile" not in command
        assert kwargs["env"]["AWS_SECRET_ACCESS_KEY"] == secret
        assert "AWS_ENDPOINT_URL" not in kwargs["env"]
        assert "AWS_DEFAULT_REGION" not in kwargs["env"]
        assert "PYTHONPATH" not in kwargs["env"]
        return {"status": "exited", "exit_code": 0, "children_stopped": True}

    monkeypatch.setattr("wintappy.coordination.writer.run_supervised", observed)
    monkeypatch.setattr(
        "wintappy.coordination.writer.read_report",
        lambda *a, **kw: {"status": "completed"},
    )
    with Ledger.open(tmp_path / "state", bound) as ledger:
        ledger.drain(NOW)
        execute_prepared(ledger, prepared, NOW)
        rows = ledger._connection.execute(
            "select intent_json from compaction_runs union all select details_json from events"
        ).fetchall()
        for row in rows:
            assert secret not in row[0]
            assert hashlib.sha256(secret.encode()).hexdigest() not in row[0]
    assert len(seen) == 1 and secret not in repr(prepared)
    assert prepared.receipt["settings"]["storage"]["extension_directory"] == str(
        tmp_path / "state/extensions"
    )
    assert all(
        secret.encode() not in p.read_bytes()
        for p in (tmp_path / "state/executions").glob("**/*.json")
    )
