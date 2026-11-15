"""Durable client identity and Linux tree lifetime, with no data before grants."""

import json
import os
import sys
from pathlib import Path

import pytest

from wintappy.coordination.protocol import (
    CoordinationError,
    canonical,
    make_grant,
    make_request,
)

from .test_coordination_execution import binding
from .test_coordination_ledger import HOUR
from .test_coordination_tick import DUE, transport

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="Linux reader supervision"
)


def request(tmp_path):
    return make_request(binding(tmp_path), "a" * 32, "shared-command", HOUR, DUE, DUE)


def test_reader_reads_only_after_exact_grant_and_releases_failed_work(tmp_path):
    from wintappy.coordination.client import run_reader

    _, store = transport()
    req = request(tmp_path)
    sleeps = []

    def wait(seconds):
        assert not (tmp_path / "read-started").exists()
        sleeps.append(seconds)
        store.publish(make_grant(req, "b" * 32, DUE))

    command = [
        sys.executable,
        "-c",
        "import pathlib,sys; pathlib.Path(sys.argv[1]).touch(); sys.exit(7)",
        str(tmp_path / "read-started"),
    ]
    result = run_reader(
        tmp_path / "clients", req, store, command, clock=lambda: DUE, sleep=wait
    )
    assert sleeps == [10] and result["exit_code"] == 7
    assert store.read("release", req["job_id"])["exit_code"] == 7


def test_cancel_before_grant_is_durable_and_never_honors_late_permission(tmp_path):
    from wintappy.coordination.client import run_reader

    _, store = transport()
    req = request(tmp_path)

    def interrupted(seconds):
        store.publish(make_grant(req, "b" * 32, DUE))
        raise KeyboardInterrupt

    command = [sys.executable, "-c", "raise AssertionError('must not run')"]
    result = run_reader(
        tmp_path / "clients", req, store, command, clock=lambda: DUE, sleep=interrupted
    )
    assert result["exit_code"] == 130
    assert store.read("cancellation", req["job_id"])
    assert (
        run_reader(tmp_path / "clients", req, store, command, clock=lambda: DUE)[
            "exit_code"
        ]
        == 130
    )


def test_uncertain_release_retries_identical_bytes_without_rerunning(
    tmp_path, monkeypatch
):
    from wintappy.coordination.client import retry_terminal, run_reader

    _, store = transport()
    req = request(tmp_path)
    store.publish(make_grant(req, "b" * 32, DUE))
    original = store.publish
    attempts = []

    def lost(message):
        original(message)
        if message["kind"] == "release":
            attempts.append(json.dumps(message, sort_keys=True))
            raise CoordinationError("lost acknowledgment")

    monkeypatch.setattr(store, "publish", lost)
    result = run_reader(
        tmp_path / "clients",
        req,
        store,
        [sys.executable, "-c", "pass"],
        clock=lambda: DUE,
    )
    assert result["terminal_pending"] and result["exit_code"] == 0
    monkeypatch.setattr(store, "publish", original)
    retried = retry_terminal(tmp_path / "clients", req["job_id"], store)
    assert not retried["terminal_pending"]
    assert attempts == [
        json.dumps(store.read("release", req["job_id"]), sort_keys=True)
    ]


def test_uncertain_supervisor_does_not_publish_release_or_relaunch(
    tmp_path, monkeypatch
):
    from wintappy.coordination import client

    _, store = transport()
    req = request(tmp_path)
    store.publish(make_grant(req, "b" * 32, DUE))

    def uncertain(*args, **kwargs):
        raise CoordinationError("uncertain child tree")

    monkeypatch.setattr(client, "run_supervised", uncertain)
    command = [sys.executable, "-c", "pass"]
    with pytest.raises(CoordinationError):
        client.run_reader(tmp_path / "clients", req, store, command, clock=lambda: DUE)
    assert store.read("release", req["job_id"]) is None
    with pytest.raises(CoordinationError, match="recovery"):
        client.run_reader(tmp_path / "clients", req, store, command, clock=lambda: DUE)


def test_local_root_routing_ignores_unused_remote_settings(tmp_path):
    from wintappy.coordination.client import coordinated_inputs

    env = {
        "WINTAP_DBT_DATASET": str(tmp_path),
        "WINTAP_DBT_PROCESSING_METRICS_ROOT": "s3://synthetic-bucket/unused",
        "WINTAP_COORDINATION_ROOT": "invalid-unused",
    }
    assert coordinated_inputs(env) is False
    assert (
        coordinated_inputs(
            {**env, "WINTAP_DBT_COMPACTED_ROOT": "s3://synthetic-bucket/compacted"}
        )
        is True
    )


def test_terminal_retry_missing_identity_does_not_create_job(tmp_path):
    from wintappy.coordination.client import retry_terminal

    _, store = transport()
    with pytest.raises(CoordinationError):
        retry_terminal(tmp_path / "missing", "a" * 32, store)
    assert not (tmp_path / "missing").exists()


@pytest.mark.parametrize("mismatch", ["job_id", "request_sha256", "binding"])
def test_mismatched_grant_never_starts_reading(tmp_path, mismatch):
    from wintappy.coordination.client import run_reader

    _, store = transport()
    req = request(tmp_path)
    grant = make_grant(req, "b" * 32, DUE)
    if mismatch == "job_id":
        grant[mismatch] = "c" * 32
    elif mismatch == "request_sha256":
        grant[mismatch] = "c" * 64
    else:
        grant[mismatch] = {**grant[mismatch], "dataset_id": "wrong-dataset"}
    store.read = lambda kind, identity: grant
    with pytest.raises(CoordinationError):
        run_reader(
            tmp_path / "clients",
            req,
            store,
            [sys.executable, "-c", "pass"],
            clock=lambda: DUE,
        )
    assert not (tmp_path / "clients" / req["job_id"] / "launch.json").exists()


@pytest.mark.parametrize(
    "status,code",
    [
        ("timeout", 0),
        ("output_limit", 0),
        ("launch_failed", None),
        ("descendants_terminated", 0),
    ],
)
def test_shutdown_result_preserves_failure_and_safe_helper_cleanup(
    tmp_path, monkeypatch, status, code
):
    from wintappy.coordination import client

    _, store = transport()
    req = request(tmp_path)
    store.publish(make_grant(req, "b" * 32, DUE))
    original = client.run_supervised

    def observed(command, directory, **kwargs):
        result = original(command, directory, **kwargs)
        result.update(status=status, exit_code=code)
        if status == "output_limit":
            result["stdout"].update(bytes_seen=1, truncated=True)
        (directory / "supervision.json").write_text(canonical(result))
        return result

    monkeypatch.setattr(client, "run_supervised", observed)
    value = client.run_reader(
        tmp_path / "clients",
        req,
        store,
        [sys.executable, "-c", "pass"],
        clock=lambda: DUE,
    )
    assert value["exit_code"] == (0 if status == "descendants_terminated" else 1)


def test_dbt_command_parsing_accepts_options_after_action(tmp_path, monkeypatch):
    from wintappy.coordination import client

    monkeypatch.setenv("WINTAP_DBT_DATASET", str(tmp_path))
    calls = []
    monkeypatch.setattr(
        client,
        "_dbt_run",
        lambda action, executable, project: (
            calls.append((action, executable, project)) or 0
        ),
    )
    assert (
        client.main(
            [
                "dbt-build",
                "--dbt-command",
                "synthetic-dbt",
                "--dbt-project",
                "synthetic-project",
            ]
        )
        == 0
    )
    assert calls == [("dbt-build", ["synthetic-dbt"], "synthetic-project")]


@pytest.mark.parametrize("code", [130, 143])
def test_supervised_signal_authenticates_evidence_before_release(
    tmp_path, monkeypatch, code
):
    from wintappy.coordination import client

    _, store = transport()
    req = request(tmp_path)
    store.publish(make_grant(req, "b" * 32, DUE))
    original = client.run_supervised

    def interrupted(*args, **kwargs):
        original(*args, **kwargs)
        raise SystemExit(code)

    monkeypatch.setattr(client, "run_supervised", interrupted)
    result = client.run_reader(
        tmp_path / "clients",
        req,
        store,
        [sys.executable, "-c", "pass"],
        clock=lambda: DUE,
    )
    assert result["exit_code"] == code
    assert store.read("release", req["job_id"])["exit_code"] == code


@pytest.mark.parametrize("damage", ["missing", "command", "shutdown"])
def test_signal_with_missing_or_changed_shutdown_evidence_keeps_reservation(
    tmp_path, monkeypatch, damage
):
    from wintappy.coordination import client

    _, store = transport()
    req = request(tmp_path)
    store.publish(make_grant(req, "b" * 32, DUE))
    original = client.run_supervised

    def interrupted(command, directory, **kwargs):
        if damage != "missing":
            original(command, directory, **kwargs)
            path = directory / (
                "command.json" if damage == "command" else "supervision.json"
            )
            value = json.loads(path.read_text())
            if damage == "command":
                value["command"] = ["synthetic-wrong-command"]
            else:
                value["children_stopped"] = False
            path.write_text(json.dumps(value))
        raise SystemExit(143)

    monkeypatch.setattr(client, "run_supervised", interrupted)
    with pytest.raises(CoordinationError):
        client.run_reader(
            tmp_path / "clients",
            req,
            store,
            [sys.executable, "-c", "pass"],
            clock=lambda: DUE,
        )
    assert store.read("release", req["job_id"]) is None
    assert (tmp_path / "clients" / req["job_id"] / "launch.json").exists()


def test_unrelated_system_exit_never_becomes_release(tmp_path, monkeypatch):
    from wintappy.coordination import client

    _, store = transport()
    req = request(tmp_path)
    store.publish(make_grant(req, "b" * 32, DUE))

    def unrelated(*args, **kwargs):
        raise SystemExit(0)

    monkeypatch.setattr(client, "run_supervised", unrelated)
    with pytest.raises(SystemExit):
        client.run_reader(
            tmp_path / "clients",
            req,
            store,
            [sys.executable, "-c", "pass"],
            clock=lambda: DUE,
        )
    assert store.read("release", req["job_id"]) is None


def test_sigterm_before_grant_retains_cancel_outcome_for_identical_retry(tmp_path):
    from wintappy.coordination.client import retry_terminal, run_reader

    _, store = transport()
    req = request(tmp_path)

    def interrupted(seconds):
        raise SystemExit(143)

    result = run_reader(
        tmp_path / "clients",
        req,
        store,
        [sys.executable, "-c", "pass"],
        clock=lambda: DUE,
        sleep=interrupted,
    )
    assert result["exit_code"] == 143
    assert (
        retry_terminal(tmp_path / "clients", req["job_id"], store)["exit_code"] == 143
    )
    assert store.read("cancellation", req["job_id"])


@pytest.mark.parametrize("damage", ["symlink", "hardlink"])
def test_client_state_and_lock_reject_aliased_paths(tmp_path, damage):
    from wintappy.coordination.client import run_reader

    _, store = transport()
    req = request(tmp_path)
    store.publish(make_grant(req, "b" * 32, DUE))
    root = tmp_path / "clients"
    if damage == "symlink":
        target = tmp_path / "aliased"
        target.mkdir()
        root.symlink_to(target, target_is_directory=True)
    else:
        job = root / req["job_id"]
        job.mkdir(parents=True)
        (job / "client.lock").touch()
        os.link(job / "client.lock", tmp_path / "alias.lock")
    with pytest.raises(CoordinationError):
        run_reader(root, req, store, [sys.executable, "-c", "pass"], clock=lambda: DUE)
    assert not store.messages("request")


def test_retry_refuses_changed_transport_without_new_terminal_put(tmp_path):
    from wintappy.coordination.client import retry_terminal, run_reader

    _, store = transport()
    req = request(tmp_path)
    store.publish(make_grant(req, "b" * 32, DUE))
    run_reader(
        tmp_path / "clients",
        req,
        store,
        [sys.executable, "-c", "pass"],
        clock=lambda: DUE,
    )
    _, other = transport()
    other.root = "s3://synthetic-bucket/other-coordination"
    with pytest.raises(CoordinationError):
        retry_terminal(tmp_path / "clients", req["job_id"], other)
    assert not other.messages("release")


def test_uncertain_request_cli_names_original_job_without_secret(
    tmp_path, monkeypatch, capsys
):
    from wintappy.coordination import client, service

    _, store = transport()

    def lost(message):
        raise CoordinationError("synthetic-secret-details")

    monkeypatch.setattr(store, "publish", lost)
    monkeypatch.setattr(service, "open_message_store", lambda *args: store)
    for key, value in {
        "WINTAP_DBT_DATASET": str(tmp_path),
        "WINTAP_COORDINATION_DATASET_ID": "synthetic-dataset",
        "WINTAP_COORDINATION_ROOT": store.root,
        "WINTAP_COORDINATION_CLIENT_STATE": str(tmp_path / "clients"),
        "WINTAP_DBT_START_DAY": "20260101",
        "WINTAP_DBT_END_DAY": "20260101",
    }.items():
        monkeypatch.setenv(key, value)
    assert (
        client.main(
            ["shared-command", "--job-id", "a" * 32, "--", sys.executable, "-c", "pass"]
        )
        == 1
    )
    output = capsys.readouterr()
    assert "a" * 32 in output.err
    assert "synthetic-secret-details" not in output.err
    assert (tmp_path / "clients" / ("a" * 32) / "request.json").exists()


@pytest.mark.parametrize("collision", ["directory", "file", "symlink"])
def test_client_shared_parent_creation_interleaving(tmp_path, monkeypatch, collision):
    from wintappy.coordination.client import run_reader

    _, store = transport()
    req = request(tmp_path)
    store.publish(make_grant(req, "b" * 32, DUE))
    parent = tmp_path / "clients"
    original = Path.mkdir

    def concurrent(self, *args, **kwargs):
        if self == parent and not self.exists():
            if collision == "directory":
                original(self, mode=0o700)
            elif collision == "file":
                self.touch()
            else:
                target = tmp_path / "aliased-parent"
                original(target)
                self.symlink_to(target, target_is_directory=True)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", concurrent)
    if collision == "directory":
        assert (
            run_reader(
                parent, req, store, [sys.executable, "-c", "pass"], clock=lambda: DUE
            )["exit_code"]
            == 0
        )
    else:
        with pytest.raises(CoordinationError):
            run_reader(
                parent, req, store, [sys.executable, "-c", "pass"], clock=lambda: DUE
            )
        assert not store.messages("request")


def cli_environment(tmp_path, monkeypatch):
    for key in list(os.environ):
        if key.startswith(("WINTAP_", "AWS_", "DUCKDB_")):
            monkeypatch.delenv(key)
    _, store = transport()
    for key, value in {
        "WINTAP_DBT_DATASET": "s3://synthetic-bucket/data",
        "WINTAP_COORDINATION_DATASET_ID": "synthetic-dataset",
        "WINTAP_COORDINATION_ROOT": store.root,
        "WINTAP_COORDINATION_CLIENT_STATE": str(tmp_path / "clients"),
        "WINTAP_DBT_START_DAY": "20260101",
        "WINTAP_DBT_END_DAY": "20260101",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(
        "wintappy.coordination.service.open_message_store", lambda *args: store
    )
    return store


def test_cached_worker_entrypoint_cannot_dispatch_dbt_again(tmp_path, monkeypatch):
    from wintappy.coordination import client

    cli_environment(tmp_path, monkeypatch)
    req = make_request(
        client.binding_from_environment(os.environ),
        "a" * 32,
        "dbt-build",
        HOUR,
        DUE,
        DUE,
    )
    job = tmp_path / "clients" / req["job_id"]
    job.mkdir(parents=True)
    for name, value in {
        "request": req,
        "grant": make_grant(req, "b" * 32, DUE),
        "launch": {
            "version": 1,
            "request_sha256": req["request_sha256"],
            "grant_id": "b" * 32,
            "started_at": DUE,
        },
    }.items():
        (job / (name + ".json")).write_text(canonical(value))
    calls = []
    monkeypatch.setattr(client, "_dbt_run", lambda *args: calls.append(args) or 0)
    for _ in range(2):
        with pytest.raises(SystemExit) as refused:
            client.main(["dbt-build", "--worker-job-directory", str(job)])
        assert refused.value.code == 2
    assert not calls


def test_retained_job_refuses_changed_dbt_window_before_dispatch(tmp_path, monkeypatch):
    from wintappy.coordination import client

    cli_environment(tmp_path, monkeypatch)
    req = make_request(
        client.binding_from_environment(os.environ),
        "a" * 32,
        "dbt-build",
        "2026-01-01T00:00:00Z",
        "2026-01-02T00:00:00Z",
        DUE,
    )
    job = tmp_path / "clients" / req["job_id"]
    job.mkdir(parents=True)
    (job / "request.json").write_text(canonical(req))
    monkeypatch.setenv("WINTAP_DBT_END_DAY", "20260103")
    monkeypatch.setattr(
        client,
        "run_reader",
        lambda *args, **kwargs: pytest.fail("changed window forwarded"),
    )
    assert client.main(["dbt-build", "--job-id", req["job_id"]]) == 1


def test_retained_request_refuses_changed_command_before_new_put(tmp_path, monkeypatch):
    from wintappy.coordination import client

    _, store = transport()
    req = request(tmp_path)
    original = store.publish
    monkeypatch.setattr(
        store,
        "publish",
        lambda message: (_ for _ in ()).throw(
            CoordinationError("synthetic uncertain request")
        ),
    )
    with pytest.raises(CoordinationError):
        client.run_reader(
            tmp_path / "clients",
            req,
            store,
            [sys.executable, "-c", "pass"],
            clock=lambda: DUE,
        )
    monkeypatch.setattr(store, "publish", original)
    store.publish(make_grant(req, "b" * 32, DUE))
    with pytest.raises(CoordinationError):
        client.run_reader(
            tmp_path / "clients",
            req,
            store,
            [sys.executable, "-c", "raise AssertionError('must not launch')"],
            clock=lambda: DUE,
        )
    assert not store.messages("request")


@pytest.mark.parametrize(
    "damage", ["shutdown", "launch", "missing", "terminal", "symlink", "hardlink"]
)
def test_terminal_retry_requires_exact_retained_launch_and_shutdown(
    tmp_path, monkeypatch, damage
):
    from wintappy.coordination import client

    _, store = transport()
    req = request(tmp_path)
    store.publish(make_grant(req, "b" * 32, DUE))
    original = store.publish

    def fail_release(message):
        if message["kind"] == "release":
            raise CoordinationError("synthetic undelivered release")
        return original(message)

    monkeypatch.setattr(store, "publish", fail_release)
    result = client.run_reader(
        tmp_path / "clients",
        req,
        store,
        [sys.executable, "-c", "pass"],
        clock=lambda: DUE,
    )
    assert result["terminal_pending"]
    monkeypatch.setattr(store, "publish", original)
    job = tmp_path / "clients" / req["job_id"]
    if damage == "missing":
        (job / "observations/supervision.json").unlink()
    elif damage in {"symlink", "hardlink"}:
        path = job / "observations/command.json"
        target = tmp_path / "aliased-command.json"
        if damage == "symlink":
            path.rename(target)
            path.symlink_to(target)
        else:
            os.link(path, target)
    else:
        path = (
            job
            / {
                "shutdown": "observations/supervision.json",
                "launch": "launch.json",
                "terminal": "terminal.json",
            }[damage]
        )
        value = json.loads(path.read_text())
        value[
            {
                "shutdown": "children_stopped",
                "launch": "grant_id",
                "terminal": "exit_code",
            }[damage]
        ] = {"shutdown": False, "launch": "c" * 32, "terminal": 7}[damage]
        path.write_text(canonical(value))
    with pytest.raises(CoordinationError):
        client.retry_terminal(tmp_path / "clients", req["job_id"], store)
    assert store.read("release", req["job_id"]) is None


@pytest.mark.parametrize("preflight_code,expected_calls", [(0, 2), (7, 1)])
def test_real_coordinated_dbt_batch_runs_only_under_one_grant(
    tmp_path, monkeypatch, preflight_code, expected_calls
):
    from wintappy.coordination import client

    store = cli_environment(tmp_path, monkeypatch)
    output = tmp_path / "dbt-calls.jsonl"
    fake_dbt = tmp_path / "ordinary-dbt.py"
    fake_dbt.write_text(
        "import json,pathlib,sys\nwith pathlib.Path(sys.argv[1]).open('a') as f: f.write(json.dumps(sys.argv[2:])+'\\n')\nraise SystemExit("
        + str(preflight_code)
        + " if sys.argv[2]=='run-operation' else 0)\n"
    )
    original = store.publish

    def grant_request(message):
        original(message)
        if message["kind"] == "request":
            original(make_grant(message, "b" * 32, DUE))

    monkeypatch.setattr(store, "publish", grant_request)
    assert (
        client.main(
            [
                "dbt-build",
                "--job-id",
                "a" * 32,
                "--dbt-command",
                f"{sys.executable} {fake_dbt} {output}",
            ]
        )
        == preflight_code
    )
    recorded = [json.loads(line) for line in output.read_text().splitlines()]
    assert len(recorded) == expected_calls
    assert recorded[0][:2] == ["run-operation", "validate_raw_sensor_source"]
    if preflight_code == 0:
        assert recorded[1][0] == "build"
    assert (
        len(store.messages("request"))
        == len(store.messages("grant"))
        == len(store.messages("release"))
        == 1
    )
    assert store.read("release", "a" * 32)["exit_code"] == preflight_code


@pytest.mark.parametrize(
    "field,value",
    [("endpoint_url", "https://other.synthetic.invalid"), ("region", "other-region")],
)
def test_terminal_retry_binds_resolved_endpoint_and_region(tmp_path, field, value):
    from wintappy.coordination.client import retry_terminal, run_reader

    _, store = transport()
    store.transport_scope = {
        "endpoint_url": "https://synthetic.invalid",
        "region": "synthetic-region",
        "addressing_style": "path",
    }
    req = request(tmp_path)
    store.publish(make_grant(req, "b" * 32, DUE))
    run_reader(
        tmp_path / "clients",
        req,
        store,
        [sys.executable, "-c", "pass"],
        clock=lambda: DUE,
    )
    _, other = transport()
    other.transport_scope = {**store.transport_scope, field: value}
    with pytest.raises(CoordinationError):
        retry_terminal(tmp_path / "clients", req["job_id"], other)
    assert not other.messages("release")


def test_contradictory_shutdown_uncertain_cannot_release(tmp_path, monkeypatch):
    from wintappy.coordination import client

    _, store = transport()
    req = request(tmp_path)
    store.publish(make_grant(req, "b" * 32, DUE))
    original = client.run_supervised

    def contradictory(command, directory, **kwargs):
        result = original(command, directory, **kwargs)
        result["status"] = "shutdown_uncertain"
        (directory / "supervision.json").write_text(canonical(result))
        return result

    monkeypatch.setattr(client, "run_supervised", contradictory)
    with pytest.raises(CoordinationError):
        client.run_reader(
            tmp_path / "clients",
            req,
            store,
            [sys.executable, "-c", "pass"],
            clock=lambda: DUE,
        )
    assert not store.messages("release")


@pytest.mark.parametrize(
    "damage", ["missing_outcome", "changed_outcome", "launch", "missing_invocation"]
)
def test_cancellation_retry_needs_original_unlaunched_evidence(tmp_path, damage):
    from wintappy.coordination.client import retry_terminal, run_reader

    _, store = transport()
    req = request(tmp_path)

    def interrupted(seconds):
        raise SystemExit(143)

    assert (
        run_reader(
            tmp_path / "clients",
            req,
            store,
            [sys.executable, "-c", "pass"],
            sleep=interrupted,
            clock=lambda: DUE,
        )["exit_code"]
        == 143
    )
    job = tmp_path / "clients" / req["job_id"]
    if damage == "missing_outcome":
        (job / "cancellation-outcome.json").unlink()
    elif damage == "missing_invocation":
        (job / "invocation.json").unlink()
    elif damage == "launch":
        (job / "launch.json").write_text(canonical({"synthetic": True}))
    else:
        (job / "cancellation-outcome.json").write_text(
            canonical({"exit_code": 0, "terminal_sha256": "c" * 64})
        )
    with pytest.raises(CoordinationError):
        retry_terminal(tmp_path / "clients", req["job_id"], store)
