"""Linux service path must not repurpose the conservative callback tick."""

import sys

import pytest

from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, make_release, make_request

from .test_coordination_completion import NOW, prepared_empty
from .test_coordination_execution import binding, config, initialize
from .test_coordination_ledger import HOUR
from .test_coordination_tick import transport

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="Linux service integration"
)


def settings(tmp_path):
    return {
        "version": 1,
        "binding": binding(tmp_path),
        "coordination_root": "s3://synthetic-bucket/coordination",
        "state_directory": str(tmp_path / "state"),
        "first_hour": HOUR,
        "writer": config(tmp_path),
        "storage": {},
    }


def req(tmp_path, identity):
    return make_request(binding(tmp_path), identity * 32, "dbt-build", HOUR, NOW, NOW)


def test_service_real_completion_then_admission_commit_order(tmp_path, monkeypatch):
    from wintappy.coordination import service

    prepared_empty(tmp_path)
    _, store = transport()
    store.publish(req(tmp_path, "a"))
    original = Ledger.admit

    def checked(ledger, now, **kwargs):
        assert ledger._connection.execute(
            "SELECT count(*) FROM completion_evidence"
        ).fetchone() == (1,)
        assert ledger.snapshot()["active_run_id"] is None
        return original(ledger, now, **kwargs)

    monkeypatch.setattr(Ledger, "admit", checked)
    result = service.service_tick(settings(tmp_path), store, clock=lambda: NOW)
    assert result["writer_invoked"] and result["readers"] == 1
    assert result["next_hour"] == "2026-01-02T00:00:00Z"
    assert len(store.messages("grant")) == 1


def test_service_waits_for_two_reservations_and_queues_later_reader(tmp_path):
    from wintappy.coordination.service import service_tick

    prepared_empty(tmp_path)
    _, store = transport()
    before = "2026-01-02T00:09:59Z"
    for identity in ("a", "b"):
        store.publish(req(tmp_path, identity))
    assert service_tick(settings(tmp_path), store, clock=lambda: before)["readers"] == 2
    store.publish(req(tmp_path, "c"))
    state = service_tick(settings(tmp_path), store, clock=lambda: NOW)
    assert state["mode"] == "DRAINING" and not state["writer_invoked"]
    grants = store.messages("grant")
    store.publish(make_release(grants[0], NOW, 1))
    assert not service_tick(settings(tmp_path), store, clock=lambda: NOW)[
        "writer_invoked"
    ]
    store.publish(make_release(grants[1], NOW, 0))
    state = service_tick(settings(tmp_path), store, clock=lambda: NOW)
    assert state["writer_invoked"] and state["readers"] == 1


def test_post_completion_clock_crossing_does_not_grant(tmp_path, monkeypatch):
    from wintappy.coordination import service

    prepared_empty(tmp_path)
    _, store = transport()
    store.publish(req(tmp_path, "a"))
    current = [NOW]
    original = service.execute_and_complete_prepared

    def crossed(*args, **kwargs):
        value = original(*args, **kwargs)
        current[0] = "2026-01-02T01:10:00Z"
        return value

    monkeypatch.setattr(service, "execute_and_complete_prepared", crossed)
    state = service.service_tick(settings(tmp_path), store, clock=lambda: current[0])
    assert state["mode"] == "DRAINING" and state["waiting"] == 1
    assert not store.messages("grant")


def test_service_missing_ledger_and_restart_block_refuse_launch(tmp_path):
    from wintappy.coordination.service import service_tick

    _, store = transport()
    with pytest.raises(CoordinationError):
        service_tick(settings(tmp_path), store, clock=lambda: NOW)
    assert not (tmp_path / "state").exists()
    initialize(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        ledger.begin_writer(HOUR, NOW, ["synthetic-unqualified"])
    result = service_tick(settings(tmp_path), store, clock=lambda: NOW)
    assert result["mode"] == "BLOCKED" and not result["writer_invoked"]


def test_advisory_failure_does_not_authorize_or_undo_completion(tmp_path, monkeypatch):
    from wintappy.coordination import service

    prepared_empty(tmp_path)
    _, store = transport()

    def failed(*args, **kwargs):
        raise OSError("synthetic-secret must not be logged")

    monkeypatch.setattr(service, "export_status", failed)
    result = service.service_tick(settings(tmp_path), store, clock=lambda: NOW)
    assert result["next_hour"] == "2026-01-02T00:00:00Z"
    assert result["advisory_exported"] is False


@pytest.mark.parametrize(
    "damage",
    [
        "unknown",
        "version_bool",
        "hours_bool",
        "grace",
        "storage_secret",
        "overlap",
        "memory",
    ],
)
def test_invalid_config_refuses_before_permissions(tmp_path, damage):
    from wintappy.coordination.service import service_tick

    prepared_empty(tmp_path)
    _, store = transport()
    store.publish(req(tmp_path, "a"))
    value = settings(tmp_path)
    if damage == "unknown":
        value["synthetic-secret"] = "excluded"
    elif damage == "version_bool":
        value["version"] = True
    elif damage == "hours_bool":
        value["writer"]["max_hours"] = True
    elif damage == "grace":
        value["writer"]["grace_minutes"] = 4
    elif damage == "storage_secret":
        value["storage"]["AWS_SECRET_ACCESS_KEY"] = "synthetic-secret"
    elif damage == "overlap":
        value["coordination_root"] = value["binding"]["roots"]["raw"]
    else:
        value["writer"]["memory_limit"] = "unbounded"
    with pytest.raises(CoordinationError):
        service_tick(value, store, clock=lambda: "2026-01-02T00:09:00Z")
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.snapshot()["readers"] == 0


def test_service_emits_committed_writing_status_before_actual_launch(
    tmp_path, monkeypatch
):
    from wintappy.coordination import service

    prepared_empty(tmp_path)
    _, store = transport()
    states = []
    original = service.export_status

    def observe(ledger, now):
        states.append(ledger.snapshot()["mode"])
        return original(ledger, now)

    monkeypatch.setattr(service, "export_status", observe)
    service.service_tick(settings(tmp_path), store, clock=lambda: NOW)
    assert states == ["WRITING", "OPEN"]


def test_service_clock_predating_prior_completion_cannot_admit(tmp_path):
    from wintappy.coordination.service import service_tick

    prepared_empty(tmp_path)
    _, store = transport()
    service_tick(settings(tmp_path), store, clock=lambda: NOW)
    store.publish(req(tmp_path, "a"))
    with pytest.raises(CoordinationError):
        service_tick(settings(tmp_path), store, clock=lambda: "2026-01-02T00:09:00Z")
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        assert ledger.snapshot()["readers"] == 0


def test_advisory_paging_has_explicit_continuation_and_no_commands(tmp_path):
    from wintappy.coordination.service import status_page

    prepared_empty(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        for number in range(101):
            ledger.ingest(
                make_request(
                    binding(tmp_path), f"{number:032x}", "dbt-build", HOUR, NOW, NOW
                ),
                NOW,
            )
        first = status_page(ledger, NOW)
        assert len(first["jobs"]) == 100 and first["next_job"]
        second = status_page(ledger, NOW, after_job=first["next_job"])
        assert len(second["jobs"]) == 1 and second["next_job"] is None
        assert first["advisory"] is True
        assert "command" not in str(first) and "intent_json" not in str(first)
