"""Recovery needs explicit attestations and fresh original evidence, never force."""

import json
import sys

import pytest

from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical, make_request
from wintappy.coordination.writer import execute_prepared

from .test_coordination_completion import NOW, prepared_empty
from .test_coordination_execution import binding
from .test_coordination_ledger import HOUR

pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="Linux audited recovery"
)


def test_manual_reader_release_requires_confirmation_and_retains_audit(tmp_path):
    from wintappy.coordination.recovery import release_reader

    prepared_empty(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        req = make_request(binding(tmp_path), "a" * 32, "dbt-build", HOUR, NOW, NOW)
        ledger.ingest(req, NOW)
        ledger.admit(NOW)
        with pytest.raises(CoordinationError):
            release_reader(
                ledger,
                req["job_id"],
                NOW,
                confirmed=False,
                reason="synthetic exclusion",
            )
        assert ledger.snapshot()["readers"] == 1
        release_reader(
            ledger,
            req["job_id"],
            NOW,
            confirmed=True,
            reason="synthetic original reader cannot resume",
        )
        assert ledger.snapshot()["readers"] == 0
        assert ledger._connection.execute(
            "SELECT count(*) FROM events WHERE kind='reader_recovered'"
        ).fetchone() == (1,)


def test_blocked_clean_run_requires_audited_fresh_resolution(tmp_path):
    from wintappy.coordination.recovery import resolve_hour

    prepared = prepared_empty(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        execute_prepared(ledger, prepared, NOW)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        before = ledger.snapshot()
        with pytest.raises(CoordinationError):
            resolve_hour(
                ledger,
                prepared,
                NOW,
                clock=lambda: NOW,
                confirmed=False,
                reason="synthetic reason",
            )
        assert ledger.snapshot() == before
        result = resolve_hour(
            ledger,
            prepared,
            NOW,
            clock=lambda: NOW,
            confirmed=True,
            reason="synthetic original writer cannot resume",
        )
        assert (
            result["mode"] == "OPEN"
            and result["next_hour"] == prepared.receipt["hour_end"]
        )
        assert ledger._connection.execute(
            "SELECT count(*) FROM events WHERE kind='writer_recovered'"
        ).fetchone() == (1,)


def test_missing_original_archive_stays_blocked_even_with_confirmation(tmp_path):
    from wintappy.coordination.recovery import resolve_hour

    prepared = prepared_empty(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        ledger.begin_writer(
            HOUR,
            NOW,
            list(prepared.command),
            execution={
                "path": str(prepared.path),
                "sha256": prepared.sha256,
                "receipt": prepared.receipt,
            },
        )
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        before = ledger.snapshot()
        with pytest.raises(CoordinationError):
            resolve_hour(
                ledger,
                prepared,
                NOW,
                clock=lambda: NOW,
                confirmed=True,
                reason="synthetic stopped writer",
            )
        assert ledger.snapshot() == before


def test_recovered_run_missing_operator_audit_refuses_open(tmp_path):
    from wintappy.coordination.recovery import resolve_hour

    prepared = prepared_empty(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        ledger.drain(NOW)
        execute_prepared(ledger, prepared, NOW)
        resolve_hour(
            ledger,
            prepared,
            NOW,
            clock=lambda: NOW,
            confirmed=True,
            reason="synthetic original writer cannot resume",
        )
        ledger._connection.execute("DELETE FROM events WHERE kind='writer_recovered'")
    with (
        pytest.raises(CoordinationError),
        Ledger.open(tmp_path / "state", binding(tmp_path)),
    ):
        pass


@pytest.mark.parametrize(
    "damage", ["remove_audit", "remove_marker", "change_reason", "change_identity"]
)
def test_reader_recovery_requires_retained_matching_audit(tmp_path, damage):
    from wintappy.coordination.recovery import release_reader

    prepared_empty(tmp_path)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        req = make_request(binding(tmp_path), "a" * 32, "dbt-build", HOUR, NOW, NOW)
        ledger.ingest(req, NOW)
        ledger.admit(NOW)
        release_reader(
            ledger,
            req["job_id"],
            NOW,
            confirmed=True,
            reason="synthetic original reader cannot resume",
        )
        if damage == "remove_audit":
            ledger._connection.execute(
                "DELETE FROM events WHERE kind='reader_recovered'"
            )
        elif damage == "remove_marker":
            ledger._connection.execute("DELETE FROM events WHERE kind='cancellation'")
        else:
            audit = ledger._connection.execute(
                "SELECT details_json FROM events WHERE kind='reader_recovered'"
            ).fetchone()[0]
            value = json.loads(audit)
            value["reason" if damage == "change_reason" else "grant_id"] = (
                "" if damage == "change_reason" else "b" * 32
            )
            ledger._connection.execute(
                "UPDATE events SET details_json=? WHERE kind='reader_recovered'",
                [canonical(value)],
            )
    with (
        pytest.raises(CoordinationError),
        Ledger.open(tmp_path / "state", binding(tmp_path)),
    ):
        pass


def blocked_checkpoint_run(tmp_path):
    from wintappy.coordination.verification import verify_authority_prepared

    from .test_coordination_authority import prepared_entities

    bound, prepared = prepared_entities(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        verify_authority_prepared(
            ledger,
            prepared,
            NOW,
            initialize=True,
            confirmed=True,
            reason="synthetic baseline",
        )
        # Real final-publication failure after entity update and raw deletion. No
        # altered runtime, invented original archive or fake supervision records.
        (tmp_path / "metrics").rmdir()
        (tmp_path / "metrics").write_text("synthetic publication obstruction")
        ledger.drain(NOW)
        with pytest.raises(CoordinationError):
            execute_prepared(ledger, prepared, NOW)
        assert ledger.snapshot()["mode"] == "BLOCKED"
    archive = tmp_path / "phase-evidence" / prepared.receipt["cli_run_id"]
    assert list(archive.glob("*-raw_complete.json"))
    assert list(archive.glob("*-checkpoint_intent.json"))
    assert list(archive.glob("*-blocked.json"))
    assert not list((tmp_path / "raw").rglob("*.parquet"))
    (tmp_path / "metrics").unlink()
    (tmp_path / "metrics").mkdir()
    return bound, prepared, {path: path.read_bytes() for path in archive.iterdir()}


def test_exclusive_checkpoint_repair_does_not_manufacture_original_completion(tmp_path):
    bound, prepared, originals = blocked_checkpoint_run(tmp_path)
    from wintappy.coordination.recovery import repair_checkpoints, resolve_hour

    with Ledger.open(tmp_path / "state", bound) as ledger:
        before = ledger.snapshot()
        with pytest.raises(CoordinationError):
            repair_checkpoints(
                ledger, prepared, NOW, confirmed=False, reason="synthetic reason"
            )
        assert ledger.snapshot() == before
        operation_id = repair_checkpoints(
            ledger,
            prepared,
            NOW,
            confirmed=True,
            reason="synthetic all original processes cannot resume",
        )
        assert (
            ledger.snapshot() == before
        )  # A repair exit is never reopening authority.
        result = resolve_hour(
            ledger,
            prepared,
            NOW,
            clock=lambda: NOW,
            confirmed=True,
            reason="synthetic repaired phases independently verified",
            maintenance_id=operation_id,
        )
        assert result["next_hour"] == prepared.receipt["hour_end"]
        proof = ledger.completion_evidence(prepared.receipt["run_id"])
        assert proof["version"] == 2 and proof["writer_supervision"]["exit_code"] == 2
        assert proof["recovery"]["operation"] == "checkpoints-repair"
    assert all(path.read_bytes() == payload for path, payload in originals.items())
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.snapshot()["mode"] == "OPEN"
