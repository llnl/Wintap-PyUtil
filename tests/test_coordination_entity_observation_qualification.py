"""Expanded input authentication: actual exclusion, windows and bounded contracts."""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from wintappy.coordination import entity_verification
from wintappy.coordination.entity_execution import prepare_entity_execution
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical, make_request
from wintappy.coordination.verification import verify_authority_prepared

from .test_coordination_authority import HOUR, NOW
from .test_coordination_entity_observations import observe, scope
from .test_coordination_entity_operations import (
    END,
    data_bytes,
    entity_config,
    ledger_rows,
    seed_scope,
)

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux genuine input-verifier supervision and unprivileged exclusion",
)


def multihour(root, *, replay=False):
    bound, _ = scope(root, replay=replay)
    source = root / "raw/raw_process/dayPK=20260101/hourPK=23/part.parquet"
    for name, day, hour in (("raw", "20260101", "22"), ("compacted", "20260102", "00")):
        target = root / name / f"raw_process/dayPK={day}/hourPK={hour}/part.parquet"
        target.parent.mkdir(parents=True)
        target.write_bytes(source.read_bytes())
    prepared = prepare_entity_execution(
        root / "state",
        bound,
        "2026-01-01T21:00:00Z",
        "2026-01-02T01:00:00Z",
        NOW,
        entity_config(root),
        operation="entities-replay" if replay else "entities-init",
    )
    return bound, prepared


def rehash_capture(proof):
    captured = (canonical(proof["result"]) + "\n").encode()
    proof["supervision"]["stdout"].update(
        sha256=hashlib.sha256(captured).hexdigest(),
        bytes_seen=len(captured),
        bytes_saved=len(captured),
    )


def count_launches(monkeypatch):
    calls = []
    actual = entity_verification.run_supervised

    def run(*args, **kwargs):
        calls.append(args)
        return actual(*args, **kwargs)

    monkeypatch.setattr(entity_verification, "run_supervised", run)
    return calls


@pytest.mark.parametrize("replay", [False, True])
def test_real_four_hour_mixed_inputs_and_empty_hour_preserve_original_scope(
    tmp_path, replay
):
    bound, prepared = multihour(tmp_path, replay=replay)
    before = data_bytes(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows = ledger_rows(ledger)
        anchor = ledger.authority_high_water()
        first = observe(ledger, prepared)
        inputs = first["result"]["inputs"]
        assert [hour["hour_start"] for hour in inputs["hours"]] == [
            "2026-01-01T21:00:00Z",
            "2026-01-01T22:00:00Z",
            HOUR,
            END,
        ]
        assert inputs["raw"]["files"] == inputs["raw"]["rows"] == 2
        assert inputs["compacted"]["files"] == inputs["compacted"]["rows"] == 1
        for name in ("raw", "compacted"):
            for key in ("files", "bytes"):
                assert (
                    sum(hour[name][key] for hour in inputs["hours"])
                    == inputs[name][key]
                )
            assert inputs["hours"][0][name]["files"] == 0
        second = observe(ledger, prepared, original=first)
        assert second["result"]["inputs"] == inputs
        assert ledger_rows(ledger) == rows
        assert ledger.authority_high_water() == anchor
    assert data_bytes(tmp_path) == before


@pytest.mark.parametrize("name", ["raw", "compacted"])
@pytest.mark.parametrize("key", ["files", "bytes"])
@pytest.mark.parametrize("change", ["hour-understates", "aggregate-overstates"])
def test_rehashed_input_contract_requires_exact_selected_hour_totals(
    tmp_path, name, key, change
):
    bound, prepared = multihour(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        proof = observe(ledger, prepared)
        bad = json.loads(canonical(proof))
        inputs = bad["result"]["inputs"]
        if change == "hour-understates":
            populated = next(hour[name] for hour in inputs["hours"] if hour[name][key])
            populated[key] -= 1
        else:
            inputs[name][key] += 1
        rehash_capture(bad)
        with pytest.raises(CoordinationError):
            entity_verification.validate_input_observation(bad, bound, ledger.ledger_id)


@pytest.mark.parametrize(
    "damage", ["boolean", "negative", "overflow", "duplicate-hour"]
)
def test_rehashed_hour_contract_requires_exact_types_bounds_and_unique_hours(
    tmp_path, damage
):
    bound, prepared = multihour(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        bad = json.loads(canonical(observe(ledger, prepared)))
        hours = bad["result"]["inputs"]["hours"]
        if damage == "duplicate-hour":
            hours[1] = json.loads(canonical(hours[0]))
        else:
            hours[0]["raw"]["bytes"] = {
                "boolean": True,
                "negative": -1,
                "overflow": 2**63,
            }[damage]
        rehash_capture(bad)
        with pytest.raises(CoordinationError):
            entity_verification.validate_input_observation(bad, bound, ledger.ledger_id)


@pytest.mark.parametrize("state", ["reader", "raw-intent", "restarted-raw-intent"])
def test_existing_committed_exclusion_refuses_before_input_verifier_launch(
    tmp_path, monkeypatch, state
):
    bound, compact = seed_scope(tmp_path, initialized=True, anchored=True)
    Ledger.upgrade(
        tmp_path / "state",
        bound,
        NOW,
        target_version=5,
        confirmed=True,
        reason="synthetic explicit exclusion qualification schema",
    )
    with Ledger.open(tmp_path / "state", bound) as ledger:
        verify_authority_prepared(ledger, compact, NOW)
        if state == "reader":
            request = make_request(bound, "c" * 32, "dbt-build", HOUR, END, NOW)
            ledger.ingest(request, NOW)
            assert len(ledger.admit(NOW)) == 1
        else:
            ledger.drain(NOW)
            ledger.begin_writer(HOUR, NOW, ["synthetic-exclusive-raw-intent"])
            assert ledger.snapshot()["mode"] == "WRITING"
            if state == "raw-intent":
                prepared = prepare_entity_execution(
                    tmp_path / "state",
                    bound,
                    HOUR,
                    END,
                    NOW,
                    entity_config(tmp_path),
                    operation="entities-replay",
                )
                rows = ledger_rows(ledger)
                before = data_bytes(tmp_path)
                calls = count_launches(monkeypatch)
                with pytest.raises(CoordinationError):
                    observe(ledger, prepared)
                assert calls == []
                assert ledger_rows(ledger) == rows
                assert data_bytes(tmp_path) == before
                return
    prepared = prepare_entity_execution(
        tmp_path / "state",
        bound,
        HOUR,
        END,
        NOW,
        entity_config(tmp_path),
        operation="entities-replay",
    )
    before = data_bytes(tmp_path)
    calls = count_launches(monkeypatch)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows = ledger_rows(ledger)
        if state == "reader":
            assert ledger.snapshot()["readers"] == 1
        else:
            assert ledger.snapshot()["mode"] == "BLOCKED"
        with pytest.raises(CoordinationError):
            observe(ledger, prepared)
        assert calls == []
        assert ledger_rows(ledger) == rows
    assert data_bytes(tmp_path) == before


@pytest.mark.parametrize("operation", ["entities-init", "entities-replay"])
def test_unanchored_existing_entities_never_become_genesis_or_replay_authority(
    tmp_path, operation
):
    bound, _ = seed_scope(tmp_path, initialized=True, anchored=False)
    Ledger.upgrade(
        tmp_path / "state",
        bound,
        NOW,
        target_version=5,
        confirmed=True,
        reason="synthetic unanchored-state qualification schema",
    )
    prepared = prepare_entity_execution(
        tmp_path / "state",
        bound,
        HOUR,
        END,
        NOW,
        entity_config(tmp_path),
        operation=operation,
    )
    before = data_bytes(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            observe(ledger, prepared)
        assert ledger_rows(ledger) == rows
        assert ledger.authority_high_water() is None
    assert data_bytes(tmp_path) == before


@pytest.mark.parametrize("damage", ["input-fingerprint", "shutdown", "command", "exit"])
def test_rehashed_dictionary_never_replaces_actual_original_observation(
    tmp_path, monkeypatch, damage
):
    bound, prepared = scope(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        first = observe(ledger, prepared)
        bad = json.loads(canonical(first))
        if damage == "input-fingerprint":
            bad["result"]["inputs"]["process_input_fingerprint"] = "a" * 64
            rehash_capture(bad)
        elif damage == "shutdown":
            bad["supervision"].update(
                status="shutdown_uncertain", children_stopped=False
            )
        elif damage == "command":
            bad["supervision"]["command_sha256"] = "a" * 64
        else:
            bad["supervision"]["exit_code"] = 2
        rows = ledger_rows(ledger)
        calls = count_launches(monkeypatch)
        with pytest.raises(CoordinationError):
            observe(ledger, prepared, original=bad)
        assert calls == []
        assert ledger_rows(ledger) == rows


@pytest.mark.parametrize(
    "artifact", ["request.json", "command.json", "supervision.json"]
)
def test_duplicate_json_fields_refuse_before_fresh_original_recheck_launch(
    tmp_path, monkeypatch, artifact
):
    bound, prepared = scope(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        first = observe(ledger, prepared)
        root = Path(first["observations_directory"])
        path = root.parent / artifact if artifact == "request.json" else root / artifact
        value = json.loads(path.read_bytes())
        key = next(iter(value))
        path.write_text(
            "{"
            + canonical(key)
            + ":"
            + canonical(value[key])
            + ","
            + canonical(value)[1:]
        )
        rows = ledger_rows(ledger)
        calls = count_launches(monkeypatch)
        with pytest.raises(CoordinationError):
            observe(ledger, prepared, original=first)
        assert calls == []
        assert ledger_rows(ledger) == rows


@pytest.mark.parametrize("budget", ["request", "result", "proof"])
def test_independent_input_byte_budgets_refuse_without_pruning_original_evidence(
    tmp_path, monkeypatch, budget
):
    bound, prepared = scope(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        first = observe(ledger, prepared)
        root = Path(first["observations_directory"]).parent
        retained = {
            str(path.relative_to(root)): path.read_bytes()
            for path in root.rglob("*")
            if path.is_file()
        }
        rows = ledger_rows(ledger)
        limit = {
            "request": "MAX_ENTITY_REQUEST_BYTES",
            "result": "MAX_ENTITY_RESULT_BYTES",
            "proof": "MAX_ENTITY_OBSERVATION_BYTES",
        }[budget]
        monkeypatch.setattr(entity_verification, limit, 1)
        with pytest.raises(CoordinationError):
            observe(ledger, prepared, original=first)
        assert ledger_rows(ledger) == rows
        assert retained == {
            str(path.relative_to(root)): path.read_bytes()
            for path in root.rglob("*")
            if path.is_file()
        }


def test_receipt_clock_regression_refuses_before_input_verifier_launch(
    tmp_path, monkeypatch
):
    bound, prepared = scope(tmp_path)
    calls = count_launches(monkeypatch)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            entity_verification.verify_entity_inputs_prepared(
                ledger, prepared, "2026-01-02T00:09:59Z"
            )
        assert calls == []
        assert ledger_rows(ledger) == rows


def test_actual_competing_process_cannot_acquire_lock_during_input_verification(
    tmp_path, monkeypatch
):
    bound, prepared = scope(tmp_path)
    actual = entity_verification.run_supervised
    attempts = []
    script = """
import json,sys
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError
try:
    with Ledger.open(sys.argv[1],json.loads(sys.argv[2])): pass
except CoordinationError as error:
    assert 'busy' in str(error).lower()
    sys.exit(88)
raise AssertionError('competing process obtained the input-verification lock')
"""

    def compete(*args, **kwargs):
        child = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                script,
                str(tmp_path / "state"),
                canonical(bound),
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
        assert child.returncode == 88, (child.stdout, child.stderr)
        attempts.append(child.returncode)
        return actual(*args, **kwargs)

    monkeypatch.setattr(entity_verification, "run_supervised", compete)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows = ledger_rows(ledger)
        first = observe(ledger, prepared)
        observe(ledger, prepared, original=first)
        assert ledger_rows(ledger) == rows
    assert attempts == [88, 88]
