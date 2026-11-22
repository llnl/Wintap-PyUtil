"""Genuine standalone inputs bound to main receipts, never entity permission."""

import hashlib
import json
import os
import sys
from pathlib import Path

import pytest

from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical, digest

from .test_coordination_authority import NOW
from .test_coordination_entity_operations import (
    data_bytes,
    ledger_rows,
    prepare_entity,
    seed_scope,
)

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux genuine independently supervised entity input observations",
)


def scope(root, *, replay=False):
    bound, _ = seed_scope(root, initialized=replay, anchored=replay)
    Ledger.upgrade(
        root / "state",
        bound,
        NOW,
        target_version=5,
        confirmed=True,
        reason="synthetic explicit entity-observation schema selection",
    )
    return bound, prepare_entity(
        root, bound, "entities-replay" if replay else "entities-init"
    )


def observe(ledger, prepared, *, original=None):
    from wintappy.coordination.entity_verification import verify_entity_inputs_prepared

    return verify_entity_inputs_prepared(ledger, prepared, NOW, original=original)


@pytest.mark.parametrize("replay", [False, True])
def test_real_entity_input_observation_and_recheck_never_changes_permissions(
    tmp_path, replay
):
    bound, prepared = scope(tmp_path, replay=replay)
    before = data_bytes(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows = ledger_rows(ledger)
        anchor = ledger.authority_high_water()
        first = observe(ledger, prepared)
        assert first["operation"] == "entity-input-observation"
        assert first["receipt"] == prepared.receipt
        assert first["result"]["operation"] == "entity-only-inputs"
        assert first["result"]["authority_high_water"] == anchor
        assert first["result"]["ledger_id"] == ledger.ledger_id
        assert first["result"]["dataset_sha256"] == digest(bound)
        assert first["result"]["inputs"]["raw"]["files"] == 1
        assert first["supervision"]["children_stopped"] is True
        second = observe(ledger, prepared, original=first)
        assert second["request"]["original"] == {
            key: value
            for key, value in first["result"].items()
            if key not in {"ledger_id", "dataset_sha256", "request_sha256"}
        }
        assert first["observations_directory"] != second["observations_directory"]
        assert ledger_rows(ledger) == rows
        assert ledger.entity_run(prepared.receipt["run_id"]) is None
        assert ledger.entity_evidence(prepared.receipt["run_id"]) is None
        assert ledger.authority_high_water() == anchor
    assert data_bytes(tmp_path) == before


def test_original_entity_observation_requires_unchanged_actual_inputs(tmp_path):
    bound, prepared = scope(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        first = observe(ledger, prepared)
        rows = ledger_rows(ledger)
        source = tmp_path / "raw/raw_process/dayPK=20260101/hourPK=23/part.parquet"
        source.with_name("late.parquet").write_bytes(source.read_bytes())
        with pytest.raises(CoordinationError):
            observe(ledger, prepared, original=first)
        assert ledger_rows(ledger) == rows
        assert ledger.authority_high_water() is None


@pytest.mark.parametrize(
    "change",
    ["run", "execution", "hour", "count-type", "hour-count", "scope", "extra"],
)
def test_retained_input_contract_refuses_consistently_rehashed_wrong_scope(
    tmp_path, change
):
    from wintappy.coordination.entity_verification import validate_input_observation

    bound, prepared = scope(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        first = observe(ledger, prepared)
        bad = json.loads(canonical(first))
        result = bad["result"]
        if change == "run":
            result["run_id"] = "a" * 32
        elif change == "execution":
            result["execution_sha256"] = "a" * 64
        elif change == "hour":
            result["window_start"] = "2026-01-01T22:00:00Z"
        elif change == "count-type":
            result["inputs"]["raw"]["files"] = True
        elif change == "hour-count":
            result["inputs"]["hours"] = []
        elif change == "scope":
            result["dataset_sha256"] = "a" * 64
        else:
            result["raw_complete"] = True
        captured = (canonical(result) + "\n").encode()
        bad["supervision"]["stdout"].update(
            sha256=hashlib.sha256(captured).hexdigest(),
            bytes_saved=len(captured),
            bytes_seen=len(captured),
        )
        with pytest.raises(CoordinationError):
            validate_input_observation(bad, bound, ledger.ledger_id)


@pytest.mark.parametrize(
    "artifact",
    ["request.json", "command.json", "supervision.json", "stdout.log", "stderr.log"],
)
@pytest.mark.parametrize("alias", ["bytes", "symlink", "hardlink"])
def test_prior_input_observation_cannot_be_reused_after_artifact_tampering(
    tmp_path, artifact, alias
):
    bound, prepared = scope(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        first = observe(ledger, prepared)
        rows = ledger_rows(ledger)
        observations = Path(first["observations_directory"])
        path = (
            observations.parent / artifact
            if artifact == "request.json"
            else observations / artifact
        )
        if alias == "bytes":
            path.write_bytes(path.read_bytes() + b"\n")
        else:
            moved = path.with_name(path.name + ".retained")
            path.rename(moved)
            if alias == "symlink":
                path.symlink_to(moved)
            else:
                os.link(moved, path)
        with pytest.raises(CoordinationError):
            observe(ledger, prepared, original=first)
        assert ledger_rows(ledger) == rows


def test_entity_observation_refuses_v4_without_implicitly_upgrading(tmp_path):
    bound, _ = seed_scope(tmp_path)
    prepared = prepare_entity(tmp_path, bound, "entities-init")
    before = data_bytes(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        rows = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            observe(ledger, prepared)
        assert ledger_rows(ledger) == rows
        assert ledger._connection.execute(
            "SELECT schema_version FROM metadata"
        ).fetchone() == (4,)
    assert data_bytes(tmp_path) == before


def test_entity_observation_refuses_byte_overflow_without_pruning(
    tmp_path, monkeypatch
):
    from wintappy.coordination import entity_verification

    bound, prepared = scope(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        first = observe(ledger, prepared)
        rows = ledger_rows(ledger)
        observations = Path(first["observations_directory"])
        original = {
            p.name: p.read_bytes() for p in observations.iterdir() if p.is_file()
        }
        monkeypatch.setattr(entity_verification, "MAX_ENTITY_OBSERVATION_BYTES", 1)
        with pytest.raises(CoordinationError):
            observe(ledger, prepared, original=first)
        assert {
            p.name: p.read_bytes() for p in observations.iterdir() if p.is_file()
        } == (original)
        assert ledger_rows(ledger) == rows
