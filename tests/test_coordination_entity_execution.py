"""Entity-only receipts share runtime safety, never raw compaction authority."""

import json
import os
from copy import deepcopy

import pytest

from wintappy.coordination.execution import validate_receipt
from wintappy.coordination.protocol import CoordinationError, canonical, digest

from .test_coordination_authority import HOUR, NOW, binding
from .test_coordination_entity_operations import END, entity_config


def prepare(tmp_path, *, operation="entities-init", end=END, changes=None):
    from wintappy.coordination.entity_execution import prepare_entity_execution

    config = entity_config(tmp_path)
    config.update(changes or {})
    return prepare_entity_execution(
        tmp_path / "state",
        binding(tmp_path),
        HOUR,
        end,
        NOW,
        config,
        operation=operation,
    )


@pytest.mark.parametrize("operation", ["entities-init", "entities-replay"])
@pytest.mark.parametrize("hours", [1, 3])
def test_real_runtime_distinct_receipt_retains_exact_window_and_bound_command(
    tmp_path, operation, hours
):
    from wintappy.coordination.entity_execution import load_entity_execution

    end = END if hours == 1 else "2026-01-02T02:00:00Z"
    prepared = prepare(tmp_path, operation=operation, end=end)
    receipt = prepared.receipt
    assert receipt["operation"] == "entity-only-execution"
    assert receipt["entity_command"] == operation
    assert receipt["hour_start"] == HOUR and receipt["hour_end"] == end
    assert receipt["runtime"]["duckdb_version"] == "2.0.0"
    assert receipt["command"] == list(prepared.command)
    assert receipt["command"][5] == operation
    assert prepared.path.parent.name == receipt["run_id"]
    assert prepared.path.parent.parent.name == "entity-executions"
    assert receipt["settings"]["max_hours"] >= hours
    for key in ("target_size_bytes", "delete_source", "grace_minutes"):
        assert key not in receipt["settings"]
    for flag in ("--delete-source", "--target-size-bytes", "--resume-init"):
        assert flag not in prepared.command
    assert prepared.path.read_bytes() == canonical(receipt).encode()
    assert load_entity_execution(prepared.path, prepared.sha256) == receipt
    with pytest.raises(CoordinationError):
        validate_receipt(receipt)


@pytest.mark.parametrize(
    "field,value",
    [
        ("target_size_bytes", 1),
        ("delete_source", False),
        ("grace_minutes", 10),
        ("delete_confirm_attempts", 1),
        ("delete_confirm_initial_delay_seconds", 1),
        ("resume_init", True),
        ("unknown", "synthetic-sensitive-token"),
    ],
)
def test_unrelated_entity_settings_refuse_before_runtime_or_storage_resolution(
    tmp_path, monkeypatch, field, value
):
    import wintappy.coordination.entity_execution as execution

    calls = []
    monkeypatch.setattr(execution, "runtime_identity", lambda *a: calls.append(a))
    monkeypatch.setattr(execution, "resolve_storage", lambda *a: calls.append(a))
    with pytest.raises(CoordinationError) as caught:
        prepare(tmp_path, changes={field: value})
    assert not calls
    assert "synthetic-sensitive-token" not in str(caught.value)
    assert not list(tmp_path.rglob("execution.json"))


@pytest.mark.parametrize(
    "field,value",
    [("operation", "compact"), ("end", HOUR), ("end", "2026-01-01T23:30:00Z")],
)
def test_invalid_entity_command_or_window_has_no_persisted_receipt(
    tmp_path, field, value
):
    with pytest.raises(CoordinationError):
        prepare(tmp_path, **{field: value})
    assert not list(tmp_path.rglob("execution.json"))


@pytest.mark.parametrize(
    "damage",
    ["version", "operation", "command", "binding", "window", "runtime", "budget"],
)
def test_entity_receipt_shape_and_exact_command_are_independently_validated(
    tmp_path, damage
):
    from wintappy.coordination.entity_execution import validate_entity_receipt

    prepared = prepare(tmp_path)
    receipt = deepcopy(prepared.receipt)
    if damage == "version":
        receipt["version"] = True
    elif damage == "operation":
        receipt["operation"] = "raw-compaction"
    elif damage == "command":
        receipt["command"].append("--allow-empty-host")
    elif damage == "binding":
        receipt["binding"]["entity"]["hosts"].append("UNBOUND-SYNTHETIC-HOST")
    elif damage == "window":
        receipt["hour_end"] = HOUR
    elif damage == "runtime":
        receipt["runtime"]["duckdb_version"] = "1.5.6"
    else:
        receipt["settings"]["max_hours"] = True
    with pytest.raises(CoordinationError):
        validate_entity_receipt(receipt)


@pytest.mark.parametrize("damage", ["newline", "checksum", "hardlink", "symlink"])
def test_entity_receipt_loader_rejects_noncanonical_changed_or_aliased_bytes(
    tmp_path, damage
):
    from wintappy.coordination.entity_execution import load_entity_execution

    prepared = prepare(tmp_path)
    path, sha256 = prepared.path, prepared.sha256
    if damage == "newline":
        path.write_bytes(path.read_bytes() + b"\n")
        sha256 = digest(prepared.receipt)  # Exact original bytes remain required.
    elif damage == "checksum":
        sha256 = "0" * 64
    elif damage == "hardlink":
        os.link(path, tmp_path / "alias")
    else:
        alias = tmp_path / "alias"
        alias.write_bytes(path.read_bytes())
        path.unlink()
        path.symlink_to(alias)
    with pytest.raises(CoordinationError):
        load_entity_execution(path, sha256)


def test_receipt_preparation_freezes_copy_not_mutable_config_or_binding(tmp_path):
    prepared = prepare(tmp_path)
    assert json.loads(prepared.path.read_bytes()) == prepared.receipt
    bound = binding(tmp_path)
    bound["entity"]["hosts"].append("UNBOUND-SYNTHETIC-HOST")
    assert prepared.binding != bound
    assert prepared.binding == prepared.receipt["binding"]
