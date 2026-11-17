"""Genuine completion artifacts and bounded contracts, never entity permission."""

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from wintappy.coordination import entity_completion, entity_intent
from wintappy.coordination.completion import _manifest
from wintappy.coordination.entity_verification import _command
from wintappy.coordination.protocol import CoordinationError, canonical, digest

from .test_coordination_entity_completion_observations import (
    completed_fixture,
    launches,
    observe,
)
from .test_coordination_entity_observation_qualification import rehash_capture
from .test_coordination_entity_operations import data_bytes, ledger_rows

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux genuine typed originals and independent supervised completion",
)


def state(root, ledger):
    return (
        ledger_rows(ledger),
        data_bytes(root),
        ledger.authority_high_water(),
        ledger._entity_live_intent,
    )


def alias_file(path, alias):
    retained = path.with_name(path.name + ".retained")
    path.rename(retained)
    if alias == "symlink":
        path.symlink_to(retained)
    else:
        os.link(retained, path)


def artifact_path(directory, artifact):
    return (
        directory.parent / artifact
        if artifact == "request.json"
        else directory / artifact
    )


def archived_report(prepared):
    return (
        Path(prepared.receipt["settings"]["run_evidence_directory"])
        / prepared.receipt["cli_run_id"]
        / "000002-entities_verified.json"
    )


def cli_bytes(report):
    return (json.dumps(report, indent=2, sort_keys=True) + "\n").encode()


def captured_info(supervision, payload):
    supervision["stdout"].update(
        sha256=hashlib.sha256(payload).hexdigest(),
        bytes_seen=len(payload),
        bytes_saved=len(payload),
    )


def replace_stdout(directory, payload):
    (directory / "stdout.log").write_bytes(payload)
    path = directory / "supervision.json"
    supervision = json.loads(path.read_bytes())
    captured_info(supervision, payload)
    path.write_text(canonical(supervision))


def duplicate_field(payload):
    value = json.loads(payload)
    key = next(iter(value))
    return (
        "{" + canonical(key) + ":" + canonical(value[key]) + "," + canonical(value)[1:]
    ).encode()


def change_statistics(report, damage):
    host = report["hosts"][0]
    if damage == "rows":
        host["rows"] += 1
    elif damage == "bootstrap":
        host["bootstrap_start"] = "2025-12-31T00:00:00Z"
    elif damage == "semantic":
        host["semantic_sha256"] = "a" * 64
    else:
        report["identity_exclusions"]["fingerprint"] = "a" * 64


def test_cli_result_accepts_cumulative_observed_host_set(tmp_path):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, _, intent, _ = fixture
        report = json.loads(
            Path(prepared.receipt["observations_directory"] + "/stdout.log").read_text()
        )
        report["hosts"].append(
            {
                **report["hosts"][0],
                "hostname": "SYNTHETIC-NEW-HOST",
            }
        )
        report["hosts"].sort(key=lambda host: host["hostname"])
        entity_completion._cli_result(
            report,
            prepared.receipt,
            intent["input_observation"]["result"],
        )


def rebind(proof):
    """Keep transport/capture hashes consistent; this does not authenticate files."""
    receipt = proof["receipt"]
    proof["request"]["supervision_sha256"] = digest(proof["execution_supervision"])
    proof["result"]["supervision_sha256"] = digest(proof["execution_supervision"])
    proof["result"]["request_sha256"] = digest(proof["request"])
    command = _command(receipt, Path(proof["observations_directory"]), proof["request"])
    proof["supervision"]["command_sha256"] = digest(_manifest(receipt, command))
    rehash_capture(proof)


@pytest.fixture(scope="module")
def retained_completion(tmp_path_factory):
    # One genuine live locked fixture for dictionary-only controls. No file or ledger
    # mutation is shared across controls; each proof is a deep canonical copy.
    root = tmp_path_factory.mktemp("synthetic-completion-contract")
    with completed_fixture(root, multiple=True) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        proof = observe(ledger, prepared, intent, prelaunch)
        yield root, fixture, proof


@pytest.mark.parametrize("alias", ["bytes", "symlink", "hardlink"])
def test_original_publication_record_aliases_refuse_before_verifier_launch(
    tmp_path, monkeypatch, alias
):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        path = archived_report(prepared)
        if alias == "bytes":
            path.write_bytes(path.read_bytes() + b"\n")
        else:
            alias_file(path, alias)
        before, original = state(tmp_path, ledger), path.read_bytes()
        calls = launches(monkeypatch)
        with pytest.raises(CoordinationError):
            observe(ledger, prepared, intent, prelaunch)
        assert calls == []
        assert path.read_bytes() == original
        assert state(tmp_path, ledger) == before


@pytest.mark.parametrize(
    "artifact",
    ["request.json", "command.json", "supervision.json", "stdout.log", "stderr.log"],
)
@pytest.mark.parametrize("alias", ["symlink", "hardlink"])
def test_actual_verifier_aliases_cannot_replace_single_link_originals(
    tmp_path, monkeypatch, artifact, alias
):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        proof = observe(ledger, prepared, intent, prelaunch)
        path = artifact_path(Path(proof["observations_directory"]), artifact)
        alias_file(path, alias)
        before = state(tmp_path, ledger)
        calls = launches(monkeypatch)
        with pytest.raises(CoordinationError):
            entity_completion.recheck_completion_observation(ledger, prepared, proof)
        assert calls == []
        assert state(tmp_path, ledger) == before


@pytest.mark.parametrize(
    "side,artifact",
    [
        ("cli", "command.json"),
        ("cli", "supervision.json"),
        ("cli", "stdout.log"),
        ("cli", "archive"),
        ("verifier", "request.json"),
        ("verifier", "command.json"),
        ("verifier", "supervision.json"),
        ("verifier", "stdout.log"),
    ],
)
def test_actual_duplicate_fields_refuse_even_with_matching_capture_hashes(
    tmp_path, monkeypatch, side, artifact
):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        proof = (
            observe(ledger, prepared, intent, prelaunch) if side == "verifier" else None
        )
        directory = Path(
            proof["observations_directory"]
            if proof is not None
            else prepared.receipt["observations_directory"]
        )
        path = (
            archived_report(prepared)
            if artifact == "archive"
            else artifact_path(directory, artifact)
        )
        payload = duplicate_field(path.read_bytes())
        if artifact == "stdout.log":
            replace_stdout(directory, payload)
        else:
            path.write_bytes(payload)
        before = state(tmp_path, ledger)
        calls = launches(monkeypatch)
        with pytest.raises(CoordinationError):
            if proof is None:
                observe(ledger, prepared, intent, prelaunch)
            else:
                entity_completion.recheck_completion_observation(
                    ledger, prepared, proof
                )
        assert calls == []
        assert path.read_bytes() == payload
        assert state(tmp_path, ledger) == before


@pytest.mark.parametrize("damage", ["rows", "bootstrap", "semantic", "exclusions"])
def test_rehashed_actual_cli_statistics_do_not_replace_original_archive(
    tmp_path, monkeypatch, damage
):
    with completed_fixture(tmp_path) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        directory = Path(prepared.receipt["observations_directory"])
        report = json.loads((directory / "stdout.log").read_bytes())
        archive = archived_report(prepared).read_bytes()
        change_statistics(report, damage)
        replace_stdout(directory, cli_bytes(report))
        before = state(tmp_path, ledger)
        calls = launches(monkeypatch)
        with pytest.raises(CoordinationError):
            observe(ledger, prepared, intent, prelaunch)
        assert calls == []
        assert archived_report(prepared).read_bytes() == archive
        assert state(tmp_path, ledger) == before


@pytest.mark.parametrize("damage", ["rows", "bootstrap", "semantic", "exclusions"])
def test_consistently_rehashed_cli_dictionary_is_contract_only_not_file_authority(
    retained_completion, monkeypatch, damage
):
    root, fixture, proof = retained_completion
    bound, prepared, ledger, intent, _ = fixture
    bad = json.loads(canonical(proof))
    change_statistics(bad["execution_result"], damage)
    captured_info(bad["execution_supervision"], cli_bytes(bad["execution_result"]))
    rebind(bad)
    # Contract validation deliberately does not authenticate archived files or data.
    core = entity_completion.validate_completion_observation(
        bad, bound, ledger.ledger_id, intent=intent
    )
    assert core["checkpoint_sha256"] == proof["result"]["checkpoint_sha256"]
    before = state(root, ledger)
    calls = launches(monkeypatch)
    with pytest.raises(CoordinationError):
        entity_completion.recheck_completion_observation(ledger, prepared, bad)
    assert calls == []
    assert state(root, ledger) == before


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "extra",
        "archive-boolean",
        "archive-negative",
        "archive-overflow",
        "hour-count",
        "checkpoint-hash",
        "entity-hash",
        "data-binding",
        "cli-run",
        "window",
        "command",
        "observation-scope",
        "clock",
    ],
)
def test_rehashed_completion_shapes_counts_hashes_clocks_and_scope_refuse(
    retained_completion, damage
):
    root, fixture, proof = retained_completion
    bound, _, ledger, intent, _ = fixture
    bad = json.loads(canonical(proof))
    result = bad["result"]
    if damage == "missing":
        result.pop("checkpoint_sha256")
    elif damage == "extra":
        result["synthetic_extra"] = "not a completion field"
    elif damage.startswith("archive-"):
        result["archive_records"] = {
            "archive-boolean": True,
            "archive-negative": -1,
            "archive-overflow": 2**63,
        }[damage]
    elif damage == "hour-count":
        result["checkpoint_hours"] += 1
    elif damage in {"checkpoint-hash", "entity-hash"}:
        result[
            "checkpoint_sha256"
            if damage == "checkpoint-hash"
            else "entity_state_sha256"
        ] = "A" * 64
    elif damage == "data-binding":
        result["binding_sha256"] = "a" * 64
    elif damage == "cli-run":
        result["cli_run_id"] = "b" * 32
    elif damage == "window":
        result["window_end"] = "2026-01-02T02:00:00Z"
    elif damage == "command":
        result["command"] = "compact"
    elif damage == "observation-scope":
        bad["observations_directory"] = str(
            root / "unrelated" / ("c" * 32) / "observations"
        )
    else:
        bad["observed_at"] = "2026-01-02T00:10:00Z"
    rebind(bad)
    before = state(root, ledger)
    with pytest.raises(CoordinationError):
        entity_completion.validate_completion_observation(
            bad, bound, ledger.ledger_id, intent=intent
        )
    assert state(root, ledger) == before


@pytest.mark.parametrize("side", ["cli", "verifier"])
@pytest.mark.parametrize(
    "damage",
    ["shutdown", "timeout", "exit", "command", "truncated", "saved-boolean", "extra"],
)
def test_both_supervision_contracts_remain_strict_after_consistent_rebinding(
    retained_completion, side, damage
):
    root, fixture, proof = retained_completion
    bound, _, ledger, intent, _ = fixture
    bad = json.loads(canonical(proof))
    supervision = bad["execution_supervision" if side == "cli" else "supervision"]
    if damage == "shutdown":
        supervision.update(status="shutdown_uncertain", children_stopped=False)
    elif damage == "timeout":
        supervision["status"] = "timeout"
    elif damage == "exit":
        supervision["exit_code"] = 2
    elif damage == "command":
        supervision["command_sha256"] = "a" * 64
    elif damage == "truncated":
        supervision["stdout"]["bytes_seen"] += 1
        supervision["stdout"]["truncated"] = True
        supervision["status"] = "output_limit"
    elif damage == "saved-boolean":
        supervision["stdout"]["bytes_saved"] = True
    else:
        supervision["synthetic_extra"] = False
    if side == "cli":
        rebind(bad)
    before = state(root, ledger)
    with pytest.raises(CoordinationError):
        entity_completion.validate_completion_observation(
            bad, bound, ledger.ledger_id, intent=intent
        )
    assert state(root, ledger) == before


@pytest.mark.parametrize(
    "budget",
    [
        "MAX_ENTITY_REQUEST_BYTES",
        "MAX_ENTITY_RESULT_BYTES",
        "MAX_ENTITY_OBSERVATION_BYTES",
    ],
)
def test_retained_completion_budgets_refuse_without_pruning_artifacts_or_rows(
    retained_completion, monkeypatch, budget
):
    root, fixture, proof = retained_completion
    _, prepared, ledger, _, _ = fixture
    originals = {
        str(p): p.read_bytes() for p in prepared.path.parent.rglob("*") if p.is_file()
    }
    before = state(root, ledger)
    calls = launches(monkeypatch)
    monkeypatch.setattr(entity_completion, budget, 1)
    with pytest.raises(CoordinationError):
        entity_completion.recheck_completion_observation(ledger, prepared, proof)
    assert calls == []
    assert all(
        Path(name).read_bytes() == payload for name, payload in originals.items()
    )
    assert state(root, ledger) == before


@pytest.mark.parametrize("name", ["entity", "metrics", "compacted"])
def test_fresh_standalone_completion_refuses_actual_corrupted_parquet_without_repair(
    tmp_path, monkeypatch, name
):
    with completed_fixture(tmp_path, multiple=True) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        path = next((tmp_path / name).rglob("*.parquet"))
        path.write_bytes(b"synthetic invalid parquet bytes")
        before = state(tmp_path, ledger)
        calls = launches(monkeypatch)
        with pytest.raises(CoordinationError):
            observe(ledger, prepared, intent, prelaunch)
        assert len(calls) == 1
        assert state(tmp_path, ledger) == before
        assert path.read_bytes() == b"synthetic invalid parquet bytes"


def test_competing_process_cannot_acquire_lock_during_completion_observation(
    tmp_path, monkeypatch
):
    with completed_fixture(tmp_path) as fixture:
        bound, prepared, ledger, intent, prelaunch = fixture
        actual, attempts = entity_completion.run_supervised, []
        script = """
import json,sys
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError
try:
    with Ledger.open(sys.argv[1],json.loads(sys.argv[2])): pass
except CoordinationError as error:
    assert 'busy' in str(error).lower()
    sys.exit(88)
raise AssertionError('competing process obtained completion-observation lock')
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

        monkeypatch.setattr(entity_completion, "run_supervised", compete)
        before = state(tmp_path, ledger)
        proof = observe(ledger, prepared, intent, prelaunch)
        assert proof["result"]["status"] == "verified"
        assert attempts == [88]
        assert state(tmp_path, ledger) == before


@pytest.mark.parametrize(
    "budget", ["MAX_ENTITY_INTENT_BYTES", "MAX_ENTITY_RUNS", "MAX_ENTITY_HISTORY_BYTES"]
)
def test_typed_history_budgets_still_precede_completion_launch(
    retained_completion, monkeypatch, budget
):
    root, fixture, _ = retained_completion
    _, prepared, ledger, intent, prelaunch = fixture
    before = state(root, ledger)
    calls = launches(monkeypatch)
    monkeypatch.setattr(entity_intent, budget, 0)
    with pytest.raises(CoordinationError):
        observe(ledger, prepared, intent, prelaunch)
    assert calls == []
    assert state(root, ledger) == before
