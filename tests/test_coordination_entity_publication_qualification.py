"""Retained originals: rehashed contracts, real SQL byte budgets and keyset tails.

One genuinely observed publication supplies isolated rollback-only corruption controls.
No historical evidence, entity service permission or successful typed completion is
fabricated. Small pages change only real SQL LIMITs, never returned rows or validators.
"""

import json
import os
import sys
from contextlib import contextmanager

import pytest

from wintappy.coordination import entity_completion, entity_intent, entity_publication
from wintappy.coordination import ledger as ledger_module
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical, digest

from .test_coordination_entity_completion_observations import completed_fixture
from .test_coordination_entity_completion_qualification import (
    duplicate_field,
    rebind,
    state,
)
from .test_coordination_entity_intent_history import ConnectionGuard, PageResult
from .test_coordination_entity_operations import data_bytes, ledger_rows
from .test_coordination_entity_publications import (
    RECORDED,
    assert_originals,
    originals,
    publication,
    retain,
)

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux genuine original publications, SQL budgets and staged history",
)


@contextmanager
def staged(ledger):
    """Always roll back injected SQL, including an unexpected acceptance/assertion."""
    ledger._connection.execute("BEGIN TRANSACTION")
    try:
        yield
    finally:
        ledger._connection.execute("ROLLBACK")


@contextmanager
def no_runtime_reads(monkeypatch):
    from wintappy.coordination import (
        entity_execution,
        entity_verification,
        verification,
    )

    def forbidden(*args, **kwargs):
        pytest.fail("Retained history must not reconstruct from runtime/artifacts/data")

    with monkeypatch.context() as guarded:
        for module, name in (
            (entity_execution, "recheck_entity_execution"),
            (entity_intent, "_read"),
            (entity_intent, "recheck_input_observation"),
            (entity_verification, "run_supervised"),
            (entity_completion, "_read"),
            (entity_completion, "_observations"),
            (entity_completion, "recheck_completion_observation"),
            (entity_completion, "run_supervised"),
            (verification, "_read"),
        ):
            guarded.setattr(module, name, forbidden)
        yield


def store(ledger, proof, *, payload=None):
    """Synchronize outer digest/audit only; neither makes corrupt scope authentic."""
    run_id = ledger.snapshot()["active_run_id"]
    sha256 = digest(proof)
    ledger._connection.execute(
        "UPDATE entity_evidence SET proof_json=?,proof_sha256=? WHERE run_id=?",
        [canonical(proof) if payload is None else payload, sha256, run_id],
    )
    ledger._connection.execute(
        "UPDATE events SET observed_at=?,details_json=? "
        "WHERE kind='entity_publication' AND identity=?",
        [
            proof["recorded_at"],
            canonical(
                {
                    "sequence": 1,
                    "intent_sha256": proof["intent_sha256"],
                    "proof_sha256": sha256,
                    "previous_sha256": "",
                }
            ),
            run_id,
        ],
    )


@pytest.fixture(scope="module")
def retained_original(tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic-publication-contract")
    with completed_fixture(root, multiple=True) as fixture:
        _, prepared, ledger, intent, prelaunch = fixture
        proof = retain(ledger, prepared, intent, prelaunch)
        # A genuine audited block, not completion, keeps every subsequent test away
        # from launch acknowledgment. Each test rolls back its own injected rows.
        entity_intent.block_entity(
            ledger,
            prepared.receipt["run_id"],
            RECORDED,
            "synthetic fixed-original publication qualification",
        )
        assert publication(ledger, prepared.receipt["run_id"]) == proof["completion"]
        yield root, fixture, proof


@pytest.mark.parametrize(
    "damage",
    [
        "version-bool",
        "operation",
        "ledger",
        "binding",
        "run",
        "cli",
        "intent",
        "recorded-before",
        "recorded-format",
        "observation-missing",
        "core-missing",
        "extra-field",
    ],
)
def test_rehashed_envelope_still_binds_exact_original_scope(
    retained_original, monkeypatch, damage
):
    root, (_, prepared, ledger, _, _), proof = retained_original
    bad = json.loads(canonical(proof))
    if damage == "version-bool":
        bad["version"] = True
    elif damage == "operation":
        bad["operation"] = "entity-only-completion"
    elif damage == "ledger":
        bad["ledger_id"] = "f" * 32
    elif damage == "binding":
        bad["binding"]["dataset_id"] = "synthetic-other-dataset"
    elif damage in {"run", "cli", "intent"}:
        key = {"run": "run_id", "cli": "cli_run_id", "intent": "intent_sha256"}[damage]
        bad[key] = "f" * (64 if damage == "intent" else 32)
    elif damage == "recorded-before":
        bad["recorded_at"] = "2026-01-02T00:10:01Z"
    elif damage == "recorded-format":
        bad["recorded_at"] = "2026-01-02T00:10:03+00:00"
    elif damage == "observation-missing":
        bad["observation"] = None
    elif damage == "core-missing":
        bad["completion"] = None
    else:
        bad["synthetic-extra"] = True
    before, files = state(root, ledger), originals(prepared)
    with staged(ledger), no_runtime_reads(monkeypatch):
        store(ledger, bad)
        injected = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            publication(ledger, prepared.receipt["run_id"])
        assert ledger_rows(ledger) == injected
    assert state(root, ledger) == before
    assert_originals(files)


@pytest.mark.parametrize(
    "damage",
    [
        "version",
        "hours-bool",
        "archive-count",
        "publication",
        "generation",
        "original",
        "inputs",
        "execution",
        "dataset",
        "run",
        "intent",
        "request-publication",
        "observation-clock",
        "prelaunch-alias",
        "result-extra",
    ],
)
def test_rehashed_observation_and_core_do_not_relax_original_contracts(
    retained_original, monkeypatch, damage
):
    root, (_, prepared, ledger, _, _), proof = retained_original
    bad = json.loads(canonical(proof))
    observation = bad["observation"]
    result = observation["result"]
    if damage == "version":
        result["version"] = True
    elif damage == "hours-bool":
        result["checkpoint_hours"] = True
    elif damage == "archive-count":
        result["archive_records"] += 1
    elif damage == "publication":
        result["publication_sha256"] = "f" * 64
    elif damage == "generation":
        result["authority_high_water"]["generation"] += 1
    elif damage in {"original", "inputs", "execution", "dataset", "run", "intent"}:
        key = {
            "original": "original_sha256",
            "inputs": "inputs_sha256",
            "execution": "execution_sha256",
            "dataset": "dataset_sha256",
            "run": "run_id",
            "intent": "intent_sha256",
        }[damage]
        result[key] = "f" * (32 if damage == "run" else 64)
    elif damage == "request-publication":
        observation["request"]["publication"] = proof["completion"]
    elif damage == "observation-clock":
        observation["observed_at"] = "2026-01-02T00:10:00Z"
    elif damage == "prelaunch-alias":
        observation["prelaunch"]["observations_directory"] = observation["receipt"][
            "observations_directory"
        ]
        # The execution directory cannot be the original post-COMMIT verifier path.
    else:
        result["synthetic-extra"] = True
    rebind(observation)
    bad["completion"] = {k: result[k] for k in entity_completion.COMPLETION_FIELDS}
    before, files = state(root, ledger), originals(prepared)
    with staged(ledger), no_runtime_reads(monkeypatch):
        store(ledger, bad)
        injected = ledger_rows(ledger)
        with pytest.raises(CoordinationError):
            publication(ledger, prepared.receipt["run_id"])
        assert ledger_rows(ledger) == injected
    assert state(root, ledger) == before
    assert_originals(files)


@pytest.mark.parametrize(
    "encoding", ["duplicate-top", "duplicate-core", "indent", "unsorted", "newline"]
)
def test_same_parsed_json_and_digest_cannot_replace_canonical_retained_bytes(
    retained_original, monkeypatch, encoding
):
    root, (_, prepared, ledger, _, _), proof = retained_original
    if encoding == "duplicate-top":
        payload = duplicate_field(canonical(proof).encode()).decode()
    elif encoding == "duplicate-core":
        core = canonical(proof["completion"])
        payload = canonical(proof).replace(
            '"completion":' + core,
            '"completion":' + duplicate_field(core.encode()).decode(),
            1,
        )
    elif encoding == "indent":
        payload = json.dumps(proof, sort_keys=True, indent=2)
    elif encoding == "unsorted":
        payload = json.dumps(dict(reversed(list(proof.items()))), separators=(",", ":"))
    else:
        payload = canonical(proof) + "\n"
    assert json.loads(payload) == proof and digest(json.loads(payload)) == digest(proof)
    assert payload != canonical(proof)
    before, files = state(root, ledger), originals(prepared)
    with staged(ledger), no_runtime_reads(monkeypatch):
        store(ledger, proof, payload=payload)
        injected = ledger_rows(ledger)
        with pytest.raises(CoordinationError, match="Stored entity publication"):
            publication(ledger, prepared.receipt["run_id"])
        assert ledger_rows(ledger) == injected
    assert state(root, ledger) == before
    assert_originals(files)


@pytest.mark.parametrize(
    "damage",
    [
        "sequence-bool",
        "sequence",
        "intent",
        "proof",
        "previous",
        "extra",
        "missing-key",
        "time",
        "duplicate",
        "noncanonical",
    ],
)
def test_publication_audit_requires_exact_unique_original_identity(
    retained_original, monkeypatch, damage
):
    root, (_, prepared, ledger, _, _), _ = retained_original
    run_id = prepared.receipt["run_id"]
    recorded, payload = ledger._connection.execute(
        "SELECT observed_at,details_json FROM events "
        "WHERE kind='entity_publication' AND identity=?",
        [run_id],
    ).fetchone()
    details = json.loads(payload)
    if damage == "sequence-bool":
        details["sequence"] = True
    elif damage == "sequence":
        details["sequence"] += 1
    elif damage in {"intent", "proof", "previous"}:
        key = {
            "intent": "intent_sha256",
            "proof": "proof_sha256",
            "previous": "previous_sha256",
        }[damage]
        details[key] = "f" * 64
    elif damage == "extra":
        details["synthetic-extra"] = True
    elif damage == "missing-key":
        del details["previous_sha256"]
    elif damage == "time":
        recorded = "2026-01-02T00:10:01Z"
    payload = canonical(details) + ("\n" if damage == "noncanonical" else "")
    before, files = state(root, ledger), originals(prepared)
    with staged(ledger), no_runtime_reads(monkeypatch):
        if damage == "duplicate":
            ledger._connection.execute(
                "INSERT INTO events VALUES (?,?,?,?,?)",
                ["f" * 32, recorded, "entity_publication", run_id, payload],
            )
        else:
            ledger._connection.execute(
                "UPDATE events SET observed_at=?,details_json=? "
                "WHERE kind='entity_publication' AND identity=?",
                [recorded, payload, run_id],
            )
        injected = ledger_rows(ledger)
        with pytest.raises(CoordinationError, match="publication/original/audit"):
            publication(ledger, run_id)
        assert ledger_rows(ledger) == injected
    assert state(root, ledger) == before
    assert_originals(files)


class PublicationSQLGuard(ConnectionGuard):
    """All rows come from the original real connection, with budget-before-fetch."""

    def __init__(self, connection, **settings):
        super().__init__(connection, **settings)
        self.pages["publications"] = []

    def cursor(self):
        return PublicationSQLGuard(
            self.connection.cursor(), independent=True, payloads=self.payloads
        )

    def execute(self, query, parameters=None):
        sql = " ".join(query.lower().split())
        history = (
            sql.startswith("select * from entity_evidence")
            and "order by sequence" in sql
        )
        if sql.startswith("select") and "from entity_evidence" in sql:
            if self.independent:
                pytest.fail(
                    "Publication SQL escaped its original transaction connection"
                )
            if sql.startswith("select count(*)"):
                self.budgets.append(sql)
            elif not self.payloads:
                pytest.fail(
                    "Publication identifiers/JSON fetched before SQL budget refusal"
                )
        if history:
            assert "limit 256" in sql
            self.scans.append(
                ("publications", self.transaction, list(parameters or []))
            )
            if self.small:
                query = query.replace("LIMIT 256", "LIMIT 1")
        result = super().execute(query, parameters)
        return (
            PageResult(result, self.pages["publications"])
            if history and self.small
            else result
        )


@contextmanager
def guarded(ledger, **settings):
    original = ledger._connection
    guard = PublicationSQLGuard(original, **settings)
    ledger._connection = guard
    try:
        yield guard
    finally:
        ledger._connection = original


def sizes(ledger):
    return {
        name: ledger._connection.execute(
            f"SELECT count(*),coalesce(sum(octet_length(encode({field}))),0),"
            f"coalesce(max(octet_length(encode({field}))),0) FROM {table} {where}"
        ).fetchone()
        for name, table, field, where in (
            ("runs", "entity_runs", "intent_json", ""),
            ("publications", "entity_evidence", "proof_json", ""),
            (
                "audits",
                "events",
                "details_json",
                "WHERE kind LIKE 'entity_%' AND kind<>'entity_history_initialized'",
            ),
        )
    }


def oversized_utf8(limit):
    payload = '{"synthetic_padding":"' + "é" * (limit // 2 + 1) + '"}'
    assert len(payload) <= limit < len(payload.encode())
    return payload


def enter(entry, ledger, prepared, intent, prelaunch):
    run_id = prepared.receipt["run_id"]
    if entry == "validate":
        return ledger._validate()
    if entry == "owner":
        return ledger.entity_run(run_id)
    if entry == "evidence":
        return ledger.entity_evidence(run_id)
    if entry == "retain":
        return retain(ledger, prepared, intent, prelaunch)
    return publication(ledger, run_id)


@pytest.mark.parametrize(
    "entry", ["validate", "owner", "evidence", "accessor", "retain"]
)
@pytest.mark.parametrize(
    "budget", ["record", "audit-record", "aggregate", "count", "audit-count"]
)
def test_real_sql_budgets_precede_all_typed_identifiers_and_payloads(
    retained_original, monkeypatch, entry, budget
):
    root, (_, prepared, ledger, intent, prelaunch), proof = retained_original
    before, files = state(root, ledger), originals(prepared)
    with staged(ledger), monkeypatch.context() as limited:
        if budget == "record":
            ledger._connection.execute(
                "UPDATE entity_evidence SET proof_json=?",
                [oversized_utf8(entity_publication.MAX_ENTITY_PUBLICATION_BYTES)],
            )
            assert (
                sizes(ledger)["publications"][2]
                > entity_publication.MAX_ENTITY_PUBLICATION_BYTES
            )
        elif budget == "audit-record":
            ledger._connection.execute(
                "UPDATE events SET details_json=? WHERE kind='entity_publication'",
                [oversized_utf8(entity_intent.MAX_ENTITY_AUDIT_BYTES)],
            )
            assert sizes(ledger)["audits"][2] > entity_intent.MAX_ENTITY_AUDIT_BYTES
        elif budget == "aggregate":
            measured = sizes(ledger)
            total = sum(row[1] for row in measured.values())
            assert 0 < total - measured["publications"][1] <= total - 1
            limited.setattr(entity_intent, "MAX_ENTITY_HISTORY_BYTES", total - 1)
        elif budget == "count":
            ledger._connection.execute(
                "INSERT INTO entity_evidence VALUES (2,?,?,?,?)",
                ["f" * 32, "{}", digest({}), digest(proof)],
            )
            limited.setattr(entity_publication, "MAX_ENTITY_PUBLICATIONS", 1)
            assert sizes(ledger)["publications"][0] == 2
        else:
            ledger._event("entity_unknown", "e" * 32, RECORDED, {})
            limited.setattr(entity_intent, "MAX_ENTITY_RUNS", 1)
            measured = sizes(ledger)
            assert measured["runs"][0] == measured["publications"][0] == 1
            # Only three audits fit this real original under the reduced count limit.
            assert measured["audits"][0] == 4
        injected = ledger_rows(ledger)
        with guarded(ledger, payloads=False) as guard, no_runtime_reads(monkeypatch):
            with pytest.raises(CoordinationError, match="budget"):
                enter(entry, ledger, prepared, intent, prelaunch)
            assert len(guard.budgets) == 3 and guard.scans == []
        assert ledger_rows(ledger) == injected
    assert state(root, ledger) == before
    assert_originals(files)


def test_exact_positive_sql_budget_boundaries_authenticate_without_pruning(
    retained_original, monkeypatch
):
    root, (_, prepared, ledger, _, _), proof = retained_original
    measured = sizes(ledger)
    before, files = state(root, ledger), originals(prepared)
    with monkeypatch.context() as limited, no_runtime_reads(monkeypatch):
        limited.setattr(
            entity_publication, "MAX_ENTITY_PUBLICATIONS", measured["publications"][0]
        )
        limited.setattr(
            entity_publication,
            "MAX_ENTITY_PUBLICATION_BYTES",
            measured["publications"][2],
        )
        limited.setattr(entity_intent, "MAX_ENTITY_RUNS", measured["runs"][0])
        limited.setattr(entity_intent, "MAX_ENTITY_INTENT_BYTES", measured["runs"][2])
        limited.setattr(entity_intent, "MAX_ENTITY_AUDIT_BYTES", measured["audits"][2])
        limited.setattr(
            entity_intent,
            "MAX_ENTITY_HISTORY_BYTES",
            sum(row[1] for row in measured.values()),
        )
        assert publication(ledger, prepared.receipt["run_id"]) == proof["completion"]
    assert state(root, ledger) == before
    assert_originals(files)


@pytest.mark.parametrize(
    "damage",
    [
        None,
        "orphan-publication",
        "tail-sequence",
        "tail-predecessor",
        "unsupported-audit",
        "orphan-audit",
    ],
)
def test_real_materialized_keyset_pages_reach_publication_and_audit_tails(
    retained_original, monkeypatch, damage
):
    root, (_, prepared, ledger, _, _), proof = retained_original
    before, files = state(root, ledger), originals(prepared)
    with (
        guarded(ledger, small=True) as guard,
        staged(ledger),
        no_runtime_reads(monkeypatch),
    ):
        evidence_tail = damage in {
            "orphan-publication",
            "tail-sequence",
            "tail-predecessor",
        }
        if evidence_tail:
            bad = json.loads(canonical(proof))
            bad["run_id"], bad["cli_run_id"] = "f" * 32, "e" * 32
            ledger._connection.execute(
                "INSERT INTO entity_evidence VALUES (?,?,?,?,?)",
                [
                    3 if damage == "tail-sequence" else 2,
                    bad["run_id"],
                    canonical(bad),
                    digest(bad),
                    "f" * 64 if damage == "tail-predecessor" else digest(proof),
                ],
            )
        elif damage:
            ledger._connection.execute(
                "INSERT INTO events VALUES (?,?,?,?,?)",
                [
                    "f" * 32,
                    RECORDED,
                    "entity_unknown"
                    if damage == "unsupported-audit"
                    else "entity_publication",
                    "e" * 32,
                    "{}",
                ],
            )
        injected = ledger_rows(ledger)
        if damage:
            match = "publication" if evidence_tail else "Unsupported/orphan"
            with pytest.raises(CoordinationError, match=match):
                ledger._validate()
        else:
            ledger._validate()
        assert [len(page) for page in guard.pages["runs"]] == [1, 0]
        assert [len(page) for page in guard.pages["publications"]] == (
            [1, 1] if evidence_tail else [1, 0]
        )
        publication_scans = [
            parameters
            for kind, transaction, parameters in guard.scans
            if kind == "publications" and transaction
        ]
        assert publication_scans == [[], [1]]
        if not evidence_tail:
            events = [row for page in guard.pages["events"] for row in page]
            ids = [row[0] for row in events]
            assert ids == sorted(set(ids)) and len(ids) == (4 if damage else 3)
            assert [len(page) for page in guard.pages["events"]] == (
                [1, 1, 1, 1] if damage else [1, 1, 1, 0]
            )
            event_scans = [
                parameters
                for kind, transaction, parameters in guard.scans
                if kind == "events" and transaction
            ]
            assert event_scans == [[], [ids[0]], [ids[1]], [ids[2]]]
        assert ledger_rows(ledger) == injected
    assert state(root, ledger) == before
    assert_originals(files)


@pytest.mark.parametrize("damage", ["noncanonical", "scope", "utf8-record"])
def test_persisted_corruption_refuses_reopen_without_erasing_originals(
    tmp_path, monkeypatch, damage
):
    with completed_fixture(tmp_path) as fixture:
        bound, prepared, ledger, intent, prelaunch = fixture
        proof = retain(ledger, prepared, intent, prelaunch)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.entity_run(prepared.receipt["run_id"])["state"] == "BLOCKED"
    files, data = originals(prepared), data_bytes(tmp_path)
    with ledger_module._connect(tmp_path / "state/coordinator.duckdb") as connection:
        ledger = Ledger(connection, tmp_path / "state", bound)
        if damage == "noncanonical":
            store(
                ledger,
                proof,
                payload=duplicate_field(canonical(proof).encode()).decode(),
            )
        elif damage == "scope":
            bad = json.loads(canonical(proof))
            bad["observation"]["result"]["checkpoint_hours"] += 1
            rebind(bad["observation"])
            bad["completion"]["checkpoint_hours"] += 1
            store(ledger, bad)
        else:
            connection.execute(
                "UPDATE entity_evidence SET proof_json=?",
                [oversized_utf8(entity_publication.MAX_ENTITY_PUBLICATION_BYTES)],
            )
        injected = ledger_rows(ledger)
    with (
        no_runtime_reads(monkeypatch),
        pytest.raises(CoordinationError),
        Ledger.open(tmp_path / "state", bound),
    ):
        pass
    with ledger_module._connect(tmp_path / "state/coordinator.duckdb") as connection:
        assert ledger_rows(Ledger(connection, tmp_path / "state", bound)) == injected
    assert data_bytes(tmp_path) == data
    assert_originals(files)
