"""Genuine completion/resolution history: early SQL budgets and transaction-local tails.

Only fixture setup launches real writers/verifiers. Controls never recreate old data
or replace SQL results; injected damage and reduced limits are always rolled back.
Small real SQL pages require materialization before nested original-owner lookups.
"""

import json
import os
import sys
from contextlib import contextmanager

import pytest

from wintappy.coordination import (
    entity_history,
    entity_intent,
    entity_resolution,
    entity_writer,
)
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical

from .test_coordination_entity_operations import (
    HOUR,
    data_bytes,
    ledger_rows,
    prepare_entity,
)
from .test_coordination_entity_publication_qualification import (
    no_runtime_reads,
    oversized_utf8,
)
from .test_coordination_entity_publications import assert_originals, originals, retain
from .test_coordination_entity_resolution import (
    blocked_completion,
    resolution_rows,
    resolve,
)
from .test_coordination_entity_writer_history import record, staged

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux genuine completion/resolution history and independent supervision",
)


@pytest.fixture(scope="module")
def genuine_history(tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic-completion-resolution-sql-history")
    bound, first = blocked_completion(root, publication=True)
    with Ledger.open(root / "state", bound) as ledger:
        resolve(ledger, first)
        second = prepare_entity(root, bound, "entities-replay")

        def fail(phase):
            if phase == "after_entity_launch":
                intent = json.loads(
                    ledger.entity_run(second.receipt["run_id"])["intent_json"]
                )
                prelaunch = json.loads(
                    (second.path.parent / "prelaunch.json").read_text()
                )
                retain(
                    ledger,
                    second,
                    intent,
                    prelaunch,
                    now="2026-01-02T00:10:08Z",
                    clock=lambda: "2026-01-02T00:10:08Z",
                )
            if phase == "after_entity_completion_commit":
                entity_writer._block_uncertain(
                    ledger, second.receipt["run_id"], "2026-01-02T00:10:09Z"
                )
                raise OSError("synthetic actual replay COMMIT acknowledgment loss")

        ledger.failure_hook = fail
        with pytest.raises(CoordinationError):
            entity_writer.execute_entity_and_complete_prepared(
                ledger,
                second,
                "2026-01-02T00:10:08Z",
                clock=lambda: "2026-01-02T00:10:08Z",
                confirmed=True,
                reason="synthetic second genuine completion for SQL history",
            )
        assert ledger.snapshot()["mode"] == "BLOCKED"
        ledger.failure_hook = lambda phase: None
        resolve(
            ledger,
            second,
            now="2026-01-02T00:10:10Z",
            clock=lambda: "2026-01-02T00:10:10Z",
        )
        assert len(resolution_rows(ledger)) == 2
        assert [
            record(ledger, p.receipt["run_id"])["sequence"] for p in (first, second)
        ] == [1, 2]
        assert ledger.authority_high_water()["generation"] == 2
        assert ledger.snapshot()["next_hour"] == HOUR
        ledger._validate()
        yield root, ledger, (first, second)


@contextmanager
def no_original_reads(monkeypatch, *, execution_receipt=False):
    """Writer entry retains its real receipt prerequisite, not historical data reads."""
    with no_runtime_reads(monkeypatch), monkeypatch.context() as guarded:
        for name in (
            "run_supervised",
            "_read",
            "_observations",
            "recheck_input_observation",
            "recheck_entity_execution",
        ):
            if execution_receipt and name == "recheck_entity_execution":
                continue
            guarded.setattr(
                entity_writer,
                name,
                lambda *a, **k: pytest.fail(
                    "history fetched runtime originals or launched verifier"
                ),
            )
        yield


def snapshot(fixture):
    root, ledger, prepared = fixture
    return (
        ledger_rows(ledger),
        data_bytes(root),
        {k: v for p in prepared for k, v in originals(p).items()},
    )


def unchanged(fixture, before):
    rows, data, artifacts = before
    root, ledger, _ = fixture
    assert ledger_rows(ledger) == rows
    assert data_bytes(root) == data
    assert_originals(artifacts)


def sizes(ledger):
    result = {}
    for name, table, field, where in (
        ("runs", "entity_runs", "intent_json", ""),
        ("publications", "entity_evidence", "proof_json", ""),
        (
            "audits",
            "events",
            "details_json",
            "WHERE kind LIKE 'entity_%' AND kind<>'entity_history_initialized'",
        ),
        (
            "completions",
            "events",
            "details_json",
            "WHERE kind='entity_completion_record'",
        ),
        (
            "resolutions",
            "events",
            "details_json",
            "WHERE kind='entity_resolution_record'",
        ),
        (
            "small-audits",
            "events",
            "details_json",
            "WHERE kind LIKE 'entity_%' AND kind NOT IN ('entity_history_initialized','entity_completion_record','entity_resolution_record')",
        ),
    ):
        result[name] = ledger._connection.execute(
            f"SELECT count(*),coalesce(sum(octet_length(encode({field}))),0),coalesce(max(octet_length(encode({field}))),0) FROM {table} {where}"
        ).fetchone()
    return result


class Page:
    def __init__(self, guard, result, kind, parameters):
        self.guard, self.result, self.kind, self.parameters = (
            guard,
            result,
            kind,
            parameters,
        )

    def fetchall(self):
        assert self.guard.pending is self
        rows = self.result.fetchall()
        assert len(rows) <= 1
        self.guard.pages[self.kind].append((self.parameters, rows))
        self.guard.pending = None
        return rows


class HistorySQLGuard:
    """Only original real SQL; reject independent cursors and unmaterialized pages."""

    def __init__(self, connection, *, payloads=True, small=False, independent=False):
        self.connection, self.payloads, self.small = connection, payloads, small
        self.independent = independent
        self.pending = None
        self.transaction = False
        self.budgets = []
        self.scans = []
        self.pages = {
            kind: []
            for kind in (
                "owners",
                "publications",
                "completions",
                "owner-resolutions",
                "resolutions",
                "audits",
            )
        }

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def cursor(self):
        # Existing unrelated reader validation may use an independent cursor.
        # It must execute original SQL, and no typed history may escape onto it.
        return HistorySQLGuard(
            self.connection.cursor(),
            payloads=self.payloads,
            small=self.small,
            independent=True,
        )

    def execute(self, query, parameters=None):
        sql, args = " ".join(query.lower().split()), list(parameters or [])
        # A guard failure must never prevent rollback of injected corruption.
        assert self.pending is None or sql == "rollback", (
            "Typed SQL page was not materialized before a nested lookup"
        )
        event = (
            "from events" in sql
            and (
                "entity_" in sql
                or any(isinstance(v, str) and v.startswith("entity_") for v in args)
            )
            and "kind='entity_history_initialized'" not in sql
        )
        typed = "from entity_runs" in sql or "from entity_evidence" in sql or event
        if sql.startswith("select") and typed:
            if self.independent:
                pytest.fail("Typed history escaped its original transaction connection")
            if sql.startswith("select count(*)"):
                self.budgets.append((sql, args))
            elif not self.payloads:
                pytest.fail(
                    "Typed identifiers/JSON/audits fetched before SQL budget refusal"
                )
        kind = None
        if sql.startswith("select * from entity_runs") and "order by run_id" in sql:
            kind = "owners"
        elif (
            sql.startswith("select * from entity_evidence")
            and "order by sequence" in sql
        ):
            kind = "publications"
        elif (
            "try_cast(json_extract_string(details_json,'$.sequence') as bigint)>?"
            in sql
        ):
            if args[0] == entity_history.RECORD_KIND:
                kind = "completions"
            elif args[0] == entity_resolution.RECORD_KIND:
                kind = "owner-resolutions" if "and identity=?" in sql else "resolutions"
        elif sql.startswith("select event_id,kind,identity from events"):
            kind = "audits"
        if kind:
            assert "limit 256" in sql
            self.scans.append((kind, self.transaction, args))
            if self.small:
                query = query.replace("LIMIT 256", "LIMIT 1")
        if sql == "begin transaction":
            self.transaction = True
        result = self.connection.execute(query, args)
        if sql in {"commit", "rollback"}:
            self.transaction = False
        if sql == "rollback":
            self.pending = None
        if kind and self.small:
            page = Page(self, result, kind, args)
            self.pending = page
            return page
        return result


@contextmanager
def guarded(ledger, **settings):
    original = ledger._connection
    guard = HistorySQLGuard(original, **settings)
    ledger._connection = guard
    try:
        yield guard
    finally:
        ledger._connection = original


def enter(entry, ledger, prepared):
    if entry == "validate":
        return ledger._validate()
    if entry == "owner":
        return ledger.entity_run(prepared.receipt["run_id"])
    if entry == "evidence":
        return ledger.entity_evidence(prepared.receipt["run_id"])
    options = {
        "clock": lambda: "2026-01-02T00:10:11Z",
        "confirmed": True,
        "reason": "synthetic budget refusal before any original reads",
    }
    function = (
        entity_writer.resolve_entity_prepared
        if entry == "resolve"
        else entity_writer.execute_entity_and_complete_prepared
    )
    return function(ledger, prepared, "2026-01-02T00:10:11Z", **options)


@pytest.mark.parametrize(
    "entry", ["validate", "owner", "evidence", "writer", "resolve"]
)
@pytest.mark.parametrize(
    "budget",
    [
        "completion-record",
        "resolution-record",
        "completion-count",
        "resolution-count",
        "audit-record",
        "aggregate",
    ],
)
def test_real_completion_resolution_budgets_precede_every_typed_fetch(
    genuine_history, monkeypatch, entry, budget
):
    _, ledger, prepared = genuine_history
    before = snapshot(genuine_history)
    with staged(ledger), monkeypatch.context() as limited:
        if budget.endswith("record"):
            kind, limit = {
                "completion-record": (
                    entity_history.RECORD_KIND,
                    entity_history.MAX_ENTITY_COMPLETION_BYTES,
                ),
                "resolution-record": (
                    entity_resolution.RECORD_KIND,
                    entity_resolution.MAX_ENTITY_RESOLUTION_BYTES,
                ),
                "audit-record": (
                    "entity_resolved",
                    entity_intent.MAX_ENTITY_AUDIT_BYTES,
                ),
            }[budget]
            payload = oversized_utf8(limit)
            ledger._connection.execute(
                "UPDATE events SET details_json=? WHERE kind=?", [payload, kind]
            )
            assert len(payload) <= limit < len(payload.encode())
        elif budget.endswith("count"):
            module, name = (
                (entity_history, "MAX_ENTITY_COMPLETIONS")
                if budget == "completion-count"
                else (entity_resolution, "MAX_ENTITY_RESOLUTIONS")
            )
            assert (
                sizes(ledger)[
                    "completions" if budget == "completion-count" else "resolutions"
                ][0]
                == 2
            )
            limited.setattr(module, name, 1)
        else:
            measured = sizes(ledger)
            total = sum(measured[k][1] for k in ("runs", "publications", "audits"))
            assert measured["completions"][1] and measured["resolutions"][1]
            limited.setattr(entity_intent, "MAX_ENTITY_HISTORY_BYTES", total - 1)
        injected = ledger_rows(ledger)
        with (
            guarded(ledger, payloads=False) as guard,
            no_original_reads(monkeypatch, execution_receipt=entry == "writer"),
        ):
            with pytest.raises(CoordinationError, match="budget"):
                enter(entry, ledger, prepared[-1])
            assert guard.scans == [] and guard.budgets
        assert ledger_rows(ledger) == injected
    unchanged(genuine_history, before)


@pytest.mark.parametrize("entry", ["validate", "owner", "evidence"])
def test_exact_genuine_completion_resolution_budget_boundaries_never_prune(
    genuine_history, monkeypatch, entry
):
    _, ledger, prepared = genuine_history
    before, measured = snapshot(genuine_history), sizes(ledger)
    with monkeypatch.context() as limited, no_original_reads(monkeypatch):
        for module, count, record_size, key in (
            (
                entity_history,
                "MAX_ENTITY_COMPLETIONS",
                "MAX_ENTITY_COMPLETION_BYTES",
                "completions",
            ),
            (
                entity_resolution,
                "MAX_ENTITY_RESOLUTIONS",
                "MAX_ENTITY_RESOLUTION_BYTES",
                "resolutions",
            ),
        ):
            limited.setattr(module, count, measured[key][0])
            limited.setattr(module, record_size, measured[key][2])
        limited.setattr(entity_intent, "MAX_ENTITY_RUNS", measured["runs"][0])
        limited.setattr(entity_intent, "MAX_ENTITY_INTENT_BYTES", measured["runs"][2])
        limited.setattr(
            entity_intent, "MAX_ENTITY_AUDIT_BYTES", measured["small-audits"][2]
        )
        limited.setattr(
            entity_intent,
            "MAX_ENTITY_HISTORY_BYTES",
            sum(measured[k][1] for k in ("runs", "publications", "audits")),
        )
        result = enter(entry, ledger, prepared[-1])
        if entry == "owner":
            assert result["state"] == "COMPLETED"
        elif entry == "evidence":
            assert result["operation"] == "entity-only-completion"
    unchanged(genuine_history, before)


@pytest.mark.parametrize(
    "kind", [entity_history.RECORD_KIND, entity_resolution.RECORD_KIND]
)
@pytest.mark.parametrize(
    "damage", [None, "sequence-null", "sequence-hole", "predecessor"]
)
def test_real_sequence_keyset_pages_reach_genuine_completion_resolution_tails(
    genuine_history, monkeypatch, kind, damage
):
    _, ledger, _ = genuine_history
    before = snapshot(genuine_history)
    with (
        guarded(ledger, small=True) as guard,
        staged(ledger),
        no_original_reads(monkeypatch),
    ):
        tail = ledger._connection.execute(
            "SELECT event_id,details_json FROM events WHERE kind=? AND json_extract_string(details_json,'$.sequence')='2'",
            [kind],
        ).fetchone()
        assert tail is not None
        if damage:
            retained = json.loads(tail[1])
            if damage == "predecessor":
                retained["previous_sha256"] = "f" * 64
            else:
                retained["sequence"] = None if damage == "sequence-null" else 3
            ledger._connection.execute(
                "UPDATE events SET details_json=? WHERE event_id=?",
                [canonical(retained), tail[0]],
            )
        injected = ledger_rows(ledger)
        entity_intent._budget(ledger)
        validate = (
            entity_history.validate_history
            if kind == entity_history.RECORD_KIND
            else entity_resolution.validate_history
        )
        if damage:
            with pytest.raises(CoordinationError, match="chain|count"):
                validate(ledger)
        else:
            validate(ledger)
        pages = guard.pages[
            "completions" if kind == entity_history.RECORD_KIND else "resolutions"
        ]
        assert [len(rows) for _, rows in pages] == (
            [1, 1]
            if damage in {"sequence-hole", "predecessor"}
            else [1, 0]
            if damage
            else [1, 1, 0]
        )
        assert [args for args, _ in pages] == (
            [[kind, 0], [kind, 1]] if damage else [[kind, 0], [kind, 1], [kind, 2]]
        )
        assert all(transaction for _, transaction, _ in guard.scans)
        assert ledger_rows(ledger) == injected
    unchanged(genuine_history, before)


@pytest.mark.parametrize("damage", [None, "unsupported-audit", "orphan-audit"])
def test_full_transaction_history_materializes_all_genuine_pages_and_tail_audits(
    genuine_history, monkeypatch, damage
):
    _, ledger, _ = genuine_history
    before = snapshot(genuine_history)
    with (
        guarded(ledger, small=True) as guard,
        staged(ledger),
        no_original_reads(monkeypatch),
    ):
        if damage:
            assert (
                ledger._connection.execute(
                    "SELECT max(event_id) FROM events"
                ).fetchone()[0]
                < "f" * 32
            )
            ledger._connection.execute(
                "INSERT INTO events VALUES (?,?,?,?,?)",
                [
                    "f" * 32,
                    "2026-01-02T00:10:11Z",
                    "entity_unknown"
                    if damage == "unsupported-audit"
                    else "entity_intent",
                    "e" * 32,
                    "{}",
                ],
            )
        injected = ledger_rows(ledger)
        if damage:
            with pytest.raises(CoordinationError, match="Unsupported/orphan"):
                ledger._validate()
        else:
            ledger._validate()
        for key in ("owners", "publications", "completions", "resolutions"):
            assert [len(rows) for _, rows in guard.pages[key]] == [1, 1, 0]
        owner_pages = guard.pages["owner-resolutions"]
        assert [len(rows) for _, rows in owner_pages] == [1, 0, 1, 0]
        audits = guard.pages["audits"]
        ids = [row[0] for _, rows in audits for row in rows]
        assert ids == sorted(set(ids))
        assert [args for args, _ in audits] == [[]] + [
            [identity] for identity in (ids[:-1] if damage else ids)
        ]
        assert audits[-1][1][-1][0] == "f" * 32 if damage else audits[-1][1] == []
        assert all(transaction for _, transaction, _ in guard.scans)
        assert ledger_rows(ledger) == injected
    unchanged(genuine_history, before)
