"""Genuine typed originals: transaction visibility, early budgets and keyset scans."""

import json
import os
import sys
from contextlib import contextmanager

import pytest

from wintappy.coordination import entity_intent
from wintappy.coordination import ledger as ledger_module
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical, digest

from .test_coordination_authority import NOW
from .test_coordination_entity_intents import begin
from .test_coordination_entity_observations import scope
from .test_coordination_entity_operations import ledger_rows, seed_scope

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux genuine typed-history and unprivileged transaction qualification",
)


class PageResult:
    def __init__(self, result, pages):
        self.result = result
        self.pages = pages

    def fetchall(self):
        rows = self.result.fetchall()
        self.pages.append(rows)
        assert len(rows) <= 1
        return rows


class ConnectionGuard:
    """Delegate real SQL; never fabricate returned rows or replace validation."""

    def __init__(self, connection, *, independent=False, payloads=True, small=False):
        self.connection = connection
        self.independent = independent
        self.payloads = payloads
        self.small = small
        self.transaction = False
        self.scans = []
        self.pages = {"runs": [], "events": []}
        self.budgets = []

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def cursor(self):
        return ConnectionGuard(self.connection.cursor(), independent=True)

    def execute(self, query, parameters=None):
        sql = " ".join(query.lower().split())
        typed = "from entity_runs" in sql or "kind like 'entity_%'" in sql
        if typed and self.independent:
            pytest.fail(
                "Typed history scan escaped the original transaction connection"
            )
        if typed and sql.startswith("select count(*)"):
            self.budgets.append(sql)
        elif typed and not self.payloads:
            pytest.fail(
                "Typed owner identifiers/JSON/audits fetched before budget refusal"
            )
        kind = (
            "runs"
            if sql.startswith("select * from entity_runs") and "order by run_id" in sql
            else "events"
            if sql.startswith("select event_id,kind,identity from events")
            else None
        )
        if kind is not None:
            assert "limit 256" in sql
            self.scans.append((kind, self.transaction, list(parameters or [])))
            if self.small:
                # Smaller real SQL pages force continuation with only one unresolved
                # run and its two genuine audits; no completed entity history exists.
                query = query.replace("LIMIT 256", "LIMIT 1")
        if sql == "begin transaction":
            self.transaction = True
        result = self.connection.execute(query, parameters or [])
        if sql in {"commit", "rollback"}:
            self.transaction = False
        return PageResult(result, self.pages[kind]) if kind and self.small else result


@contextmanager
def guarded(ledger, **settings):
    original = ledger._connection
    guard = ConnectionGuard(original, **settings)
    ledger._connection = guard
    try:
        yield guard
    finally:
        ledger._connection = original


def blocked_scope(root):
    bound, prepared = scope(root)
    with Ledger.open(root / "state", bound) as ledger:
        begin(ledger, prepared)
    with Ledger.open(root / "state", bound) as ledger:
        assert ledger.entity_run(prepared.receipt["run_id"])["state"] == "BLOCKED"
    return bound, prepared


def test_upgrade_validates_staged_v5_tables_on_its_original_connection(
    tmp_path, monkeypatch
):
    bound, _ = seed_scope(tmp_path)
    connect = ledger_module._connect
    guards = []

    def traced_connect(*args, **kwargs):
        guard = ConnectionGuard(connect(*args, **kwargs))
        guards.append(guard)
        return guard

    with monkeypatch.context() as traced:
        traced.setattr(ledger_module, "_connect", traced_connect)
        Ledger.upgrade(
            tmp_path / "state",
            bound,
            NOW,
            target_version=5,
            confirmed=True,
            reason="synthetic staged-table visibility control",
        )
    assert len(guards) == 1
    assert {kind for kind, transaction, _ in guards[0].scans if transaction} == {
        "runs",
        "events",
    }
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.snapshot()["active_run_id"] is None


def test_intent_validation_observes_new_run_before_its_commit(tmp_path, monkeypatch):
    bound, prepared = scope(tmp_path)
    validate = entity_intent.validate_intent
    seen = []
    with Ledger.open(tmp_path / "state", bound) as ledger:
        with guarded(ledger) as guard:

            def traced_validate(owner, run):
                seen.append((guard.transaction, run["run_id"]))
                return validate(owner, run)

            with monkeypatch.context() as traced:
                traced.setattr(entity_intent, "validate_intent", traced_validate)
                intent = begin(ledger, prepared)
            assert (True, prepared.receipt["run_id"]) in seen
        assert ledger.entity_run(prepared.receipt["run_id"])[
            "intent_json"
        ] == canonical(intent)


@pytest.mark.parametrize("damage", ["audit", "json", "new-run"])
def test_staged_typed_corruption_refuses_and_rolls_back_exact_rows(tmp_path, damage):
    bound, prepared = scope(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        intent = begin(ledger, prepared)
        rows = ledger_rows(ledger)
        match = "Unsupported/orphan" if damage == "audit" else "Stored typed entity"
        with pytest.raises(CoordinationError, match=match), ledger._transaction():
            if damage == "audit":
                ledger._event("entity_unknown", prepared.receipt["run_id"], NOW, {})
            elif damage == "json":
                ledger._connection.execute(
                    "UPDATE entity_runs SET intent_json='{}',intent_sha256=?",
                    [digest({})],
                )
            else:
                bad = json.loads(canonical(intent))
                bad["run_id"], bad["cli_run_id"] = "f" * 32, "e" * 32
                ledger._connection.execute(
                    "INSERT INTO entity_runs VALUES (?,?,?,?,?,'INTENT',?,?,?,'')",
                    [
                        bad["run_id"],
                        bad["cli_run_id"],
                        bad["entity_command"],
                        bad["hour_start"],
                        bad["hour_end"],
                        NOW,
                        canonical(bad),
                        digest(bad),
                    ],
                )
            ledger._validate()
        assert ledger_rows(ledger) == rows
        ledger._validate()


@pytest.mark.parametrize("entry", ["validate", "lookup"])
@pytest.mark.parametrize(
    "budget", ["runs", "record", "total", "audit-record", "audits"]
)
def test_typed_budget_refuses_before_any_owner_or_payload_fetch(
    tmp_path, monkeypatch, entry, budget
):
    bound, prepared = blocked_scope(tmp_path)
    name, limit = {
        "runs": ("MAX_ENTITY_RUNS", 0),
        "record": ("MAX_ENTITY_INTENT_BYTES", 0),
        "total": ("MAX_ENTITY_HISTORY_BYTES", 0),
        "audit-record": ("MAX_ENTITY_AUDIT_BYTES", 0),
        "audits": ("MAX_ENTITY_RUNS", 1),
    }[budget]
    with Ledger.open(tmp_path / "state", bound) as ledger:
        if budget == "audits":
            # The genuine single run fits its limit, but a third typed audit does not.
            ledger._event("entity_unknown", "e" * 32, NOW, {})
        rows = ledger_rows(ledger)
        with monkeypatch.context() as limited, guarded(ledger, payloads=False) as guard:
            limited.setattr(entity_intent, name, limit)
            with pytest.raises(CoordinationError, match="exceeds budget"):
                if entry == "validate":
                    ledger._validate()
                else:
                    ledger.entity_run(prepared.receipt["run_id"])
            assert len(guard.budgets) == 2
            assert guard.scans == []
        assert ledger_rows(ledger) == rows


@pytest.mark.parametrize("damage", [None, "unsupported", "orphan"])
def test_real_keyset_pages_survive_nested_lookups_and_reach_tail_audits(
    tmp_path, damage
):
    bound, prepared = blocked_scope(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        if damage:
            ledger._connection.execute(
                "INSERT INTO events VALUES (?,?,?,?,?)",
                [
                    "f" * 32,
                    NOW,
                    "entity_unknown" if damage == "unsupported" else "entity_intent",
                    "e" * 32,
                    "{}",
                ],
            )
        rows = ledger_rows(ledger)
        with guarded(ledger, small=True) as guard:
            if damage:
                with pytest.raises(CoordinationError, match="Unsupported/orphan"):
                    ledger._validate()
            else:
                ledger._validate()
        assert [len(page) for page in guard.pages["runs"]] == [1, 0]
        assert [len(page) for page in guard.pages["events"]] == [
            1,
            1,
            1 if damage else 0,
        ]
        event_ids = [row[0] for page in guard.pages["events"] for row in page]
        assert event_ids == sorted(set(event_ids))
        assert len(event_ids) == (3 if damage else 2)
        scans = [parameters for kind, _, parameters in guard.scans if kind == "events"]
        assert scans == [[], [event_ids[0]], [event_ids[1]]]
        assert ledger_rows(ledger) == rows
        assert ledger.snapshot()["active_run_id"] == prepared.receipt["run_id"]


@pytest.mark.parametrize(
    "damage", ["reason", "cursor", "upgrade", "clock", "version", "authority"]
)
def test_rehashed_typed_semantic_corruption_never_reopens_or_prunes(tmp_path, damage):
    bound, prepared = blocked_scope(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        run_id = prepared.receipt["run_id"]
        intent = json.loads(ledger.entity_run(run_id)["intent_json"])
        if damage == "reason":
            intent["reason"] = None
        elif damage == "cursor":
            intent["raw_next_hour"] = "2026-01-02T00:00:00Z"
        elif damage == "upgrade":
            intent["schema_upgrade_sha256"] = "f" * 64
        elif damage == "clock":
            intent["started_at"] = "2026-01-02T00:09:59Z"
        elif damage == "version":
            intent["version"] = True
        else:
            intent["authority_before"] = {"synthetic-unverified": True}
        sha256 = digest(intent)
        with ledger._transaction():
            ledger._connection.execute(
                "UPDATE entity_runs SET intent_json=?,intent_sha256=?,started_at=? WHERE run_id=?",
                [canonical(intent), sha256, intent["started_at"], run_id],
            )
            ledger._connection.execute(
                "UPDATE events SET observed_at=?,details_json=? WHERE kind='entity_intent' AND identity=?",
                [intent["started_at"], canonical({"intent_sha256": sha256}), run_id],
            )
            ledger._connection.execute(
                "UPDATE events SET details_json=? WHERE kind='entity_unresolved' AND identity=?",
                [
                    canonical(
                        {
                            "reason": ledger.snapshot()["block_reason"],
                            "intent_sha256": sha256,
                        }
                    ),
                    run_id,
                ],
            )
        rows = ledger_rows(ledger)
    with pytest.raises(CoordinationError), Ledger.open(tmp_path / "state", bound):
        pass
    with ledger_module._connect(tmp_path / "state/coordinator.duckdb") as connection:
        assert ledger_rows(Ledger(connection, tmp_path / "state", bound)) == rows
