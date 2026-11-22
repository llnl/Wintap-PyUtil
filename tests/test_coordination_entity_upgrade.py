"""Explicit opt-in v5 preserves real v4 evidence; no ordinary-open migration."""

import os
import subprocess
import sys

import duckdb
import pytest

from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical, make_request

from .test_coordination_completion import NOW, complete, prepared_empty
from .test_coordination_execution import binding
from .test_coordination_intentional_retry_history import retained_rows
from .test_coordination_ledger import HOUR

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux actual upgrade boundaries and independently completed original",
)


def fixture(root):
    prepared = prepared_empty(root)
    bound = binding(root)
    with Ledger.open(root / "state", bound) as ledger:
        ledger.drain(NOW)
        complete(ledger, prepared)
        request = make_request(
            bound, "a" * 32, "dbt-build", HOUR, prepared.receipt["hour_end"], NOW
        )
        ledger.ingest(request, NOW)
        assert len(ledger.admit(NOW)) == 1
        rows = retained_rows(ledger)
    assert rows["metadata"][0][1] == 4
    return bound, prepared, rows


@pytest.mark.parametrize(
    "boundary", ["before_upgrade_commit", "after_upgrade_commit", "lost_ack"]
)
def test_v4_entity_upgrade_actual_boundaries_preserve_original_completion_and_reader(
    tmp_path, boundary
):
    bound, prepared, rows = fixture(tmp_path)
    code = """
import json,os,sys
from wintappy.coordination.ledger import Ledger
original=Ledger.__init__
def intercepted(self,*args,**kwargs):
    original(self,*args,**kwargs)
    def failure(phase):
        if sys.argv[3]=='lost_ack' and phase=='after_upgrade_commit':
            raise OSError('synthetic entity-upgrade acknowledgment loss')
        if phase==sys.argv[3]: os._exit(84)
    self.failure_hook=failure
Ledger.__init__=intercepted
try:
    Ledger.upgrade(sys.argv[1],json.loads(sys.argv[2]),sys.argv[4],
        target_version=5,confirmed=True,reason='synthetic explicit entity upgrade')
except OSError:
    if sys.argv[3]=='lost_ack': sys.exit(85)
    raise
raise AssertionError('actual entity-upgrade boundary not reached')
"""
    child = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            code,
            str(tmp_path / "state"),
            canonical(bound),
            boundary,
            NOW,
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=90,
    )
    assert child.returncode == (85 if boundary == "lost_ack" else 84), child.stderr
    committed = boundary != "before_upgrade_commit"
    with Ledger.open(tmp_path / "state", bound) as ledger:
        observed = retained_rows(ledger)
        assert observed["metadata"][0] == (
            1,
            5 if committed else 4,
            *rows["metadata"][0][2:],
        )
        for table, original in rows.items():
            if table not in {"metadata", "events"}:
                assert observed[table] == original
        added = [row for row in observed["events"] if row not in rows["events"]]
        assert sorted(row[2] for row in added) == (
            ["entity_history_initialized", "schema_upgraded"] if committed else []
        )
        assert ledger.snapshot()["readers"] == 1
        assert ledger.snapshot()["next_hour"] == prepared.receipt["hour_end"]
        assert ledger.completion_evidence(prepared.receipt["run_id"]) is not None
    if not committed:
        Ledger.upgrade(
            tmp_path / "state",
            bound,
            NOW,
            target_version=5,
            confirmed=True,
            reason="synthetic retry of uncommitted entity upgrade",
        )
    else:
        with pytest.raises(CoordinationError):
            Ledger.upgrade(
                tmp_path / "state",
                bound,
                NOW,
                target_version=5,
                confirmed=True,
                reason="synthetic committed entity upgrade must not repeat",
            )
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger._connection.execute(
            "SELECT schema_version FROM metadata"
        ).fetchone() == (5,)
        assert ledger.entity_run("b" * 32) is None
        assert ledger.entity_evidence("b" * 32) is None


@pytest.mark.parametrize("problem", ["confirmation", "reason", "target", "corrupt"])
def test_entity_upgrade_refuses_invalid_request_without_touching_original(
    tmp_path, problem
):
    bound, _, rows = fixture(tmp_path)
    values = {"target_version": 5, "confirmed": True, "reason": "synthetic upgrade"}
    if problem == "confirmation":
        values["confirmed"] = False
    elif problem == "reason":
        values["reason"] = None
    elif problem == "target":
        values["target_version"] = True
    else:
        with duckdb.connect(str(tmp_path / "state/coordinator.duckdb")) as connection:
            connection.execute("UPDATE outbox SET payload_sha256=?", ["f" * 64])
            rows["outbox"] = connection.execute(
                "SELECT * FROM outbox ORDER BY ALL"
            ).fetchall()
    with pytest.raises(CoordinationError):
        Ledger.upgrade(tmp_path / "state", bound, NOW, **values)
    with duckdb.connect(str(tmp_path / "state/coordinator.duckdb")) as connection:
        for table, original in rows.items():
            assert (
                connection.execute(f"SELECT * FROM {table} ORDER BY ALL").fetchall()
                == original
            )
