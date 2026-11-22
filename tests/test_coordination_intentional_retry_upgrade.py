"""Actual v3 upgrade crash/ack boundaries preserve genuine completion and readers."""

import os
import subprocess
import sys

import duckdb
import pytest

from wintappy.coordination.ledger import SCHEMA_V3, TABLES, TABLES_V3, Ledger
from wintappy.coordination.protocol import (
    CoordinationError,
    canonical,
    make_cancellation,
    make_release,
    make_request,
)

from .test_coordination_completion import NOW, complete, prepared_empty
from .test_coordination_execution import binding
from .test_coordination_intentional_retry_history import retained_rows
from .test_coordination_ledger import HOUR

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux genuine supervised completion and actual v3 upgrade process boundaries",
)


@pytest.mark.parametrize(
    "boundary", ["before_upgrade_commit", "after_upgrade_commit", "lost_ack"]
)
def test_v3_upgrade_actual_commit_boundaries_preserve_every_original_row(
    tmp_path, boundary
):
    prepared = prepared_empty(tmp_path)
    bound = binding(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        ledger.drain(NOW)
        complete(ledger, prepared)
        for token in ("a", "b"):
            ledger.ingest(
                make_request(
                    bound,
                    token * 32,
                    "dbt-build",
                    HOUR,
                    prepared.receipt["hour_end"],
                    NOW,
                ),
                NOW,
            )
        grants = ledger.admit(NOW)
        ledger.ingest(make_release(grants[0], NOW, 0), NOW)
        cancelled = make_request(
            bound, "c" * 32, "dbt-build", HOUR, prepared.receipt["hour_end"], NOW
        )
        ledger.ingest(make_cancellation(cancelled, NOW), NOW)
        rows = retained_rows(ledger)
    # A synthetic older schema carries actual independently completed proof bytes;
    # never create a historical completion or invent a migration archive.
    legacy = tmp_path / "synthetic-v3"
    legacy.mkdir()
    database = legacy / "coordinator.duckdb"
    metadata = rows.pop("metadata")[0]
    assert not rows.pop("retry_dispositions")
    rows["events"] = [
        row for row in rows["events"] if row[2] != "retry_history_initialized"
    ]
    with duckdb.connect(str(database)) as connection:
        connection.execute(SCHEMA_V3)
        connection.execute(
            "INSERT INTO metadata VALUES (1,3,?,?,?)", list(metadata[2:])
        )
        for table, values in rows.items():
            if values:
                connection.executemany(
                    f"INSERT INTO {table} VALUES ({','.join('?' for _ in values[0])})",
                    values,
                )
    code = """
import json,os,sys
from wintappy.coordination.ledger import Ledger
original=Ledger.__init__
def intercepted(self,*args,**kwargs):
    original(self,*args,**kwargs)
    def failure(phase):
        if sys.argv[3]=='lost_ack' and phase=='after_upgrade_commit':
            raise OSError('synthetic committed-upgrade acknowledgment loss')
        if phase==sys.argv[3]: os._exit(84)
    self.failure_hook=failure
Ledger.__init__=intercepted
try:
    Ledger.upgrade(sys.argv[1],json.loads(sys.argv[2]),sys.argv[4],
        confirmed=True,reason='synthetic explicit v3 upgrade boundary')
except OSError:
    if sys.argv[3]=='lost_ack': sys.exit(85)
    raise
raise AssertionError('actual upgrade boundary was not reached')
"""
    child = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            code,
            str(legacy),
            canonical(bound),
            boundary,
            NOW,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert child.returncode == (85 if boundary == "lost_ack" else 84), child.stderr
    before_commit = boundary == "before_upgrade_commit"
    with duckdb.connect(str(database)) as connection:
        assert connection.execute("SELECT * FROM metadata").fetchone() == (
            1,
            3 if before_commit else 4,
            *metadata[2:],
        )
        assert {row[0] for row in connection.execute("SHOW TABLES").fetchall()} == (
            TABLES_V3 if before_commit else TABLES
        )
        for table, values in rows.items():
            observed = connection.execute(
                f"SELECT * FROM {table} ORDER BY ALL"
            ).fetchall()
            if table == "events" and not before_commit:
                added = [row for row in observed if row not in values]
                assert sorted(row[2] for row in added) == [
                    "retry_history_initialized",
                    "schema_upgraded",
                ]
                observed = [row for row in observed if row not in added]
            assert observed == values
    if before_commit:
        with pytest.raises(CoordinationError), Ledger.open(legacy, bound):
            pass
        Ledger.upgrade(
            legacy,
            bound,
            NOW,
            confirmed=True,
            reason="synthetic explicit retry of uncommitted v3 upgrade",
        )
    else:
        with pytest.raises(CoordinationError, match="Only compatible v1/v2/v3"):
            Ledger.upgrade(
                legacy,
                bound,
                NOW,
                confirmed=True,
                reason="synthetic committed upgrade must not repeat",
            )
    with Ledger.open(legacy, bound) as ledger:
        after = retained_rows(ledger)
        assert ledger.ledger_id == metadata[2]
        assert ledger.snapshot()["next_hour"] == prepared.receipt["hour_end"]
        assert ledger.snapshot()["readers"] == 1
        assert len(ledger.pending_grants()) == 1
        assert ledger.job(cancelled["job_id"])["state"] == "CANCELLED"
        assert ledger.completion_evidence(prepared.receipt["run_id"]) is not None
        assert not after["retry_dispositions"]
        for table, values in rows.items():
            if table != "events":
                assert after[table] == values
