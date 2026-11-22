"""Entity permissions need independent, fresh authority under the ledger lock."""

import json
import subprocess
import sys

import pytest

from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import (
    CoordinationError,
    dataset_binding,
    make_request,
)

HOUR = "2026-01-01T23:00:00Z"
NOW = "2026-01-02T00:10:00Z"


def binding(tmp_path):
    return dataset_binding(
        "synthetic-high-water",
        {key: str(tmp_path / key) for key in ("raw", "compacted", "entity", "metrics")},
        {
            "namespace": "synthetic-namespace",
            "hosts": ["SYNTHETIC-HOST"],
            "identity_policy": "quarantine",
            "identity_max_ratio": 0.025,
        },
    )


def test_entity_reader_requires_independently_initialized_high_water(tmp_path):
    bound = binding(tmp_path)
    Ledger.initialize(tmp_path / "state", bound, HOUR)
    req = make_request(bound, "a" * 32, "dbt-build", HOUR, "2026-01-02T00:00:00Z", NOW)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        ledger.ingest(req, NOW)
        with pytest.raises(CoordinationError, match="authority|high-water"):
            ledger.admit(NOW)
        assert ledger.snapshot()["readers"] == 0
        assert ledger.job(req["job_id"])["state"] == "WAITING"


def test_entity_writer_requires_independently_initialized_high_water(tmp_path):
    bound = binding(tmp_path)
    Ledger.initialize(tmp_path / "state", bound, HOUR)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        ledger.drain(NOW)
        with pytest.raises(CoordinationError, match="authority|high-water"):
            ledger.begin_writer(HOUR, NOW, ["synthetic-compact"])
        assert ledger.snapshot()["active_run_id"] is None


@pytest.mark.parametrize("confirmed", [False, True])
def test_schema_upgrade_is_explicit_and_preserves_existing_authority(
    tmp_path, confirmed
):
    import duckdb

    from wintappy.coordination.ledger import SCHEMA_V1
    from wintappy.coordination.protocol import canonical

    bound = binding(tmp_path)
    state = tmp_path / "state"
    state.mkdir()
    with duckdb.connect(str(state / "coordinator.duckdb")) as conn:
        conn.execute(SCHEMA_V1)
        conn.execute(
            "INSERT INTO metadata VALUES (1,1,?,?)", ["a" * 32, canonical(bound)]
        )
        conn.execute("INSERT INTO dataset_state VALUES (1,'OPEN',?,NULL,'')", [HOUR])
    with pytest.raises(CoordinationError, match="upgrade"), Ledger.open(state, bound):
        pass
    if not confirmed:
        with pytest.raises(CoordinationError):
            Ledger.upgrade(
                state, bound, NOW, confirmed=False, reason="synthetic upgrade"
            )
    else:
        Ledger.upgrade(state, bound, NOW, confirmed=True, reason="synthetic upgrade")
        with Ledger.open(state, bound) as ledger:
            assert ledger.ledger_id == "a" * 32
            assert ledger.authority_high_water() is None
            assert ledger.snapshot()["next_hour"] == HOUR


def prepared_entities(tmp_path):
    from wintappy.coordination.execution import prepare_execution

    from .test_coordination_execution import PROJECT, config

    bound = binding(tmp_path)
    Ledger.initialize(tmp_path / "state", bound, HOUR)
    script = """
import sys,duckdb
from pathlib import Path
from raw_sensor_compact.config import Dataset,hour_window
from raw_sensor_compact.inventory import Store
from raw_sensor_compact.entity_store import EntityStore,update_entities
root=Path(sys.argv[1]); c=duckdb.connect()
stores=[]
for name in ('raw','compacted','entity','metrics'):
    path=root/name; path.mkdir(); stores.append(Store(Dataset.parse(str(path),event_root=name in ('raw','compacted'))))
path=root/'raw/raw_process/dayPK=20260101/hourPK=23/part.parquet'; path.parent.mkdir(parents=True)
c.execute("COPY (SELECT 'SYNTHETIC-HOST' hostname,'synthetic-child' pidhash,10::BIGINT pid,'synthetic-parent' parentpidhash,9::BIGINT parentpid,'START' activitytype,100::BIGINT eventtime,'synthetic-process' processname,'/synthetic' processpath,'synthetic-account' username,'' processargs,'synthetic-agent' agentid) TO ? (FORMAT PARQUET)",[str(path)])
entities=EntityStore(stores[2],'synthetic-namespace')
update_entities(c,stores[0],stores[1],entities,hour_window('2026-01-01T23:00:00Z','2026-01-02T00:00:00Z'),['SYNTHETIC-HOST'],initialize=True,identity_policy='quarantine',identity_max_ratio=0.025,metrics_root=stores[3].dataset.root)
"""
    subprocess.run(
        [str(PROJECT / ".venv/bin/python"), "-I", "-B", "-c", script, str(tmp_path)],
        check=True,
        timeout=20,
        env={},
    )
    return bound, prepare_execution(
        tmp_path / "state", bound, HOUR, NOW, config(tmp_path)
    )


@pytest.mark.skipif(
    sys.platform != "linux", reason="Qualified authority supervision requires Linux"
)
def test_real_verified_authority_initialization_and_fresh_session_gate(tmp_path):
    from wintappy.coordination.verification import verify_authority_prepared

    bound, prepared = prepared_entities(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        anchor = verify_authority_prepared(
            ledger,
            prepared,
            NOW,
            initialize=True,
            confirmed=True,
            reason="synthetic verified initialization",
        )
        assert anchor == ledger.authority_high_water()
        assert anchor["generation"] == 1
    with Ledger.open(tmp_path / "state", bound) as ledger:
        req = make_request(
            bound, "b" * 32, "dbt-build", HOUR, "2026-01-02T00:00:00Z", NOW
        )
        ledger.ingest(req, NOW)
        with pytest.raises(CoordinationError):
            ledger.admit(NOW)
        assert verify_authority_prepared(ledger, prepared, NOW) == anchor
        assert len(ledger.admit(NOW)) == 1
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.snapshot()["readers"] == 1
        assert ledger.authority_high_water() == anchor


@pytest.mark.parametrize(
    "problem",
    [
        "no_confirmation",
        "no_reason",
        "missing_snapshot",
        "missing_authority",
        "changed_receipt",
    ],
)
@pytest.mark.skipif(
    sys.platform != "linux", reason="Qualified authority supervision requires Linux"
)
def test_initialization_refuses_unverified_or_unconfirmed_baseline(tmp_path, problem):
    from wintappy.coordination.verification import verify_authority_prepared

    bound, prepared = prepared_entities(tmp_path)
    if problem == "missing_snapshot":
        next((tmp_path / "entity").glob("hostPK=*/entities.parquet")).unlink()
    elif problem == "missing_authority":
        (tmp_path / "entity/_authority/authority.parquet").unlink()
    elif problem == "changed_receipt":
        prepared.path.write_bytes(prepared.path.read_bytes() + b" ")
    with Ledger.open(tmp_path / "state", bound) as ledger:
        with pytest.raises(CoordinationError):
            verify_authority_prepared(
                ledger,
                prepared,
                NOW,
                initialize=True,
                confirmed=problem != "no_confirmation",
                reason=None if problem == "no_reason" else "synthetic initialization",
            )
        assert ledger.authority_high_water() is None
        assert ledger.snapshot()["active_run_id"] is None


def evolve(tmp_path, eventtime=200):
    from .test_coordination_execution import PROJECT

    script = """
import sys,duckdb
from pathlib import Path
from raw_sensor_compact.config import Dataset,hour_window
from raw_sensor_compact.inventory import Store
from raw_sensor_compact.entity_store import EntityStore,update_entities
root=Path(sys.argv[1]); c=duckdb.connect()
source=Store(Dataset.parse(str(root/'raw'),event_root=True))
compacted=Store(Dataset.parse(str(root/'compacted'),event_root=True))
entities=EntityStore(Store(Dataset.parse(str(root/'entity'))),'synthetic-namespace')
path=root/'raw/raw_process/dayPK=20260102/hourPK=00/part.parquet'; path.parent.mkdir(parents=True,exist_ok=True)
c.execute("COPY (SELECT * REPLACE ('STOP' AS activitytype,?::BIGINT AS eventtime) FROM read_parquet(?,hive_partitioning=false)) TO ? (FORMAT PARQUET)",[int(sys.argv[2]),str(root/'raw/raw_process/dayPK=20260101/hourPK=23/part.parquet'),str(path)])
update_entities(c,source,compacted,entities,hour_window('2026-01-02T00:00:00Z','2026-01-02T01:00:00Z'),['SYNTHETIC-HOST'],identity_policy='quarantine',identity_max_ratio=0.025,metrics_root=str(root/'metrics'))
"""
    subprocess.run(
        [
            str(PROJECT / ".venv/bin/python"),
            "-I",
            "-B",
            "-c",
            script,
            str(tmp_path),
            str(eventtime),
        ],
        check=True,
        timeout=20,
        env={},
    )


@pytest.mark.skipif(
    sys.platform != "linux", reason="Qualified authority supervision requires Linux"
)
def test_complete_journal_snapshot_rollback_fails_retained_ledger_but_forward_is_valid(
    tmp_path,
):
    from wintappy.coordination.verification import verify_authority_prepared

    bound, prepared = prepared_entities(tmp_path)
    original = {p: p.read_bytes() for p in (tmp_path / "entity").rglob("*.parquet")}
    with Ledger.open(tmp_path / "state", bound) as ledger:
        initial = verify_authority_prepared(
            ledger,
            prepared,
            NOW,
            initialize=True,
            confirmed=True,
            reason="synthetic baseline",
        )
        evolve(tmp_path)
        current = verify_authority_prepared(ledger, prepared, NOW)
        assert current["generation"] > initial["generation"]
        req = make_request(
            bound, "b" * 32, "dbt-build", HOUR, "2026-01-02T00:00:00Z", NOW
        )
        ledger.ingest(req, NOW)
        for path, payload in original.items():
            path.write_bytes(payload)
        with pytest.raises(CoordinationError):
            verify_authority_prepared(ledger, prepared, NOW)
        assert ledger.authority_high_water() == current
        with pytest.raises(CoordinationError):
            ledger.admit(NOW)
        assert ledger.snapshot()["readers"] == 0
        with pytest.raises(CoordinationError):
            verify_authority_prepared(
                ledger,
                prepared,
                NOW,
                initialize=True,
                confirmed=True,
                reason="synthetic forbidden rebasing",
            )


@pytest.mark.parametrize("phase", ["before_commit", "after_commit"])
@pytest.mark.skipif(
    sys.platform != "linux", reason="Qualified authority supervision requires Linux"
)
def test_authority_commit_uncertainty_never_admits_or_fabricates_evidence(
    tmp_path, phase
):
    from wintappy.coordination.verification import verify_authority_prepared

    bound, prepared = prepared_entities(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:

        def fail(observed):
            if observed == phase:
                raise OSError("synthetic authority COMMIT uncertainty")

        ledger.failure_hook = fail
        with pytest.raises(CoordinationError):
            verify_authority_prepared(
                ledger,
                prepared,
                NOW,
                initialize=True,
                confirmed=True,
                reason="synthetic baseline",
            )
        assert (ledger.authority_high_water() is not None) == (phase == "after_commit")
        ledger.failure_hook = lambda observed: None
        with pytest.raises(CoordinationError):
            ledger.admit(NOW)


@pytest.mark.skipif(
    sys.platform != "linux", reason="Qualified authority supervision requires Linux"
)
def test_authority_history_budget_never_prunes_and_failed_check_clears_freshness(
    tmp_path, monkeypatch
):
    from wintappy.coordination import authority
    from wintappy.coordination.verification import verify_authority_prepared

    bound, prepared = prepared_entities(tmp_path)
    monkeypatch.setattr(authority, "MAX_AUTHORITY_CHECKS", 1)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        anchor = verify_authority_prepared(
            ledger,
            prepared,
            NOW,
            initialize=True,
            confirmed=True,
            reason="synthetic baseline",
        )
        with pytest.raises(CoordinationError):
            verify_authority_prepared(ledger, prepared, NOW)
        assert ledger.authority_high_water() == anchor
        assert (
            ledger._connection.execute(
                "SELECT count(*) FROM authority_checks"
            ).fetchone()[0]
            == 1
        )
        with pytest.raises(CoordinationError):
            ledger.admit(NOW)


@pytest.mark.skipif(
    sys.platform != "linux", reason="Qualified authority supervision requires Linux"
)
def test_authority_writer_intent_consumes_freshness_and_reopen_never_relaunches(
    tmp_path,
):
    from wintappy.coordination.verification import verify_authority_prepared

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
        ledger.drain(NOW)
        run = ledger.begin_writer(
            HOUR,
            NOW,
            list(prepared.command),
            execution={
                "path": str(prepared.path),
                "sha256": prepared.sha256,
                "receipt": prepared.receipt,
            },
        )
        assert ledger._authority_checked is None
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert ledger.snapshot()["mode"] == "BLOCKED"
        assert ledger.writer_run(run["run_id"])["state"] == "BLOCKED"
        with pytest.raises(CoordinationError):
            verify_authority_prepared(ledger, prepared, NOW)


@pytest.mark.skipif(
    sys.platform != "linux", reason="Qualified authority supervision requires Linux"
)
def test_authority_observation_corruption_fails_ordinary_open_without_replacing_anchor(
    tmp_path,
):
    from wintappy.coordination.verification import verify_authority_prepared

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
        payload = ledger._connection.execute(
            "SELECT proof_json FROM authority_checks"
        ).fetchone()[0]
        ledger._connection.execute(
            "UPDATE authority_checks SET proof_json=?", [payload + " "]
        )
    with pytest.raises(CoordinationError), Ledger.open(tmp_path / "state", bound):
        pass


@pytest.mark.skipif(
    sys.platform != "linux",
    reason="Qualified writer/verifier supervision requires Linux",
)
def test_real_entity_completion_verifier_uses_retained_ledger_anchor_without_reopening(
    tmp_path,
):
    from wintappy.coordination.verification import (
        verify_authority_prepared,
        verify_prepared,
    )
    from wintappy.coordination.writer import execute_prepared

    bound, prepared = prepared_entities(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        anchor = verify_authority_prepared(
            ledger,
            prepared,
            NOW,
            initialize=True,
            confirmed=True,
            reason="synthetic baseline",
        )
        ledger.drain(NOW)
        execute_prepared(ledger, prepared, NOW)
        before = ledger.snapshot()
        result = verify_prepared(ledger, prepared)
        assert result["authority_high_water"]["generation"] >= anchor["generation"]
        assert result["checkpoint_sha256"]
        assert result["raw_source_rows"] == result["raw_output_rows"] == 1
        assert ledger.snapshot() == before
        assert ledger.authority_high_water() == anchor


@pytest.mark.parametrize(
    "damage", ["command_digest", "result_capture", "observation_path"]
)
@pytest.mark.skipif(
    sys.platform != "linux", reason="Qualified authority supervision requires Linux"
)
def test_rehashed_authority_observation_still_authenticates_execution_and_capture(
    tmp_path, damage
):
    from wintappy.coordination.protocol import canonical, digest
    from wintappy.coordination.verification import verify_authority_prepared

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
        proof = json.loads(
            ledger._connection.execute(
                "SELECT proof_json FROM authority_checks"
            ).fetchone()[0]
        )
        if damage == "command_digest":
            proof["supervision"]["command_sha256"] = "0" * 64
        elif damage == "result_capture":
            proof["result"]["authority_high_water"]["generation_id"] = "0" * 64
        else:
            proof["observations_directory"] = str(tmp_path / "unrelated-observations")
        ledger._connection.execute(
            "UPDATE authority_checks SET proof_json=?,proof_sha256=?",
            [canonical(proof), digest(proof)],
        )
    with pytest.raises(CoordinationError), Ledger.open(tmp_path / "state", bound):
        pass


@pytest.mark.parametrize("writer", [False, True])
@pytest.mark.parametrize("version", [1, 2])
def test_upgrade_preserves_reservations_outbox_tombstones_intent_and_events(
    tmp_path, writer, version
):
    import duckdb

    from wintappy.coordination.ledger import SCHEMA_V1, SCHEMA_V2
    from wintappy.coordination.protocol import (
        canonical,
        make_cancellation,
        make_release,
    )

    from .test_coordination_protocol import binding as raw_binding
    from .test_coordination_protocol import request

    bound, state = raw_binding(tmp_path), tmp_path / "state"
    state.mkdir()
    with duckdb.connect(str(state / "coordinator.duckdb")) as conn:
        conn.execute(SCHEMA_V1 if version == 1 else SCHEMA_V2)
        conn.execute(
            "INSERT INTO metadata VALUES (1,?,?,?)",
            [version, "a" * 32, canonical(bound)],
        )
        conn.execute("INSERT INTO dataset_state VALUES (1,'OPEN',?,NULL,'')", [HOUR])
        ledger = Ledger(conn, state, bound)
        req = request(tmp_path)
        ledger.ingest(req, NOW)
        grant = ledger.admit(NOW)[0]
        ledger.ingest(make_cancellation(request(tmp_path, "b" * 32), NOW), NOW)
        if writer:
            ledger.ingest(make_release(grant, NOW, 0), NOW)
            ledger.drain(NOW)
            run = ledger.begin_writer(HOUR, NOW, ["synthetic-writer"])
        jobs, outbox, events = (
            conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
            for table in ("jobs", "outbox", "events")
        )
    Ledger.upgrade(
        state, bound, NOW, confirmed=True, reason="synthetic compatible upgrade"
    )
    with Ledger.open(state, bound) as ledger:
        assert (
            ledger._connection.execute("SELECT * FROM jobs ORDER BY 1").fetchall()
            == jobs
        )
        assert (
            ledger._connection.execute("SELECT * FROM outbox ORDER BY 1").fetchall()
            == outbox
        )
        assert set(events) <= set(
            ledger._connection.execute("SELECT * FROM events").fetchall()
        )
        assert ledger.ledger_id == "a" * 32
        assert ledger.authority_high_water() is None
        if writer:
            after = ledger.writer_run(run["run_id"])
            assert after["state"] == "BLOCKED"
            assert after["intent_json"] == canonical(run)
            assert after["cli_run_id"] == run["cli_run_id"]
            assert ledger.snapshot()["next_hour"] == HOUR
        else:
            assert ledger.snapshot()["readers"] == 1
            assert ledger.pending_grants() == [grant]


@pytest.mark.parametrize("phase", ["before_commit", "after_commit"])
@pytest.mark.skipif(
    sys.platform != "linux", reason="Qualified authority supervision requires Linux"
)
def test_abrupt_death_recovers_verified_high_water_without_reopening(tmp_path, phase):
    from wintappy.coordination.protocol import canonical
    from wintappy.coordination.verification import verify_authority_prepared

    bound, prepared = prepared_entities(tmp_path)
    script = """
import json,os,sys
from pathlib import Path
from wintappy.coordination.execution import load_execution,PreparedExecution
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.verification import verify_authority_prepared
receipt=load_execution(sys.argv[3],sys.argv[4])
prepared=PreparedExecution(Path(sys.argv[3]),sys.argv[4],receipt,
                           tuple(receipt['command']),receipt['binding'],{})
with Ledger.open(sys.argv[1],json.loads(sys.argv[2])) as ledger:
    def crash(observed):
        if observed==sys.argv[5]:
            os._exit(37)
    ledger.failure_hook=crash
    verify_authority_prepared(ledger,prepared,sys.argv[6],initialize=True,
                              confirmed=True,reason='synthetic crash baseline')
raise AssertionError('failure point not reached')
"""
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            script,
            str(tmp_path / "state"),
            canonical(bound),
            str(prepared.path),
            prepared.sha256,
            phase,
            NOW,
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 37, result.stdout + result.stderr
    with Ledger.open(tmp_path / "state", bound) as ledger:
        committed = phase == "after_commit"
        assert (ledger.authority_high_water() is not None) == committed
        assert ledger.snapshot()["next_hour"] == HOUR
        with pytest.raises(CoordinationError):
            ledger.admit(NOW)
        verify_authority_prepared(
            ledger,
            prepared,
            NOW,
            initialize=not committed,
            confirmed=not committed,
            reason="synthetic verified retry",
        )
        assert ledger.authority_high_water()["generation"] == 1


@pytest.mark.parametrize("phase", ["before_commit", "after_commit"])
def test_abrupt_schema_upgrade_is_atomic_and_retains_ledger_identity(tmp_path, phase):
    import duckdb

    from wintappy.coordination.ledger import SCHEMA_V1, SCHEMA_VERSION
    from wintappy.coordination.protocol import canonical

    bound, state = binding(tmp_path), tmp_path / "state"
    state.mkdir()
    with duckdb.connect(str(state / "coordinator.duckdb")) as conn:
        conn.execute(SCHEMA_V1)
        conn.execute(
            "INSERT INTO metadata VALUES (1,1,?,?)", ["a" * 32, canonical(bound)]
        )
        conn.execute("INSERT INTO dataset_state VALUES (1,'OPEN',?,NULL,'')", [HOUR])
    script = """
import json,os,sys
from wintappy.coordination.ledger import Ledger
original=Ledger.__init__
def intercepted(self,*args,**kwargs):
    original(self,*args,**kwargs)
    def crash(observed):
        if observed==sys.argv[3]:
            os._exit(37)
    self.failure_hook=crash
Ledger.__init__=intercepted
Ledger.upgrade(sys.argv[1],json.loads(sys.argv[2]),sys.argv[4],
               confirmed=True,reason='synthetic crash upgrade')
raise AssertionError('failure point not reached')
"""
    result = subprocess.run(
        [
            sys.executable,
            "-B",
            "-c",
            script,
            str(state),
            canonical(bound),
            phase,
            NOW,
        ],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 37, result.stdout + result.stderr
    with duckdb.connect(str(state / "coordinator.duckdb")) as conn:
        expected = 1 if phase == "before_commit" else SCHEMA_VERSION
        assert conn.execute(
            "SELECT schema_version,ledger_id FROM metadata"
        ).fetchone() == (expected, "a" * 32)
        assert (
            "authority_checks" in {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        ) == (expected >= 2)
        assert (
            "completion_evidence"
            in {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
        ) == (expected >= 3)
    if phase == "before_commit":
        with pytest.raises(CoordinationError), Ledger.open(state, bound):
            pass
        Ledger.upgrade(
            state, bound, NOW, confirmed=True, reason="synthetic upgrade retry"
        )
    with Ledger.open(state, bound) as ledger:
        assert ledger.authority_high_water() is None
        assert ledger.ledger_id == "a" * 32
        with pytest.raises(CoordinationError):
            ledger.admit(NOW)


@pytest.mark.parametrize(
    "problem", ["missing", "corrupt", "binding", "already_upgraded"]
)
def test_upgrade_refuses_invalid_source_without_replacing_it(tmp_path, problem):
    import duckdb

    from wintappy.coordination.ledger import SCHEMA_V1
    from wintappy.coordination.protocol import canonical

    bound, state = binding(tmp_path), tmp_path / "state"
    state.mkdir()
    path = state / "coordinator.duckdb"
    if problem == "corrupt":
        path.write_bytes(b"synthetic invalid ledger")
    elif problem == "already_upgraded":
        Ledger.initialize(state, bound, HOUR)
    elif problem == "binding":
        with duckdb.connect(str(path)) as conn:
            conn.execute(SCHEMA_V1)
            conn.execute(
                "INSERT INTO metadata VALUES (1,1,?,?)", ["a" * 32, canonical(bound)]
            )
            conn.execute(
                "INSERT INTO dataset_state VALUES (1,'OPEN',?,NULL,'')", [HOUR]
            )
        bound = {**bound, "dataset_id": "synthetic-different-dataset"}
    before = path.read_bytes() if path.exists() else None
    with pytest.raises(CoordinationError):
        Ledger.upgrade(
            state, bound, NOW, confirmed=True, reason="synthetic invalid upgrade"
        )
    assert (path.read_bytes() if path.exists() else None) == before


@pytest.mark.skipif(
    sys.platform != "linux", reason="Qualified authority supervision requires Linux"
)
def test_same_ordinal_fork_fails_retained_ledger_without_rebasing(tmp_path):
    from wintappy.coordination.verification import verify_authority_prepared

    from .test_coordination_execution import PROJECT

    bound, prepared = prepared_entities(tmp_path)
    original = {p: p.read_bytes() for p in (tmp_path / "entity").rglob("*.parquet")}
    with Ledger.open(tmp_path / "state", bound) as ledger:
        verify_authority_prepared(
            ledger,
            prepared,
            NOW,
            initialize=True,
            confirmed=True,
            reason="synthetic fork baseline",
        )
        evolve(tmp_path)
        retained = verify_authority_prepared(ledger, prepared, NOW)
        for path, payload in original.items():
            path.write_bytes(payload)
        (tmp_path / "raw/raw_process/dayPK=20260102/hourPK=00/part.parquet").unlink()
        evolve(tmp_path, eventtime=300)
        # This fork is valid in isolation: only the retained independent ledger
        # distinguishes it from the previously accepted generation at this ordinal.
        candidate = subprocess.run(
            [
                str(PROJECT / ".venv/bin/python"),
                "-I",
                "-B",
                "-c",
                (
                    "import json,sys; from raw_sensor_compact.cli import parser; "
                    "from raw_sensor_compact.completion import verify_authority; "
                    "args=parser().parse_args(sys.argv[1:]); "
                    "print(json.dumps(verify_authority(args)))"
                ),
                *prepared.command[5:],
            ],
            capture_output=True,
            text=True,
            check=True,
            timeout=20,
            env={},
        )
        branch = json.loads(candidate.stdout)
        assert branch["generation"] == retained["generation"]
        assert branch["generation_id"] != retained["generation_id"]
        with pytest.raises(CoordinationError):
            verify_authority_prepared(ledger, prepared, NOW)
        assert ledger.authority_high_water() == retained
        ledger.drain(NOW)
        with pytest.raises(CoordinationError):
            ledger.begin_writer(HOUR, NOW, ["synthetic-forbidden-writer"])
        assert ledger.snapshot()["active_run_id"] is None


@pytest.mark.skipif(
    sys.platform != "linux", reason="Qualified authority supervision requires Linux"
)
def test_entire_history_is_validated_beyond_first_bounded_batch(tmp_path):
    from wintappy.coordination.protocol import canonical, digest
    from wintappy.coordination.verification import verify_authority_prepared

    bound, prepared = prepared_entities(tmp_path)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        verify_authority_prepared(
            ledger,
            prepared,
            NOW,
            initialize=True,
            confirmed=True,
            reason="synthetic history baseline",
        )
        verify_authority_prepared(ledger, prepared, NOW)
        # Synthetic metadata-only extension of an authenticated repeat-check proof.
        # No fixture authority/data generation or completion is manufactured.
        proof = json.loads(
            ledger._connection.execute(
                "SELECT proof_json FROM authority_checks WHERE sequence=2"
            ).fetchone()[0]
        )
        for index in range(255):
            proof["reason"] = f"Synthetic repeated history check {index}"
            ledger._record_authority_check(proof)
    with Ledger.open(tmp_path / "state", bound) as ledger:
        assert (
            ledger._connection.execute(
                "SELECT count(*) FROM authority_checks"
            ).fetchone()[0]
            == 257
        )
        proof["result"]["version"] = True
        ledger._connection.execute(
            "UPDATE authority_checks SET proof_json=?,proof_sha256=? WHERE sequence=257",
            [canonical(proof), digest(proof)],
        )
    with pytest.raises(CoordinationError), Ledger.open(tmp_path / "state", bound):
        pass
