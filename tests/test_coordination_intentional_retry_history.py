"""Genuine paged retry history, read budgets and uninterrupted operator exclusion."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from wintappy.coordination.execution import prepare_execution
from wintappy.coordination.intentional_retry import retry_and_complete_prepared
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError, canonical, make_request
from wintappy.coordination.writer import execute_prepared

from .test_coordination_completion import NOW
from .test_coordination_execution import binding, config
from .test_coordination_intentional_retry import retry
from .test_coordination_ledger import HOUR
from .test_coordination_retry import failed_merge
from .test_coordination_service import settings

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux actual unprivileged merge failures and competing ledger processes",
)


TABLES = (
    "metadata",
    "dataset_state",
    "jobs",
    "outbox",
    "compaction_runs",
    "events",
    "authority_checks",
    "completion_evidence",
    "retry_dispositions",
)


def retained_rows(ledger):
    # Compare every column, independent of physical insertion/reinsertion order.
    return {
        table: ledger._connection.execute(
            f"SELECT * FROM {table} ORDER BY ALL"
        ).fetchall()
        for table in TABLES
    }


@pytest.fixture(scope="module")
def retry_chain(tmp_path_factory):
    """Three actual failed originals and three independent successful new runs."""
    root = tmp_path_factory.mktemp("synthetic-retry-chain")
    first, archive = failed_merge(root)
    originals = [first]
    with Ledger.open(root / "state", binding(root)) as ledger:
        retry(ledger, first)
    seed = next((root / "raw_sensor").rglob("*.parquet")).read_bytes()
    for hour, now in (
        ("2026-01-02T00:00:00Z", "2026-01-02T01:10:00Z"),
        ("2026-01-02T01:00:00Z", "2026-01-02T02:10:00Z"),
    ):
        parent = root / "raw_sensor/raw_file/dayPK=20260102" / f"hourPK={hour[11:13]}"
        parent.mkdir(parents=True)
        (parent / "part.parquet").write_bytes(seed)
        prepared = prepare_execution(
            root / "state",
            binding(root),
            hour,
            now,
            {**config(root), "event_types": ["raw_file"], "delete_source": False},
        )
        # Block creation of the genuinely absent next output hour, not a mocked
        # report/archive. Restore only this isolated fixture's original mode.
        output_parent = root / "compacted/raw_file/dayPK=20260102"
        output_parent.mkdir(exist_ok=True)
        output_parent.chmod(0o555)
        try:
            with Ledger.open(root / "state", binding(root)) as ledger:
                ledger.drain(now)
                with pytest.raises(
                    CoordinationError, match="Receipt-bound writer failed"
                ):
                    execute_prepared(ledger, prepared, now)
        finally:
            output_parent.chmod(0o755)
        observations = Path(prepared.receipt["observations_directory"])
        assert (
            json.loads((observations / "supervision.json").read_text())["exit_code"]
            == 2
        )
        report = json.loads((observations / "stderr.log").read_text())
        assert report["phase"] == "merge" and report["phase_evidence"]["records"] == 3
        directory = root / "phase-evidence" / prepared.receipt["cli_run_id"]
        archive.update({path: path.read_bytes() for path in directory.iterdir()})
        originals.append(prepared)
        with Ledger.open(root / "state", binding(root)) as ledger:
            retry_and_complete_prepared(
                ledger,
                prepared,
                now,
                clock=lambda observed=now: observed,
                confirmed=True,
                reason="synthetic successive exact-original retry",
            )
    assert all(path.read_bytes() == payload for path, payload in archive.items())
    return root, originals


@pytest.mark.parametrize("budget", ["record", "count", "aggregate"])
def test_retained_retry_read_budget_refuses_without_pruning(
    retry_chain, monkeypatch, budget
):
    from wintappy.coordination import intentional_retry

    root, _ = retry_chain
    with Ledger.open(root / "state", binding(root)) as ledger:
        before = retained_rows(ledger)
        payloads = [row[3].encode() for row in before["retry_dispositions"]]
    name, limit = {
        "record": ("MAX_RETRY_BYTES", max(map(len, payloads)) - 1),
        "count": ("MAX_RETRIES", len(payloads) - 1),
        "aggregate": ("MAX_RETRY_TOTAL_BYTES", sum(map(len, payloads)) - 1),
    }[budget]
    with monkeypatch.context() as scoped:
        scoped.setattr(intentional_retry, name, limit)
        with (
            pytest.raises(
                CoordinationError, match="Retry history exceeds bounded budgets"
            ),
            Ledger.open(root / "state", binding(root)),
        ):
            pass
    with Ledger.open(root / "state", binding(root)) as ledger:
        assert retained_rows(ledger) == before


def test_genuine_retry_history_pages_and_authenticates_the_last_record(retry_chain):
    from wintappy.coordination.intentional_retry import validate_history

    root, originals = retry_chain
    with Ledger.open(root / "state", binding(root)) as ledger:
        before = retained_rows(ledger)
        connection, pages = ledger._connection, []

        class OneRowPages:
            def execute(self, statement, parameters=None):
                if statement.startswith(
                    "SELECT * FROM retry_dispositions WHERE sequence>"
                ):
                    assert "LIMIT 256" in statement
                    pages.append(parameters[0])
                    statement = statement.replace("LIMIT 256", "LIMIT 1")
                return connection.execute(statement, parameters or [])

            def cursor(self):
                return connection.cursor()

            def __getattr__(self, name):
                return getattr(connection, name)

        ledger._connection = OneRowPages()
        try:
            ledger._validate()
            assert pages == [0, 1, 2, 3]
            assert retained_rows(ledger) == before
            for original in originals:
                assert (
                    ledger.writer_run(original.receipt["run_id"])["state"] == "BLOCKED"
                )
                assert ledger.completion_evidence(original.receipt["run_id"]) is None
            last = originals[-1].receipt["run_id"]
            audit = connection.execute(
                "SELECT * FROM events WHERE kind='writer_retried' AND identity=?",
                [last],
            ).fetchone()
            connection.execute(
                "DELETE FROM events WHERE kind='writer_retried' AND identity=?", [last]
            )
            pages.clear()
            with pytest.raises(
                CoordinationError, match="Retry history/ownership/audit"
            ):
                validate_history(ledger)
            assert pages == [0, 1, 2]
            connection.execute("INSERT INTO events VALUES (?,?,?,?,?)", list(audit))
            ledger._validate()
            assert retained_rows(ledger) == before
        finally:
            ledger._connection = connection


def test_completed_retry_reopen_reads_retained_json_not_historical_runtime(
    retry_chain, monkeypatch
):
    from wintappy.coordination import execution, verification

    root, _ = retry_chain
    with Ledger.open(root / "state", binding(root)) as ledger:
        before = retained_rows(ledger)

    def forbidden(*args, **kwargs):
        pytest.fail(
            "Ordinary retained-history validation must not reopen historical runtime evidence"
        )

    monkeypatch.setattr(execution, "_read_bytes", forbidden)
    monkeypatch.setattr(verification, "_read", forbidden)
    with Ledger.open(root / "state", binding(root)) as ledger:
        assert retained_rows(ledger) == before
        assert ledger.snapshot()["next_hour"] == "2026-01-02T02:00:00Z"


def test_actual_competing_ledger_open_is_excluded_through_retry_completion(tmp_path):
    prepared, archive = failed_merge(tmp_path)
    seen = []
    phases = {
        "before_retry_commit",
        "after_retry_commit",
        "before_retry_launch",
        "after_retry_launch",
        "before_completion_commit",
        "after_completion_commit",
    }
    code = """
import json,sys
from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError
try:
    with Ledger.open(sys.argv[1],json.loads(sys.argv[2])): pass
except CoordinationError as error:
    if "busy" in str(error).lower(): sys.exit(23)
    raise
sys.exit(24)
"""
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        request = make_request(
            binding(tmp_path),
            "b" * 32,
            "dbt-build",
            HOUR,
            prepared.receipt["hour_end"],
            NOW,
        )
        ledger.ingest(request, NOW)
        job = ledger.job(request["job_id"])

        def compete(phase):
            if phase not in phases:
                return
            child = subprocess.run(
                [
                    sys.executable,
                    "-B",
                    "-c",
                    code,
                    str(ledger.directory),
                    canonical(binding(tmp_path)),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            assert child.returncode == 23, child.stderr
            assert ledger.job(request["job_id"]) == job
            assert not ledger.pending_grants()
            seen.append(phase)

        ledger.failure_hook = compete
        retry(ledger, prepared)
        assert set(seen) == phases and len(seen) == len(phases)
        assert ledger.snapshot()["readers"] == 0
        assert len(ledger.admit(NOW)) == 1
    assert all(path.read_bytes() == payload for path, payload in archive.items())


@pytest.mark.parametrize(
    "extra",
    [
        ["--job-id", "a" * 32],
        ["--maintenance-id", "b" * 32],
        ["--after-job", "c" * 32],
        ["--after-run", "d" * 32],
        ["--after-maintenance", "e" * 32],
        ["--confirm-replace-checkpoints"],
    ],
)
def test_service_retry_rejects_unrelated_flags_before_config_or_storage(
    monkeypatch, extra
):
    from wintappy.coordination import service

    monkeypatch.setattr(
        service,
        "load_config",
        lambda _: pytest.fail(
            "No configuration/storage resolution on invalid retry flags"
        ),
    )
    assert (
        service.main(
            [
                "retry-hour",
                "--config",
                "synthetic",
                "--run-id",
                "f" * 32,
                "--confirm",
                "--reason",
                "synthetic narrow retry",
                *extra,
            ]
        )
        == 1
    )


def test_service_retry_preserves_original_settings_and_observes_completion_clock(
    tmp_path, monkeypatch, capsys
):
    from wintappy.coordination import service

    prepared, _ = failed_merge(tmp_path)
    value = settings(tmp_path)
    value["writer"].update(event_types=["raw_process"], delete_source=True)
    current, observed = [NOW], []

    def clock():
        observed.append(current[0])
        return current[0]

    from wintappy.coordination import intentional_retry

    launch = intentional_retry._launch_prepared

    def actual_launch(ledger, new, environment):
        assert new.receipt["settings"] == prepared.receipt["settings"]
        result = launch(ledger, new, environment)
        current[0] = "2026-01-02T01:10:00Z"
        return result

    monkeypatch.setattr(service, "load_config", lambda _: value)
    monkeypatch.setattr(service, "_now", clock)
    monkeypatch.setattr(intentional_retry, "_launch_prepared", actual_launch)
    for name in ("prepare_execution", "open_message_store"):
        monkeypatch.setattr(
            service,
            name,
            lambda *a, **k: pytest.fail(
                "Retry uses original receipt, not current writer or transport"
            ),
        )
    for name in ("admit", "flush_grants"):
        monkeypatch.setattr(
            Ledger,
            name,
            lambda *a, **k: pytest.fail(
                "Operator retry must not reserve or publish grants"
            ),
        )
    assert (
        service.main(
            [
                "retry-hour",
                "--config",
                "synthetic",
                "--run-id",
                prepared.receipt["run_id"],
                "--confirm",
                "--reason",
                "synthetic original-scope evolving-clock retry",
            ]
        )
        == 0
    )
    result = json.loads(capsys.readouterr().out)
    assert (
        result["mode"] == "DRAINING"
        and result["next_hour"] == prepared.receipt["hour_end"]
    )
    assert observed[0] == NOW and observed[-1] == current[0] and len(observed) > 2
