"""Genuine local retries reuse frozen invocation credentials, never ambient scope."""

import os
import sys

import pytest

from wintappy.coordination.ledger import Ledger
from wintappy.coordination.protocol import CoordinationError

from .test_coordination_completion import NOW
from .test_coordination_execution import binding
from .test_coordination_intentional_retry import retry
from .test_coordination_retry import failed_merge

pytestmark = pytest.mark.skipif(
    sys.platform != "linux" or os.geteuid() == 0,
    reason="Linux actual unprivileged failed originals and independent retry completion",
)


@pytest.mark.parametrize("mode", ["normal", "audited"])
def test_retry_completion_reuses_exact_frozen_credentials_without_storage_resolution(
    tmp_path, monkeypatch, mode
):
    from wintappy.coordination import execution, intentional_retry, recovery

    prepared, archive = failed_merge(tmp_path)
    frozen = prepared.credentials
    observed = []
    recheck = intentional_retry.recheck_execution

    def forbidden(*args, **kwargs):
        pytest.fail("Completion must not resolve ambient SDK/profile storage again")

    def exact_frozen(candidate, ledger):
        assert candidate.credentials is frozen
        observed.append(candidate.receipt["run_id"])
        return recheck(candidate, ledger)

    monkeypatch.setattr(execution, "resolve_storage", forbidden)
    monkeypatch.setattr(recovery, "resolve_storage", forbidden)
    monkeypatch.setattr(intentional_retry, "recheck_execution", exact_frozen)
    with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
        if mode == "audited":

            def lost_ack(phase):
                if phase == "after_retry_launch":
                    raise OSError(
                        "synthetic acknowledgment loss after actual clean retry"
                    )

            ledger.failure_hook = lost_ack
            with pytest.raises(CoordinationError):
                retry(ledger, prepared)
            new_id = ledger.snapshot()["active_run_id"]
        else:
            assert retry(ledger, prepared)["mode"] == "OPEN"
            new_id = ledger._connection.execute(
                "SELECT retry_run_id FROM retry_dispositions"
            ).fetchone()[0]
    if mode == "audited":
        with Ledger.open(tmp_path / "state", binding(tmp_path)) as ledger:
            new = recovery.load_original_execution(ledger, new_id, {})
            # Local fixtures have no remote credential provider. Explicitly carry
            # this invocation's frozen object to test reuse, not S3 qualification.
            new = new._replace(credentials=frozen)
            assert (
                recovery.resolve_hour(
                    ledger,
                    new,
                    NOW,
                    clock=lambda: NOW,
                    confirmed=True,
                    reason="synthetic independently audited clean retry",
                )["mode"]
                == "OPEN"
            )
    assert {prepared.receipt["run_id"], new_id} <= set(observed)
    assert all(path.read_bytes() == payload for path, payload in archive.items())
