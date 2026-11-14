"""Qualified receipt-bound launch. Execution alone never authorizes reopening."""

from pathlib import Path

from .execution import execution_environment, recheck_execution
from .ledger import _mkdir
from .protocol import CoordinationError
from .supervisor import read_report, run_supervised


def execute_prepared(ledger, prepared, now):
    """Conservative launch API: even clean execution remains blocked."""
    report = _execute_prepared(ledger, prepared, now)
    ledger.block_writer(
        prepared.receipt["run_id"],
        now,
        "Execution is not completion evidence; independent phase verification required",
    )
    return report


def _execute_prepared(ledger, prepared, now):
    receipt = recheck_execution(prepared, ledger)
    environment = execution_environment(prepared)
    execution = {
        "path": str(prepared.path),
        "sha256": prepared.sha256,
        "receipt": receipt,
    }
    try:
        ledger.begin_writer(
            receipt["hour_start"], now, list(prepared.command), execution=execution
        )
        # Receipt loss after COMMIT cannot authorize an invocation or retry.
        receipt = recheck_execution(prepared, ledger)
        settings = receipt["settings"]
        _mkdir(Path(settings["temp_directory"]))
        if settings["storage"] and settings["storage"]["extension_directory"]:
            _mkdir(Path(settings["storage"]["extension_directory"]))
        result = run_supervised(
            list(prepared.command),
            receipt["observations_directory"],
            timeout_seconds=settings["supervision_timeout_seconds"],
            stop_grace_seconds=settings["stop_grace_seconds"],
            shutdown_seconds=settings["shutdown_seconds"],
            max_log_bytes=settings["max_log_bytes"],
            cwd=receipt["cwd"],
            env=environment,
        )
        recheck_execution(prepared, ledger)
        if (
            result.get("status") != "exited"
            or result.get("exit_code") != 0
            or result.get("children_stopped") is not True
        ):
            raise CoordinationError("Writer execution did not establish clean shutdown")
        report = read_report(
            receipt["observations_directory"], max_bytes=settings["max_report_bytes"]
        )
    except BaseException as error:
        state = ledger.snapshot()
        if state["mode"] == "WRITING" and state["active_run_id"] == receipt["run_id"]:
            ledger.block_writer(
                receipt["run_id"],
                now,
                "Receipt-bound execution uncertain; independent phase verification required",
            )
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        raise CoordinationError(
            "Receipt-bound writer failed; details suppressed"
        ) from None
    return report


def execute_and_complete_prepared(ledger, prepared, now, *, clock):
    """Qualified normal success under one lock; never resolve an existing blocked run.

    Requires an evolving UTC clock. No caller-provided report, callback or verifier
    result can grant completion. This library does not admit readers or publish grants.
    """
    from .completion import _complete_live_prepared
    from .coordinator import _Clock, writer_due
    from .protocol import timestamp

    observed = _Clock(now, clock)
    if not callable(clock):
        raise CoordinationError("Completion requires a fresh evolving UTC clock")
    ledger._completion_live_run = None
    try:
        receipt = recheck_execution(prepared, ledger)
        now = observed.observe()
        floor = ledger._completion_clock_floor()
        if floor is not None and timestamp(now) < timestamp(floor):
            raise CoordinationError(
                "Completion clock predates retained prior completion"
            )
        if not writer_due(
            receipt["hour_start"],
            now,
            grace_minutes=receipt["settings"]["grace_minutes"],
        ):
            raise CoordinationError("Selected completion hour is not due")
        _execute_prepared(ledger, prepared, now)
        ledger._completion_live_run = receipt["run_id"]
        observed.observe()
        return _complete_live_prepared(ledger, prepared, observed)
    except BaseException as error:
        ledger._completion_live_run = None
        ledger._authority_checked = None
        state = ledger.snapshot()
        if (
            state["mode"] == "WRITING"
            and state["active_run_id"] == prepared.receipt["run_id"]
        ):
            ledger.block_writer(
                state["active_run_id"],
                observed.last,
                "Atomic completion uncertain; retain original intent/evidence and require audited recovery",
            )
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        raise CoordinationError(
            "Qualified completion failed; details suppressed"
        ) from None
    finally:
        ledger._completion_live_run = None
