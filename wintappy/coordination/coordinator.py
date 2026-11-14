"""One locked admission/draining tick; writer completion is not yet certified.

The optional writer callback is an integration/test seam, not a supervisor or
permission to deploy. Every invocation commits intent first and then blocks,
regardless of its returned claim. No completion, cursor advance or reopening is
implemented here. The caller supplies a canonical UTC observation time and may
inject an evolving canonical UTC clock; backwards/unreadable clocks fail closed.
"""

from datetime import timedelta
from itertools import chain

from .ledger import Ledger, validate_writer_command
from .protocol import CoordinationError, separate_transport_root, timestamp


def _grace(minutes):
    if type(minutes) is not int or not 5 <= minutes <= 1440:
        raise CoordinationError(
            "Writer grace must be an integer between 5 and 1440 minutes"
        )
    return timedelta(minutes=minutes)


def writer_due(hour, now, *, grace_minutes=10):
    grace = _grace(grace_minutes)
    start = timestamp(hour, hour=True)
    observed = timestamp(now)
    # Subtraction avoids overflowing the latest representable hour.
    return observed - start >= timedelta(hours=1) + grace


class _Clock:
    def __init__(self, now, clock):
        timestamp(now)
        if clock is not None and not callable(clock):
            raise CoordinationError("Coordinator clock must be callable")
        self.last = now
        self.clock = clock

    def observe(self):
        if self.clock is not None:
            try:
                value = self.clock()
                observed = timestamp(value)
            except Exception:  # noqa: BLE001 -- an injected clock must fail closed on any access error
                raise CoordinationError(
                    "Coordinator clock observation failed"
                ) from None
            if observed < timestamp(self.last):
                raise CoordinationError("Coordinator clock moved backwards")
            self.last = value
        return self.last


def tick(
    directory,
    binding,
    store,
    now,
    *,
    grace_minutes=10,
    writer=None,
    writer_command=None,
    clock=None,
):
    """Ingest terminals before admission; hold the local lock through callback.

    API/conflict failures propagate and never authorize a launch. Reader grants
    already committed, even without a delivery acknowledgment, still exclude all
    writers. A missing writer adapter leaves due work draining, not open.
    """
    _grace(grace_minutes)
    observed_clock = _Clock(now, clock)
    separate_transport_root(store.root, binding)
    if writer is not None and not callable(writer):
        raise CoordinationError("Writer adapter must be callable")
    if writer is not None or writer_command is not None:
        validate_writer_command(writer_command)
    with Ledger.open(directory, binding) as ledger:
        # Complete bounded listings before committing anything. Ingest all three
        # in one transaction so a later conflicting request/terminal cannot erase
        # a reservation from an earlier terminal in the same tick.
        messages = [
            store.messages(kind) for kind in ("release", "cancellation", "request")
        ]
        now = observed_clock.observe()
        ledger.ingest_many(chain.from_iterable(messages), now)
        state = ledger.snapshot()
        invoked = False
        if state["mode"] == "BLOCKED":
            return {**state, "writer_invoked": invoked}
        due = writer_due(state["next_hour"], now, grace_minutes=grace_minutes)
        if due:
            ledger.drain(now)
        if ledger.snapshot()["mode"] == "OPEN":
            ledger.admit(now)
        # A pending reservation is still a reader while draining. Retrying its
        # exact grant cannot create a new permission or erase its reservation.
        ledger.flush_grants(store)
        # Publication can cross grace while a pre-due grant is being delivered.
        # Preserve that reader reservation, but drain before any possible writer.
        now = observed_clock.observe()
        due = writer_due(
            ledger.snapshot()["next_hour"], now, grace_minutes=grace_minutes
        )
        if due:
            ledger.drain(now)
        state = ledger.snapshot()
        if (
            due
            and state["mode"] == "DRAINING"
            and not state["readers"]
            and writer is not None
        ):
            run = ledger.begin_writer(state["next_hour"], now, writer_command)
            run_id = run["run_id"]
            invoked = True
            reason = "Unverified writer callback result; completion adapter and child shutdown are unqualified"
            observation = {"kind": "interrupted_callback"}
            try:
                observation = _writer_observation(writer(run))
            except Exception as error:  # noqa: BLE001 -- any callback failure retains committed intent
                reason = (
                    "Writer callback failed; reconcile committed intent and all phases"
                )
                observation = {"kind": "exception", "type": type(error).__name__[:128]}
            finally:
                # Also persists on Ctrl-C/SystemExit. Abrupt process death instead
                # leaves WRITING, which Ledger.open blocks before any new effects.
                try:
                    observed_clock.observe()
                finally:
                    # Even a broken clock after writes cannot skip blocking.
                    ledger.block_writer(
                        run_id, observed_clock.last, reason, observation=observation
                    )
        return {**ledger.snapshot(), "writer_invoked": invoked}


def _writer_observation(value):
    """Retain only bounded scalar diagnostics, never treat them as a receipt."""
    if not isinstance(value, dict):
        return {"kind": "invalid_result"}
    observation = {"kind": "result_summary"}
    status = value.get("status")
    if (
        isinstance(status, str)
        and len(status) <= 128
        and all(ord(c) >= 32 for c in status)
    ):
        observation["status"] = status
    exit_code = value.get("exit_code")
    if type(exit_code) is int and -255 <= exit_code <= 255:
        observation["exit_code"] = exit_code
    stopped = value.get("children_stopped")
    if type(stopped) is bool:
        observation["children_stopped"] = stopped
    return observation
