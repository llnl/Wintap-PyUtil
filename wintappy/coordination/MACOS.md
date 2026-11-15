# Native macOS coordinated reader supervision

The coordinated reader client has a native macOS path. It launches an isolated
supervisor process, starts the reader command in a dedicated process group, captures
bounded stdout/stderr, and records `shutdown_scope: "process_group"` in the immutable
supervision observation. The worker observes wrapper exit with kqueue and enumerates
live group members through `/bin/ps` while the direct child remains unreaped. It
signals the group only while that unreaped leader pins the group ID, and does not
query or signal the numeric group ID after reaping. Zombie entries are not live work.
The parent masks SIGINT/SIGTERM across worker creation; the worker installs its
cleanup handlers before unblocking inherited signals. If member enumeration or
shutdown is ambiguous, the run fails closed. The path does not use a Linux VM,
Docker, `memfd`, or the Linux subreaper implementation; Linux supervision remains
separate and unchanged.

## Accepted release rule and limits

For ordinary completion, the client may release its reader reservation after the
wrapper exits, no live members remain in the dedicated process group, and captured
output pipes reach EOF. This is an
explicitly accepted best-effort Mac rule, **not** proof that every descendant has
stopped: a child that creates another session/process group and closes inherited
capture pipes may outlive the reader while remaining invisible to this check. A
detached child that keeps a captured pipe open instead makes the shutdown observation
uncertain.

The worker stops remaining members of the managed group after wrapper exit, timeout,
or interruption, first with SIGTERM and then SIGKILL. If the group does not disappear,
capture pipes do not close after the group empties, the worker dies, or its
acknowledgment is missing, the client does not publish a release. The durable launch
marker/reservation remains for manual verification; there is no automatic expiry.
If the supervisor is killed, its workload may also survive: no durable completion
observation means the coordinator must continue to treat the reservation as held.

This backend is intended for cooperative local workloads. Process groups are not a
security boundary, hostile-workload containment mechanism, or service fencing
primitive. A successful synthetic lifecycle suite is not deployment acceptance.

## Local-only DBT

The ordinary local-only Make/DBT route remains direct and does not initialize the
coordination client or supervisor. The native coordinated path is selected only
when the existing active-root routing identifies coordinated inputs. Linux behavior
and its descendant-tree shutdown contract are unchanged.

## Actual-Mac lifecycle controls

Run on macOS with the project's locked Python environment:

```sh
uv run --locked --python 3.12 --group dev pytest -q tests/test_coordination_macos_supervisor.py
```

The controls cover normal/nonzero exit, same-group child shutdown, timeout,
missing executable, output limits, interrupt, killed wrapper, killed supervisor,
missing acknowledgment, coordinated-client reservation retention, and the accepted
detached-child limitation. The file skips on non-macOS hosts. Passing it does not
qualify actual S3, production DBT/data behavior, service installation, representative
resources, or production deployment.
