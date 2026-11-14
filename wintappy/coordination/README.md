# Coordination protocol, ledger, tick and supervision (implementation slices)

This is a **library prototype**, not a running coordination service. The controlled-
clock admission/draining tick and Linux command-tree supervision exist; there is
no production scheduler/CLI, DBT client wrapper, qualified compactor adapter or
operator recovery command yet. The existing
local-only `make dbt-build` path is unchanged. Do not enable a production cron job
or treat these modules as dataset-wide fencing.

## Protocol

`protocol.py` defines strict version-one requests, grants, releases and
cancellations. Job, grant, coordinator-run and CLI-run identities use 32 lowercase
hexadecimal characters. Requests checksum canonical JSON and bind the dataset ID,
canonical active raw/compacted roots and, when enabled, entity/metrics roots,
namespace, canonical expected hosts and identity policy/ceiling. Inactive metrics
are not part of a disabled-entity binding.

`MessageStore` uses exact per-job S3 keys. Each key has a single owner: clients own
their requests/terminal records, and one authoritative coordinator owns grants.
Retries use identical bytes at the original key, GET-verify publication and reject
conflicts without overwriting. No conditional PUT assumption is made; these checks
are not safe against concurrent owners of the same key. Missing objects are
distinct from API/transfer failures. Listings must paginate successfully, validate
key/payload identity and stay within all budgets:

- 1 MiB per message;
- 64 MiB of original message bytes per listing;
- 10,000 messages and 10,000 pages per listing.

Exhaustion fails the whole operation, never returns a truncated permission view.
Records are retained; eventual collection must preserve terminal knowledge.
`separate_transport_root` rejects coordination prefixes overlapping data scan
roots; the server and future client must apply this before using a transport.

## Server-local authority

`Ledger.initialize` explicitly creates a new version-one DuckDB ledger with its
immutable dataset binding and first unfinished UTC hour. `Ledger.open` refuses a
missing/corrupt/incompatible ledger; it cannot automatically initialize or promote
a backup. Both take the same nonblocking `fcntl.flock` on a persistent
`coordinator.lock` inode **before opening DuckDB**. Never unlink the lock to unlock
it. The context remains locked through any caller-managed external effects.

Use the main **DuckDB 1.5.6** environment, not the standalone compactor's 2.0.0
environment. Provision persistent local Linux disk, outside all local data roots;
network-filesystem detection/fencing is not implemented. Symlinked state paths,
S3 state directories and overlapping data roots are rejected. The connection uses
two threads and 128 MB DuckDB memory; this is not a process RSS cap or a qualified
production capacity limit.

The library's current operations are:

- `ingest`: validate immutable requests/terminal messages; retain tombstones even
  when cancellation arrives before request visibility. Release must name the
  recorded grant. Conflicts cannot retire a reservation.
- `ingest_many`: atomically ingest a complete bounded tick input. A later request,
  terminal conflict, iterator failure or budget exhaustion rolls back the entire
  batch, including earlier releases. The tick completes all transport listings
  before ingestion. The ledger additionally caps a batch at 30,000 messages and
  192 MiB of canonical payloads (three listing budgets); each transport listing
  still enforces its original-byte limit. This is not a total-process RSS budget.
- `admit`: in `OPEN`, transactionally reserve readers and store immutable grant
  outbox payloads. `GRANTED` counts even before delivery. COMMIT precedes PUT.
- `flush_grants`: publish pending grants, then record delivery. Lost acknowledgments
  and crashes retain the same reservation/payload for retry. Terminal records
  suppress pending publication; clients must never act on a late canceled grant.
- `drain`: enter `DRAINING`, admitting no new readers. All reserved readers, not
  S3 listings or object ages, exclude a writer. No reader automatically expires.
- `begin_writer`: with zero readers, commit `WRITING` and a checksummed command,
  selected hour and independent coordinator/CLI run identities. Only then may a
  supervisor launch a child. Reopening an unresolved intent enters `BLOCKED`
  and retains the hour; it cannot launch a replacement or grant new readers.
- `block_writer`: record the active intent's unresolved outcome and bounded
  observational diagnostics transactionally. This is not a successful completion
  or a retry/reopening operation. Reopen rejects missing/mismatched block reasons.
- `snapshot`/`job`: inspect the current authoritative state inside the locked
  context. Transition audit rows commit with the state they describe.

There is deliberately **no writer-success, cursor-advance or reopening API** in
this slice. Full service integration, independent phase archive verification,
entity-authority high-water enforcement and audited recovery
must be integrated and qualified first. Exit zero or a PID check is not completion
evidence. Checksums cannot detect a coherent rollback of the entire ledger/store;
restore/recovery requires external exclusion and independent retained evidence.

## Controlled-clock tick

`coordinator.writer_due(hour, now, grace_minutes=10)` uses the persisted UTC hour,
not object age or a moving `now minus one hour` cursor. Grace is an integer from
5 through 1,440 minutes. `coordinator.tick` takes an explicit canonical UTC time,
validates binding/transport/adapter configuration and holds the ledger lock across
all listings, atomic terminal-first ingestion, admission, grant retries and the
optional writer callback. Work observed as due enters `DRAINING` before admission.
Delivered and undelivered reservations both exclude the writer.

Optional `clock=` supplies an evolving canonical UTC observation. The tick refreshes
after listings, after grant delivery and after the callback. A listing that crosses
grace cannot admit new readers; a pre-due reservation delivered across grace remains
reserved and the dataset drains. Clock rollback/access errors fail rather than
authorize new work. A post-writer clock failure still records `BLOCKED` using the
last valid observation before propagating the error. Without `clock=`, the explicit
`now` remains a fixed observation for compatibility; production clock/CLI wiring
is not implemented.

Collaborative review reproduced an uncovered writer-priority defect: ingestion
can cross grace after the clock observation and still admit a new reader before
the post-publication refresh drains. Formal regression and repair remain pending;
the earlier passing suites do not cover this boundary. Requests not yet reserved
at the deadline must remain queued; existing reservations must not be revoked.
This is not demonstrated reader/writer overlap: the reservation excludes writers.

With zero readers and due work, an optional callback receives the committed
writer/run intent for exactly the oldest unfinished hour. **This callback seam is
not a supervisor:** it cannot certify the command actually executed, full child
shutdown, migration, entities or checkpoints. Every returned result, including
exit-zero `completed`/`empty` claims, becomes `BLOCKED`, with unchanged cursor.
Only small status/exit-code/child-stopped summaries are retained as untrusted
diagnostics, not full worker reports or recovery receipts. Callback failure or
interruption cannot reopen; abrupt death leaves `WRITING` for blocked restart.
No adapter leaves due work `DRAINING`. The returned snapshot is advisory; no S3
`status.json`, operator CLI or periodic scheduling is implemented yet.

The dcc-02 gate remains open until integrated supervision, verified completion/empty
hours, transactional cursor advance and backlog/reopening behavior are integrated.

## Linux command-tree supervision

`supervisor.run_supervised(command, directory, ...)` is a Linux/main-thread library
operation. Supply a new run directory on persistent local disk outside telemetry
and phase archives; its parent must already exist. It refuses existing directories
instead of overwriting previous observations. The caller is responsible for the
directory's exclusion/root binding until the full service adapter is implemented.

A fresh main-runtime supervising exec becomes a Linux child subreaper **before**
launch. It adopts/reaps orphaned and detached descendants, including `setsid` and
nested forks. Root exit with surviving descendants triggers TERM/KILL cleanup and
is reported as `descendants_terminated`, not clean completion. Timeout and caller
interruption also stop/reap the tree. A parent-death signal requests cleanup if the
caller dies, but restart still requires blocked-intent recovery. A killed or
unresponsive supervising exec cannot assert shutdown; callers fail closed even
when the root exit status is zero. Tests exercise a surviving workload after worker
death. Only unreaped direct children of the isolated subreaper are signaled; it
does not sweep unrelated processes or change the ledger process into a subreaper.
This covers cooperative descendant trees, **not** hostile workloads, external
services, cgroup containment, power loss or non-Linux clients.

The supervising exec receives at most **1 MiB** of control in a sealed memory file,
not a blocking stdin pipe or persisted environment/argv payload. Environment
overrides apply only to the workload. Other descriptors, including ledger locks,
are not inherited. Workload timeout defaults to 3,600 seconds, stop grace to two
and shutdown budget to 30; process waits include a ten-second worker-start allowance
and bounded cleanup waits. These are not bounds on stalled filesystem/kernel I/O
or total process RSS. Capability/observation errors never become success.

Run artifacts are a command/budget manifest, stdout/stderr logs and a supervision
observation JSON. File/directory links are fsynced and private permissions used.
Each stream defaults to **8 MiB**, configurable up to **64 MiB**, drained in 64 KiB
chunks; excess bytes are counted/discarded with explicit overflow failure. The
manifest excludes environment values, so its digest is not a complete configuration
receipt. The JSON observation is capped at 16 KiB. `read_report` accepts only an
exact, untruncated captured JSON object (default **1 MiB**, maximum **64 MiB**),
verifies its saved-byte checksum and parses those same bytes. Duplicate keys,
nonfinite values, malformed metadata and changed/missing/oversized logs fail.
Neither a parsed report nor `children_stopped` certifies raw/entity/checkpoint
completion. The tested supervised tick callback still becomes `BLOCKED`; no real
compactor integration or permission to reopen is added.

The human-required service adapter must retain an allowlisted, non-secret effective
execution/configuration receipt bound to committed writer intent before launch.
Actual executable/runtime/arguments, working directory, active roots, non-secret
endpoint/region selection, hour/run IDs, events, entity policy, deletion options
and budgets must match resolved defaults/overrides/allowed environment settings.
Missing/mismatched settings refuse launch; post-launch uncertainty remains blocked.
Whole-environment dumps, credentials/tokens and secret hashes are not receipts.
Identity, integrity, budget and synthetic-secret-exclusion tests remain pending;
this requirement is not implemented by the current supervisor manifest.

Tests use real local DuckDB, synthetic messages and fake S3, including abrupt
`os._exit` failures around COMMIT and WAL recovery and a child surviving abrupt
tick death, real Linux detached/nested tree shutdown, caller interruption/death,
worker/logging/setup failures and evolving-clock boundaries. They do not qualify
power loss, the actual S3 endpoint, non-Linux supervision, shared-runtime scheduling
or destructive writer operation.
