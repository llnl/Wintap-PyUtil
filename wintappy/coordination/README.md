# Coordination libraries (implementation slices)

This is a **library prototype**, not a running coordination service. The controlled-
clock admission/draining tick and Linux command-tree supervision exist; there is
no production scheduler/CLI, DBT client wrapper or operator recovery command yet.
Receipt-bound launch, separate read-only verification and entity-authority
initialization/freshness checks are tested on local synthetic data. The new ledger-v3
retained-history/normal-completion prototype passes hardened local entity, history,
crash, compatibility and secret controls. Final combined coordination focus passes
298 tests; frozen full regressions pass 641 main tests (three skips) and 439
standalone tests (two skips) on the unchanged revision.
This is not service or endpoint acceptance.
They are not qualified for actual-endpoint scheduling. The existing
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

`Ledger.initialize` explicitly creates a new version-three DuckDB ledger with its
immutable dataset binding and first unfinished UTC hour. `Ledger.open` refuses a
missing/corrupt/incompatible ledger; it cannot automatically initialize or promote
a backup. Both take the same nonblocking `fcntl.flock` on a persistent
`coordinator.lock` inode **before opening DuckDB**. Never unlink the lock to unlock
it. The context remains locked through any caller-managed external effects.

`Ledger.upgrade(..., confirmed=True, reason=...)` explicitly upgrades a compatible
v1/v2 ledger under that same lock. It preserves the ledger/dataset identity, cursor,
reader reservations, grant outbox, terminal tombstones, audits and writer identities.
Unresolved intents become/stay `BLOCKED`; no execution or completion evidence is
manufactured. Ordinary open does not upgrade, and missing/corrupt/mismatched or
already-upgraded sources refuse this upgrade. Schema replacement is transactional;
the unresolved-writer blocking transition may commit before the schema transaction.

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
  The qualified launch library supplies an optional `execution` receipt binding
  in this same COMMIT. Existing prototype intents are retained without fabricated
  receipts and remain unresolved; they cannot be retroactively certified.
- `block_writer`: record the active intent's unresolved outcome and bounded
  observational diagnostics transactionally. This is not a successful completion
  or a retry/reopening operation. Reopen rejects missing/mismatched block reasons.
- `snapshot`/`job`: inspect the current authoritative state inside the locked
  context. Transition audit rows commit with the state they describe.
- `ledger_id`/`authority_high_water()`: inspect the retained independent entity
  anchor. An anchor alone is not fresh permission; see the authority gate below.

The new `writer.execute_and_complete_prepared(..., clock=...)` prototype obtains
fresh independent evidence internally before one completion-evidence/run/optional-
authority/exact-hour-cursor/backlog-mode COMMIT. It requires an uninterrupted clean
live intent under the same lock. Local hardening, combined coordination focus
and full local regressions pass; service integration remains pending.
`execute_prepared` still blocks every outcome, and every previously blocked/restarted
intent requires explicit
audited recovery, whose command is not implemented. Exit zero or a PID check is not
completion evidence. Checksums cannot detect a coherent rollback of the entire ledger/store;
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
after listings and ingestion, after grant delivery and after the callback. A listing
or ingestion that crosses
grace cannot admit new readers; a pre-due reservation delivered across grace remains
reserved and the dataset drains. Clock rollback/access errors fail rather than
authorize new work. A post-writer clock failure still records `BLOCKED` using the
last valid observation before propagating the error. Without `clock=`, the explicit
`now` remains a fixed observation for compatibility; production clock/CLI wiring
is not implemented.

The review's ingestion/grace defect is repaired by a post-ingestion observation
before admission. Formal tests cover exact grace and clock faults, preserving
existing reservations while unreserved requests remain queued. The original defect
delayed writer priority; it did not demonstrate reader/writer overlap. Earlier
suite results did not cover this boundary; new repair evidence is tracked separately.

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

The dcc-02 gate remains open until the separate qualified completion path is wired
into scheduling and service behavior, with its full required regression evidence.

## Effective execution receipts and local launch

`execution.prepare_execution` resolves one invocation for the explicit hour, using
independent coordinator/CLI IDs. It allowlists settings, fixes all effective CLI
defaults explicitly and writes an exclusive, fsynced `execution.json` under
`<state>/executions/<coordinator-run-id>/` before any intent. Preparation grants no
permission. `writer.execute_prepared` requires the locked draining ledger with no
readers; the intent transaction retains the receipt, checksum and path before launch.

The receipt covers canonical active roots, events, entity policy (or explicit
inactivity), deletion and confirmation settings, processing/capture/timeout/archive
budgets, spill/phase/observation paths, working directory and the actual standalone
Python/DuckDB 2.0.0 identity. Public executable, native engine and compactor source
hashes detect runtime changes. Only this checkout's installed standalone CLI is
supported; this is not an arbitrary-command adapter or hostile-process attestation.
The CLI is launched with `-I -B`, without inherited Python import injection.

For active S3 roots, resolve SDK/profile/environment endpoint and region once,
record the resolved non-secret selection and path-style/retry settings, then pass
the origin/region/cache directory explicitly. Frozen credentials travel only in an
ephemeral allowlisted environment; no profile re-resolution, inherited endpoint,
credential-file fallback, environment dump or secret hash is retained. Inactive
S3 settings are explicitly null. Resolution tests use injected SDK helpers and make
no endpoint requests. Actual remote execution is not qualified by these tests.

Receipt reads require one nonsymlink regular inode, bounded exact canonical bytes,
strict shape/types, no duplicate/nonfinite fields and a matching checksum. Command,
binding, runtime and path identities are rechecked before intent, after intent and
after supervised shutdown. Missing/mismatched pre-intent evidence refuses launch;
any uncertainty after COMMIT remains blocked, including a lost COMMIT acknowledgment.
Launch exceptions are sanitized. Spill, retained phase archive, ledger and runtime
must be separate; individual engine memory/spill settings cannot exceed the initial
32 GB/64 GB qualification envelope. These ceilings do not bound total process RSS
or aggregate disk use and are not representative reduced-limit acceptance.

The local launch tests include a real empty hour and a real synthetic migration
with source deletion and retained original phase records. **Both still become
`BLOCKED`, with no cursor advance.** Receipt/report/exit-zero evidence cannot replace
independent raw/entity/checkpoint verification, authority high-water enforcement or
audited recovery. The old callback seam remains synthetic/diagnostic only; never
use it to bypass the receipt contract for destructive service work.

## Independent read-only verification (partial completion foundation)

`verification.verify_prepared` requires the matching committed writer intent,
zero readers and retained exclusion. It authenticates the execution receipt,
writer command, clean supervised shutdown and exact captured bytes; then publishes
an immutable bounded request under the execution's `verifications/<id>/` directory
and separately supervises `raw_sensor_compact.completion` in the standalone runtime.
It rechecks receipt/runtime, original observations, request and intent afterward.
Failure blocks a matching live intent; success changes no permission or cursor.

The standalone verifier never opens the ledger or writes data. It independently
checks original archive chain/order, exact run/hour/root/event scope, migration/
deletion inputs, surviving hashes/counts/schemas, source absence and bounded empty
hours. Skips, missing originals, partial outcomes and malformed types refuse.
Entity verification checks current SQL integrity, retained committed lineage and
the exact original checkpoint against its historical generation, not today's
aggregate hash. An exact retained ordinal/ID anchor permits forward evolution and
rejects rollback/forks. `verify_authority` returns only a rechecked initialization
candidate, never ledger initialization authority.

Main-runtime entity verification now requires the initialized ledger anchor and
passes it to the standalone verifier. A real local entity migration/checkpoint case
passes without changing retained high-water, cursor or permissions. Its returned
candidate grants nothing alone; only the internal normal-success completion path
may retain it and advance permissions atomically. Caller-supplied
anchors cannot bypass initialization. Verification reports, execution receipts and
supervisor observations are not completion certificates.

Recorded active-path negatives precede regular single-link archive reads, strict
boolean/integer checks, live-intent blocking, initialization-candidate rechecks and
sanitized request-argument errors. Final affected tests pass 259 standalone and
204 main cases. The preceding receipt revision's full main suite passes 537 tests
(three skips). Full verifier revision regressions pass 547 main tests (three skips)
and 431 standalone tests (two skips), before subsequent ledger integration.
Actual macOS/S3/resource qualification remains a separate gate.

### Ledger-v2 entity authority gate

Under the locked `OPEN`/`DRAINING` ledger, use
`verification.verify_authority_prepared(..., initialize=True, confirmed=True,
reason=...)` only for explicit initial verification of the configured entity
journal/snapshots. Initialization requires no prior baseline and zero readers; it
does not create entity state or bless a caller-provided report/anchor. A separately
supervised standalone `--authority-only` request binds the effective execution,
ledger/dataset identities and prior anchor. The adapter authenticates command,
capture and shutdown, rechecks current evidence, then retains proof and audit in
one transaction. Ordinary checks omit `initialize` and must contain the exact
retained committed generation in the live chain: forward evolution is allowed,
rollback and same-ordinal forks are rejected.

Entity reader admission and writer intent require this initialized anchor plus a
fresh successful check in the same locked session. Reopening resets freshness;
failed checks clear it and successful writer intent consumes it. Previously issued
reader reservations are never revoked. The freshness token is not durable permission,
and `_record_authority_check` is an internal verifier seam, not a public anchor setter.
Raw-only operation needs no entity baseline.

`authority_checks` retains canonical predecessor-linked proofs, capped at 64 KiB
each and 10,000 checks. Open validates the entire chain in 256-row batches; corruption
or exhaustion refuses rather than pruning. The authority revision passes 236 main
coordination and 299 standalone focus tests, including upgrade/authority COMMIT
death and lost-acknowledgment, forward/rollback/fork, request-integrity and entity
integration controls. Frozen full authority regressions pass 579 main tests (three
skips) and 439 standalone tests (two skips), before completion/history work. The 257-row
batch control extends authenticated repeat-check metadata synthetically; it is not
257 real verifier runs or manufactured data/recovery evidence. A coherent rollback
of both the ledger and entity authority still requires independent external retained
evidence and exclusion; these local tests do not qualify that recovery.

### Ledger-v3 normal completion and retained history

`writer.execute_and_complete_prepared` requires an evolving UTC clock and the
selected due hour. It rechecks the effective receipt, commits intent before launch,
supervises the standalone writer, and obtains fresh independent data/authority
verification internally under the uninterrupted lock. It rechecks original captures,
requests, receipt/runtime and bounded original phase-archive byte identities before
COMMIT. Main archive checks do not import compactor SQL; all data verification runs
separately in DuckDB 2.0.0. A caller-provided report cannot certify completion.

One transaction retains the predecessor-linked completion proof, marks the run
`COMPLETED`, retains/advances optional authority, advances exactly one hour and
selects `DRAINING` for due backlog or `OPEN` otherwise. It does not admit readers or
publish grants; service integration must reobserve time before doing so. The local
origin audit, proof/run/audit/cursor consistency and prior-completion clock floor
detect tested local corruption/regression, not coherent whole-ledger/store rollback.

Proofs are capped at **128 KiB each / 10,000 completed runs**, validated in 256-row
pages without pruning. Receipt capture/report ceilings also apply to retained proof
semantics. Completed entity proofs retain the fresh authority observation in the
same transaction. Any pre-COMMIT uncertainty retains the original blocked intent;
a lost acknowledgment after COMMIT retains the completed outcome. Existing blocked
or restart-blocked intents cannot use this normal-success path and require explicit
audited recovery, whose commands are not implemented.

Ten active hardening failures precede repairs. The repaired completion/upgrade
focus passes **46**, and independent semantics/compatibility/paging/secret controls
pass **24**, before three final budget/clock red/green controls pass. The three-hour
paging control uses genuine completions with instrumented two-row pages; it is not
257 real completions or representative retained-state sizing. Final combined focus
passes **298 tests in 517.39 s**; full standalone passes **439 tests, two skips in
170.76 s**. Full main passes **641 tests, three skips in 1055.19 s** on the
unchanged revision. These close local regression, not service/platform/endpoint
acceptance. Linux raw/entity abrupt-death
tests cover external success before COMMIT and before/after completion COMMIT,
not physical power loss, production scheduling or actual macOS/S3 acceptance.

## Human-run Mac baseline (next task)

The committed `scripts/dcc_macos_baseline.py` and
`scripts/dcc_macos_source_manifest.json` replace the scratch/uncommitted overlay
procedure. Use a fresh disposable checkout at the **full qualification commit**
recorded in `../Wintap-Analytics/wiki/work/dbt-compaction-coordination/macos-qualification.md`.
Provision the main and standalone environments separately from their locked
dependencies; never copy Linux virtual environments to the Mac.

From that PyUtil checkout, on an actual Mac:

```sh
uv sync --locked --dev --python 3.12
uv sync --locked --dev --python 3.12 --project extras/raw-sensor-compact
.venv/bin/python -B scripts/dcc_macos_baseline.py \
  --root . --manifest scripts/dcc_macos_source_manifest.json \
  --commit <full-qualification-commit> --output <new-private-directory-outside-checkout>
```

The harness requires exact HEAD, a clean tracked tree, the reviewed ancestor and
unchanged public source/lockfile hashes. It runs nine main and three standalone
test modules sequentially with private reports, isolated CWD/HOME/temp paths and
no inherited AWS credentials/profiles. Each stage has a 1,200-second ceiling.
Ordinary test failure still runs the other runtime; timeout stops the harness
because descendant shutdown is not certified. Return `summary.json` and sanitized
failure/skip details; raw reports remain private. This is **offline baseline only**,
not full-suite reruns, macOS reader-shutdown acceptance, actual-S3 qualification or
representative resource acceptance. The macOS reader backend/client remain absent.

Local harness controls are separate from the 641/439 runtime regression results:
`python -B scripts/test_dcc_macos_baseline.py`.

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
callback-based compactor certification or permission to reopen is added.

The supervisor manifest remains only a command observation, not an effective
configuration receipt. The separate execution library above supplies the required
pre-launch binding and negative tests; full service/completion integration remains
pending. Neither artifact permits reopening by itself.

Tests use real local DuckDB, synthetic messages and fake S3, including abrupt
`os._exit` failures around COMMIT and WAL recovery and a child surviving abrupt
tick death, real Linux detached/nested tree shutdown, caller interruption/death,
worker/logging/setup failures and evolving-clock boundaries. They do not qualify
power loss, the actual S3 endpoint, non-Linux supervision, shared-runtime scheduling
or actual-endpoint destructive writer operation. Receipt tests add local synthetic
CLI execution and injected remote resolution without changing those limits.
