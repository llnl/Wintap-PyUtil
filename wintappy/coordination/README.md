# Coordination libraries (implementation slices)

The Linux `service` CLI and durable reader wrapper integrate the existing
libraries. **Actual S3, representative resources, native-Mac DBT/platform acceptance
and partial-run recovery remain open gates; do not enable production cron.** Read
[Linux integration](LINUX.md) and [native macOS reader supervision](MACOS.md) for
platform-specific commands and limits. Library-slice
history below retains its original scope; references to missing integration describe
those historical revisions, not the current Linux service.
Current Linux audited recovery supports operator reader cancellation, fresh
clean-original-hour resolution and exact checkpoint-publication repair for an
already blocked entity/deletion writer. Repair alone keeps the original block;
resolution retains original exit-2 evidence plus a separate authenticated repair.
Failed/crashed maintenance requires audited cannot-resume abandonment before another
attempt. Missing migration/deletion evidence, unresolved entity publication and
general entity maintenance remain unsupported. The provisional Linux intentional-
retry path below has closed local regressions; endpoint/resource qualification stays open.
Local maintenance/recovery checks pass 32 tests, standalone repaired/verifier checks
pass 66, and nine later CLI/crash/replacement/history checks pass on intermediate
revisions. Full standalone now passes 460 tests/two skips on the 261-identity freeze;
affected main finishes with 303 passes/one missing-cursor test-wrapper failure.
The wrapper-only paging/corruption repair passes; combined rerun passes 304 tests
in 715.34 s with all 261 identities unchanged after notification. Full main passes
755 tests/three skips in 1156.34 s, with the complete report inspected and all 261
identities unchanged after notification. These counts are not
full Linux acceptance. Commands and limits are in [LINUX.md](LINUX.md).
The subsequent original-bound `repair-deletions` slice passes local regressions: it validates
retained original migrations and unchanged survivors/outputs, never replays entities
and retains original exit 2 plus separate disposition records. Repair preserves the
block; independent `resolve-hour --maintenance-id` is a separate atomic transition.
Public completion/authority verification stays one-hour-only. Separate frozen deletion
focuses pass 33 main / 49 standalone tests, with complete reports inspected and all
265 identities unchanged after notifications. Affected standalone passes 203 tests
in 177.37 s; full standalone passes 509 tests/two opt-in S3 skips in 274.88 s.
Both complete reports are inspected and all 265 identities match after notifications.
Affected main passes 515 tests/one explicit isolated-local migration skip in 1345.33 s,
with complete report inspected and all identities unchanged. Full main passes 788
tests/three skips in 1492.66 s; complete report inspected and all 265 identities
unchanged after notification. This closes deletion's local regression gate, not
Linux/endpoint/resource qualification. Retain this baseline and require separate
evidence for subsequent retry/entity-maintenance changes. Scoped source/test/wiki/privacy audits and
Ruff lint/format pass separately. The reader/checkpoint counts
above do not cover this subsequent slice.
Receipt-bound launch, separate read-only verification and entity-authority
initialization/freshness checks are tested on local synthetic data. The new ledger-v3
retained-history/normal-completion prototype passes hardened local entity, history,
crash, compatibility and secret controls. Final combined coordination focus passes
298 tests; frozen full regressions pass 641 main tests (three skips) and 439
standalone tests (two skips) on the unchanged revision.
This is not service or endpoint acceptance.
They are not qualified for actual-endpoint scheduling. The existing
local-only `make dbt-build` path remains direct. Do not enable a production cron job
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

`Ledger.initialize` now explicitly creates a version-four DuckDB ledger with its
immutable dataset binding and first unfinished UTC hour. `Ledger.open` refuses a
missing/corrupt/incompatible ledger; it cannot automatically initialize or promote
a backup. Both take the same nonblocking `fcntl.flock` on a persistent
`coordinator.lock` inode **before opening DuckDB**. Never unlink the lock to unlock
it. The context remains locked through any caller-managed external effects.

`Ledger.upgrade(..., confirmed=True, reason=...)` explicitly upgrades a compatible
v1/v2/v3 ledger under that same lock. It preserves the ledger/dataset identity, cursor,
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
and full local regressions pass; the uncommitted Linux service uses this adapter,
with endpoint/resource qualification still pending.
`execute_prepared` still blocks every outcome, and every previously blocked/restarted
intent requires explicit
audited recovery through the Linux service's narrow supported operations. Exit zero
or a PID check is not completion evidence. Checksums cannot detect a coherent
rollback of the entire ledger/store;
restore/recovery requires external exclusion and independent retained evidence.

`retry.verify_retry_prepared` is a separate **read-only prerequisite**, not an
operator retry command or permission transition. It authenticates the exact blocked
original's exit-2 failed merge and stopped-child capture, publishes an exact request
before supervising standalone DuckDB 2.0.0 verification, and rechecks original
receipt/intent/archive/captures and request/result evidence. Scope is deliberately
one raw-only event/hour, deletion disabled, unchanged original inputs and absent
output. The standalone verifier rechecks hashes and both prefixes after data
observations. The failed original, cursor, ledger history and admission block remain
unchanged. Main requires the independently reported source rows/files/bytes to match
bounded original migration-intent totals authenticated by the original failure
capture; main reads JSON only, not current data SQL. This API supplies no durable
retry or general entity-maintenance permission. Separate preflight hardening passes 33 main/45 standalone frozen
controls. On the unchanged 269-identity revision, affected main passes 548 tests/one
isolated-local migration opt-in skip, affected standalone passes 248, and full
standalone passes 554/two opt-in S3 skips. Full main passes 821/three skips. Complete
reports and all identities are checked after notifications; each full began only
after its own affected checks. This closes the read-only preflight local regression
gate; retain that baseline for separate durable retry/entity work. These local gates
do not supersede the accepted deletion baseline or
qualify retry permission, actual S3, resources or native-Mac shutdown.

The active successor adds `intentional_retry.retry_and_complete_prepared` and Linux
`service retry-hour`. After human confirmation that original processes cannot resume,
fresh independent verification under one uninterrupted ledger lock may atomically
commit a bounded predecessor-linked disposition plus a new receipt-bound owner. The
failed original remains exactly `BLOCKED`, with no original completion, cursor change
or admission-open interval. A distinct post-COMMIT/pre-launch observation and equal
original/new migration identities are required; normal completion and audited
resolution share these checks. Uncertain retries remain blocked and cannot resume
automatically. Frozen ephemeral credentials are reused under the original storage
scope; no secret is retained. Public verification remains one-hour-only.

The repaired pre-freeze run passes 42 genuine retry/compatibility/service controls;
corrected retained-history/exclusion controls pass 13 and actual v3 upgrade crash/
lost-ack controls pass three; local frozen-credential controls pass two. A separate
275-identity successor freeze is created/checked. Frozen standalone focus passes 45
and affected passes 248; main focus passes 119, with unchanged identities after each
result. Standalone full passes 554/two opt-in S3 skips with unchanged identities.
Main affected passes 607/one isolated-local migration opt-in skip with unchanged
identities; full main passes 880/three opt-in skips with unchanged identities.
Complete reports and each runtime's prerequisite checks pass, closing the combined
intentional-retry local gate. Retain the 275-identity freeze/reports before separate
entity-only work with its own controls/regressions. Linux acceptance is not claimed.
Entity-only ownership remains separate and
unimplemented. See [LINUX.md](LINUX.md) before any isolated qualification; no production
cron, remote dataset mutation or grant expiry is authorized.

Separate entity-only ownership has 22 lint/format-clean controls, all red at the
missing execution API (35.05 s); report/failed fixtures are retained and all accepted
275 identities match. The four child crash controls have not reached their boundaries.
No coordinated init/replay permission exists. Distinct receipts/verifier/ownership
and original-bound non-mutation/authority/checkpoint evidence remain required; public
one-hour compaction/authority validators stay strict.

Standalone entity-verifier first run retains 64 missing-module failures (29.53 s),
with complete report inspection and all 275 accepted identities unchanged. Genuine
original fixtures exist, but no verifier control passes. Distinct receipts retain
26 first-red failures (0.51 s); new bounded receipt preparation passes 26 controls
(6.16 s), with complete report inspection, fresh lint/format and unchanged accepted
identities. Separate standalone verifier passes its initial 64 controls (36.47 s),
with complete report and accepted-identity checks. Corrected review red retains 21
failures/two passes (6.94 s), with complete report/identity checks. Verifier-only
streaming SQL, original-hour, early-configuration and historical-publication repairs
are unqualified. Both repair reruns retain 12 failures/75 passes (35.57 s / 35.77 s),
with complete report/identity checks. Bounded one-record SQL staging avoids the UDF's
missing optional NumPy dependency without runtime-lock changes. Fresh five-target
lint/format passes. Expanded first red retains 35 failures/9 passes (20.71 s), with
complete report/identity review. Repaired Inventory-count defect; combined run passes
131 controls (91.07 s). Ten expanded controls retain five fixture failures/five passes;
fixture-only legacy Unicode-selection/SQL-order corrections pass ten (8.49 s), with
complete report/identity checks. Fresh five-target lint/format passes; distinct expanded
combined rerun passes 141 controls (97.91 s), with complete report/identity checks.
Standalone verifier focused evidence is closed; durable publication authentication
and entity ownership remain missing. Latest explicit-v5 controls retain seven unsupported-argument failures
(35.10 s); post-verifier ownership rerun retains 22 missing-module failures (44.08 s).
Complete reports reviewed, failed fixtures preserved and all 275 accepted identities
unchanged before implementation; no upgrade or entity crash boundary executes.
Preparation grants no launch permission. Public one-hour validators stay strict.

Explicit confirmed `Ledger.upgrade(..., target_version=5)` now stages separate
entity-run/evidence tables. Defaults remain v4; ordinary open never migrates. Source
validation and one actual upgrade COMMIT preserve original rows/proof bytes/cursor.
Seven actual v4 crash/lost-ack controls pass (36.93 s); expanded compatibility passes
37 (48.43 s), complete reports reviewed. New entity history deliberately refuses
nonempty records at schema closure, before the focused-only typed overlay. Schema selection does not confer
launch/completion permission or a fresh authority token. The 43-control genuine-history
focus passes (109.80 s), complete report reviewed and all 286 identities match.
Existing-ledger first compatibility retains two failures/149 passes (401.75 s),
complete report/identities reviewed. Actual COMMIT/lost-ack/row checks precede the
repeat-upgrade diagnostic mismatch; later assertions did not execute. Only original
default-v4 refusal wording is restored, retaining tests, semantics and opt-in-v5
wording. Failed fixtures remain preserved. Separate 286-identity repair freeze binds
repaired 46-control focus/151-control compatibility runs. Focus passes 46 (127.49 s),
complete report reviewed and all 286 repair identities match; compatibility passes 151
(399.51 s), complete report/identities reviewed. Schema compatibility is closed, not
entity permission or successor full gates. Main-side observation authentication comes
before typed ownership/history. Preserve both schema freezes and original failures.
At schema closure only `ledger.py` differed from the accepted retry baseline; later
input-adapter additions are separate source work.

New main-side `entity_verification.py` is a read-only input-observation sub-slice,
not ownership or completion. It requires owner-free explicitly upgraded v5 exclusion,
zero readers, bound receipts and actual canonical single-link request/command/capture/
shutdown evidence. Prior observations are rechecked before a fresh unchanged-input
verification; no main data SQL, dataset/ledger-row mutation, authority initialization,
cursor movement or reader admission is added. Result/request/proof budgets are
64 KiB/1 MiB/128 KiB, refusing rather than pruning. Twenty-one controls retain their
missing-module first red (16.90 s), complete report/identity review done. First adapter
rerun retains one command-byte acceptance failure/20 passes (65.77 s), fully reviewed.
Exact command/shutdown-byte rechecks and six further artifact controls pass 27
(83.28 s), complete report reviewed and all 288 artifact-repair identities match.
Artifact focus is closed, not broader input qualification or successor full gates.
Failed fixtures remain preserved. All 286 schema-repair
identities remained unchanged at input-focus closure. The later typed overlay has
focused evidence only; durable original-generation publication/completion has no permissive fallback.
Separate expanded qualification adds 31 controls for genuine mixed-hour inputs, actual
committed exclusion/competing processes, exact contracts, clocks and budgets. First
run retains nine failures/22 passes (102.92 s), complete report reviewed and all 289
identities match before repair. Missing exact hour/aggregate equality and late clock
refusal are repaired only in the adapter, preserving all controls and other 288
identities. Combined rerun passes 58 (185.12 s); complete report reviewed and all 289
contract-repair identities match. Main-side input-authentication focus is closed; no
input-focus test remains active. Explicit-v5 typed intent/restart-exclusion is separate,
not entity launch/completion/publication or successor-regression acceptance. No input
dictionary alone can authorize typed ownership or publication.

The separate `entity_intent.py` prototype now commits explicit-v5 typed exclusion
only, retaining original receipt/input observations and upgrade/authority identities.
An acknowledged same-session original-bound input check grants no entity launch;
failed checks/reopen/lost acknowledgment retain BLOCKED ownership without moving the
raw cursor or authority. Entity completion/publication/recovery history still refuses.
First 34-control run retains import-masked failures (47.41 s), full report/290 identities
reviewed; implementation retains 33 fixture-upgrade failures/one v4 pass (34.61 s),
complete lossless report/291 identities reviewed. No typed assertions or crash-child
launch executed in that run. Same-connection keyset paging repairs transaction visibility, with
typed history/budget validation before owner-ID/JSON reads. A distinct unchanged
34-control repair rerun passes (125.03 s), complete report and all 291 identities
reviewed. Genuine typed COMMIT/restart/lost-ack/post-COMMIT input evidence is established.
A separate staged-history/budget-before-fetch/keyset-tail/semantic run passes 24
(79.82 s), complete report/all 292 identities reviewed, preserving repair source/
controls. Main combined typed/input/schema/receipt/original-ledger compatibility focus
passes 336 (884.70 s) on the unchanged successor freeze; standalone entity-verifier focus
passes 141 (105.56 s), complete reports/all 292 identities reviewed. Exclusion-only focus
is closed. Next add genuine main current-generation completion-observation authentication,
without ownership/authority/cursor transitions or caller historical-publication fallback.
Affected/full regressions
remain separate. No entity operation launch, service integration or Linux acceptance
is inferred.

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
`now` remains a fixed observation for compatibility. The separate Linux service
wires an evolving clock; it does not change this conservative callback contract.

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
No adapter leaves due work `DRAINING`. The returned snapshot is advisory. This
describes the library baseline. The subsequent [Linux integration](LINUX.md)
implements service/client CLIs, advisory local `status.json`, independent completion
and narrow audited recovery; no S3 status mirror is implemented. Scheduling examples
exist, but no scheduler installation or production rollout is implied.

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
audited recovery. The current Linux commands and narrow checkpoint scope are
documented above and in [LINUX.md](LINUX.md), not part of this historical prototype result.

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
unchanged public source/lockfile hashes in both local files and the qualification
commit's blobs. An ignored local lockfile cannot fill an incomplete commit. The
exact reviewed main and standalone `uv.lock` files are tracked; do not regenerate
them for qualification. It runs nine main and three standalone
test modules sequentially with private reports, isolated CWD/HOME/temp paths and
no inherited AWS credentials/profiles. Each stage has a 1,200-second ceiling.
Ordinary test failure still runs the other runtime; timeout stops the harness
because descendant shutdown is not certified. Return `summary.json` and sanitized
failure/skip details; raw reports remain private. This is **offline baseline only**,
not full-suite reruns, macOS reader-shutdown acceptance, actual-S3 qualification or
representative resource acceptance. The macOS reader backend/client remain absent.

The native `scripts/dcc_macos_restriction_probe.py` is feasibility-only. It runs a
fixed synthetic DBT build/test using deprecated Apple sandbox APIs and private
CPython tracker/finalizer seams. It is not wired into the supervisor/client,
does not publish releases and does not qualify interruption or complete shutdown.

The original `8ee714c` Mac attempt was blocked by the missing committed main lock
before either test stage ran. Preserve that result and rerun the corrected full
commit named in the runbook with a new private report directory.

Local harness controls are separate from the 641/439 runtime regression results:
`python -B scripts/test_dcc_macos_baseline.py`.

## Linux command-tree supervision

`supervisor.run_supervised(command, directory, ...)` is a Linux/main-thread library
operation. Supply a new run directory on persistent local disk outside telemetry
and phase archives; its parent must already exist. It refuses existing directories
instead of overwriting previous observations. The caller is responsible for the
directory's exclusion/root binding. The separate Linux service supplies that
adapter; direct library callers still own this responsibility.

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
pre-launch binding and negative tests, and the separate Linux service integrates
independent completion. Broader qualification remains pending; neither artifact
permits reopening by itself.

Tests use real local DuckDB, synthetic messages and fake S3, including abrupt
`os._exit` failures around COMMIT and WAL recovery and a child surviving abrupt
tick death, real Linux detached/nested tree shutdown, caller interruption/death,
worker/logging/setup failures and evolving-clock boundaries. They do not qualify
power loss, the actual S3 endpoint, non-Linux supervision, shared-runtime scheduling
or actual-endpoint destructive writer operation. Receipt tests add local synthetic
CLI execution and injected remote resolution without changing those limits.
