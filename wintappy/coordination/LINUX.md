# Linux coordination integration

Linux service/client integration is available for isolated qualification, not
production deployment. Native-Mac reader shutdown remains with its native worker;
the accepted offline Mac baseline is not a backend pass. Windows is outside scope.
Actual S3, representative resources and broader partial-run recovery remain open
gates. The narrow deletion-disposition path below passes local regressions on the
retained 265-identity revision: 788 main tests/three skips and 509 standalone tests/
two opt-in S3 skips, exit 0. Complete reports and post-notification identities are
checked. This is not Linux, actual-endpoint or representative-resource qualification.

## Service

Run from the main locked environment; only DuckDB 1.5.6 opens the ledger:

```sh
.venv/bin/python -B -m wintappy.coordination.service init --config /path/to/config.json --confirm --reason 'External exclusion established; new dataset authority'
.venv/bin/python -B -m wintappy.coordination.service tick --config /path/to/config.json
.venv/bin/python -B -m wintappy.coordination.service status --config /path/to/config.json
```

`config/coordination.example.json` is a template, not a selected deployment or
qualified capacity budget. Replace synthetic roots, profile, first unfinished UTC
hour, event allowlist and paths. Writer project/executable must identify this
checkout's separately provisioned DuckDB 2.0.0 environment. Keep ledger, spill,
phase evidence, analysis outputs and data roots separate. Entity mode adds exact
entity/metrics roots and namespace/hosts/policy/ceiling to the binding; initialize
its independently verified high-water using `init-authority --confirm --reason ...`
only after explicit external initialization/cutover under exclusion. `upgrade`
requires confirmation/reason and preserves old reservations/intents.

The service completes bounded terminal-first ingestion, refreshes UTC before
admission, drains due work and retries committed grants. With zero reservations,
it prepares the effective receipt and uses the normal same-lock writer→independent
verifier→atomic completion path. It launches at most one hour per tick and refreshes
UTC again after completion COMMIT before admitting queued readers. Due backlog
retains priority. Restart-blocked intents never enter this normal-success path.
The old callback `coordinator.tick` remains conservative and cannot complete a run.

## Readers

For S3-backed DBT, configure the matching dataset and separate local retry state,
alongside the usual DBT roots/window/profile settings:

```sh
export WINTAP_COORDINATION_DATASET_ID=research-dataset
export WINTAP_COORDINATION_ROOT=s3://example-bucket/coordination/research-dataset
export WINTAP_COORDINATION_CLIENT_STATE=/path/to/local-client-state
make dbt-build
```

The Makefile routes source/build/test/docs/debug through offline active-root
classification. Entirely local inputs call ordinary DBT directly: no SDK, grant,
client state, compactor installation or entity bootstrap. Inactive metrics overrides
and unused coordination/AWS settings do not activate coordination. A build holds one
grant through source preflight, all DBT nodes/tests and descendant shutdown. An
unfinished entity hour fails preflight and releases after shutdown; it never waits
under a grant for the excluded writer or silently changes the window.

Remote readers retain their exact request before PUT, poll their exact grant key
(default ten seconds), and durably mark launch before any data-reading child starts.
Requests/grants/terminal records and private bounded observations live under
`<client-state>/<job-id>/`. An immutable invocation manifest precedes request PUT;
launch binds that manifest to the exact grant. The original coordination root and
resolved non-secret endpoint/region/addressing style are retained there too;
retrying through a different transport or command/window scope refuses. State paths
cannot traverse symlinks,
and the client lock is a persistent single-link regular inode. Identical terminal
retries do not rerun DBT. A successful
release PUT returns immediately without a coordinator acknowledgment; uncertain
delivery returns an outstanding job ID and `terminal_pending: true`. Shutdown
uncertainty or wrapper death retains the launch marker and reservation. Never delete
job state or create a replacement identity to bypass recovery.

SIGINT/SIGTERM preserve exit 130/143. An interrupted reader releases only after
durable command-matched supervision establishes complete child shutdown; missing,
changed or uncertain evidence retains the reservation. Pre-grant cancellation
retains its signal outcome locally for identical terminal retries. Unrelated
`SystemExit` values are not interpreted as shutdown evidence.

```sh
.venv/bin/python -B -m wintappy.coordination.client retry-terminal --job-id <job-id>
.venv/bin/python -B -m wintappy.coordination.client shared-command -- <source-reading-command>
```

Generic commands use the same binding/window/settings and lifetime contract.
Commands delegated to external services are not contained by the Linux descendant
backend. Native-Mac coordinated execution currently refuses explicitly. Local-only
DBT remains available there. Optional polling interval is selected with
`WINTAP_COORDINATION_POLL_SECONDS`; log/time/shutdown budgets remain bounded.

## Advisory producer interface

`<state>/status.json` is a private atomically replaced **advisory** snapshot,
exported after intent COMMIT (before launch) and after service transitions. It never
authorizes data access or changes the ledger. Export errors are sanitized and cannot
undo completion or permit reads. A crashed/busy writer may leave stale status:
missing/old observations mean unknown, never inactive. The separately planned utility
may read this file without opening DuckDB; it is not implemented here.

`status_page` / CLI `status --after-job ... --after-run ... --after-maintenance ...`
returns version, explicit advisory marker/time, binding/ledger identity,
mode/cursor/reservation counts, optional high-water, at most 100 summaries each for
jobs, runs and maintenance, and explicit continuations. Maintenance summaries
identify the original run and show `PENDING`, `FINISHED` or `ABANDONED`; none of
these states certifies hour completion. Each
page is limited to 1 MiB and has its own observation time. Pages are not an immutable
full inventory; records may arrive between pages. No commands/environment, raw
reports, credentials, partition catalog or fabricated phase timestamps are exported.
Every CLI status/operation still takes the same ledger lock.

## Audited recovery boundary

`release-reader --job-id ... --confirm --reason ...` records operator cancellation
and its audit atomically. The human must establish that the original wrapper and
all descendants cannot resume with cached permission; age, missing objects or an
isolated PID observation are insufficient. It does not invent a DBT exit outcome.
Both its operator marker and exact reason/identity/terminal-bound audit must survive
reopen; missing or changed audit evidence refuses ordinary ledger operation.

`resolve-hour --run-id ... --confirm --reason ...` resolves original clean, complete
executions, including external success before completion COMMIT.
It reloads the committed immutable receipt, repeats fresh independent original
archive/data/current-authority/historical-checkpoint/shutdown verification under the
same lock, then atomically retains completion plus a reasoned cannot-resume audit.
It never relaunches the original writer, accepts caller-supplied proof or treats
confirmation as data evidence. Recovery audits are checked on reopen.

### Exact checkpoint-publication repair

The additional supported case is an **already blocked, entity-enabled,
deletion-enabled original writer** whose recorded exit-2 failure follows complete
raw migration/deletion and retained checkpoint candidates. This is not general
partial-run recovery. First establish that all original processes cannot resume,
preserve the original receipt, observations and phase archive, and correct the
publication obstruction under external exclusion. Then, from the main runtime:

```sh
.venv/bin/python -B -m wintappy.coordination.service repair-checkpoints --config /path/to/config.json --run-id <original-run-id> --confirm --reason 'Original processes cannot resume; exclusive checkpoint repair'
.venv/bin/python -B -m wintappy.coordination.service resolve-hour --config /path/to/config.json --run-id <original-run-id> --maintenance-id <returned-maintenance-id> --confirm --reason 'Exclusive repaired-hour resolution; original processes cannot resume'
```

Repair authenticates the original receipt/command/exit-2 shutdown/captures before
launch. It publishes a separate immutable maintenance receipt and commits its
intent **before** supervising the qualified standalone `checkpoints-repair` command
under the same ledger lock. Roots, original hour, runtime and budgets remain bound
to the original execution. A successful repair retains its own observations/audit
but leaves the original block and cursor unchanged.

Only `resolve-hour` with that exact finished maintenance ID repeats independent
raw/entity/historical-checkpoint and both-archive verification, rechecks original
and maintenance captures, and atomically retains completion plus the recovery
audit/cursor/high-water transition. The completion proof preserves actual original
exit 2 and separately authenticated repair exit 0; it never rewrites the original
archive as success. Forward entity evolution is allowed only through retained
committed lineage; historical checkpoint hashes are not compared with today's
aggregate.

A conflicting checkpoint needs **separate explicit**
`--confirm-replace-checkpoints` on `repair-checkpoints`; prior rows remain in the
repair archive. A failed or crashed maintenance attempt stays `PENDING` and prevents
another repair or resolution, even if data-side repair appeared successful. Once
the human establishes that that attempt also cannot resume:

```sh
.venv/bin/python -B -m wintappy.coordination.service abandon-maintenance --config /path/to/config.json --maintenance-id <pending-maintenance-id> --confirm --reason 'Original and maintenance processes cannot resume'
```

Abandonment records an audit; it does not resolve the original writer, advance the
cursor or admit readers. A new repair has a new identity and still requires all
original evidence. Never delete the pending audit or turn an orphan receipt into
permission. Maintenance history refuses beyond 128 KiB per audit record, 10,000
operations or 64 MiB aggregate; reads use 256-row batches without pruning. Retained
completion/history validation on reopen uses ledger evidence, not historical
runtime files; fresh resolution still requires those files.

### Exact partial-deletion disposition

`repair-deletions` supports an **already blocked, deletion-enabled original writer**
with actual recorded exit 2, stopped children and complete retained migration
evidence for every selected event/protocol. One or more original hour outcomes must
record deletion failure. Entity mode additionally requires valid current committed
authority and the original historical checkpoint candidates/receipts. Missing
original migration coverage or unresolved entity publication is not repairable here.

After establishing that all original processes cannot resume and correcting the
deletion obstruction under external exclusion, use the main locked runtime:

```sh
.venv/bin/python -B -m wintappy.coordination.service repair-deletions --config /path/to/config.json --run-id <original-run-id> --confirm --reason 'Original processes cannot resume; exclusive deletion disposition'
.venv/bin/python -B -m wintappy.coordination.service resolve-hour --config /path/to/config.json --run-id <original-run-id> --maintenance-id <returned-maintenance-id> --confirm --reason 'Exclusive disposition resolution; original processes cannot resume'
```

The maintenance command binds the original roots/hour/event scope, runtime, budgets
and deletion-confirmation settings. Its separate receipt and committed intent precede
standalone `deletions-repair` launch under uninterrupted coordinator exclusion.
Before deleting anything, it authenticates original input/output identities, counts
and schemas; every survivor must be an unchanged original object and every output
must still match. Extra objects, markers or changed outputs refuse, even if survivor
row counts happen to match. Prefix traversal counts all objects and local directory
nodes against aggregate ceilings before Parquet discovery, with a 64-level local
depth ceiling. All selected-hour survivors, outputs, checkpoints and current entity
authority are revalidated throughout mutation.

Successful repair records actual survivor/count/absence observations separately.
Entities are validated, **never replayed**. New disposition checkpoints retain the
original historical entity provenance, using the repair run/operation and actual
new status/time; original archived candidates and failure records stay untouched.
Conflicting coverage requires `--confirm-replace-checkpoints` on `repair-deletions`
and retains prior rows. Repair alone does not advance the cursor or admit readers.

Separate `resolve-hour` authenticates original exit 2 and maintenance exit 0, binds
both archive digests/counts to their retained captures and repeats independent
read-only disposition/current-authority verification before atomic completion.
Public completion and authority verification remain limited to **one hour**; the
standalone repair's multi-hour capability does not widen coordinator completion.
Failed/crashed maintenance requires the same audited cannot-resume abandonment
described above; abandonment never certifies data or resolves the original block.
Generic `reconcile-delete` compares surviving rows and can replay entities; it is
not a substitute for this original-bound disposition and cannot certify the failed
original. Receipt v2, completion-request v4 and completion-proof v3 carry deletion
disposition; existing formats remain supported without a ledger schema change.

Incomplete migration archives, missing original evidence, unresolved entity
publication and general exclusive entity init/replay remain unsupported and blocked.
The provisional raw-only retry path below does not cover these cases. Do not run standalone mutating recovery commands
alongside coordinated clients or use init/an old backup to clear a block. Preserve
original records and require explicit externally managed exclusion. Actual-S3 and
representative-resource qualification remain pending for both narrow repair paths.

### Retry preflight is not a recovery command

The internal `retry.verify_retry_prepared` prerequisite performs independent
read-only verification for exactly one raw-only failed-merge event/hour with
deletion disabled, original inputs unchanged and selected output absent. Original
receipt/intent/archive, actual exit 2 and stopped-child captures remain authenticated.
A durable request precedes supervised standalone verification; original/context
checks repeat afterward. It returns evidence only, leaving the failed original,
cursor and blocked exclusion unchanged. Independently reported row/file/byte totals
must match the bounded authenticated original migration intent exactly; current
Parquet verification stays in standalone DuckDB 2.0.0. This read-only API supplies no
durable retry disposition or entity init/replay permission. Do not use a
passing preflight to launch another writer or release readers. Frozen preflight
controls pass 33 main/45 standalone; affected main passes 548 tests/one isolated-local
migration opt-in skip, affected standalone passes 248, and full standalone passes
554/two opt-in S3 skips. All 269 source/test/DBT/runtime-lock identities match after
notifications. Full main passes 821/three skips, launched only after its successful
affected notification, complete-report inspection and unchanged-identity check.
Its complete report and all identities also check out. The read-only preflight local
regression gate is closed; retain its baseline before separate durable retry/entity
work. Local preflight regressions remain separate from deletion's accepted baseline
and from retry, endpoint/resource or native-Mac qualification.

### Provisional Linux intentional retry — qualification open

`retry-hour` is implemented for an exact raw-only failed-merge event/hour, deletion
disabled, original inputs unchanged and output absent. It is not production-qualified.
Under externally established exclusion, and only after verifying that all original
processes cannot resume, the main locked runtime accepts:

```sh
.venv/bin/python -B -m wintappy.coordination.service retry-hour --config /path/to/config.json --run-id <original-run-id> --confirm --reason 'Original processes cannot resume; exclusive exact-original retry'
```

The service loads original receipt settings, not current writer overrides. It rejects
unrelated reader/maintenance/paging flags before configuration resolution and makes
no transport, admission or grant calls. Ledger v4 requires an explicit compatible
v1/v2/v3 upgrade; ordinary open never upgrades. A fresh independently observed
preflight, immutable new receipt and disposition precede atomic new-intent ownership.
The failed original stays `BLOCKED` with its original audit/archive, no completion
proof and no cursor change. There is no admission-open interval.

Distinct independent post-COMMIT/pre-launch evidence is durable before launch. Fresh
independent completion binds equal original/new source hashes, partitions, schema and
counts before atomic new-run completion/cursor/backlog mode. An evolving clock sets
the final mode; queued readers are not admitted by this operator command. Normal
completion and audited `resolve-hour` share the source-identity checks. Failure,
restart or acknowledgment uncertainty preserves the block; never resume a committed
retry or treat its orphan receipt as launch permission. Retained retry history refuses
beyond 128 KiB/record, 10,000 operations or 64 MiB aggregate, with bounded paged reads
and no evidence pruning. Ordinary reopen validates retained JSON, not historical
runtime files; fresh resolution still requires authenticated original/new files.

The repaired pre-freeze combined run passes 42 controls. Corrected genuine retained-
history/exclusion controls pass 13; actual v3 upgrade crash/lost-ack controls pass three.
Local frozen-credential controls pass two. Separate 275-identity successor freeze
created/checked; frozen standalone focus passes 45 and affected passes 248, with all
identities unchanged after each result. Main focus passes 119 with unchanged identities;
standalone full passes 554/two opt-in S3 skips with unchanged identities. Main affected
passes 607/one isolated-local migration opt-in skip with unchanged identities; full
main passes 880/three opt-in skips with unchanged identities. Complete reports and
prerequisite stage checks pass, closing the combined intentional-retry local gate.
Retain the 275-identity freeze/reports before separate entity-only controls/regressions.
Actual S3 and representative resources remain open. General entity-only init/replay
ownership remains unimplemented. Native-Mac shutdown stays its worker's separate gate.

The first entity-only control run retains 22 missing-API failures in 35.05 s, not
accepted refusals or executed crash boundaries. No init/replay permission path exists.
Separate receipts/verifier/ownership and original-bound recovery require their own
red/green and successor gates. Do not route entity commands through public one-hour
`compact` validators or run standalone mutation around ledger exclusion.

Standalone entity-verifier first run retains 64 missing-module failures (29.53 s);
complete report inspected and all 275 accepted identities unchanged. Distinct receipts
retain 26 first-red failures (0.51 s); bounded receipt preparation now passes 26 controls
(6.16 s). Complete reports, fresh lint/format and unchanged accepted identities checked;
separate standalone verifier passes its initial 64 controls (36.47 s), with complete
report/identity checks. Corrected review red retains 21 failures/two passes (6.94 s),
with complete report/identity checks. Verifier-only streaming SQL, original-hour,
early-configuration and historical-publication repairs pass fresh scoped lint/format;
both repair reruns retain 12 failures/75 passes (35.57 s / 35.77 s), with complete
report/identity checks. Bounded SQL staging avoids the missing optional UDF dependency
without changing runtime locks; fresh five-target lint/format passes. Expanded
qualification first red retains 35 failures/9 passes (20.71 s), with complete
report/identity review. Inventory-count defect is repaired; combined run passes 131
controls (91.07 s). Ten expanded controls retain five fixture failures/five passes;
fixture-only legacy Unicode-selection/SQL-order corrections pass ten (8.49 s), with
complete report/identity checks. Fresh five-target lint/format passes; distinct expanded
combined rerun passes 141 controls (97.91 s), with complete report/identity checks.
Standalone verifier focused evidence is closed; durable publication authentication and ownership
remain missing. Explicit-v5 controls retain seven unsupported-
argument failures (35.10 s); post-verifier ownership rerun retains 22 missing-module
failures (44.08 s). Both complete reports reviewed and failed fixtures preserved;
all 275 accepted identities match before implementation. No crash boundary executed.
Receipt preparation grants no launch permission; public
one-hour validators remain unchanged.

Explicit confirmed `Ledger.upgrade(..., target_version=5)` is schema staging only:
default initialization/upgrade still targets v4, ordinary open never migrates and
nonempty entity history was rejected at schema closure, before the focused-only typed overlay. Original raw
permissions/evidence/cursor survive conversion. Seven actual v4 upgrade crash/lost-ack
and 37 expanded compatibility controls pass. The 43-control genuine-history focus
passes (109.80 s), complete report reviewed and all 286 identities match. Existing-
ledger first compatibility retains two failures/149 passes (401.75 s), complete
report/identity review done. Actual COMMIT/lost-ack/row checks precede a repeat-upgrade
diagnostic mismatch; later assertions did not execute. Default-v4 wording alone is
restored, without weakening tests/refusals or changing opt-in-v5 wording. Failed
fixtures remain preserved. A separate 286-identity repair freeze binds repaired
46-control focus/151-control compatibility runs. Focus passes 46 (127.49 s), complete
report reviewed and all identities match; compatibility passes 151 (399.51 s), complete
report/identities reviewed. Schema compatibility is closed; retain both freezes and
original failures before distinct main-side entity observation/ownership source work.
Do not launch entities around unsupported-history refusals or interpret schema selection
as fresh authority, entity completion, service permission or Linux acceptance.

The qualified main-side entity input-observation slice is read-only and owner-free v5 only.
It authenticates actual canonical single-link verifier evidence and fresh original-input
equality; it never launches entities or changes ledger rows/authority/cursor/admission.
Twenty-one controls retain missing-module first reds (16.90 s), complete report/identity
review done. First adapter rerun retains one command-byte acceptance failure/20 passes
(65.77 s), complete report/identities reviewed. Exact command/shutdown-byte repair and
six expanded artifact controls pass 27 (83.28 s), complete report reviewed and all 288
artifact-repair identities match. Artifact focus is closed, not broader input qualification
or successor full gates. Failed fixtures remain preserved. All 286 schema-repair identities
remained unchanged at input-focus closure. Wider typed post-COMMIT qualification,
durable original-generation publication and completion still require separate work.
Expanded input qualification retains nine failures/22 passes (102.92 s), complete
report reviewed and all 289 identities match before repair. Adapter-only exact totals/
early-clock repair preserves all controls and other 288 identities. Combined 58-control
rerun passes 58 (185.12 s); complete report reviewed and all 289 contract-repair
identities match. Main-side input-authentication focus is closed; no input-focus test remains active.
Explicit-v5 typed intent/restart-exclusion remains separate. Historical failures and
fixtures retained; no entity launch/completion/publication or Linux acceptance inferred.

Exclusion-only `entity_intent.py` implementation retains 33 fixture-upgrade failures/
one v4 pass (34.61 s), after complete import-masked first-red/290-identity review.
Complete implementation report/291 identities reviewed; typed assertions/crash-child
launch remain masked in that run. Same-connection keyset paging and early typed history/
budgets repair transaction visibility. The unchanged repair rerun passes 34 (125.03 s),
complete report/all 291 identities reviewed; genuine COMMIT/restart/lost-ack and
post-COMMIT input evidence is established. Separate staged-history/budget/keyset-tail/
semantic focus passes 24 (79.82 s), complete report/all 292 identities reviewed, with
repair source/controls unchanged. Main combined typed/input/schema/receipt/original-
ledger compatibility focus passes 336 (884.70 s) on the unchanged successor freeze;
standalone entity-verifier focus passes 141 (105.56 s), complete reports/all 292 identities
reviewed. Exclusion-only focus is closed. Next add genuine read-only current-generation
main completion authentication, without ownership/authority/cursor transitions or caller
historical-publication fallback.
Affected/full gates remain separate.
Typed intents bind original receipt/input evidence and unchanged raw cursor; post-COMMIT input checks
require acknowledged same-session ownership and actual original artifacts. Failures,
reopen and lost acknowledgments remain BLOCKED. No entity operation is launched;
completion/publication/recovery/service dispatch and broader acceptance remain open.

This is cooperative exclusion, not fencing of sensor uploads or hostile/external
services. Whole-ledger-plus-authority rollback freshness needs independently retained
external evidence and exclusion. Crash/WAL tests do not qualify physical power loss.
The status utility's Live diagnostic exception cannot bypass DBT, authoritative
verification, ledger locking or recovery.
