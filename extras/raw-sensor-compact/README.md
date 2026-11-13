# Raw sensor hourly compaction

Standalone Python/UV tool pinned to **DuckDB 2.0.0**. Direct Parquet reads/writes,
without changing the sensor or Wintappy environment.

```text
source: lintap/raw_sensor/<event>/dayPK=YYYYMMDD/hourPK=HH[/protoPK=...]/...
output: lintap/compacted/<event>/dayPK=YYYYMMDD/hourPK=HH[/protoPK=...]/<merged>.parquet
```

For each **event/day/hour**, if the destination hour exists, skip it. Otherwise
merge all source files, separately for each optional protocol leaf. No run IDs,
generation directories, manifests, atomic reservations, or conditional PUT.

Rows are name-aligned `UNION ALL`: no deduplication, sorting, event-time filtering,
or normalization. Missing fields become NULL. Identical types and signed-integer
widening are supported; other drift fails before writing that hour. Each merged
leaf is reread for schema and source/output `COUNT(*)` equality. Counts include
NULLs and duplicates, but cannot detect same-count value edits or balanced
dropped/extra rows. Single-file/zero-row inputs retain a typed output. Multi-file
success must reduce file count.

## Setup and commands

From the Wintap-PyUtil repository root (the compactor has its own environment):

```bash
uv sync --project extras/raw-sensor-compact --locked --group dev
uv run --project extras/raw-sensor-compact --locked pytest extras/raw-sensor-compact/tests

AWS_PROFILE=lintap-alpha uv run --project extras/raw-sensor-compact --locked \
  raw-sensor-compact compact \
  --source-root s3://lintap-alpha/lintap/raw_sensor \
  --destination-root s3://lintap-alpha/lintap/compacted \
  --start-hour 2026-11-11T08:00:00Z --end-hour 2026-11-11T09:00:00Z \
  --event-type pidstat --target-size-bytes 134217728
```

Replace `compact` with `plan` and omit `--target-size-bytes` for a read-only
preview. Replace it with `verify` and omit the target for fresh operational row
counts. **Roots are direct event roots**, not parents to which `raw_sensor/` is
appended. The destination may be a sibling of source in the same S3 prefix.
There is no extra `raw_sensor/` directory inside `compacted/`.

Hours are completed whole UTC hours, inclusive start/exclusive end. Repeated
`--event-type` narrows scope; omission discovers immediate event-family prefixes.
No automatic lookback or event-time ordering. Approximate shard target is explicit
until a representative benchmark selects a default; 128 MiB above is an experiment
value. Shard filenames are implementation-defined, not source provenance fields.

JSON reports show each hour's `merged`, `planned`, `empty`, or `skipped_existing`
status, with source/merged files/bytes/row totals and delta for merged leaves.
`verify` checks the union of source/output leaf keys, so deltas cannot cancel
between protocols. Exit codes: 0 success/skip/empty, 1 verification count mismatch,
2 invalid input/read/schema/drift/merge failure. Errors identify the failed
destination hour when available. A skipped hour is **not** certified as a good merge.

## Skip and manual recovery

Any S3 object under the event/day/hour prefix means "exists", including a partial
Parquet, directory marker, or unrelated file. An existing local directory also
means skip, even if empty. The check is at the hour parent: if TCP exists, the
entire event/hour is skipped rather than appending a missing UDP leaf.
Skipped hours are not scanned/opened on the source side.

**Run one writer per destination; don't read an hour while it is being written.**
There is no concurrent-writer lock or all-or-nothing visibility. If a write or
validation fails, already written objects remain. Rerunning skips that hour until
you manually remove its failed destination subtree. Inspect the report and remove
only that exact derived hour, not source objects or a broad dataset root. The tool
never overwrites an existing destination hour. Failed output still needs manual
cleanup; an existing destination never triggers automatic source deletion.

Source inventory is rechecked before and after merging. This assumes immutable
source keys and quiescent selected windows, not an atomic snapshot. Late arrivals
to a previously merged hour are not automatically incorporated: existence still
means skip. A deliberate rebuild requires manual removal of its derived hour.

### Validated source-prefix deletion

`compact --delete-source` enables **merge → validate all leaves → delete the
entire source event/hour prefix**. Without the flag, source is retained.
`--grace-minutes` defaults to 10 and must be at least 5. The selected hours must
be past their UTC end plus this grace before any write occurs. The deployment
contract is that sensors upload into current UTC partitions; rare clock skew is
an explicitly accepted risk, not a reason for per-object deletion here.

Deletion includes non-Parquet markers under the exact source hour, not adjacent
hours. S3 keys are listed before batched deletion; per-key errors and remaining
objects fail the command. Local deletion refuses symlinks. Saved merge reports
include source/output totals/delta and deleted-object counts; `verify` comparing
source to output is useful **before deletion or in retain-source mode**, not after
source has been removed. Use a saved baseline/unioned consumer check after migration.

Run DBT only after a successful migration. Temporary overlap and partial deletion
on failures are accepted. The tool stops on failure and reports the source hour,
destination hour and phase; reconcile manually before DBT. Rerun still skips an
existing destination rather than completing a failed source deletion automatically.
Live deletion testing is approved on isolated copies only; original live objects
have not been authorized for a rollout deletion.

Daily rollup/scheduling and the upstream actual-upload-time correction remain separate.
Wintappy's shared input macros now scan both `<dataset>/raw_sensor/<event>/...`
and `<dataset>/compacted/<event>/...` with name alignment and UNION ALL semantics.
`WINTAP_DBT_COMPACTED_ROOT` overrides the direct compacted event root if needed.
No deduplication or compacted-preference rule masks overlap.

## S3 and limits

Use standard AWS credentials or `AWS_PROFILE`/`--profile`. Optional flags:
`--region`, `--endpoint-url` (HTTP(S) origin), `--extension-directory`.
Endpoint precedence: flag, `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT_URL`, SDK/profile.
boto3 and DuckDB share a frozen session identity, resolved endpoint and path-style
addressing; TLS follows the URL. Credential refresh is unsupported. `httpfs` must
be cached or downloadable on first use. Internal results are fully drained with
`.fetchall()` because DuckDB 2.0.0 extension APIs reject unfinished results.

Defaults: 2 threads, 1 GB memory, 1 GB spill, 300-second cooperative timeout,
24 hours and 10,000 processed source files. Caps fail rather than truncate.
Skipped source hours do not consume the file cap. In-flight SDK calls/extension
installation may overrun the deadline; this is not a hard process timeout.
Use private `--temp-directory` for sensitive spill. Backend errors suppress
secret SQL/signed URLs; reports contain dataset paths and should stay private.

## Bounded live tests

**Cleanup (2026-11-12):** the human requested removal of this work's S3 artifacts.
All test-copy/probe/generation objects and the two initial retain-source outputs
under `lintap/compacted` were removed. The historical fixtures cited below no
longer exist; their recorded qualification results remain valid. All 399 original
source hashes matched before and after cleanup. New live tests require explicit
approval and fresh isolated roots; the saved-copy DBT command is historical.

S3 tests skip unless explicitly approved. Set `RAW_COMPACT_S3_TEST_APPROVED` to
`isolated-read-only` for pre-seeded count checks or `isolated-write` for a fresh
destination-hour merge/verify/rerun-skip test. Set all `RAW_COMPACT_S3_TEST_`
variables: `SOURCE` (direct raw event root), `DESTINATION_ROOT` (direct compacted
event root), `START_HOUR`, `END_HOUR` (one hour), comma-separated `EVENT_TYPES`,
positive `EXPECTED_ROWS`. The write test requires absent destination hours,
hashes all source objects before/after, and confirms rerun leaves output bytes
unchanged. It writes only the supplied destination, never deletes anything.

```bash
uv run --project extras/raw-sensor-compact --locked pytest \
  extras/raw-sensor-compact/tests/test_s3_transport.py -m s3 -s
```

Actual fixed-root test at `lintap/compacted`: 897,395 rows preserved, 29 → 2 files,
all 29 source SHA-256 unchanged, rerun skipped both hours without changing output.
Pidstat output bytes increased about 3.37× under current settings; fewer files is
not a measured size/scan speedup. Benchmark/default, broad family/large-leaf coverage,
credential expiry and multipart fault tests are still pending. The endpoint's
ignored conditional PUT is no longer a blocker: no conditional writes are used.

See the feature [plan](../../../Wintap-Analytics/wiki/work/raw-sensor-file-aggregation/implementation_plan.md)
and [verification](../../../Wintap-Analytics/wiki/work/raw-sensor-file-aggregation/verification.md)
in the sibling analytics wiki checkout.

### DBT migration qualification

From the Wintap-PyUtil repository root, `RAW_COMPACT_DBT_TEST_APPROVED=isolated-local uv run
--locked --dev pytest tests/test_compacted_inputs.py -s` exercises the full local
raw/mixed/compacted migration and explicit non-masked overlap. This standalone compactor
is invoked in its own DuckDB 2.0.0 environment. All 54 DBT models match with 139
build/test gates per state; 28 fixture source files are deleted after validation.

The repository's `tests/test_compacted_s3_migration.py` requires explicit
`RAW_COMPACT_DBT_TEST_APPROVED=isolated-s3-copy-delete` and a new
`RAW_COMPACT_DBT_TEST_ROOT=s3://lintap-alpha/compaction-tests/dbt-union-<unique>`.
The passing `dbt-union-20261112-02` run copied the qualified pidstat/host hour,
verified three fresh scoped DBT builds (4 equivalent models, 10 gates per state),
preserved 897,395 rows and deleted all 29 copied sources. Original hashes stayed
unchanged. The first failed fixture's raw copies and passing fixture's compacted
copies remain; no original live migration or cleanup was performed.

Expanded all-family fixture `dbt-union-all-20261112-02` passes all 54 DBT models
and 139 gates in raw/mixed/compacted states. All 11 bronze results/schemas are
exact; 2,668,265 rows conserved, 399 copied source objects → 10 outputs, all 399
original hashes unchanged. The human accepts existing ANY_VALUE quality debt
as non-blocking: explicit `RAW_COMPACT_DBT_COMPARISON=any-value-diagnostics`
reports differences only within the ANY_VALUE model/dependent closure, requiring
exact schemas/bronze/unaffected results. It does not claim every downstream
result is equivalent or repair DBT semantics. Registry/image-load are absent.
See the [DBT guide](../../wintap_dbt/README.md#read-only-full-model-test-of-the-surviving-compacted-copy) for a read-only full build of the surviving copy;
do not rerun the initially-absent-root migration regression against that root.
