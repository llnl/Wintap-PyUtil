# Wintap DBT Pipeline

DBT/DuckDB implementation of the canonical Wintap/Lintap post-processing pipeline.

## Scope

Input is the name-aligned union of remaining `raw_sensor` and validated `compacted` parquet:

```text
<dataset>/raw_sensor/<event_type>/dayPK=YYYYMMDD/hourPK=HH/*.parquet
<dataset>/raw_sensor/raw_process_conn_incr/dayPK=YYYYMMDD/hourPK=HH/protoPK=tcp|udp/*.parquet
<dataset>/compacted/<event_type>/dayPK=YYYYMMDD/hourPK=HH[/protoPK=...]/<merged>.parquet
```

## Configuration

DBT configuration is environment-driven. The intended single source of truth is the run root:

```text
WINTAP_DATA_ROOT
```

With the Makefile, DBT-specific values are derived from that root when unset:

```text
WINTAP_DBT_DATASET=$WINTAP_DATA_ROOT
WINTAP_DBT_RAW_SENSOR_DATASET=$WINTAP_DBT_DATASET
WINTAP_DBT_COMPACTED_ROOT=$WINTAP_DBT_RAW_SENSOR_DATASET/compacted
WINTAP_DBT_DATABASE=$WINTAP_DATA_ROOT/duckdb/wintap.duckdb
WINTAP_DBT_START_DAY=<minimum discovered dayPK>
WINTAP_DBT_END_DAY=<maximum discovered dayPK>
```

You can still export any of those variables to override the defaults. DBT itself requires `WINTAP_DBT_DATASET`, `WINTAP_DBT_START_DAY`, `WINTAP_DBT_END_DAY`, and `WINTAP_DBT_DATABASE`; the Makefile fills them from `WINTAP_DATA_ROOT` for normal runs.

For split I/O, keep `WINTAP_DBT_DATABASE` local and point `WINTAP_DBT_RAW_SENSOR_DATASET` at the parquet dataset root that contains `raw_sensor/`. If that raw dataset lives on S3, the dbt DuckDB profile now loads `httpfs` and configures an S3 secret from the usual AWS environment variables. Set `AWS_PROFILE` to select a named shared AWS profile; DBT passes it explicitly to DuckDB's credential-chain secret. DuckDB does not copy a profile's custom `endpoint_url` into the secret, so Garage/path-style users must also set `AWS_ENDPOINT_URL`, `AWS_DEFAULT_REGION` or `AWS_REGION`, and `DUCKDB_USE_SSL` to match that endpoint.

Date discovery uses **both effective local input roots**, not the output root.
S3 input requires explicit `WINTAP_DBT_START_DAY` and `WINTAP_DBT_END_DAY`.
No local `raw_sensor/` placeholder or downloaded copy is required for S3:

```sh
export WINTAP_DATA_ROOT="$HOME/wintap-analysis/rhel8"
export WINTAP_DBT_DATASET="$WINTAP_DATA_ROOT"
export WINTAP_DBT_DATABASE="$WINTAP_DATA_ROOT/duckdb/wintap.duckdb"
export WINTAP_DBT_RAW_SENSOR_DATASET="s3://lintap-alpha/lintap"
export WINTAP_DBT_START_DAY=20261031
export WINTAP_DBT_END_DAY=20261103
# For a named profile with a configured S3-compatible endpoint:
export AWS_PROFILE=lintap-alpha
export AWS_DEFAULT_REGION="$(aws configure get region --profile "$AWS_PROFILE")"
export AWS_ENDPOINT_URL="$(aws configure get endpoint_url --profile "$AWS_PROFILE")"
case "$AWS_ENDPOINT_URL" in
  https://*) export DUCKDB_USE_SSL=true ;;
  http://*) export DUCKDB_USE_SSL=false ;;
esac
make print-dbt-config
make dbt-check-source
make dbt-build
```

The raw root above resolves to `s3://lintap-alpha/lintap/raw_sensor/...`;
do not append `raw_sensor` or an event name to the configured root.
The compacted root defaults to `s3://lintap-alpha/lintap/compacted/...`.
`WINTAP_DBT_COMPACTED_ROOT` can override that **direct event root**, without any
extra `raw_sensor/` directory. Both roots may be local or S3; they must not overlap.
If either is remote, supply explicit dates and configure S3 access.

### Compaction migration contract

Input discovery, event/alias and optional-column checks include both roots, with
matching day/hour globs and nested protocol leaves. Missing roots/optional families
are allowed; a requested supported event with no files anywhere still fails the
source preflight. `parquet_scan(..., union_by_name=true)` preserves duplicate rows
and fills missing fields with NULL. Existing model-specific normalization and
duplicate accounting are unchanged; no new deduplication or compacted-preference
rule hides overlap.

Operate sequentially: **merge → validate → delete source hour prefix → DBT build**.
The compactor's `--delete-source` uses the sensors' UTC upload-partition contract
and a default 10-minute grace after hour end (minimum 5). Temporary overlap and
partial deletion failures are accepted; reconcile failed hours manually before
running DBT. Source deletion never runs for an already existing destination hour.
Fresh databases are used in migration tests; rebuild affected materialized models
after cutover. Bronze views capture discovered physical paths and should also be
rebuilt after migration, not reused after their raw files have been deleted.

### Validation targets

- `dbt-check-config`: offline source/configuration checks. Local input must
  have a readable `raw_sensor/` or compacted directory; S3 requires a valid bucket/prefix URI,
  not a local directory. Other URI schemes are rejected. Dates/hours are checked,
  and the local DuckDB parent directory is created and checked for writability.
- `print-dbt-config`: the same offline checks plus the effective raw source,
  source type and date/hour window. It does not print credentials.
- `dbt-check-source`: uses a DBT operation and the existing DuckDB S3 secret
  configuration to list supported bronze event/alias Parquet files within the
  requested partitions and read one matching file's schema. No AWS CLI is needed.
  Listing stops after finding a sample; it does not scan all data or check every
  file. Missing optional events remain valid, but no supported event files
  anywhere in the requested window is an error. Access/endpoint/TLS and corrupt
  sample errors retain DuckDB's diagnostics, rather than becoming "empty input".
- `dbt-build`: automatically runs configuration and source checks before building.
- `qa-dashboard` / `qa-pidstat`: require an existing local database, not reachable
  raw input or a date window. Opening materialized results works offline; queries
  against raw-backed bronze views still require raw-source access.

The probe knows the current bronze input names and historical aliases; new bronze
events must also be added to `macros/validate_source.sql`. S3 LIST prefixes include
the event and requested day (and hour, when configured). Large windows can still
require multiple listings. A successful probe is basic access evidence, **not** a
full dataset-integrity or model-compatibility qualification.

Raw parquet scans also narrow their file globs to the requested `dayPK=` partitions from `WINTAP_DBT_START_DAY` through `WINTAP_DBT_END_DAY`, which helps when reading a small window out of a much larger remote dataset. The compile-time raw-event alias and optional-column checks use the same narrowed day partition set instead of probing the full event prefix.

If `WINTAP_DBT_START_HOUR` and/or `WINTAP_DBT_END_HOUR` are set, dbt further narrows raw scans to `hourPK=HH` partitions within the inclusive `start day/hour` to `end day/hour` window. When omitted, all hours in the requested day range are included.

## Data Flow

The DBT path is layered deliberately so the same models can read local parquet or remote S3 parquet with the same SQL.

1. Raw data source

`raw_sensor` parquet lives under a dataset root on a filesystem path or S3 prefix:

```text
<raw_sensor_dataset>/raw_sensor/<event_type>/dayPK=YYYYMMDD/hourPK=HH/*.parquet
```

2. Environment variables

- `WINTAP_DATA_ROOT` is the local run root used by the Makefile defaults.
- `WINTAP_DBT_DATASET` is the general dataset root, defaulting to `$WINTAP_DATA_ROOT`.
- `WINTAP_DBT_RAW_SENSOR_DATASET` optionally overrides only the raw parquet input root. Use this for split I/O, including S3-backed raw input with a local DuckDB output.
- `WINTAP_DBT_COMPACTED_ROOT` overrides the direct compacted event root; default is `<raw_sensor_dataset>/compacted`.
- `WINTAP_DBT_START_DAY` / `WINTAP_DBT_END_DAY` and optional `WINTAP_DBT_START_HOUR` / `WINTAP_DBT_END_HOUR` define the inclusive partition window.

3. DBT vars

`wintap_dbt/dbt_project.yml` maps those env vars into dbt vars:

- `dataset`
- `raw_sensor_dataset`
- `compacted_root`
- `start_day`, `end_day`
- `start_hour`, `end_hour`

4. Shared macros

- `macros/paths.sql` turns both roots plus the day/hour window into `raw_sensor/...` and `compacted/...` globs and SQL predicates.
- `macros/raw_sources.sql` adds optional-event and alias helpers such as `raw_event_exists()` and `raw_scan_for()`.
- `macros/relations.sql` wraps the final `parquet_scan(...)` shape used by raw models.
- `macros/s3.sql` configures DuckDB S3 access on run start when either input root points at `s3://...`.

5. DBT model layers

- `models/bronze` reads raw parquet through the shared macros and normalizes source drift into stable raw staging views.
- `models/silver` builds normalized detail tables such as `host`, `process`, `process_file`, and `pidstat_metrics` from bronze inputs.
- `models/gold` builds analyst-facing summaries such as `process_summary` and `process_uber_summary`.
- `models/monitoring` builds operational summaries and QA/dashboard inputs over the same DBT graph.

In practice, a model such as `stg_raw_host` or `stg_pidstat_metrics` does not hardcode filesystem or S3 details. It calls shared macros, those macros resolve `raw_sensor_dataset` and the day/hour window, and DuckDB scans the matching parquet partitions.

## Running

Use the Makefile from the repository root:

```sh
cd Wintap-PyUtil
cp wintap-run.env.example wintap-run.env
# edit wintap-run.env
source wintap-run.env
make dbt-build
make dbt-test
make qa-pid-hash
make qa-dashboard
```

For the minimal workflow, only `WINTAP_DATA_ROOT` is required:

```sh
export WINTAP_DATA_ROOT=/path/to/run-root
make dbt-build
make qa-dashboard
```

Pidstat parquet data is an optional raw event. If `<raw_sensor_dataset>/raw_sensor/pidstat` contains parquet files in the canonical `dayPK=/hourPK=` layout, DBT loads them into `pidstat_metrics`; otherwise the model is built as an empty typed table.

### Pidstat CPU Units

Raw `pidstat.cpu_percent` is retained for compatibility and means **core-summed
process CPU percent**: `100` equals one fully occupied logical CPU, so a process
can exceed `100` on a multicore host. The normalized `pidstat_metrics` model
also exposes this value as `cpu_core_percent`, and the gold process summary uses
`max_cpu_core_percent` and `avg_cpu_core_percent`. Divide by the host's logical
CPU count to compare it with host-normalized CPU values such as `.NET
System.Runtime cpu-usage`. The gold `max_cpu_percent`/`avg_cpu_percent` and
monitoring `max_cpu` columns remain compatibility aliases of these core-summed
values; new queries should use the explicit `*_cpu_core_percent` names.

### SELinux telemetry

Optional `raw_selinuxavc`, `raw_selinuxinteraction`, and `raw_selinuxtransition`
sources produce typed empty relations when absent. Each family has bronze staging,
normalized silver detail, and gold summaries; `selinux_event_detail`,
`selinux_context`, and `selinux_process_summary` provide shared analysis views.
The QA dashboard reports family counts, contexts, and attribution coverage.

`EventTime` is Unix seconds; `FirstSeen`/`LastSeen` are FILETIME. Silver retains
raw values and normalizes timestamps. Bronze removes identical rows and records
`num_dups`; silver/gold preserve producer `EventCount` rather than multiplying it
by duplicate counts. Interaction output is folded/permission-expanded: neither
its rows nor its `EventCount` should be presented as distinct kernel events.
Attribution accepts exact identity or a single host/PID/lifetime candidate;
ambiguous and unmatched observations remain visible without assigned ownership.

### Temporary network identity compatibility change

`process_conn_incr.conn_id` currently replaces producer `connid` with an uppercase
MD5 of a canonical JSON five-tuple (protocol and ordered IP/port endpoint pairs,
with context `llnl`). Reversed endpoints share an ID; missing tuple fields yield
NULL. This is a temporary DBT validation shim, **not** the authoritative Wintap
.NET implementation or a connection-instance identity: repeated uses of the same
five-tuple share the key. Existing IDs and joins to producer IDs can change;
rebuild dependent models together and review downstream consumers before release.
See `macros/network_keys.sql` and `tests/temporary_five_tuple_conn_id_contract.sql`.

## Layers

- `models/bronze` — raw parquet scans and compatibility layer.
- `models/silver` — normalized detail tables and process paths.
- `models/gold` — process summaries and `process_uber_summary`.
- `models/monitoring` — build/row-count monitoring, TeleTap-style event/chart summaries, and dashboard inputs.

## Current status

Implemented:

- Bronze models for core raw events.
- Silver models ported from legacy detail SQL and process-path logic.
- Gold models ported from legacy summary SQL.
- `process_uber_summary` with typed empty enrichment stubs.
- Schema tests for core keys and canonical network partitions.
- Optional pidstat parquet loading into DBT, including `hostname` and container-attribution columns.
- TeleTap-style summary/chart views in DBT monitoring models.
- QA script for `pid_hash` primary/foreign-key orphan checks.
- Marimo QA/overview dashboard.

## Known limitations

- This is not yet wired to a `wintap-etl` wrapper command.
- Parquet export is not implemented yet; DBT builds into DuckDB.
- Label/Sigma/MITRE/LOLBAS source loading is stubbed for now.
- The historical `merged` binary/legacy layout remains unsupported; the new
  `compacted/<event>/dayPK/hourPK/` Parquet layout is supported alongside raw input.

### Migration tests

Ordinary regressions: `uv run --locked --dev pytest tests`.
Opt-in end-to-end local test uses this repository's standalone compactor on an isolated
known-value fixture, builds the full graph in fresh databases for raw/mixed/compacted
states, and compares model row counts and sorted-result SHA-256 (excluding only
pidstat's derived source-object filename). It includes genuine duplicates, NULLs,
column drift, UTC rollover, and separate TCP/UDP leaves:

```sh
RAW_COMPACT_DBT_TEST_APPROVED=isolated-local uv run --locked --dev pytest \
  tests/test_compacted_inputs.py -s
```

`tests/test_compacted_s3_migration.py` requires explicit
`RAW_COMPACT_DBT_TEST_APPROVED=isolated-s3-copy-delete` and an initially absent
`RAW_COMPACT_DBT_TEST_ROOT=s3://lintap-alpha/compaction-tests/dbt-union-<unique>`.
It copies the bounded qualified pidstat/host hour, exercises three fresh scoped
DBT builds, deletes only test-copy prefixes and checks original source hashes.
Never reuse an existing fixture root or point it at the live raw/compacted roots.

Set `RAW_COMPACT_DBT_TEST_SCOPE=all-models` to copy every available DBT input
family for the same 2026-11-11 08:00–09:00 UTC hour and build the entire graph
in all three states. Optional absent families still produce their typed empty
models. The fixture is bounded to 1,000 source files/256 MiB before any copies;
invalid scopes fail rather than silently falling back to the smaller test.
Comparison includes column names/types, row counts and sorted-result SHA-256,
excluding only pidstat's derived filename. Source inventory, merge reports,
per-state snapshots and the successful summary are saved under pytest's fixture
directory. Production models are not normalized or relaxed for comparison.

```sh
RAW_COMPACT_DBT_TEST_APPROVED=isolated-s3-copy-delete \
RAW_COMPACT_DBT_TEST_SCOPE=all-models \
RAW_COMPACT_DBT_COMPARISON=any-value-diagnostics \
RAW_COMPACT_DBT_TEST_ROOT=s3://lintap-alpha/compaction-tests/dbt-union-all-<unique> \
uv run --locked --dev pytest tests/test_compacted_s3_migration.py -s
```

The initial full live qualification **failed strict result equivalence**, not DBT
build/tests: the 399-file/9-family fixture passes 139 gates in raw and mixed
states, but differs in 17 model snapshots. An unchanged-input rebuild passes
139 gates and differs in 18 snapshots. Existing unordered `ANY_VALUE` selections
in process models can change derived filenames/file IDs and downstream counts.
The human accepts this existing quality debt as **non-blocking for compaction**.
Use explicit `RAW_COMPACT_DBT_COMPARISON=any-value-diagnostics` with full scope:
all model schemas/names and all bronze results must match exactly; models
outside the manifest's ANY_VALUE-containing/dependent closure must also match.
Differences in the closure are saved with counts/hashes as diagnostics, not
discarded, normalized or claimed equivalent. Membership does not prove each
difference's cause. Default comparison remains `strict`; unknown modes fail.
Production representative semantics are unchanged and remain separate quality
work. The initial `dbt-union-all-20261112-01` mixed inspection fixture retained
394 raw objects plus compacted host during qualification; the approved comparison rerun used a new
fixture root to repeat all three states and original hashes.

Approved all-family rerun passed at
`s3://lintap-alpha/compaction-tests/dbt-union-all-20261112-02`: 54 models and 139
gates per state, all 11 bronze results/all model schemas exact, 2,668,265 source
rows conserved, 399 copied sources deleted and 399 original hashes unchanged.
Thirty-six model results match across every state; differences in 18 affected
models are recorded as diagnostics. There are 10 compacted outputs across 9
families, including separate TCP/UDP leaves. Registry/image-load inputs are absent.

#### Read-only full-model test of the surviving compacted copy

**Historical command:** on 2026-11-12 the human requested cleanup, and all
fixtures created by this work were removed, including this compacted copy.
The command below no longer has a live input dataset. For future testing, obtain
approval for a fresh isolated fixture and substitute its root. Qualification
results and local evidence are preserved; original raw telemetry is untouched.

From the PyUtil repository, use your existing profile setup and a **fresh local
database**, leaving the private environment file and existing databases unchanged:

```sh
source wintap-run.env-lintap-alpha
export WINTAP_DATA_ROOT="$(mktemp -d)"
export WINTAP_DBT_DATASET="$WINTAP_DATA_ROOT"
export WINTAP_DBT_DATABASE="$WINTAP_DATA_ROOT/wintap.duckdb"
export WINTAP_DBT_RAW_SENSOR_DATASET=s3://lintap-alpha/compaction-tests/dbt-union-all-20261112-02
export WINTAP_DBT_COMPACTED_ROOT="$WINTAP_DBT_RAW_SENSOR_DATASET/compacted"
export WINTAP_DBT_START_DAY=20261111 WINTAP_DBT_END_DAY=20261111
export WINTAP_DBT_START_HOUR=08 WINTAP_DBT_END_HOUR=08
uv run --locked --dev dbt build \
  --project-dir wintap_dbt --profiles-dir wintap_dbt \
  --target-path "$WINTAP_DATA_ROOT/target" \
  --log-path "$WINTAP_DATA_ROOT/logs" --no-partial-parse
```

No `--select`: this builds/tests the full graph. It only reads S3 and writes
local DBT output. Expect 139 successful gates, not stable hashes for existing
ANY_VALUE representatives. Do not reuse this root for the write/delete regression,
which requires an initially absent new fixture root.

### Combined-feature release follow-up

- Qualify full builds/tests against representative split-local and authenticated
  S3 inputs, including SELinux families. A successful source preflight alone is
  not a full build or all-file integrity check.
- Approve the temporary network-ID compatibility change and track migration to
  the authoritative producer implementation.
- Harden/test `scripts/lintap-pull-s3.sh` before treating it as a supported release
  interface: calendar/date-order checks, GNU/BSD date behavior, discovery errors,
  literal host filtering, and configurable defaults. `-n` prevents sync writes
  but still makes discovery/list requests when event types are omitted.
