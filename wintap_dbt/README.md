# Wintap DBT Pipeline

DBT/DuckDB implementation of the canonical Wintap/Lintap post-processing pipeline.

## Scope

Input is canonical `raw_sensor` parquet:

```text
<dataset>/raw_sensor/<event_type>/dayPK=YYYYMMDD/hourPK=HH/*.parquet
<dataset>/raw_sensor/raw_process_conn_incr/dayPK=YYYYMMDD/hourPK=HH/protoPK=tcp|udp/*.parquet
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
WINTAP_DBT_DATABASE=$WINTAP_DATA_ROOT/duckdb/wintap.duckdb
WINTAP_DBT_START_DAY=<minimum discovered dayPK>
WINTAP_DBT_END_DAY=<maximum discovered dayPK>
```

You can still export any of those variables to override the defaults. DBT itself requires `WINTAP_DBT_DATASET`, `WINTAP_DBT_START_DAY`, `WINTAP_DBT_END_DAY`, and `WINTAP_DBT_DATABASE`; the Makefile fills them from `WINTAP_DATA_ROOT` for normal runs.

For split I/O, keep `WINTAP_DBT_DATABASE` local and point `WINTAP_DBT_RAW_SENSOR_DATASET` at the parquet dataset root that contains `raw_sensor/`. If that raw dataset lives on S3, the dbt DuckDB profile now loads `httpfs` and configures an S3 secret from the usual AWS environment variables. For Garage/path-style endpoints, also set `AWS_ENDPOINT_URL`, `AWS_DEFAULT_REGION` or `AWS_REGION`, and `DUCKDB_USE_SSL`.

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
- `WINTAP_DBT_START_DAY` / `WINTAP_DBT_END_DAY` and optional `WINTAP_DBT_START_HOUR` / `WINTAP_DBT_END_HOUR` define the inclusive partition window.

3. DBT vars

`wintap_dbt/dbt_project.yml` maps those env vars into dbt vars:

- `dataset`
- `raw_sensor_dataset`
- `start_day`, `end_day`
- `start_hour`, `end_hour`

4. Shared macros

- `macros/paths.sql` turns `raw_sensor_dataset` plus the day/hour window into canonical `raw_sensor/...` globs and SQL predicates.
- `macros/raw_sources.sql` adds optional-event and alias helpers such as `raw_event_exists()` and `raw_scan_for()`.
- `macros/relations.sql` wraps the final `parquet_scan(...)` shape used by raw models.
- `macros/s3.sql` configures DuckDB S3 access on run start when `raw_sensor_dataset` points at `s3://...`.

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
- The historical `merged` flow is intentionally not supported here.
