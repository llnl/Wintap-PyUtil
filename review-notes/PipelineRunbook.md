# Pipeline Runbook

This is the current manual runbook for the canonical DBT-based Wintap/Lintap ETL path.

## Single source of truth

Use one run root:

```text
WINTAP_DATA_ROOT
```

Everything else in the DBT processing path is derived from that root.

Recommended layout:

```text
$WINTAP_DATA_ROOT/
  parquet/
    raw_sensor/
  pidstat/
  duckdb/
    wintap.duckdb
```

DBT uses:

```text
WINTAP_DBT_DATASET=$WINTAP_DATA_ROOT
WINTAP_DBT_DATABASE=$WINTAP_DATA_ROOT/duckdb/wintap.duckdb
WINTAP_DBT_START_DAY=YYYYMMDD
WINTAP_DBT_END_DAY=YYYYMMDD
```

## Inputs

A dataset directory containing canonical raw sensor parquet:

```text
<dataset>/raw_sensor/<event_type>/dayPK=YYYYMMDD/hourPK=HH/<file>.parquet
<dataset>/raw_sensor/raw_process_conn_incr/dayPK=YYYYMMDD/hourPK=HH/protoPK=tcp/<file>.parquet
<dataset>/raw_sensor/raw_process_conn_incr/dayPK=YYYYMMDD/hourPK=HH/protoPK=udp/<file>.parquet
```

`dayPK`, `hourPK`, and `protoPK` are required canonical Hive partition names.

DBT expects canonical `protoPK=` network partitions. Datasets that only contain `proto=` should be regenerated from the updated source output.

## Configure a local run

From `Wintap-PyUtil`:

```sh
cp wintap-run.env.example wintap-run.env
# edit wintap-run.env
source wintap-run.env
```

`wintap-run.env` is ignored by git. Commit changes to `wintap-run.env.example` only when changing the documented defaults/shape.

Check the active config:

```sh
make print-dbt-config
```

DBT Makefile targets use `uv run --isolated --dev` by default so a missing or broken local `.venv` does not block processing. Override with `DBT_UV_RUN='uv run'` only if you intentionally want to use the project virtual environment.

## Build into DuckDB

```sh
make dbt-build
```

The DBT database path is controlled by:

```text
WINTAP_DBT_DATABASE
```

## Run tests

```sh
make dbt-test
```

## Run pid_hash orphan QA

```sh
make qa-pid-hash
```

This runs:

```text
wintap_dbt/qa/pid_hash_orphan_checks.sql
```

against `WINTAP_DBT_DATABASE`.

## Inspect output manually

```sh
duckdb "$WINTAP_DBT_DATABASE"
```

Useful checks:

```sql
select * from build_summary order by model_name;
select count(*) from process;
select count(*) from process_summary;
select count(*) from process_uber_summary;
select * from process_summary limit 10;
```

## Generate DBT docs

```sh
make dbt-docs
```

DBT docs artifacts are generated under:

```text
Wintap-PyUtil/wintap_dbt/target/
```

## Known validated examples

ACME4 Windows sample values:

```sh
export WINTAP_DATA_ROOT=/home/ubuntu/data/lintap/lintap-dev/ACME4
export WINTAP_DBT_DATASET=$WINTAP_DATA_ROOT
export WINTAP_DBT_DATABASE=/tmp/acme4.duckdb
export WINTAP_DBT_START_DAY=20240904
export WINTAP_DBT_END_DAY=20240904
make dbt-build
```

LINTAP Linux sample values:

```sh
export WINTAP_DATA_ROOT=/home/ubuntu/data/debug
export WINTAP_DBT_DATASET=$WINTAP_DATA_ROOT
export WINTAP_DBT_DATABASE=$WINTAP_DATA_ROOT/duckdb/wintap.duckdb
export WINTAP_DBT_START_DAY=20260520
export WINTAP_DBT_END_DAY=20260520
make dbt-build
```

Expected successful build shape at time of writing:

```text
PASS=46 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=46
```

## Legacy commands

These old commands remain in `pyproject.toml` for compatibility/reference, but DBT should be used for new ETL work:

- `rawtorolling`
- `rawtostdview`
- `ubersummary`

The legacy `mergedtoraw.py` scripts have been removed. New data should already be materialized as `raw_sensor` by the sensor/.NET side.
