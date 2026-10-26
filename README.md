<img width="200" src="https://user-images.githubusercontent.com/50601643/218871643-2d3af433-0923-4786-b5e5-24c6a72e803e.png">

# Wintap-PyUtil

Python, DuckDB, and DBT utilities for processing Wintap/Lintap telemetry data.

DBT is now the primary post-processing path. The canonical input is a `raw_sensor` parquet dataset:

```text
<dataset>/raw_sensor/<event_type>/dayPK=YYYYMMDD/hourPK=HH/*.parquet
<dataset>/raw_sensor/raw_process_conn_incr/dayPK=YYYYMMDD/hourPK=HH/protoPK=tcp|udp/*.parquet
```

For the full current run process, start with:

```text
review-notes/PipelineRunbook.md
```

## Minimum system requirements

- Python 3.10, 3.11, or 3.12
- [uv](https://docs.astral.sh/uv/)
- DuckDB CLI for QA scripts
- DuckDB-compatible parquet data

Install uv if needed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## DBT pipeline quick setup

Create a local run config from the example:

```bash
cd Wintap-PyUtil
cp wintap-run.env.example wintap-run.env
# edit wintap-run.env
source wintap-run.env
```

The local config defines one run root and derived DBT settings:

```text
WINTAP_DATA_ROOT
WINTAP_DBT_DATASET
WINTAP_DBT_DATABASE
WINTAP_DBT_START_DAY
WINTAP_DBT_END_DAY
```

Useful targets:

```bash
make print-dbt-config
make dbt-debug
make dbt-build
make dbt-test
make qa-pid-hash
make dbt-docs
```

DBT Makefile targets use `uv run --isolated --dev` by default so a missing or broken local `.venv` does not block processing. Override with `DBT_UV_RUN='uv run'` only if you intentionally want to use the project virtual environment.

## Development setup

Create the development environment and install the package in editable mode:

The canonical Python/DBT environment pins DuckDB **1.5.6** in `pyproject.toml`.
The DBT adapter remains on the 1.9 line; DuckDB 2.0 is not part of this update.

```bash
uv sync --dev
```

Run commands through uv:

```bash
uv run python -c "import wintappy; print(wintappy.VERSION)"
```

## Legacy Python ETL commands

The pre-DBT Python/DuckDB pipeline remains available as legacy/reference tooling while the DBT path is hardened:

```bash
uv run rawtorolling --help
uv run rawtostdview --help
uv run ubersummary --help
```

Use DBT for new processing work.

## Development tasks

```bash
make fmt          # black + isort
make fmt-check    # formatting checks
make lint         # mypy + sqlfluff
make test         # pytest
make ci           # fmt-check + lint + test + dbt-build
make clean
```

## Release

LLNL-CODE-837816
