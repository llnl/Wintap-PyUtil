packages=./wintappy
analytics=./wintappy/analytics

UV_RUN=uv run
DBT_UV_RUN ?= uv run --dev
DBT_DIR=wintap_dbt
DBT=$(DBT_UV_RUN) --project . dbt
DBT_ROUTE=$(DBT_UV_RUN) --project . python -m wintappy.coordination.client --dbt-command '$(DBT)' --dbt-project $(DBT_DIR)
DBT_DIRECT_ROUTE=WINTAP_DBT_COORDINATION_MODE=direct WINTAP_DBT_INCLUDE_COMPACTED=false $(DBT_ROUTE)

WINTAP_DBT_DATASET ?= $(WINTAP_DATA_ROOT)
WINTAP_DBT_RAW_SENSOR_DATASET ?= $(WINTAP_DBT_DATASET)
WINTAP_DBT_COMPACTED_ROOT ?= $(WINTAP_DBT_RAW_SENSOR_DATASET)/compacted
WINTAP_DBT_INCLUDE_COMPACTED ?= true
WINTAP_DBT_PROCESS_ENTITY_ROOT ?=
WINTAP_DBT_PROCESS_ENTITY_HOSTS ?=
WINTAP_DBT_PROCESS_ENTITY_NAMESPACE ?=
WINTAP_DBT_PROCESS_UNKNOWN_IDENTITY_POLICY ?=reject
WINTAP_DBT_PROCESS_UNKNOWN_IDENTITY_MAX_RATIO ?=0.05
WINTAP_DBT_PROCESSING_METRICS_ROOT ?= $(WINTAP_DBT_RAW_SENSOR_DATASET)/processing_metrics
WINTAP_DBT_DATABASE ?= $(WINTAP_DATA_ROOT)/duckdb/wintap.duckdb
WINTAP_DBT_START_DAY ?= $(if $(findstring ://,$(WINTAP_DBT_RAW_SENSOR_DATASET) $(WINTAP_DBT_COMPACTED_ROOT)),,$(shell for root in "$(WINTAP_DBT_RAW_SENSOR_DATASET)/raw_sensor" "$(WINTAP_DBT_COMPACTED_ROOT)"; do if [ -d "$$root" ]; then find "$$root" -type d -name 'dayPK=*' 2>/dev/null; fi; done | sed 's|.*/dayPK=||' | sort | head -1))
WINTAP_DBT_END_DAY ?= $(if $(findstring ://,$(WINTAP_DBT_RAW_SENSOR_DATASET) $(WINTAP_DBT_COMPACTED_ROOT)),,$(shell for root in "$(WINTAP_DBT_RAW_SENSOR_DATASET)/raw_sensor" "$(WINTAP_DBT_COMPACTED_ROOT)"; do if [ -d "$$root" ]; then find "$$root" -type d -name 'dayPK=*' 2>/dev/null; fi; done | sed 's|.*/dayPK=||' | sort | tail -1))

export WINTAP_DBT_DATASET
export WINTAP_DBT_RAW_SENSOR_DATASET
export WINTAP_DBT_COMPACTED_ROOT
export WINTAP_DBT_INCLUDE_COMPACTED
export WINTAP_DBT_PROCESS_ENTITY_ROOT
export WINTAP_DBT_PROCESS_ENTITY_HOSTS
export WINTAP_DBT_PROCESS_ENTITY_NAMESPACE
export WINTAP_DBT_PROCESS_UNKNOWN_IDENTITY_POLICY
export WINTAP_DBT_PROCESS_UNKNOWN_IDENTITY_MAX_RATIO
export WINTAP_DBT_PROCESSING_METRICS_ROOT
export WINTAP_DBT_DATABASE
export WINTAP_DBT_START_DAY
export WINTAP_DBT_END_DAY

fmt:
	$(UV_RUN) black $(packages)
	$(UV_RUN) isort $(packages)

fmt-check:
	$(UV_RUN) black --check $(packages)
	$(UV_RUN) isort --check $(packages)

lint:
	$(UV_RUN) mypy $(packages)
	$(UV_RUN) sqlfluff lint $(analytics)

test:
	$(UV_RUN) pytest

ci: fmt-check lint test dbt-build

venv:
	uv sync --all-extras --dev

rebuild-venv:
	rm -rf .venv
	uv sync --all-extras --dev

build:
	rm -rf dist/
	uv build

clean:
	rm -rf .venv dist build *.egg-info .pytest_cache .mypy_cache
	rm -rf $(DBT_DIR)/target $(DBT_DIR)/dbt_packages $(DBT_DIR)/logs

source-install:
	uv sync --dev

setup: venv cleanpynb

requirements:
	uv pip freeze > requirements.txt

cleanpynb:
	$(UV_RUN) nbstripout --install --attributes .gitattributes

dbt-check-output:
	@$(DBT_UV_RUN) --project . python scripts/check_dbt_config.py --output-only

dbt-check-config:
	@$(DBT_UV_RUN) --project . python scripts/check_dbt_config.py

print-dbt-config: dbt-check-config
	@echo "WINTAP_DATA_ROOT=$$WINTAP_DATA_ROOT"
	@echo "WINTAP_DBT_DATASET=$$WINTAP_DBT_DATASET"
	@echo "WINTAP_DBT_RAW_SENSOR_DATASET=$$WINTAP_DBT_RAW_SENSOR_DATASET"
	@echo "WINTAP_DBT_COMPACTED_ROOT=$$WINTAP_DBT_COMPACTED_ROOT"
	@echo "WINTAP_DBT_INCLUDE_COMPACTED=$$WINTAP_DBT_INCLUDE_COMPACTED"
	@echo "WINTAP_DBT_PROCESS_ENTITY_ROOT=$$WINTAP_DBT_PROCESS_ENTITY_ROOT"
	@echo "WINTAP_DBT_PROCESS_ENTITY_HOSTS=$$WINTAP_DBT_PROCESS_ENTITY_HOSTS"
	@echo "WINTAP_DBT_PROCESS_ENTITY_NAMESPACE=$$WINTAP_DBT_PROCESS_ENTITY_NAMESPACE"
	@echo "WINTAP_DBT_PROCESS_UNKNOWN_IDENTITY_POLICY=$$WINTAP_DBT_PROCESS_UNKNOWN_IDENTITY_POLICY"
	@echo "WINTAP_DBT_PROCESS_UNKNOWN_IDENTITY_MAX_RATIO=$$WINTAP_DBT_PROCESS_UNKNOWN_IDENTITY_MAX_RATIO"
	@echo "WINTAP_DBT_PROCESSING_METRICS_ROOT=$$WINTAP_DBT_PROCESSING_METRICS_ROOT"
	@case "$$WINTAP_DBT_RAW_SENSOR_DATASET" in s3://*) echo "RAW_SOURCE_TYPE=s3";; *) echo "RAW_SOURCE_TYPE=local";; esac
	@echo "WINTAP_DBT_DATABASE=$$WINTAP_DBT_DATABASE"
	@echo "WINTAP_DBT_START_DAY=$$WINTAP_DBT_START_DAY"
	@echo "WINTAP_DBT_END_DAY=$$WINTAP_DBT_END_DAY"
	@echo "WINTAP_DBT_START_HOUR=$$WINTAP_DBT_START_HOUR"
	@echo "WINTAP_DBT_END_HOUR=$$WINTAP_DBT_END_HOUR"

dbt-check-source: dbt-check-config
	$(DBT_ROUTE) dbt-source

dbt-debug: dbt-check-config
	$(DBT_ROUTE) dbt-debug

dbt-build: dbt-check-config
	$(DBT_ROUTE) dbt-build

# Explicit read-only path for a new S3 dataset before compaction/coordination
# initialization. The route enforces raw-only input and never falls back to it.
dbt-build-direct: dbt-check-config
	$(DBT_DIRECT_ROUTE) dbt-build

dbt-test: dbt-check-config
	$(DBT_ROUTE) dbt-test

dbt-docs: dbt-check-config
	$(DBT_ROUTE) dbt-docs

qa-pid-hash: dbt-check-config
	duckdb -cmd ".maxwidth 240" "$$WINTAP_DBT_DATABASE" < $(DBT_DIR)/qa/pid_hash_orphan_checks.sql

qa-dashboard: dbt-check-output
	$(DBT_UV_RUN) --project . marimo run notebooks/wintap_dbt_overview.py

qa-pidstat: dbt-check-output
	$(DBT_UV_RUN) --project . marimo run ../Lintap/teletap/grokdata_marimo.py

.PHONY: fmt fmt-check lint test ci venv rebuild-venv build clean source-install setup requirements cleanpynb dbt-check-output dbt-check-config dbt-check-source print-dbt-config dbt-debug dbt-build dbt-build-direct dbt-test dbt-docs qa-pid-hash qa-dashboard qa-pidstat
