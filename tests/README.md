# Test inventory — iterative first cut

This is a **partial classification**, not a replacement for the existing full
pytest suite. The default `testpaths = ["tests"]` and selection behavior remain
unchanged. Unclassified coordination, processing, platform, and evidence tests
have not been moved or excluded. The isolated compactor and DBT SQL suites retain
their own collection commands/environments.

## Classified slice (updated 2026-11-23)

| File | Primary marker | Collected cases | Execution boundary |
|---|---|---:|---|
| `unit/config/test_config.py` (formerly `test_config.py`) | `unit` | 4 | Synthetic argument parsing; no external I/O. |
| `test_rawutil_conn_id_passthrough.py` | `unit` | 2 | Mocked raw-view generation; producer `ConnId` passthrough and missing-`AgentId` compatibility. |
| `test_filetime_precision_sql.py` | `contract` | 127 | Shipping SQL macros/models, in-memory DuckDB. |
| `test_network_activity_sql.py` | `contract` | 76 | Shipping and legacy SQL against synthetic events. |
| `test_process_gold_quality_sql.py` | `contract` | 8 | Shipping gold SQL and shared network fixtures. |
| `test_selinux_sql.py` | `contract` | 9 | Synthetic normalization/attribution/counter contracts. |
| `test_any_value_contract_sql.py` | `contract` | 6 | Column-specific ANY_VALUE invariance, conflict, pairing, and as-of contracts. |
| **Total** | | **232** | **6 unit + 226 contract**; parametrized case counts, not function counts. |

SQL files stay in place for this draft. They share relative imports, and
`test_compacted_inputs.py` also imports the network renderer. A future relocation
must update those dependents and compare collected identities, not just totals.
Existing assertion bodies and fixtures are retained.

## Lightweight commands

Run from the PyUtil repository root in its existing development environment:

```bash
.venv/bin/python -m pytest \
  tests/unit/config \
  tests/test_rawutil_conn_id_passthrough.py \
  -m unit --strict-markers -q

.venv/bin/python -m pytest \
  tests/test_filetime_precision_sql.py \
  tests/test_network_activity_sql.py \
  tests/test_process_gold_quality_sql.py \
  tests/test_selinux_sql.py \
  tests/test_any_value_contract_sql.py \
  -m contract --strict-markers --collect-only -q

.venv/bin/python -m pytest \
  tests/unit/config/test_config.py \
  tests/test_filetime_precision_sql.py::test_nonempty_file_and_registry_consumers \
  tests/test_filetime_precision_sql.py::test_nonempty_image_load_consumer \
  tests/test_network_activity_sql.py::test_mixed_spellings_are_counted_together \
  tests/test_process_gold_quality_sql.py::test_network_name_changes_keep_one_lifetime_and_conserve_counters \
  tests/test_selinux_sql.py::test_gold_preserves_family_rows_and_producer_counts \
  --strict-markers -q
```

The last command runs **17 representative cases**, not all 226 collected cases.
`uv run --locked --offline --no-sync --dev python` can replace `.venv/bin/python`
when the existing environment is available. No dependency upgrade is part of this
first cut. Keep the main DuckDB environment separate from the compactor project.

Use explicit paths as well as markers for small iterations: `pytest -m unit` alone
still imports/collects the whole default suite before filtering. These markers do
not classify the remaining files or make broad selection a no-live-I/O guarantee.
New real-capture checks must use explicit authorized, read-only dataset scope and
must not read operator datasets through default synthetic discovery.

No qualification/history case may be removed solely because its name or age looks
stale. Map coverage, preserve provenance, and review before any merge/archive/delete.
