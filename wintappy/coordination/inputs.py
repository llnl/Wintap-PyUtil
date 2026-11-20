"""Offline active-root resolution shared by configuration and reader routing."""


def include_compacted_input(env):
    """Return whether DBT should include the optional compacted input root."""
    value = env.get("WINTAP_DBT_INCLUDE_COMPACTED", "true").strip().lower()
    if value not in {"1", "true", "yes", "on", "0", "false", "no", "off"}:
        raise ValueError("WINTAP_DBT_INCLUDE_COMPACTED must be true or false")
    return value in {"1", "true", "yes", "on"}


def effective_input_roots(env):
    source = env.get("WINTAP_DBT_RAW_SENSOR_DATASET", env.get("WINTAP_DBT_DATASET", ""))
    if not source.strip():
        raise ValueError("WINTAP_DBT_RAW_SENSOR_DATASET must not be empty")
    source = source.rstrip("/")
    roots = {"raw": source + "/raw_sensor"}
    if include_compacted_input(env):
        roots["compacted"] = (
            env.get("WINTAP_DBT_COMPACTED_ROOT", "").rstrip("/") or source + "/compacted"
        )
    entity = env.get("WINTAP_DBT_PROCESS_ENTITY_ROOT", "").rstrip("/")
    if entity:
        roots["entity"] = entity
        roots["metrics"] = (
            env.get("WINTAP_DBT_PROCESSING_METRICS_ROOT", "").rstrip("/")
            or source + "/processing_metrics"
        )
    return roots
