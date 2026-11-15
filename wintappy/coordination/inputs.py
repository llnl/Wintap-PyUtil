"""Offline active-root resolution shared by configuration and reader routing."""


def effective_input_roots(env):
    source = env.get("WINTAP_DBT_RAW_SENSOR_DATASET", env.get("WINTAP_DBT_DATASET", ""))
    if not source.strip():
        raise ValueError("WINTAP_DBT_RAW_SENSOR_DATASET must not be empty")
    source = source.rstrip("/")
    roots = {
        "raw": source + "/raw_sensor",
        "compacted": env.get("WINTAP_DBT_COMPACTED_ROOT", "").rstrip("/")
        or source + "/compacted",
    }
    entity = env.get("WINTAP_DBT_PROCESS_ENTITY_ROOT", "").rstrip("/")
    if entity:
        roots["entity"] = entity
        roots["metrics"] = (
            env.get("WINTAP_DBT_PROCESSING_METRICS_ROOT", "").rstrip("/")
            or source + "/processing_metrics"
        )
    return roots
