"""Offline Makefile configuration checks; remote access is checked by DBT."""

import argparse
import datetime as dt
import os
import re
import sys
from pathlib import Path
from urllib.parse import urlsplit


def required(env, name):
    value = env.get(name, "")
    if not value.strip():
        raise ValueError(f"{name} is required")
    return value


def source_type(source):
    if source.startswith("s3://"):
        uri = urlsplit(source)
        if (
            not uri.netloc
            or uri.username is not None
            or ":" in uri.netloc
            or uri.query
            or uri.fragment
            or any(char.isspace() for char in source)
            or any(char in source for char in "*?[]")
        ):
            raise ValueError(
                "Raw S3 source must be s3://bucket[/prefix], without credentials or globs"
            )
        return "s3"
    if "://" in source:
        raise ValueError("Unsupported raw source URI; use a local path or s3://bucket[/prefix]")
    return "local"


def validate(env, output_only=False):
    required(env, "WINTAP_DATA_ROOT")
    dataset = required(env, "WINTAP_DBT_DATASET")
    database = required(env, "WINTAP_DBT_DATABASE")
    if "://" in dataset or "://" in database or database == ":memory:":
        raise ValueError(
            "WINTAP_DBT_DATASET and WINTAP_DBT_DATABASE must be local filesystem paths"
        )
    db_path = Path(database)
    if output_only:
        if not db_path.is_file() or not os.access(db_path, os.R_OK):
            raise ValueError(
                f"DuckDB database does not exist or is not readable: {database}; run make dbt-build first"
            )
        return
    if db_path.exists() and (not db_path.is_file() or not os.access(db_path, os.W_OK)):
        raise ValueError(f"DuckDB output is not a writable file: {database}")

    source = env.get("WINTAP_DBT_RAW_SENSOR_DATASET", dataset)
    if not source.strip():
        raise ValueError("WINTAP_DBT_RAW_SENSOR_DATASET must not be empty")
    kind = source_type(source)
    compacted = (
        env.get("WINTAP_DBT_COMPACTED_ROOT", "").rstrip("/") or source.rstrip("/") + "/compacted"
    )
    compacted_kind = source_type(compacted)
    raw = source.rstrip("/") + "/raw_sensor"
    # Do not accidentally read one namespace twice through overlapping roots.
    roots = [raw, compacted]
    if kind == compacted_kind == "local":
        roots = [str(Path(root).resolve()) for root in roots]
    a, b = (root.rstrip("/") + "/" for root in roots)
    if a.startswith(b) or b.startswith(a):
        raise ValueError("Raw and compacted input roots must not overlap")
    entity = env.get("WINTAP_DBT_PROCESS_ENTITY_ROOT", "").rstrip("/")
    if entity:
        entity_kind = source_type(entity)
        entity_path = str(Path(entity).resolve()) if entity_kind == "local" else entity
        for other in roots:
            a, b = entity_path.rstrip("/") + "/", other.rstrip("/") + "/"
            if a.startswith(b) or b.startswith(a):
                raise ValueError("Process entity root must not overlap raw/compacted inputs")
        hosts = required(env, "WINTAP_DBT_PROCESS_ENTITY_HOSTS").split(",")
        if any(not h.strip() for h in hosts):
            raise ValueError("Expected entity hosts must be a nonempty comma-separated list")
        required(env, "WINTAP_DBT_PROCESS_ENTITY_NAMESPACE")
        if entity_kind == "local" and not Path(entity).is_dir():
            raise ValueError("Process entity root does not exist")
    present = False
    for root, root_kind in ((raw, kind), (compacted, compacted_kind)):
        if root_kind == "s3":
            present = True  # Actual S3 existence/access is checked by DBT.
        elif Path(root).exists():
            if not Path(root).is_dir() or not os.access(root, os.R_OK | os.X_OK):
                raise ValueError(f"Local raw source directory is not readable: {root}")
            present = True
    if not present:
        raise ValueError(f"Local raw source directory does not exist: {raw} or {compacted}")

    bounds = []
    for edge, default_hour in (("START", "00"), ("END", "23")):
        name = f"WINTAP_DBT_{edge}_DAY"
        day = env.get(name, "")
        if not re.fullmatch(r"[0-9]{8}", day):
            extra = (
                "; S3 requires explicit partition dates" if "s3" in (kind, compacted_kind) else ""
            )
            raise ValueError(f"{name} must be YYYYMMDD{extra}")
        try:
            date = dt.datetime.strptime(day, "%Y%m%d")
        except ValueError as exc:
            raise ValueError(f"{name} is not a valid calendar date: {day}") from exc
        hour_name = f"WINTAP_DBT_{edge}_HOUR"
        hour = env.get(hour_name, "").strip() or default_hour
        if not re.fullmatch(r"[0-9]{1,2}", hour) or int(hour) > 23:
            raise ValueError(f"{hour_name} must be an hour from 00 to 23")
        bounds.append(date.replace(hour=int(hour)))
    if bounds[1] < bounds[0]:
        raise ValueError("End day/hour must be greater than or equal to start day/hour")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    if not os.access(db_path.parent, os.W_OK | os.X_OK):
        raise ValueError(f"DuckDB output directory is not writable: {db_path.parent}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-only", action="store_true")
    args = parser.parse_args()
    try:
        validate(os.environ, args.output_only)
    except (ValueError, OSError) as exc:
        print(f"DBT configuration error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
