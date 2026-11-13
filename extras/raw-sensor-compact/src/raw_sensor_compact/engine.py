"""DuckDB 2.0.0 union-all writer and shared COUNT(*) validation."""

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import Event, Thread
from urllib.parse import urlsplit

import duckdb

from .config import CompactionError, Dataset, Leaf, Limits, separate_roots


@contextmanager
def connection(limits: Limits | None = None):
    limits = limits or Limits()
    if duckdb.__version__ != "2.0.0":
        raise CompactionError("This tool requires duckdb==2.0.0")
    config = {
        "threads": limits.threads,
        "memory_limit": limits.memory_limit,
        "max_temp_directory_size": limits.max_temp_size,
    }
    if limits.temp_directory:
        config["temp_directory"] = str(
            Path(limits.temp_directory).expanduser().resolve()
        )
    conn = duckdb.connect(":memory:", config=config)
    stop, expired = Event(), Event()

    def interrupt_after_timeout():
        if not stop.wait(limits.timeout):
            expired.set()
            conn.interrupt()
            # Interrupt subsequent SQL too if the deadline elapsed between queries.
            while not stop.wait(0.1):
                conn.interrupt()

    thread = Thread(target=interrupt_after_timeout, daemon=True)
    thread.start()
    try:
        if conn.execute("SELECT version()").fetchall()[0][0] != "v2.0.0":
            raise CompactionError("SQL engine must be DuckDB 2.0.0")
        yield conn
        if expired.is_set():
            raise CompactionError("Command timeout exceeded")
    except duckdb.Error:
        message = (
            "Command timeout exceeded"
            if expired.is_set()
            else ("DuckDB Parquet operation failed; details suppressed")
        )
        raise CompactionError(message) from None
    finally:
        stop.set()
        thread.join()
        conn.close()


def _files(files) -> list[str]:
    values = [str(file) for file in files]
    if len(set(values)) != len(values):
        raise CompactionError("File lists must not include duplicate paths")
    if any(
        not f.endswith(".parquet") or any(c in f for c in "*?[]\x00\n\r")
        for f in values
    ):
        raise CompactionError("Parquet inputs must be explicit file paths, not globs")
    return values


def _skip_field(fields, index):
    children = fields[index][1] or 0
    index += 1
    for _ in range(children):
        index = _skip_field(fields, index)
    return index


def schema(conn, files) -> list[dict]:
    """Conservative schema compatibility: same types or signed integer widening."""
    values = _files(files)
    types: dict[str, tuple[str, str]] = {}
    signed = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT"}
    for file in values:
        fields = conn.execute(
            "SELECT name, num_children FROM parquet_schema(?)", [file]
        ).fetchall()
        names = []

        index = 1
        for _ in range(fields[0][1] or 0):
            names.append(fields[index][0])
            index = _skip_field(fields, index)
        if len({name.casefold() for name in names}) != len(names):
            raise CompactionError("Ambiguous duplicate/case-colliding physical columns")
        columns = conn.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)", [[file]]
        ).fetchall()
        for name, dtype, *_ in columns:
            previous = types.get(name.casefold())
            if previous:
                old_name, old_type = previous
                if name != old_name:
                    raise CompactionError(
                        "Column name case differs between source files"
                    )
                if dtype != old_type and not {dtype, old_type} <= signed:
                    raise CompactionError(
                        f"Incompatible types in column {name!r}: "
                        f"{old_type} versus {dtype}"
                    )
            if not previous or dtype not in signed:
                types[name.casefold()] = (name, dtype)
            elif previous[1] in signed:
                rank = ["TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT"]
                if rank.index(dtype) > rank.index(previous[1]):
                    types[name.casefold()] = (name, dtype)
    if not values:
        return []
    columns = conn.execute(
        "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true, "
        "hive_partitioning=false)",
        [values],
    ).fetchall()
    result = [{"name": name, "type": dtype} for name, dtype, *_ in columns]
    if any(types[col["name"].casefold()][1] != col["type"] for col in result):
        raise CompactionError("Union-by-name changed a physical type unexpectedly")
    return result


def count_rows(conn, files) -> int:
    values = _files(files)
    if not values:
        return 0
    return conn.execute(
        "SELECT count(*) FROM read_parquet(?, union_by_name=true, "
        "hive_partitioning=false)",
        [values],
    ).fetchall()[0][0]


@dataclass(frozen=True)
class CountCheck:
    source_rows: int
    merged_rows: int
    source_files: int
    merged_files: int

    @property
    def row_delta(self) -> int:
        return self.merged_rows - self.source_rows

    @property
    def passed(self) -> bool:
        return self.row_delta == 0

    def as_dict(self) -> dict:
        return {**asdict(self), "row_delta": self.row_delta, "passed": self.passed}


def validate_counts(conn, source_files, merged_files) -> CountCheck:
    source, merged = _files(source_files), _files(merged_files)
    return CountCheck(
        count_rows(conn, source), count_rows(conn, merged), len(source), len(merged)
    )


def check_hive_fields(conn, files, leaf: Leaf, columns: list[dict]) -> None:
    keys = {"dayPK": leaf.day, "hourPK": leaf.hour, "protoPK": leaf.protocol}
    for column in columns:
        name = column["name"]
        if name not in keys:
            continue
        # Parameters/identifier escaping avoid injecting payload or partition text.
        identifier = '"' + name.replace('"', '""') + '"'
        rows = conn.execute(
            f"SELECT DISTINCT CAST({identifier} AS VARCHAR) "
            "FROM read_parquet(?, union_by_name=true, hive_partitioning=false)",
            [_files(files)],
        ).fetchall()
        expected = keys[name]
        for (value,) in rows:
            if value is None:
                # Preserve NULL payloads, including NULLs from missing columns.
                continue
            # Hive engines may represent day/hour as integers, dropping leading zeros.
            equal = value == expected or (
                name != "protoPK" and value.isdigit() and int(value) == int(expected)
            )
            if not equal:
                raise CompactionError("Physical hive field conflicts with directory")


def compact_leaf(conn, files, output_directory, leaf: Leaf, *, target_size_bytes: int):
    """Write an empty LOCAL candidate leaf; never publish, append, or overwrite."""
    source = _files(files)
    if type(target_size_bytes) is not int or target_size_bytes <= 0:
        raise CompactionError("A positive explicit target-size byte count is required")
    if not source:
        raise CompactionError("No source files to compact")
    if str(output_directory).startswith("s3://"):
        raise CompactionError(
            "S3 writes await the publication/endpoint qualification step"
        )
    output = Path(output_directory).expanduser().resolve()
    _files([str(output / "part.parquet")])
    # Assert the exact leaf and separation, including resolved symlink targets.
    leaf_suffix = "/" + leaf.relative.rstrip("/")
    if not output.as_posix().endswith(leaf_suffix):
        raise CompactionError("Output directory must mirror the exact source leaf")
    depth = len(Path(leaf.relative).parts)
    destination_dataset = Dataset.parse(str(output.parents[depth - 1]))
    for file in source:
        if urlsplit(file).scheme:
            raise CompactionError("This development writer accepts local fixtures only")
        path = Path(file).resolve()
        if not path.parent.as_posix().endswith(leaf_suffix):
            raise CompactionError("Source file is not in the requested leaf")
        if path.is_relative_to(output) or output.is_relative_to(path.parent):
            raise CompactionError("Source and output leaf overlap")
        separate_roots(Dataset.parse(str(path.parents[depth])), destination_dataset)
    columns = schema(conn, source)
    check_hive_fields(conn, source, leaf, columns)
    source_rows = count_rows(conn, source)
    # Exclusive mkdir guards concurrent writers and preserves existing user data.
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        output.mkdir()
    except FileExistsError:
        raise CompactionError(
            "Candidate output already exists; use a fresh directory"
        ) from None
    written = copy_parquet(conn, source, str(output), source_rows, target_size_bytes)
    merged = sorted(str(p) for p in output.glob("*.parquet"))
    return finish_leaf(conn, source, merged, written, leaf, columns)


def copy_parquet(conn, source, output, source_rows, target_size_bytes):
    """COPY into an already reserved empty candidate; not a publication API."""
    if type(target_size_bytes) is not int or target_size_bytes <= 0:
        raise CompactionError("A positive explicit target-size byte count is required")
    single = len(source) == 1 or source_rows == 0
    destination = output.rstrip("/") + "/part-00000.parquet" if single else output
    options = "FORMAT PARQUET, COMPRESSION ZSTD, RETURN_FILES"
    if not single:
        options += f", FILE_SIZE_BYTES {target_size_bytes}, ROW_GROUP_SIZE 2048"
    result = conn.execute(
        f"COPY (SELECT * FROM read_parquet(?, union_by_name=true, "
        f"hive_partitioning=false)) TO ? ({options})",
        [source, destination],
    ).fetchall()[0]
    return result[1]


def finish_leaf(conn, source, merged, written, leaf, columns):
    if set(merged) != set(written):
        raise CompactionError("Actual output file list differs from COPY result")
    check = validate_counts(conn, source, merged)
    if not check.passed or not merged:
        raise CompactionError("Partition source/merged row totals differ")
    if schema(conn, merged) != columns:
        raise CompactionError("Output schema differs from source union")
    if len(source) > 1 and len(merged) >= len(source):
        raise CompactionError(
            "Target size did not reduce file count; candidate not complete"
        )
    return {**leaf.as_dict(), **check.as_dict(), "files": merged, "schema": columns}
