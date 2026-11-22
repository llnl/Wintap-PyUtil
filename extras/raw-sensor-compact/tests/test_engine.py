import hashlib
from pathlib import Path

import duckdb
import pytest

from raw_sensor_compact.config import CompactionError, Leaf, Limits
from raw_sensor_compact.engine import (
    compact_leaf,
    connection,
    count_rows,
    schema,
    validate_counts,
)

LEAF = Leaf("pidstat", "20260101", "00")


def test_runtime_and_connection_lifecycle():
    assert duckdb.__version__ == "2.0.0"
    with connection() as conn:
        # Extension APIs reject unfinished results in DuckDB 2.0.0.
        conn.load_extension("parquet")
        assert conn.execute("SELECT version()").fetchone() == ("v2.0.0",)
    with pytest.raises(duckdb.Error):
        conn.execute("SELECT 1")


def test_union_all_null_columns_duplicates_and_source_bytes(tmp_path, conn, parquet):
    folder = tmp_path / "source" / LEAF.relative
    first = parquet(
        folder / "host1.parquet",
        (
            "SELECT 'host1' AS hostname, 1::INTEGER AS pid, NULL::VARCHAR AS optional "
            "FROM range(2)"
        ),
    )
    second = parquet(
        folder / "host2.parquet",
        ("SELECT 1::BIGINT AS pid, 'host2' AS hostname, 'capture.txt' AS filename"),
    )
    sources = [first, second]
    before = [hashlib.sha256(Path(file).read_bytes()).hexdigest() for file in sources]
    result = compact_leaf(
        conn,
        sources,
        tmp_path / "merged" / LEAF.relative,
        LEAF,
        target_size_bytes=128 * 1024**2,
    )
    assert result["source_rows"] == result["merged_rows"] == 3
    assert result["row_delta"] == 0
    assert len(result["files"]) == 1
    rows = conn.execute(
        "SELECT hostname, pid, optional, filename FROM read_parquet(?, "
        "hive_partitioning=false) ORDER BY hostname",
        [result["files"]],
    ).fetchall()
    assert rows == [
        ("host1", 1, None, None),
        ("host1", 1, None, None),
        ("host2", 1, None, "capture.txt"),
    ]
    assert {column["name"] for column in result["schema"]} == {
        "hostname",
        "pid",
        "optional",
        "filename",
    }
    assert [
        hashlib.sha256(Path(file).read_bytes()).hexdigest() for file in sources
    ] == before


def test_shared_count_check_detects_removed_and_extra_rows(tmp_path, conn, parquet):
    sources = [
        parquet(tmp_path / "a.parquet", "SELECT NULL::INTEGER AS value FROM range(3)"),
        parquet(tmp_path / "b.parquet", "SELECT 1 AS value FROM range(2)"),
    ]
    for count, expected in [(5, True), (4, False), (6, False)]:
        merged = parquet(
            tmp_path / f"merged-{count}.parquet", f"SELECT 1 AS x FROM range({count})"
        )
        check = validate_counts(conn, sources, [merged])
        assert check.source_rows == 5
        assert check.merged_rows == count
        assert check.passed == expected
        assert check.row_delta == count - 5
    assert validate_counts(conn, [], []).passed
    assert not validate_counts(conn, sources, []).passed
    assert count_rows(conn, []) == 0
    assert count_rows(conn, sources) == 5
    conn.load_extension("parquet")
    with pytest.raises(CompactionError, match="duplicate"):
        count_rows(conn, [sources[0], sources[0]])


@pytest.mark.parametrize(
    "column_a,column_b",
    [
        ("1::INTEGER AS x", "'1'::VARCHAR AS x"),
        ("1::UBIGINT AS x", "1::BIGINT AS x"),
        ("1 AS x", "1 AS X"),
        ("[1] AS x", "1 AS x"),
    ],
)
def test_schema_conflict_fails_before_write(
    tmp_path, conn, parquet, column_a, column_b
):
    source = tmp_path / "source" / LEAF.relative
    files = [
        parquet(source / "a.parquet", f"SELECT {column_a}"),
        parquet(source / "b.parquet", f"SELECT {column_b}"),
    ]
    output = tmp_path / "merged" / LEAF.relative
    with pytest.raises(CompactionError):
        compact_leaf(conn, files, output, LEAF, target_size_bytes=1_000_000)
    assert not output.exists()


def test_existing_candidate_not_overwritten(tmp_path, conn, parquet):
    source = parquet(tmp_path / "source" / LEAF.relative / "a.parquet", "SELECT 1 AS x")
    output = tmp_path / "merged" / LEAF.relative
    result = compact_leaf(conn, [source], output, LEAF, target_size_bytes=1)
    original = Path(result["files"][0]).read_bytes()
    with pytest.raises(CompactionError, match="already exists"):
        compact_leaf(conn, [source], output, LEAF, target_size_bytes=1)
    assert Path(result["files"][0]).read_bytes() == original


def test_zero_row_parquet_retains_schema(tmp_path, conn, parquet):
    source = tmp_path / "source" / LEAF.relative
    files = [
        parquet(source / "a.parquet", "SELECT 1::BIGINT AS x WHERE false"),
        parquet(source / "b.parquet", "SELECT ''::VARCHAR AS y WHERE false"),
    ]
    result = compact_leaf(
        conn,
        files,
        tmp_path / "merged" / LEAF.relative,
        LEAF,
        target_size_bytes=1_000_000,
    )
    assert result["source_rows"] == result["merged_rows"] == 0
    assert result["schema"] == [
        {"name": "x", "type": "BIGINT"},
        {"name": "y", "type": "VARCHAR"},
    ]


def test_physical_hive_values_preserved_or_fail(tmp_path, conn, parquet):
    source = tmp_path / "source" / LEAF.relative
    good = parquet(source / "a.parquet", "SELECT '20260101' AS dayPK, 0 AS hourPK")
    result = compact_leaf(
        conn,
        [good],
        tmp_path / "merged" / LEAF.relative,
        LEAF,
        target_size_bytes=1_000_000,
    )
    assert conn.execute(
        "SELECT * FROM read_parquet(?, hive_partitioning=false)", [result["files"]]
    ).fetchone() == ("20260101", 0)
    bad = parquet(source / "b.parquet", "SELECT '20260101' AS dayPK, 1 AS hourPK")
    with pytest.raises(CompactionError, match="conflicts"):
        compact_leaf(
            conn,
            [bad],
            tmp_path / "bad" / LEAF.relative,
            LEAF,
            target_size_bytes=1_000_000,
        )


def test_multiple_shards_and_count_across_all_of_them(tmp_path, conn, parquet):
    source = tmp_path / "source" / LEAF.relative
    files = [
        parquet(
            source / f"host{i}.parquet",
            (
                f"SELECT j, md5(CAST(j + {i} * 10000 AS VARCHAR)) AS text "
                "FROM range(10000) t(j)"
            ),
        )
        for i in range(10)
    ]
    result = compact_leaf(
        conn,
        files,
        tmp_path / "merged" / LEAF.relative,
        LEAF,
        target_size_bytes=600_000,
    )
    assert 1 < len(result["files"]) < len(files)
    assert result["source_rows"] == result["merged_rows"] == 100_000
    assert not validate_counts(conn, files, result["files"][:-1]).passed


def test_wrong_leaf_or_source_output_overlap_is_rejected(tmp_path, conn, parquet):
    source = parquet(tmp_path / "source" / LEAF.relative / "a.parquet", "SELECT 1 AS x")
    with pytest.raises(CompactionError, match="overlap"):
        compact_leaf(conn, [source], Path(source).parent, LEAF, target_size_bytes=1)
    with pytest.raises(CompactionError, match="exact source leaf"):
        compact_leaf(
            conn, [source], tmp_path / "merged" / "wrong", LEAF, target_size_bytes=1
        )
    with pytest.raises(CompactionError, match="not in"):
        compact_leaf(
            conn,
            [source],
            tmp_path / "merged" / Leaf("other", "20260101", "00").relative,
            Leaf("other", "20260101", "00"),
            target_size_bytes=1,
        )


def test_nested_schema_preserved(tmp_path, conn, parquet):
    file = parquet(
        tmp_path / "a.parquet", "SELECT {'name': 'a', 'items': [1,2]} AS payload"
    )
    assert schema(conn, [file]) == [
        {"name": "payload", "type": 'STRUCT("name" VARCHAR, items INTEGER[])'}
    ]


def test_timeout_closes_connection():
    with pytest.raises(CompactionError, match="timeout"):
        with connection(Limits(timeout=0.01)) as conn:
            conn.execute(
                "SELECT sum(sqrt(a.i + b.i)) "
                "FROM range(100000) a(i), range(100000) b(i)"
            )


def test_null_and_missing_hive_fields_are_payload(tmp_path, conn, parquet):
    folder = tmp_path / "source" / LEAF.relative
    files = [
        parquet(folder / "a.parquet", "SELECT NULL::VARCHAR AS dayPK, 1 AS value"),
        parquet(folder / "b.parquet", "SELECT 2 AS value"),
    ]
    result = compact_leaf(
        conn,
        files,
        tmp_path / "merged" / LEAF.relative,
        LEAF,
        target_size_bytes=1_000_000,
    )
    assert conn.execute(
        "SELECT dayPK, value FROM read_parquet(?, hive_partitioning=false) "
        "ORDER BY value",
        [result["files"]],
    ).fetchall() == [(None, 1), (None, 2)]


def test_writer_rejects_nested_dataset_namespaces(tmp_path, conn, parquet):
    source = parquet(
        tmp_path / "source" / LEAF.relative / "a.parquet", "SELECT 1 AS value"
    )
    with pytest.raises(CompactionError, match="overlap"):
        compact_leaf(
            conn,
            [source],
            tmp_path / "source" / "merged" / LEAF.relative,
            LEAF,
            target_size_bytes=1_000_000,
        )


def test_duplicates_across_files_survive(tmp_path, conn, parquet):
    folder = tmp_path / "source" / LEAF.relative
    files = [
        parquet(
            folder / f"host{i}.parquet",
            "SELECT NULL::VARCHAR AS optional, 1 AS value FROM range(2)",
        )
        for i in range(2)
    ]
    result = compact_leaf(
        conn,
        files,
        tmp_path / "merged" / LEAF.relative,
        LEAF,
        target_size_bytes=1_000_000,
    )
    assert result["source_rows"] == result["merged_rows"] == 4
    assert (
        conn.execute(
            "SELECT * FROM read_parquet(?, hive_partitioning=false)", [result["files"]]
        ).fetchall()
        == [(None, 1)] * 4
    )
