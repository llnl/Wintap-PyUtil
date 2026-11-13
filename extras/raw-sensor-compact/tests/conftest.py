from pathlib import Path

import duckdb
import pytest


@pytest.fixture
def conn():
    connection = duckdb.connect(":memory:", config={"threads": 2})
    yield connection
    connection.close()


@pytest.fixture
def parquet(conn):
    def write(path, query):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        conn.execute(f"COPY ({query}) TO ? (FORMAT PARQUET)", [str(path)])
        return str(path)

    return write
