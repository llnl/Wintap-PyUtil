"""Legacy raw views preserve the authoritative producer connection identity."""

from types import SimpleNamespace
from unittest import mock

import pytest

from wintappy.datautils import rawutil

pytestmark = pytest.mark.unit


def test_raw_view_does_not_recompute_conn_id():
    schema = SimpleNamespace(names=["AgentId", "ConnId"])
    with mock.patch.object(rawutil, "glob", return_value=["synthetic.parquet"]), mock.patch.object(
        rawutil.pq, "read_schema", return_value=schema
    ):
        sql = rawutil.get_raw_view("raw_processconnincr", "synthetic/*.parquet")

    assert "md5(" not in sql.lower()
    assert "list_sort(" not in sql.lower()
    assert "exclude (connid" not in sql.lower()
    assert "select *, agentid, count(*)" in sql.lower()


def test_raw_view_adds_agent_id_only_when_missing():
    schema = SimpleNamespace(names=["ConnId"])
    with mock.patch.object(rawutil, "glob", return_value=["synthetic.parquet"]), mock.patch.object(
        rawutil.pq, "read_schema", return_value=schema
    ):
        sql = rawutil.get_raw_view("raw_processconnincr", "synthetic/*.parquet")

    assert "select *, cast(null as varchar) agentid, count(*)" in sql.lower()
