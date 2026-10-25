"""Execute the real DBT SQL with synthetic current and legacy event names."""
from pathlib import Path

import duckdb
import jinja2
import pytest


DBT = Path(__file__).resolve().parents[1] / "wintap_dbt"


def render(path):
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    # These macros are pure SQL renderers; no DBT context is needed here.
    macro_source = "\n".join((DBT / "macros" / name).read_text() for name in ("time.sql", "ip.sql"))
    module = env.from_string(macro_source).module
    env.globals.update({name: getattr(module, name) for name in dir(module) if not name.startswith("_")})
    env.globals["ref"] = lambda name: name
    return env.from_string(path.read_text()).render()


@pytest.fixture
def connection():
    with duckdb.connect(":memory:") as conn:
        conn.execute("""
            create table stg_raw_process_conn_incr as select
              'agent' agentid, 'test-host' hostname, 'process' pidhash,
              3291 pid, 'sshd' processname, 'connection' connid, 'TCP' protocol,
              134372000000000000::bigint firstseenms,
              134372000000000000::bigint lastseenms,
              '192.168.252.40' sourceipaddressstring,
              '192.168.252.1' destinationipaddressstring,
              3232300072::bigint localipaddr, '' localipprivategateway, 22 localport,
              3232300033::bigint remoteipaddr, '' remoteipprivategateway, 58969 remoteport,
              7 eventcount, 100 packetsize, 10 minpacketsize, 50 maxpacketsize,
              10000 packetsizesquared, '' ipevent
            where false
        """)
        yield conn


def insert_activity(conn, name):
    conn.execute("""
        insert into stg_raw_process_conn_incr values
        ('agent', 'test-host', 'process', 3291, 'sshd', 'connection', 'TCP',
         134372000000000000, 134372000000000000, '192.168.252.40', '192.168.252.1',
         3232300072, '', 22, 3232300033, '', 58969, 7, 100, 10, 50, 10000, ?)
    """, [name])


def model_result(conn, model="dbt"):
    if model == "dbt":
        sql = render(DBT / "models/silver/process_conn_incr.sql")
    else:
        conn.execute("create view raw_process_conn_incr as select * from stg_raw_process_conn_incr")
        # The legacy builder normally registers these UDFs in Python.
        env = jinja2.Environment()
        time = env.from_string((DBT / "macros/time.sql").read_text()).module
        ip = env.from_string((DBT / "macros/ip.sql").read_text()).module
        conn.execute("create macro win32_to_epoch(wts) as " + time.win32_to_epoch_expr("wts"))
        conn.execute("create macro to_timestamp_micros(seconds) as " + time.to_timestamp_micros_expr("seconds"))
        conn.execute("create macro int_to_ip(i) as " + ip.int_to_ip_expr("i"))
        legacy = (DBT.parent / "wintappy/datautils/rawtostdview.sql").read_text()
        sql = legacy.split("CREATE TABLE IF NOT EXISTS process_conn_incr\nAS\n", 1)[1].split(";", 1)[0]
    # Fetch numeric aggregates without DuckDB's optional Python timezone adapter.
    cursor = conn.execute("select * exclude(incr_start, first_seen, last_seen) from (" + sql + ")")
    return dict(zip([col[0] for col in cursor.description], cursor.fetchone()))


ACTIVITIES = {
    "TcpIpAccept": "tcp_accept", "TcpIpConnect": "tcp_connect",
    "TcpIpDisconnect": "tcp_disconnect", "TcpIpReconnect": "tcp_reconnect",
    "TcpIpRecv": "tcp_recv", "TcpIpRetransmit": "tcp_retransmit",
    "TcpIpSend": "tcp_send", "TcpIpTCPCopy": "tcp_tcpcopy",
    "UdpIpRecv": "udp_recv", "UdpIpSend": "udp_send",
    "TcpIpRecvIPV6": "tcp_recv", "TcpIpSendIPV6": "tcp_send",
}


@pytest.mark.parametrize("activity,prefix", ACTIVITIES.items())
@pytest.mark.parametrize("spelling", ["current", "legacy", "lowercase"])
@pytest.mark.parametrize("model", ["dbt", "legacy"])
def test_real_model_counts_all_activity_spellings(connection, activity, prefix, spelling, model):
    name = activity[:5] + "/" + activity[5:] if spelling == "legacy" else activity
    if spelling == "lowercase":
        name = name.lower()
    insert_activity(connection, name)
    result = model_result(connection, model)
    expected = {
        "count": 7, "size": 100, "max_count": 7,
        "min_size": 10, "max_size": 50, "sq_size": 10000,
    }
    for column, value in result.items():
        # Verify all directional aggregates, including squared size and extrema,
        # not just the count that originally exposed the bug.
        suffix = column.removeprefix(prefix + "_")
        if column.startswith(prefix + "_") and suffix in expected:
            assert value == expected[suffix], column
        if column == "max_" + prefix + "_count":
            assert value == 7
        if column == "sq_" + prefix + "_size":
            assert value == 10000
        if column == "min_" + prefix + "_size":
            assert value == 10
        if column == "max_" + prefix + "_size":
            assert value == 50
        if column.startswith(("tcp_", "udp_")) and column.endswith("_count") and column != prefix + "_count":
            assert value is None, column
    assert result["total_events"] == 7


def test_vocabulary_gate_flags_unknown_and_null_names(connection):
    for name in ["TcpIpRecv", "TcpIp/Recv", "UdpIpSend", "TcpIpRecvv", None]:
        insert_activity(connection, name)
    rejected = {row[0] for row in connection.execute(render(DBT / "tests/network_activity_names.sql")).fetchall()}
    assert rejected == {"TcpIpRecvv", None}


def test_mixed_spellings_are_counted_together(connection):
    for name in ["TcpIpRecv", "TcpIp/Recv", "tcpiprecv", "TcpIpSend"]:
        insert_activity(connection, name)
    result = model_result(connection)
    assert result["tcp_recv_count"] == 21
    assert result["tcp_send_count"] == 7
    assert result["total_events"] == 28
