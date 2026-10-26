"""Execute current gold models, preserving lifetime identity through name changes."""
from pathlib import Path
import duckdb
import pytest
from .test_network_activity_sql import connection, insert_activity, render as render_network

DBT = Path(__file__).resolve().parents[1] / 'wintap_dbt'


def render(name):
    import jinja2
    env = jinja2.Environment(undefined=jinja2.StrictUndefined)
    env.globals['ref'] = lambda name: name
    return env.from_string((DBT / 'models/gold' / (name + '.sql')).read_text()).render()


@pytest.mark.parametrize('spelling', ['title', 'upper', 'lower'])
def test_file_name_changes_keep_one_lifetime_and_conserve_counters(spelling):
    with duckdb.connect(':memory:') as c:
        c.execute('''create table process_file as select 'agent' agent_id, 'host' hostname,
          'fork-image' process_name, 'hash' pid_hash, 123 os_pid, 'Read' activity_type,
          100::bigint bytes_requested, 2::bigint event_count, 1 num_raw_rows,
          'file' file_hash, '/file' filename, timestamp '2026-10-23' first_seen,
          timestamp '2026-10-23' last_seen''')
        c.execute("insert into process_file select 'other-agent',hostname,'exec-image',pid_hash,os_pid,'Write',200,3,1,file_hash,filename,first_seen,last_seen+interval '1 second' from process_file")
        c.execute("insert into process_file select agent_id,hostname,process_name,pid_hash,os_pid,'Open',0,1,1,file_hash,filename,first_seen,last_seen from process_file limit 1")
        if spelling == 'upper':
            c.execute('update process_file set activity_type=upper(activity_type)')
        elif spelling == 'lower':
            c.execute('update process_file set activity_type=lower(activity_type)')
        row = c.execute('select * from (' + render('process_file_summary') + ')').fetchone()
        result = dict(zip([d[0] for d in c.description], row))
        assert result['process_name'] == 'exec-image'
        assert result['Read_Events'] == 2 and result['Write_Events'] == 3 and result['Open_Events'] == 1
        assert result['Read_Bytes'] == 100 and result['Write_Bytes'] == 200
        assert result['num_raw_rows'] == 3
        assert c.execute('select count(*) from (' + render('process_file_summary') + ')').fetchone()[0] == 1


@pytest.mark.parametrize('elapsed,expected', [('11 hours 27 minutes 0.123 seconds', 41220.123),
                                              ('1.3 seconds', 1.3), ('0 seconds', 0), (None, None)])
def test_duration_expression_uses_full_elapsed_seconds(elapsed, expected):
    sql = render('process_summary')
    # Execute the model's actual select expression, not a duplicate formula.
    expression = next(line.strip().rstrip(',') for line in sql.splitlines() if ' duration_seconds' in line)
    with duckdb.connect(':memory:') as c:
        c.execute('create table p as select timestamp \'2026-10-23\' process_started, timestamp \'2026-10-23\' + cast(? as interval) process_term', [elapsed])
        assert c.execute('select ' + expression + ' from p').fetchone()[0] == expected


def test_network_name_changes_keep_one_lifetime_and_conserve_counters(connection):
    insert_activity(connection, 'TcpIpSend')
    connection.execute('create table process_conn_incr as ' + render_network(DBT / 'models/silver/process_conn_incr.sql'))
    connection.execute("insert into process_conn_incr select * replace ('exec-image' as process_name, last_seen+interval '1 second' as last_seen) from process_conn_incr")
    connection.execute('create table process_net_conn as ' + render_network(DBT / 'models/silver/process_net_conn.sql'))
    result = connection.execute('select count(*),sum(net_total_events),sum(tcp_send_count),sum(net_send_size) from (' + render('process_net_summary') + ')').fetchone()
    assert result == (1, 14, 14, 200)
