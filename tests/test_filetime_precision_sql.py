"""Exercise live DBT macros; raw FILETIME and UTC instants are different contracts."""
from pathlib import Path

import duckdb
import jinja2
import pytest


EPOCH = 116444736000000000
MACROS = Path(__file__).resolve().parents[1] / 'wintap_dbt/macros/time.sql'
COLLECTOR_BIRTHS = [134373309947100000,134373310855200000,134373311700500000,
    134373312758700000,134373309980800000,134373311024200000,134373311930100000,
    134373313131600000,134373341938000000,134373341940600000,
    134373342590900000,134373342608100000]


def macros():
    return jinja2.Environment(undefined=jinja2.StrictUndefined).from_string(MACROS.read_text()).module


@pytest.mark.parametrize('zone', ['UTC','America/Los_Angeles','Asia/Kolkata'])
@pytest.mark.parametrize('ft', COLLECTOR_BIRTHS + [None,0,EPOCH-11,EPOCH-10,EPOCH-9,
    EPOCH-1,EPOCH,EPOCH+1,EPOCH+9,EPOCH+10,EPOCH+11,2**63-1,-2**63])
def test_direct_filetime_exact_floor_and_timezone(ft, zone):
    expr = macros().win32_to_timestamp_expr('ft')
    with duckdb.connect(':memory:') as c:
        c.execute('set TimeZone=?',[zone])
        us, typename = c.execute(f'select epoch_us({expr}),typeof({expr}) from (select ?::bigint ft)',[ft]).fetchone()
    assert us == (None if ft is None else (ft-EPOCH)//10)
    assert typename == 'TIMESTAMP WITH TIME ZONE'


@pytest.mark.parametrize('offset', [-600000001,-600000000,-599999999,-11,-10,-9,-1,0,1,9,10,599999999,600000000,600000001])
@pytest.mark.parametrize('base', [EPOCH,134373310200000000])
def test_integer_minute_bin_never_rounds_over_boundary(offset, base):
    expr = macros().win32_to_minute_timestamp_expr('ft')
    with duckdb.connect(':memory:') as c:
        actual = c.execute(f'select epoch_us({expr}) from (select ?::bigint ft)',[base+offset]).fetchone()[0]
    assert actual == ((base+offset-EPOCH)//600000000)*60000000


@pytest.mark.parametrize('text,expected', [(None,None),('',None),('bad',None),('NaN',None),
    ('1.5',None),('134373309947100000.0',None),('1.343733099471e17',None),
    ('9223372036854775808',None),(' 134373309947100000 ',1792857394710000),
    (str(EPOCH-1),-1),('0',-EPOCH//10)])
def test_generic_integer_text_without_double_rounding(text, expected):
    expr = macros().win32_or_timestamp_expr('v')
    with duckdb.connect(':memory:') as c:
        actual = c.execute(f'select epoch_us({expr}) from (select ?::varchar as v)',[text]).fetchone()[0]
    assert actual == expected


@pytest.mark.parametrize('zone', ['UTC','America/Los_Angeles'])
def test_generic_numeric_and_aware_timestamp_preserve_instant(zone):
    m = macros()
    with duckdb.connect(':memory:') as c:
        c.execute('set TimeZone=?',[zone])
        numeric = c.execute('select epoch_us('+m.win32_or_timestamp_expr('v')+') from (select ?::bigint as v)',[COLLECTOR_BIRTHS[0]]).fetchone()[0]
        aware = c.execute("select epoch_us("+m.win32_or_timestamp_expr('v')+") from (select timestamptz '2026-10-24 15:56:34.710000+00' as v)").fetchone()[0]
    assert numeric == (COLLECTOR_BIRTHS[0]-EPOCH)//10
    assert aware == numeric


def test_naive_timestamp_keeps_existing_session_timezone_semantics():
    expr = macros().win32_or_timestamp_expr('v')
    with duckdb.connect(':memory:') as c:
        c.execute("set TimeZone='America/Los_Angeles'")
        actual, expected = c.execute(f"select epoch_us({expr}),epoch_us(cast(v as timestamptz)) from (select timestamp '2026-10-24 08:56:34.710000' as v)").fetchone()
    assert actual == expected


@pytest.mark.parametrize('model,source,time_columns,extra', [
    ('process_file','stg_raw_process_file',('firstseen','lastseen'),
     "'host' hostname,'/proof' file_path,'file-hash' file_hash,12 bytesrequested"),
    ('process_registry','stg_raw_process_registry',('firstseenms','lastseenms'),
     "'host' hosthame,'/reg' reg_path,'value' reg_value,'data' reg_data")])
@pytest.mark.parametrize('ft', [COLLECTOR_BIRTHS[0],EPOCH-1,EPOCH+600000000-1])
def test_nonempty_file_and_registry_consumers(model, source, time_columns, extra, ft):
    from .test_network_activity_sql import render, DBT
    with duckdb.connect(':memory:') as c:
        c.execute(f"create table {source} as select 'agent' agentid,'hash' pidhash,42 pid,'process' processname,'Read' activitytype,7 eventcount,1700000000::bigint eventtime,?::bigint {time_columns[0]},?::bigint {time_columns[1]}, {extra}",[ft,ft+21])
        c.execute('create table actual as '+render(DBT/'models/silver'/f'{model}.sql'))
        assert c.execute('select event_count,epoch_us(first_seen),epoch_us(last_seen),epoch_us(min_event) from actual').fetchone() == (7,(ft-EPOCH)//10,(ft+21-EPOCH)//10,1700000000000000)


@pytest.mark.parametrize('kind', ['bigint','varchar','timestamptz','timestamp'])
def test_nonempty_image_load_consumer(kind):
    from .test_network_activity_sql import render, DBT
    numeric = str(COLLECTOR_BIRTHS[0])
    value = numeric if kind == 'bigint' else f"'{numeric}'" if kind == 'varchar' else f"{kind} '2026-10-24 15:56:34.710000+00'"
    with duckdb.connect(':memory:') as c:
        c.execute("set TimeZone='UTC'")
        c.execute(f"create table stg_raw_imageload as select 'hash' pidhash,42 pid,'/proof.dll' filename,'agent' agentid,'host' computername,'process' processname,'digest' md5,0 buildtime,0 imagechecksum,'0' defaultbase,'0' imagebase,12 imagesize,'LOAD' activitytype,{value} eventtime")
        c.execute('create table actual as '+render(DBT/'models/silver/process_image_load.sql'))
        assert c.execute('select num_load,epoch_us(first_seen),epoch_us(last_seen) from actual').fetchone() == (1,(COLLECTOR_BIRTHS[0]-EPOCH)//10,(COLLECTOR_BIRTHS[0]-EPOCH)//10)
