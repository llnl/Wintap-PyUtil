-- TEMPORARY DBT validation for the direction-independent CONN_ID contract.
-- Keep endpoint IPs paired with their ports when ordering the two endpoints.
with vectors as (
    select * from (values
        ('forward', 'TCP', 167772161::bigint, 49152::integer, 167772162::bigint, 22::integer),
        ('reverse', 'tcp', 167772162::bigint, 22::integer, 167772161::bigint, 49152::integer),
        ('same_ip_forward', 'UDP', 167772161::bigint, 53000::integer, 167772161::bigint, 53::integer),
        ('same_ip_reverse', 'udp', 167772161::bigint, 53::integer, 167772161::bigint, 53000::integer),
        ('changed_port', 'TCP', 167772161::bigint, 49153::integer, 167772162::bigint, 22::integer),
        ('changed_ip', 'TCP', 167772161::bigint, 49152::integer, 167772163::bigint, 22::integer),
        ('changed_protocol', 'UDP', 167772161::bigint, 49152::integer, 167772162::bigint, 22::integer),
        ('missing_endpoint', 'TCP', 167772161::bigint, 49152::integer, cast(null as bigint), 22::integer)
    ) as v(case_name, protocol, local_ip, local_port, remote_ip, remote_port)
), keyed as (
    select
        case_name,
        {{ temporary_five_tuple_conn_id(
            'protocol', 'local_ip', 'local_port', 'remote_ip', 'remote_port'
        ) }} as conn_id
    from vectors
)
select 'reversed endpoint order changed CONN_ID' as issue
where (select conn_id from keyed where case_name = 'forward')
   is distinct from (select conn_id from keyed where case_name = 'reverse')
union all
select 'same-IP port ordering changed CONN_ID'
where (select conn_id from keyed where case_name = 'same_ip_forward')
   is distinct from (select conn_id from keyed where case_name = 'same_ip_reverse')
union all
select 'changing an endpoint port did not change CONN_ID'
where (select conn_id from keyed where case_name = 'forward')
   is not distinct from (select conn_id from keyed where case_name = 'changed_port')
union all
select 'changing an endpoint IP did not change CONN_ID'
where (select conn_id from keyed where case_name = 'forward')
   is not distinct from (select conn_id from keyed where case_name = 'changed_ip')
union all
select 'changing protocol did not change CONN_ID'
where (select conn_id from keyed where case_name = 'forward')
   is not distinct from (select conn_id from keyed where case_name = 'changed_protocol')
union all
select 'incomplete tuple generated a CONN_ID'
where (select conn_id from keyed where case_name = 'missing_endpoint') is not null
