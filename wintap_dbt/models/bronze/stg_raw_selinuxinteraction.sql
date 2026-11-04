{% if first_existing_raw_event(['raw_selinuxinteraction']) is not none %}
select *, count(*) as num_dups
from {{ raw_scan_for(['raw_selinuxinteraction']) }}
where {{ partition_filter() }}
group by all
{% else %}
select
    cast(null as varchar) MessageType,
    cast(null as integer) PID,
    cast(null as varchar) PidHash,
    cast(null as varchar) ProcessName,
    cast(null as varchar) AgentId,
    cast(null as varchar) Hostname,
    cast(null as bigint) FirstSeen,
    cast(null as bigint) LastSeen,
    cast(null as bigint) EventTime,
    cast(null as integer) EventCount,
    cast(null as integer) PolicyEpoch,
    cast(null as bigint) SSid,
    cast(null as bigint) TSid,
    cast(null as varchar) SContext,
    cast(null as varchar) TContext,
    cast(null as varchar) TClass,
    cast(null as varchar) Permission,
    cast(null as bigint) RequestedMask,
    cast(null as boolean) Novel,
    cast(null as bigint) dayPK,
    cast(null as varchar) hourPK,
    cast(0 as bigint) num_dups
where false
{% endif %}
