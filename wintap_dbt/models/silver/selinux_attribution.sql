with event_rows as (
    select
        event_id,
        event_family,
        hostname,
        pid,
        pid_hash,
        process_name,
        agent_id,
        event_time,
        first_seen,
        last_seen,
        event_count,
        num_raw_rows,
        case
            when pid_hash is not null
                and trim(pid_hash) <> ''
                and lower(trim(pid_hash)) not in ('fixmepid', 'fixparentpidhash')
                and lower(trim(pid_hash)) not like 'unknown-%'
            then true else false
        end as has_valid_raw_identity
    from {{ ref('selinux_avc') }}

    union all

    select
        event_id, event_family, hostname, pid, pid_hash, process_name, agent_id,
        event_time, first_seen, last_seen, event_count, num_raw_rows,
        case
            when pid_hash is not null
                and trim(pid_hash) <> ''
                and lower(trim(pid_hash)) not in ('fixmepid', 'fixparentpidhash')
                and lower(trim(pid_hash)) not like 'unknown-%'
            then true else false
        end
    from {{ ref('selinux_interaction') }}

    union all

    select
        event_id, event_family, hostname, pid, pid_hash, process_name, agent_id,
        event_time, first_seen, last_seen, event_count, num_raw_rows,
        case
            when pid_hash is not null
                and trim(pid_hash) <> ''
                and lower(trim(pid_hash)) not in ('fixmepid', 'fixparentpidhash')
                and lower(trim(pid_hash)) not like 'unknown-%'
            then true else false
        end
    from {{ ref('selinux_transition') }}
), process_identities as (
    select
        lower(hostname) as host_key,
        lower(pid_hash) as pid_hash_key,
        any_value(pid_hash) as pid_hash,
        any_value(process_name) as process_name,
        any_value(os_pid) as os_pid,
        any_value(process_started) as process_started,
        any_value(process_term) as process_term
    from {{ ref('process') }}
    group by lower(hostname), lower(pid_hash)
), identity_keys as (
    select distinct
        lower(hostname) as host_key,
        lower(pid_hash) as pid_hash_key
    from event_rows
    where has_valid_raw_identity
), identity_matches as (
    select
        k.host_key,
        k.pid_hash_key,
        p.pid_hash as exact_pid_hash,
        p.process_name as exact_process_name,
        p.os_pid as exact_os_pid,
        p.process_started as exact_process_started,
        p.process_term as exact_process_term
    from identity_keys k
    left join process_identities p
      on p.host_key = k.host_key
     and p.pid_hash_key = k.pid_hash_key
), exact_identity as (
    select
        e.*,
        p.exact_pid_hash,
        p.exact_process_name,
        p.exact_os_pid,
        p.exact_process_started,
        p.exact_process_term
    from event_rows e
    left join identity_matches p
      on e.has_valid_raw_identity
     and p.host_key = lower(e.hostname)
     and p.pid_hash_key = lower(e.pid_hash)
), temporal_keys as (
    select distinct
        lower(hostname) as host_key,
        pid,
        event_time
    from exact_identity
    where exact_pid_hash is null
), temporal_candidates as (
    select
        k.host_key,
        k.pid,
        k.event_time,
        p.pid_hash,
        p.process_name,
        p.os_pid,
        p.process_started,
        p.process_term
    from temporal_keys k
    join {{ ref('process') }} p
      on lower(p.hostname) = k.host_key
     and p.os_pid = k.pid
     and k.event_time >= coalesce(p.process_started, p.first_seen)
     and k.event_time <= coalesce(p.process_term, p.last_seen)
), temporal_summary as (
    select
        host_key,
        pid,
        event_time,
        count(distinct pid_hash) as candidate_count,
        any_value(pid_hash) as temporal_pid_hash,
        any_value(process_name) as temporal_process_name,
        any_value(os_pid) as temporal_os_pid,
        any_value(process_started) as temporal_process_started,
        any_value(process_term) as temporal_process_term
    from temporal_candidates
    group by host_key, pid, event_time
)
select
    e.event_id,
    e.event_family,
    e.hostname,
    e.pid,
    e.pid_hash as raw_pid_hash,
    e.process_name as raw_process_name,
    e.agent_id,
    e.event_time,
    e.first_seen,
    e.last_seen,
    e.event_count,
    e.num_raw_rows,
    case
        when e.exact_pid_hash is not null then e.exact_pid_hash
        when coalesce(t.candidate_count, 0) = 1 then t.temporal_pid_hash
        else null
    end as attributed_pid_hash,
    case
        when e.exact_pid_hash is not null then e.exact_process_name
        when coalesce(t.candidate_count, 0) = 1 then t.temporal_process_name
        else null
    end as attributed_process_name,
    case
        when e.exact_pid_hash is not null then e.exact_os_pid
        when coalesce(t.candidate_count, 0) = 1 then t.temporal_os_pid
        else null
    end as attributed_os_pid,
    case
        when e.exact_pid_hash is not null then e.exact_process_started
        when coalesce(t.candidate_count, 0) = 1 then t.temporal_process_started
        else null
    end as attributed_process_started,
    case
        when e.exact_pid_hash is not null then e.exact_process_term
        when coalesce(t.candidate_count, 0) = 1 then t.temporal_process_term
        else null
    end as attributed_process_term,
    coalesce(t.candidate_count, case when e.exact_pid_hash is not null then 1 else 0 end) as candidate_count,
    case
        when e.exact_pid_hash is not null then 'direct_identity'
        when t.candidate_count = 1 then 'temporal_fallback'
        else null
    end as match_method,
    case
        when e.exact_pid_hash is not null then 'exact'
        when t.candidate_count = 1 then 'unique_temporal'
        when t.candidate_count > 1 then 'ambiguous'
        else 'unmatched'
    end as attribution_status
from exact_identity e
left join temporal_summary t
  on e.exact_pid_hash is null
 and t.host_key = lower(e.hostname)
 and t.pid = e.pid
 and t.event_time = e.event_time
