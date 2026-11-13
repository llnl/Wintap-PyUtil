{{ config(enabled=env_var('WINTAP_DBT_PROCESS_ENTITY_ROOT', '') != '') }}
-- Activity-only keys are context references, not new lifecycle observations or summaries.
with recursive seeds as (
    select distinct upper(trim(hostname)) hostname, pidhash pid_hash, true window_lifecycle
    from {{ ref('stg_raw_process') }}
    {% for family in ['raw_process_file', 'raw_process_conn_incr', 'raw_process_registry', 'raw_registry', 'raw_imageload', 'raw_image_load'] %}
    {% if raw_event_exists(family) and raw_column_exists([family], 'PidHash') %}
    union all
    select distinct upper(trim(hostname)), pidhash, false
    from {{ parquet_relation(family) }} where {{ partition_filter() }}
    {% endif %}
    {% endfor %}
), keys(hostname, pid_hash) as (
    select hostname, pid_hash from seeds where pid_hash is not null and pid_hash != ''
    union
    select e.hostname, e.parent_pid_hash
    from keys k join {{ ref('process_entities') }} e using(hostname, pid_hash)
    where e.parent_status = 'resolved' and e.parent_pid_hash is not null
), seed_flags as (
    select hostname, pid_hash, bool_or(window_lifecycle) window_lifecycle
    from seeds group by hostname, pid_hash
)
select k.hostname, k.pid_hash, e.* exclude(hostname, pid_hash),
       coalesce(s.window_lifecycle, false) window_lifecycle,
       s.pid_hash is null context_only,
       s.pid_hash is not null and not s.window_lifecycle activity_only,
       case when e.pid_hash is null then 'missing_entity'
            when e.identity_conflict then 'identity_conflict'
            when e.parent_status = 'conflicted' then 'parent_conflict'
            when e.parent_pid_hash = e.pid_hash and e.os_pid in ({{ process_root_pids() }})
                 and (e.parent_os_pid is null or e.parent_os_pid=e.os_pid) then 'root'
            when e.parent_pid_hash = e.pid_hash then 'cycle'
            when e.parent_status = 'sentinel' then 'sentinel'
            when e.parent_pid_hash is null then 'missing_parent'
            when parent.pid_hash is null then 'missing_ancestor'
            when e.parent_os_pid is not null and parent.os_pid is distinct from e.parent_os_pid then 'parent_pid_mismatch'
            else 'linked' end context_status
from keys k
left join {{ ref('process_entities') }} e using(hostname, pid_hash)
left join seed_flags s using(hostname, pid_hash)
left join {{ ref('process_entities') }} parent
    on parent.hostname=e.hostname and parent.pid_hash=e.parent_pid_hash
