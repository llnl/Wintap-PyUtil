{{ config(enabled=env_var('WINTAP_DBT_PROCESS_ENTITY_ROOT', '') != '', materialized='table') }}
-- One verdict per existing window seed. Longest available chain is not completeness.
select p.hostname, p.pid_hash, p.level,
       list_extract(p.ptree_list, -1) terminal_pid_hash,
       case when c.context_status = 'root' then 'root_reached'
            when c.context_status != 'linked' then c.context_status
            when list_contains(p.ptree_list, c.parent_pid_hash) then 'cycle'
            else 'unresolved' end ancestry_status,
       c.parent_pid_hash missing_or_conflicted_parent,
       c.parent_status,
       c.bootstrap_start,
       c.from_store as terminal_from_store
from {{ ref('process_path') }} p
left join {{ ref('process_context') }} c
    on c.hostname=upper(trim(p.hostname)) and c.pid_hash=list_extract(p.ptree_list, -1)
