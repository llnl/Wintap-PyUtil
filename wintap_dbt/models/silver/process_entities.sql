{{ config(enabled=env_var('WINTAP_DBT_PROCESS_ENTITY_ROOT', '') != '') }}
-- Resolve paired parents from ALL retained and current-window evidence, not ANY_VALUE.
with parent_candidates as (
    select e.hostname, e.pid_hash,
           json_extract_string(j.value, '$[0]') parent_hash,
           cast(json_extract(j.value, '$[1]') as bigint) parent_pid
    from {{ ref('stg_process_entities') }} e, json_each(e.evidence_json, '$.parents') j
    union
    select upper(trim(hostname)), pidhash, nullif(parentpidhash, ''), parentpid
    from {{ ref('stg_raw_process') }}
), parents as (
    {{ resolve_process_parents('select * from parent_candidates') }}
), contexts as (
    select hostname, pid_hash, os_pid, process_name, process_path, user_name, args, agent_id,
           true from_store, identity_status='conflicted' stored_identity_conflict, bootstrap_start,
           metadata_conflict stored_metadata_conflict
    from {{ ref('stg_process_entities') }}
    union all
    select upper(trim(hostname)), pidhash, pid, nullif(processname, ''), nullif(processpath, ''),
           nullif(username, ''), nullif(processargs, ''), agentid, false, false, null::varchar, false
    from {{ ref('stg_raw_process') }}
    where {{ valid_process_key('pidhash', 'hostname') }} and pid >= 0
), instances as (
    select hostname, pid_hash,
           case when count(distinct os_pid) = 1 then min(os_pid) end os_pid,
           min(process_name) process_name, min(process_path) process_path,
           min(user_name) user_name, min(args) args, min(agent_id) agent_id,
           bool_or(from_store) from_store,
           min(bootstrap_start) bootstrap_start,
           bool_or(stored_metadata_conflict) or count(distinct process_name)>1
               or count(distinct process_path)>1 or count(distinct user_name)>1
               or count(distinct args)>1 as metadata_conflict,
           count(distinct os_pid) > 1 or bool_or(stored_identity_conflict) identity_conflict
    from contexts group by hostname, pid_hash
)
select i.*,
       case when not i.identity_conflict then p.parent_pid_hash end parent_pid_hash,
       case when not i.identity_conflict then p.parent_os_pid end parent_os_pid,
       p.parent_status
from instances i join parents p using(hostname, pid_hash)
