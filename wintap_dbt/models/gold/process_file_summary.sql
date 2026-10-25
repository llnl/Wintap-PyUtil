select
    arg_max(agent_id, (last_seen, agent_id)) agent_id,
    min(hostname) hostname,
    arg_max(process_name, (last_seen, process_name)) process_name,
    pid_hash,
    any_value(os_pid) os_pid,
    sum(case when lower(activity_type) = 'open' then event_count else 0 end) Open_Events,
    sum(case when lower(activity_type) = 'close' then event_count else 0 end) Close_Events,
    sum(case when lower(activity_type) = 'create' then event_count else 0 end) Create_Events,
    sum(case when lower(activity_type) = 'delete' then event_count else 0 end) Delete_Events,
    sum(case when lower(activity_type) = 'rename' then event_count else 0 end) Rename_Events,
    sum(case when lower(activity_type) = 'setinfo' then event_count else 0 end) SetInfo_Events,
    sum(case when lower(activity_type) = 'read' then bytes_Requested else 0 end) Read_Bytes,
    sum(case when lower(activity_type) = 'read' then event_count else 0 end) Read_Events,
    sum(case when lower(activity_type) = 'write' then bytes_Requested else 0 end) Write_Bytes,
    sum(case when lower(activity_type) = 'write' then event_count else 0 end) Write_Events,
    sum(num_raw_rows) num_raw_rows,
    count(distinct file_hash) num_uniq_file_hash,
    sum(case when filename is null then 1 else 0 end) num_null_filename,
    min(first_seen) first_seen,
    max(last_seen) last_seen
from {{ ref('process_file') }}
group by pid_hash
