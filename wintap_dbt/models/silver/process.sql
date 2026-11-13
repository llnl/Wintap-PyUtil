{% set entity_mode = env_var is defined and env_var('WINTAP_DBT_PROCESS_ENTITY_ROOT', '') != '' %}
with process_base as (
    select
        p.pidhash pid_hash,
        any_value('windows') os_family,
        any_value(agentid) agent_id,
        count(distinct agentid) num_agent_id,
        {% if entity_mode %}p.hostname{% else %}any_value(p.hostname){% endif %} hostname,
        any_value(pid) os_pid,
        any_value(case when p.processname = '' then null else p.processname end) process_name,
        count(distinct p.processname) num_process_name,
        any_value(case when p.processargs = '' then null else p.processargs end) args,
        count(distinct p.processargs) num_args,
        any_value(case when p.username = '' or lower(p.username) = 'na' then null else p.username end) user_name,
        count(distinct p.username) num_user_name,
        any_value(case when p.parentpidhash = '' then null else p.parentpidhash end) parent_pid_hash,
        count(distinct p.parentpidhash) num_parent_pid_hash,
        any_value(p.parentpid) parent_os_pid,
        count(distinct p.parentpid) num_parent_os_pid,
        any_value(case when p.processpath = '' then null else p.processpath end) process_path,
        count(distinct p.processpath) num_process_path,
        any_value(case when p.filemd5 = '' then null else p.filemd5 end) file_md5,
        count(distinct p.filemd5) num_file_md5,
        any_value(case when p.filesha2 = '' then null else p.filesha2 end) file_sha2,
        count(distinct p.filesha2) num_file_sha2,
        min(case when upper(p.activitytype) in ('START', 'REFRESH') then {{ win32_to_epoch_expr('cast(p.eventtime as bigint)') }} else null end) process_started_seconds,
        min(case when upper(p.activitytype) in ('START', 'REFRESH') then {{ win32_to_timestamp_expr('cast(p.eventtime as bigint)') }} else null end) process_started,
        {{ win32_to_timestamp_expr('min(cast(p.eventtime as bigint))') }} first_seen,
        {{ win32_to_timestamp_expr('max(cast(p.eventtime as bigint))') }} last_seen,
        sum(case when upper(p.activitytype) in ('START', 'REFRESH') then 1 else 0 end) num_process_start,
        max(case when upper(p.activitytype) in ('STOP') then {{ win32_to_epoch_expr('cast(p.eventtime as bigint)') }} else null end) process_stop_seconds,
        max(case when upper(p.activitytype) in ('STOP') then {{ win32_to_timestamp_expr('cast(p.eventtime as bigint)') }} else null end) process_term,
        max(cpucyclecount) cpu_cycle_count,
        max(cpuutilization) cpu_utilization,
        max(commitcharge) commit_charge,
        max(commitpeak) commit_peak,
        max(readoperationcount) read_operation_count,
        max(writeoperationcount) write_operation_count,
        max(readtransferkilobytes) read_transfer_kilobytes,
        max(writetransferkilobytes) write_transfer_kilobytes,
        max(hardfaultcount) hard_fault_count,
        max(tokenelevationtype) token_elevation_type,
        max(exitcode) exit_code,
        sum(case when upper(p.activitytype) = 'STOP' then 1 else 0 end) num_process_stop
    from {{ ref('stg_raw_process') }} p
    group by all
){% if entity_mode %}, enriched as (
    select p.* replace (
        e.parent_pid_hash as parent_pid_hash,
        e.parent_os_pid as parent_os_pid,
        coalesce(e.process_name, p.process_name) as process_name,
        coalesce(e.process_path, p.process_path) as process_path,
        coalesce(e.user_name, p.user_name) as user_name,
        coalesce(e.args, p.args) as args
    ), e.parent_status, e.identity_conflict, e.metadata_conflict, e.from_store as entity_enriched
    from process_base p left join {{ ref('process_entities') }} e
        on upper(trim(p.hostname))=e.hostname and p.pid_hash=e.pid_hash
){% endif %}, with_filename as (
    select
        *,
        case
            when process_path is null then process_name
            when substring(process_path, -1) = '\\' then concat(process_path, process_name)
            else process_path
        end filename
    from {% if entity_mode %}enriched{% else %}process_base{% endif %}
), with_file_id as (
    select
        *,
        case when filename is not null then md5(concat_ws('||', hostname, lower(filename))) else null end file_id
    from with_filename
)
select * from with_file_id
