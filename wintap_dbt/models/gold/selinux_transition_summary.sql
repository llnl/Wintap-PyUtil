select
    a.hostname,
    a.attributed_pid_hash as pid_hash,
    a.raw_pid_hash,
    a.pid,
    a.raw_process_name,
    a.attributed_process_name,
    a.attribution_status,
    s.old_sid,
    s.new_sid,
    s.old_context,
    s.new_context,
    s.exe_file,
    s.policy_epoch,
    count(*) as observation_rows,
    sum(a.event_count) as event_count,
    min(s.first_seen) as first_seen,
    max(s.last_seen) as last_seen
from {{ ref('selinux_transition') }} s
join {{ ref('selinux_attribution') }} a on a.event_id = s.event_id
group by all
