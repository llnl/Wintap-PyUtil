select
    event_family,
    attribution_status,
    hostname,
    attributed_pid_hash as pid_hash,
    raw_pid_hash,
    pid,
    raw_process_name,
    attributed_process_name,
    count(*) as observation_rows,
    sum(event_count) as event_count,
    min(first_seen) as first_seen,
    max(last_seen) as last_seen
from {{ ref('selinux_attribution') }}
group by all
