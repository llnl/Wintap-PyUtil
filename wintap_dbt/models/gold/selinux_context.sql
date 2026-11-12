with context_rows as (
    select event_family, 'source' as context_role, s_context as context,
        event_count, first_seen, last_seen, policy_epoch
    from {{ ref('selinux_avc') }}
    union all
    select event_family, 'target', t_context, event_count, first_seen, last_seen, policy_epoch
    from {{ ref('selinux_avc') }}
    union all
    select event_family, 'source', s_context, event_count, first_seen, last_seen, policy_epoch
    from {{ ref('selinux_interaction') }}
    union all
    select event_family, 'target', t_context, event_count, first_seen, last_seen, policy_epoch
    from {{ ref('selinux_interaction') }}
    union all
    select event_family, 'old', old_context, event_count, first_seen, last_seen, policy_epoch
    from {{ ref('selinux_transition') }}
    union all
    select event_family, 'new', new_context, event_count, first_seen, last_seen, policy_epoch
    from {{ ref('selinux_transition') }}
)
select
    event_family,
    context_role,
    context,
    policy_epoch,
    count(*) as observation_rows,
    sum(event_count) as event_count,
    min(first_seen) as first_seen,
    max(last_seen) as last_seen
from context_rows
group by all
