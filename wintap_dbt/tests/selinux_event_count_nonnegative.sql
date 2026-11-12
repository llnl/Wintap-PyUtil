select event_id, event_family, event_count
from {{ ref('selinux_avc') }}
where event_count < 0
union all
select event_id, event_family, event_count
from {{ ref('selinux_interaction') }}
where event_count < 0
union all
select event_id, event_family, event_count
from {{ ref('selinux_transition') }}
where event_count < 0
