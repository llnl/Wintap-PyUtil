select event_id, event_family, first_seen, last_seen, event_time
from {{ ref('selinux_avc') }}
where first_seen > last_seen
   -- EventTime is serialized as Unix whole seconds from FirstSeen, while
   -- FirstSeen/LastSeen retain FILETIME sub-second precision.
   or event_time < first_seen - interval '1 second'
   or event_time > last_seen
union all
select event_id, event_family, first_seen, last_seen, event_time
from {{ ref('selinux_interaction') }}
where first_seen > last_seen
   or event_time < first_seen - interval '1 second'
   or event_time > last_seen
union all
select event_id, event_family, first_seen, last_seen, event_time
from {{ ref('selinux_transition') }}
where first_seen > last_seen
   or event_time < first_seen - interval '1 second'
   or event_time > last_seen
