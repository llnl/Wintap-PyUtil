-- An incomplete five-tuple cannot produce a stable CONN_ID; fail visibly rather
-- than allowing null keys into downstream network models.
select *
from {{ ref('process_conn_incr') }}
where conn_id is null
