-- Wintap is the sole CONN_ID producer; fail visibly when producer identity is
-- absent rather than allowing null keys into downstream network models.
select *
from {{ ref('process_conn_incr') }}
where conn_id is null
