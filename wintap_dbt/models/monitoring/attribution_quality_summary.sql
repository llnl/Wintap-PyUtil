-- Relationship and identity-quality telemetry. This is intentionally a
-- monitoring view rather than a failing DBT test: historical captures can
-- contain known orphans, and remediation is evaluated by their measured rate
-- and composition rather than silently excluding them from analysis.
with attribution_quality as (
    select
        'process_file' as child_relation,
        count(*) as child_rows,
        count(distinct c.pid_hash) as child_pid_hashes,
        count(*) filter (where p.pid_hash is null) as orphan_rows,
        count(distinct c.pid_hash) filter (where p.pid_hash is null) as orphan_pid_hashes,
        count(*) filter (where lower(coalesce(c.process_name, '')) = 'unknown') as unknown_process_name_rows,
        count(distinct c.pid_hash) filter (where lower(coalesce(c.process_name, '')) = 'unknown') as unknown_process_name_hashes
    from {{ ref('process_file') }} c
    left join {{ ref('process') }} p on c.pid_hash = p.pid_hash

    union all

    select
        'process_conn_incr',
        count(*),
        count(distinct c.pid_hash),
        count(*) filter (where p.pid_hash is null),
        count(distinct c.pid_hash) filter (where p.pid_hash is null),
        count(*) filter (where lower(coalesce(c.process_name, '')) = 'unknown'),
        count(distinct c.pid_hash) filter (where lower(coalesce(c.process_name, '')) = 'unknown')
    from {{ ref('process_conn_incr') }} c
    left join {{ ref('process') }} p on c.pid_hash = p.pid_hash

    union all

    select
        'process_net_conn',
        count(*),
        count(distinct c.pid_hash),
        count(*) filter (where p.pid_hash is null),
        count(distinct c.pid_hash) filter (where p.pid_hash is null),
        count(*) filter (where lower(coalesce(c.process_name, '')) = 'unknown'),
        count(distinct c.pid_hash) filter (where lower(coalesce(c.process_name, '')) = 'unknown')
    from {{ ref('process_net_conn') }} c
    left join {{ ref('process') }} p on c.pid_hash = p.pid_hash
)
select
    *,
    orphan_rows::double / nullif(child_rows, 0) as orphan_row_rate,
    unknown_process_name_rows::double / nullif(child_rows, 0) as unknown_process_name_row_rate
from attribution_quality
order by child_relation
