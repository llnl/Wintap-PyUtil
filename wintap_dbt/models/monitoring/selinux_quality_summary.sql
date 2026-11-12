select
    event_family,
    count(*) as normalized_rows,
    sum(num_raw_rows) as raw_rows,
    sum(num_raw_rows - 1) as duplicate_rows,
    sum(event_count) as event_count,
    count(*) filter (where attribution_status = 'exact') as exact_rows,
    count(*) filter (where attribution_status = 'unique_temporal') as unique_temporal_rows,
    count(*) filter (where attribution_status = 'ambiguous') as ambiguous_rows,
    count(*) filter (where attribution_status = 'unmatched') as unmatched_rows,
    count(*) filter (where candidate_count > 1) as rows_with_multiple_candidates,
    count(*) filter (where s_context is not null and trim(s_context) = '') as blank_source_context_rows,
    count(*) filter (where t_context is not null and trim(t_context) = '') as blank_target_context_rows,
    min(first_seen) as first_seen,
    max(last_seen) as last_seen
from {{ ref('selinux_event_detail') }}
group by event_family
