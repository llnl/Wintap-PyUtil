select 'host' table_name, count(*) num_rows from {{ ref('host') }}
union all select 'host_ip', count(*) from {{ ref('host_ip') }}
union all select 'process', count(*) from {{ ref('process') }}
union all select 'process_conn_incr', count(*) from {{ ref('process_conn_incr') }}
union all select 'process_net_conn', count(*) from {{ ref('process_net_conn') }}
union all select 'process_file', count(*) from {{ ref('process_file') }}
union all select 'process_registry', count(*) from {{ ref('process_registry') }}
union all select 'process_image_load', count(*) from {{ ref('process_image_load') }}
union all select 'process_path', count(*) from {{ ref('process_path') }}
union all select 'process_summary', count(*) from {{ ref('process_summary') }}
union all select 'process_uber_summary', count(*) from {{ ref('process_uber_summary') }}
union all select 'pidstat_metrics', count(*) from {{ ref('pidstat_metrics') }}
union all select 'pidstat_process_summary', count(*) from {{ ref('pidstat_process_summary') }}
union all select 'telemetry_event_summary', count(*) from {{ ref('telemetry_event_summary') }}
union all select 'attribution_quality_summary', count(*) from {{ ref('attribution_quality_summary') }}
order by table_name
