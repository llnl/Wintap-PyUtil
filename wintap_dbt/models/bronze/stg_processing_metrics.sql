{{ config(enabled=env_var('WINTAP_DBT_PROCESS_ENTITY_ROOT', '') != '', materialized='table') }}

select *
from read_parquet(
    '{{ env_var("WINTAP_DBT_PROCESSING_METRICS_ROOT", raw_sensor_dataset_path() ~ "/processing_metrics") | replace("'", "''") }}/**/*.parquet',
    hive_partitioning=false,
    union_by_name=true
)
