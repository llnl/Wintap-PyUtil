{# Probe the actual engine/settings used by the build, without scanning all data. #}
{% macro validate_raw_sensor_source() -%}
    {%- if not execute -%}
        {{ return('') }}
    {%- endif -%}
    {% do configure_raw_sensor_access() %}
    {% do validate_process_entities() %}
    {%- set source = raw_sensor_dataset_path() ~ '/raw_sensor + ' ~ compacted_root_path() -%}
    {% do log('Checking raw source: ' ~ source ~ ' window ' ~ var('start_day') ~ '/' ~ start_hour_pk() ~ ' through ' ~ var('end_day') ~ '/' ~ end_hour_pk(), info=true) %}
    {%- set ns = namespace(sample=none) -%}
    {# Keep event names before the first wildcard so S3 LIST stays partition-scoped.
       Include all bronze inputs/aliases, but do not require any individual event.
       Recursive partition globs also include nested protoPK directories. #}
    {%- set events = ['raw_host', 'raw_process', 'raw_process_file',
        'raw_process_conn_incr', 'raw_macip', 'raw_macip_sensor',
        'raw_process_registry', 'raw_registry', 'raw_imageload',
        'raw_image_load', 'raw_selinuxavc', 'raw_selinuxinteraction',
        'raw_selinuxtransition', 'pidstat'] -%}
    {%- for event_type in events -%}
        {%- if ns.sample is none -%}
            {%- for glob_path in raw_sensor_partition_globs(event_type) -%}
                {%- if ns.sample is none -%}
                    {%- set escaped_glob = glob_path | replace("'", "''") -%}
                    {%- set files = run_query("select file from glob('" ~ escaped_glob ~ "') limit 1") -%}
                    {%- if files is not none and files | length > 0 -%}
                        {% set ns.sample = files.columns[0].values()[0] %}
                    {%- endif -%}
                {%- endif -%}
            {%- endfor -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if ns.sample is none -%}
        {{ exceptions.raise_compiler_error('No input found: no supported raw_sensor event Parquet files in the requested day/hour window at ' ~ source ~ '. Check the dataset root and stored dayPK/hourPK partitions.') }}
    {%- endif -%}
    {% do log('Checking Parquet read access: ' ~ ns.sample, info=true) %}
    {%- set escaped_sample = ns.sample | replace("'", "''") -%}
    {% do run_query("describe select * from parquet_scan('" ~ escaped_sample ~ "', hive_partitioning=false)") %}
    {% do log('Raw source check passed (listing and one Parquet schema; full validation occurs during build).', info=true) %}
    {{ return('') }}
{%- endmacro %}
