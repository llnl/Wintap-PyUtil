{# Return the canonical raw_sensor directory for one event type. #}
{% macro raw_event_path(event_type) -%}
    {{ raw_sensor_path(event_type) }}
{%- endmacro %}

{# Report whether the requested raw event has any files in the selected window. #}
{% macro raw_event_exists(event_type) -%}
    {%- if not execute -%}
        {{ return(true) }}
    {%- endif -%}
    {{ return(matching_raw_sensor_partition_globs(event_type) | length > 0) }}
{%- endmacro %}

{# Return the first alias that resolves to existing raw parquet. #}
{% macro first_existing_raw_event(event_types) -%}
    {%- for event_type in event_types -%}
        {%- if raw_event_exists(event_type) -%}
            {{ return(event_type) }}
        {%- endif -%}
    {%- endfor -%}
    {{ return(none) }}
{%- endmacro %}

{# Scan the first available raw event from a list of historical aliases. #}
{% macro raw_scan_for(event_types) -%}
    {%- set event_type = first_existing_raw_event(event_types) -%}
    {%- if event_type is none -%}
        {{ exceptions.raise_compiler_error('No raw_sensor event parquet files found for aliases: ' ~ event_types | join(', ')) }}
    {%- endif -%}
    {{ parquet_relation(event_type) }}
{%- endmacro %}

{# Return the columns from the first available raw event alias. #}
{% macro raw_columns(event_types) -%}
    {%- if not execute -%}
        {{ return([]) }}
    {%- endif -%}
    {%- set ns = namespace(event_type=none, globs=[]) -%}
    {%- for candidate in event_types -%}
        {%- if ns.event_type is none -%}
            {%- set candidate_globs = matching_raw_sensor_partition_globs(candidate) -%}
            {%- if candidate_globs | length > 0 -%}
                {%- set ns.event_type = candidate -%}
                {%- set ns.globs = candidate_globs -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
    {%- if ns.event_type is none -%}
        {{ return([]) }}
    {%- endif -%}
    {%- set description = run_query(
        'describe select * from parquet_scan(' ~ raw_globs_sql(ns.globs) ~ ', hive_partitioning=1, union_by_name=true)'
    ) -%}
    {%- set columns = [] -%}
    {%- for row in description.rows -%}
        {% do columns.append(row[0] | lower) %}
    {%- endfor -%}
    {{ return(columns) }}
{%- endmacro %}

{# Probe whether a column exists in the first available raw event alias. #}
{% macro raw_column_exists(event_types, column_name) -%}
    {%- if not execute -%}
        {{ return(true) }}
    {%- endif -%}
    {{ return(column_name | lower in raw_columns(event_types)) }}
{%- endmacro %}
