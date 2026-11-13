{# Return the general dataset root used for outputs such as the DuckDB database. #}
{% macro dataset_path() -%}
    {{ var('dataset') }}
{%- endmacro %}

{# Return the dataset root that contains canonical raw_sensor parquet input. #}
{% macro raw_sensor_dataset_path() -%}
    {{ var('raw_sensor_dataset').rstrip('/') }}
{%- endmacro %}

{# Build the canonical raw_sensor directory for one event type. #}
{% macro raw_sensor_path(event_type) -%}
    {{ raw_sensor_dataset_path() }}/raw_sensor/{{ event_type }}
{%- endmacro %}

{# Direct compacted event root; default is a sibling of raw_sensor. #}
{% macro compacted_root_path() -%}
    {{ return(var('compacted_root', '').rstrip('/') or raw_sensor_dataset_path() ~ '/compacted') }}
{%- endmacro %}

{% macro raw_event_input_paths(event_type) -%}
    {{ return([raw_sensor_path(event_type), compacted_root_path() ~ '/' ~ event_type]) }}
{%- endmacro %}

{# Build a recursive parquet glob for one raw_sensor event type. #}
{% macro raw_sensor_glob(event_type) -%}
    {{ raw_sensor_path(event_type) }}/**/*.parquet
{%- endmacro %}

{# Expand the configured inclusive day window into dayPK strings. #}
{% macro requested_day_pks() -%}
    {%- set start_day = modules.datetime.datetime.strptime(var('start_day') | string, '%Y%m%d') -%}
    {%- set end_day = modules.datetime.datetime.strptime(var('end_day') | string, '%Y%m%d') -%}
    {%- if end_day < start_day -%}
        {{ exceptions.raise_compiler_error('WINTAP_DBT_END_DAY must be greater than or equal to WINTAP_DBT_START_DAY') }}
    {%- endif -%}
    {%- set ns = namespace(current_day=start_day, day_pks=[]) -%}
    {%- for _ in range((end_day - start_day).days + 1) -%}
        {% do ns.day_pks.append(ns.current_day.strftime('%Y%m%d')) %}
        {% set ns.current_day = ns.current_day + modules.datetime.timedelta(days=1) %}
    {%- endfor -%}
    {{ return(ns.day_pks) }}
{%- endmacro %}

{# Normalize an optional hour env var to HH format and enforce 00-23. #}
{% macro normalized_hour(hour_value, default_hour) -%}
    {%- set raw_hour = hour_value | string | trim -%}
    {%- if raw_hour == '' -%}
        {{ return(default_hour) }}
    {%- endif -%}
    {%- if not raw_hour.isdigit() or raw_hour | length > 2 -%}
        {{ exceptions.raise_compiler_error('Hour values must be numeric hours between 00 and 23') }}
    {%- endif -%}
    {%- set hour_int = raw_hour | int -%}
    {%- if hour_int < 0 or hour_int > 23 -%}
        {{ exceptions.raise_compiler_error('Hour values must be between 00 and 23') }}
    {%- endif -%}
    {{ return('%02d' % hour_int) }}
{%- endmacro %}

{# Report whether the build is constrained to an hour-level window. #}
{% macro hour_window_enabled() -%}
    {{ return((var('start_hour') | string | trim) != '' or (var('end_hour') | string | trim) != '') }}
{%- endmacro %}

{# Return the inclusive lower hour bound for partition filtering. #}
{% macro start_hour_pk() -%}
    {{ return(normalized_hour(var('start_hour'), '00')) }}
{%- endmacro %}

{# Return the inclusive upper hour bound for partition filtering. #}
{% macro end_hour_pk() -%}
    {{ return(normalized_hour(var('end_hour'), '23')) }}
{%- endmacro %}

{# Build partition globs for one physical event directory. #}
{% macro partition_globs_for_path(event_path) -%}
    {%- set ns = namespace(globs=[]) -%}
    {%- if not hour_window_enabled() -%}
        {%- for day_pk in requested_day_pks() -%}
            {% do ns.globs.append(event_path ~ '/dayPK=' ~ day_pk ~ '/**/*.parquet') %}
        {%- endfor -%}
        {{ return(ns.globs) }}
    {%- endif -%}

    {%- set start_day = var('start_day') | string -%}
    {%- set end_day = var('end_day') | string -%}
    {%- set start_hour = start_hour_pk() -%}
    {%- set end_hour = end_hour_pk() -%}
    {%- if (end_day ~ end_hour) < (start_day ~ start_hour) -%}
        {{ exceptions.raise_compiler_error('WINTAP_DBT_END_DAY/WINTAP_DBT_END_HOUR must be greater than or equal to WINTAP_DBT_START_DAY/WINTAP_DBT_START_HOUR') }}
    {%- endif -%}

    {%- for day_pk in requested_day_pks() -%}
        {%- set day_start_hour = start_hour if day_pk == start_day else '00' -%}
        {%- set day_end_hour = end_hour if day_pk == end_day else '23' -%}
        {%- for hour_int in range(day_start_hour | int, (day_end_hour | int) + 1) -%}
            {% do ns.globs.append(event_path ~ '/dayPK=' ~ day_pk ~ '/hourPK=' ~ ('%02d' % hour_int) ~ '/**/*.parquet') %}
        {%- endfor -%}
    {%- endfor -%}
    {{ return(ns.globs) }}
{%- endmacro %}

{# Scan both stores with UNION ALL semantics; no overlap suppression. #}
{% macro raw_sensor_partition_globs(event_type) -%}
    {%- set ns = namespace(globs=[]) -%}
    {%- for event_path in raw_event_input_paths(event_type) -%}
        {% do ns.globs.extend(partition_globs_for_path(event_path)) %}
    {%- endfor -%}
    {{ return(ns.globs) }}
{%- endmacro %}

{# Keep only requested globs that currently contain at least one file. #}
{% macro matching_raw_sensor_partition_globs(event_type) -%}
    {%- set requested_globs = raw_sensor_partition_globs(event_type) -%}
    {%- if not execute -%}
        {{ return(requested_globs) }}
    {%- endif -%}

    {%- set ns = namespace(existing_globs=[]) -%}
    {%- for glob_path in requested_globs -%}
        {%- set sql -%}
            select count(*) as num_files
            from glob('{{ glob_path | replace("'", "''") }}')
        {%- endset -%}
        {%- set results = run_query(sql) -%}
        {%- if results is not none and results|length > 0 and results.columns[0].values()[0] > 0 -%}
            {% do ns.existing_globs.append(glob_path) %}
        {%- endif -%}
    {%- endfor -%}

    {{ return(ns.existing_globs) }}
{%- endmacro %}

{# Prefer existing globs at runtime, but fall back to requested globs for compilation. #}
{% macro raw_sensor_partition_globs_for_scan(event_type) -%}
    {%- set matching_globs = matching_raw_sensor_partition_globs(event_type) -%}
    {%- if matching_globs | length > 0 -%}
        {{ return(matching_globs) }}
    {%- endif -%}
    {{ return(raw_sensor_partition_globs(event_type)) }}
{%- endmacro %}

{# Render one or more globs as SQL for DuckDB parquet_scan/glob calls. #}
{% macro raw_globs_sql(globs) -%}
    {%- if globs | length == 1 -%}
        '{{ globs[0] | replace("'", "''") }}'
    {%- else -%}
        [
        {%- for glob_path in globs -%}
            '{{ glob_path | replace("'", "''") }}'{% if not loop.last %}, {% endif %}
        {%- endfor -%}
        ]
    {%- endif -%}
{%- endmacro %}

{# Render the final SQL glob expression used to scan one raw event type. #}
{% macro raw_sensor_partition_globs_sql(event_type) -%}
    {{ raw_globs_sql(raw_sensor_partition_globs_for_scan(event_type)) }}
{%- endmacro %}

{# Build a day-only partition predicate for already-scanned raw rows. #}
{% macro day_filter(alias='') -%}
    {%- set prefix = alias ~ '.' if alias else '' -%}
    cast({{ prefix }}dayPK as bigint) between {{ var('start_day') }} and {{ var('end_day') }}
{%- endmacro %}

{# Build the inclusive day/hour predicate used after parquet_scan. #}
{% macro partition_filter(alias='') -%}
    {%- if not hour_window_enabled() -%}
        {{ day_filter(alias) }}
    {%- else -%}
        {%- set prefix = alias ~ '.' if alias else '' -%}
        (cast({{ prefix }}dayPK as bigint) * 100 + cast({{ prefix }}hourPK as bigint)) between {{ (var('start_day') | int) * 100 + (start_hour_pk() | int) }} and {{ (var('end_day') | int) * 100 + (end_hour_pk() | int) }}
    {%- endif -%}
{%- endmacro %}
