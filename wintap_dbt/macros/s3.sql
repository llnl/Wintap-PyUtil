{# Create the DuckDB S3 secret when raw_sensor input is read from S3. #}
{% macro configure_raw_sensor_access() -%}
    {%- set raw_sensor_dataset = var('raw_sensor_dataset') -%}
    {%- if not execute or not raw_sensor_dataset.startswith('s3://') -%}
        {{ return('') }}
    {%- endif -%}

    {%- set aws_endpoint_url = env_var('AWS_ENDPOINT_URL', '') -%}
    {%- set duckdb_endpoint = aws_endpoint_url | replace('https://', '') | replace('http://', '') -%}
    {%- set duckdb_region = env_var('AWS_DEFAULT_REGION', env_var('AWS_REGION', '')) -%}
    {%- set aws_profile = env_var('AWS_PROFILE', '') -%}
    {%- set duckdb_use_ssl = env_var('DUCKDB_USE_SSL', 'true') -%}
    {%- set use_ssl = duckdb_use_ssl | lower in ['1', 'true', 'yes', 'on'] -%}
    {%- set clauses = [
        'TYPE S3',
        'PROVIDER CREDENTIAL_CHAIN'
    ] -%}

    {%- if aws_profile -%}
        {%- set escaped_aws_profile = aws_profile | replace("'", "''") -%}
        {% do clauses.append("CHAIN 'config'") %}
        {% do clauses.append("PROFILE '" ~ escaped_aws_profile ~ "'") %}
    {%- endif -%}
    {%- if duckdb_region -%}
        {% do clauses.append("REGION '" ~ duckdb_region ~ "'") %}
    {%- endif -%}
    {%- if duckdb_endpoint -%}
        {% do clauses.append("ENDPOINT '" ~ duckdb_endpoint ~ "'") %}
        {% do clauses.append("URL_STYLE 'path'") %}
    {%- endif -%}
    {% do clauses.append('USE_SSL ' ~ ('true' if use_ssl else 'false')) %}

    {%- set sql -%}
        create or replace secret wintap_raw_sensor (
            {{ clauses | join(',\n            ') }}
        )
    {%- endset -%}

    {% do run_query(sql) %}
    {{ return('') }}
{%- endmacro %}
