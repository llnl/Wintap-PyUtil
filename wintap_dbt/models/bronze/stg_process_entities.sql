{{ config(enabled=env_var('WINTAP_DBT_PROCESS_ENTITY_ROOT', '') != '', materialized='table') }}
{% for file in process_entity_files() %}
select e.*,
       json_array_length(evidence_json, '$.names') > 1
       or json_array_length(evidence_json, '$.paths') > 1
       or json_array_length(evidence_json, '$.users') > 1
       or json_array_length(evidence_json, '$.args') > 1 as metadata_conflict,
       '{{ env_var("WINTAP_DBT_PROCESS_ENTITY_NAMESPACE") | replace("'", "''") }}' identity_namespace,
       (select decode(value) from parquet_kv_metadata('{{ file | replace("'", "''") }}')
        where decode(key)='bootstrap_start') bootstrap_start
from read_parquet('{{ file | replace("'", "''") }}', hive_partitioning=false) e
{% if not loop.last %}union all{% endif %}
{% endfor %}
