{# Generation receipts are Parquet interchange, not a Python entity cache. #}
{% macro entity_authority_relation() -%}
    read_parquet('{{ env_var('WINTAP_DBT_PROCESS_ENTITY_ROOT').rstrip('/') | replace("'", "''") }}/_authority/authority.parquet', hive_partitioning=false)
{%- endmacro %}

{% macro entity_semantic_hash(file, columns) -%}
    with hashed as (
        select row_number() over(order by pid_hash)-1 ordinal,
               sha256(to_json(struct_pack({% for column in columns %}{{ column }} := {{ column }}{% if not loop.last %},{% endif %}{% endfor %}))) row_hash
        from read_parquet('{{ file | replace("'", "''") }}', hive_partitioning=false)
    ), chunks as (
        select ordinal//1024 chunk,
               sha256(string_agg(row_hash, '\n' order by ordinal)) chunk_hash
        from hashed group by chunk
    )
    select sha256(coalesce(string_agg(chunk_hash, '\n' order by chunk), '')) from chunks
{%- endmacro %}

{% macro validate_entity_authority(current_hashes, bootstraps) -%}
    {% set path = env_var('WINTAP_DBT_PROCESS_ENTITY_ROOT').rstrip('/') ~ '/_authority/authority.parquet' %}
    {% if run_query("select count(*) from glob('" ~ path | replace("'", "''") ~ "')").rows[0][0] != 1 %}
        {{ exceptions.raise_compiler_error('Entity authority missing; explicitly rebuild and cut over legacy state') }}
    {% endif %}
    {% set schema = {
        'authority_version': 'VARCHAR', 'generation': 'BIGINT', 'generation_id': 'VARCHAR',
        'parent_id': 'VARCHAR', 'status': 'VARCHAR', 'binding_json': 'VARCHAR',
        'bootstrap_starts': 'VARCHAR', 'before_hashes': 'VARCHAR', 'snapshot_hashes': 'VARCHAR',
        'semantic_hashes': 'VARCHAR', 'window_start': 'VARCHAR', 'window_end': 'VARCHAR',
        'input_fingerprint': 'VARCHAR'
    } %}
    {% set actual = run_query('describe select * from ' ~ entity_authority_relation()) %}
    {% if actual | length != schema | length %}{{ exceptions.raise_compiler_error('Entity authority schema mismatch') }}{% endif %}
    {% for row in actual.rows %}
        {% if schema.get(row[0]) != row[1] %}{{ exceptions.raise_compiler_error('Entity authority column/type mismatch') }}{% endif %}
    {% endfor %}
    {% set check %}
        select count(*) from {{ entity_authority_relation() }}
        where not coalesce(json_valid(binding_json), false)
        {% for field in ['bootstrap_starts', 'before_hashes', 'snapshot_hashes', 'semantic_hashes'] %}
           or not coalesce(json_valid({{ field }}), false)
        {% endfor %}
    {% endset %}
    {% if run_query(check).rows[0][0] != 0 %}{{ exceptions.raise_compiler_error('Entity authority JSON evidence invalid') }}{% endif %}
    {% set roots = {
        'raw_root': raw_sensor_dataset_path() ~ '/raw_sensor',
        'compacted_root': compacted_root_path(),
        'entity_root': env_var('WINTAP_DBT_PROCESS_ENTITY_ROOT').rstrip('/'),
        'metrics_root': processing_metrics_root(),
        'namespace': env_var('WINTAP_DBT_PROCESS_ENTITY_NAMESPACE'),
        'identity_policy': env_var('WINTAP_DBT_PROCESS_UNKNOWN_IDENTITY_POLICY', 'reject')
    } %}
    {% set check %}
        with chain as (
            select *, row_number() over(order by generation) ordinal,
                lag(generation_id) over(order by generation) previous_id,
                lag(snapshot_hashes) over(order by generation) previous_hashes,
                lag(bootstrap_starts) over(order by generation) previous_bootstraps,
                lag(binding_json) over(order by generation) previous_binding
            from {{ entity_authority_relation() }}
        )
        select count(*), count(distinct generation_id),
            count(*) filter(where authority_version is distinct from '1'
                or generation is distinct from ordinal
                or not coalesce(regexp_full_match(generation_id, '[a-f0-9]{64}'), false)
                or generation_id is distinct from sha256(to_json(struct_pack({% for field in schema if field not in ['status', 'generation_id'] %}{{ field }} := {{ field }}{% if not loop.last %},{% endif %}{% endfor %})))
                or parent_id is distinct from coalesce(previous_id, '')
                or status is distinct from 'committed'
                or (generation>1 and (before_hashes is distinct from previous_hashes
                    or bootstrap_starts is distinct from previous_bootstraps
                    or binding_json is distinct from previous_binding))
                or json_type(binding_json) is distinct from 'OBJECT'
                or (select count(*) from json_each(chain.binding_json)) <> 8
                or (select count(distinct key) from json_each(chain.binding_json)) <> 8
                {% for key, value in roots.items() %}
                or json_extract_string(binding_json, '$.{{ key }}') is distinct from '{{ value | replace("'", "''") }}'
                {% endfor %}
                or try_cast(json_extract_string(binding_json, '$.identity_max_ratio') as double) is distinct from cast('{{ env_var('WINTAP_DBT_PROCESS_UNKNOWN_IDENTITY_MAX_RATIO', '0.05') | replace("'", "''") }}' as double)
                or json_extract(binding_json, '$.hosts') is distinct from cast('{{ tojson(bootstraps.keys() | sort | list) | replace("'", "''") }}' as json)
                or not coalesce(regexp_full_match(input_fingerprint, '[a-f0-9]{64}'), false)
                or try_strptime(window_start, '%Y-%m-%dT%H:00:00Z') is null
                or try_strptime(window_end, '%Y-%m-%dT%H:00:00Z') is null
                or window_start>=window_end)
        from chain
    {% endset %}
    {% set result = run_query(check).rows[0] %}
    {% if result[0] == 0 or result[0] != result[1] or result[2] != 0 %}
        {{ exceptions.raise_compiler_error('Entity authority binding/lineage invalid or publication unresolved; Processing metrics require the same roots, hosts and identity policy/ceiling') }}
    {% endif %}
    {% for field in ['bootstrap_starts', 'before_hashes', 'snapshot_hashes', 'semantic_hashes'] %}
        {% set check %}
            select count(*) from {{ entity_authority_relation() }} a
            where json_type({{ field }}) is distinct from 'OBJECT'
               or (select count(*) from json_each(a.{{ field }})) <> {{ bootstraps | length }}
               or (select count(distinct key) from json_each(a.{{ field }})) <> {{ bootstraps | length }}
               or exists (select 1 from json_each(a.{{ field }}) j where
                    j.key not in ({% for host in bootstraps %}'{{ host | replace("'", "''") }}'{% if not loop.last %},{% endif %}{% endfor %})
                    {% if field == 'bootstrap_starts' %}
                    or j.type <> 'VARCHAR' or try_strptime(json_extract_string(j.value, '$'), '%Y-%m-%dT%H:00:00Z') is null
                    {% else %}
                    or not (j.type='VARCHAR' and regexp_full_match(json_extract_string(j.value, '$'), '[a-f0-9]{64}'))
                       {% if field == 'before_hashes' %}and not (a.generation=1 and j.type='NULL'){% endif %}
                    {% endif %})
               {% if field == 'before_hashes' %}
               or exists (select 1 from json_each(a.before_hashes) j where a.generation=1 and j.type<>'NULL')
               {% endif %}
        {% endset %}
        {% if run_query(check).rows[0][0] != 0 %}{{ exceptions.raise_compiler_error('Entity authority host evidence invalid') }}{% endif %}
    {% endfor %}
    {% set latest = run_query('select semantic_hashes, bootstrap_starts from ' ~ entity_authority_relation() ~ ' order by generation desc limit 1').rows[0] %}
    {% if fromjson(latest[0]) != current_hashes or fromjson(latest[1]) != bootstraps %}
        {{ exceptions.raise_compiler_error('Entity authority does not match current snapshot/provenance') }}
    {% endif %}
{%- endmacro %}
