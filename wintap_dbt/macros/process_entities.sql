{% macro process_root_pids() -%}
    {% set values = env_var('WINTAP_DBT_PROCESS_ROOT_PIDS', '0,1,4').split(',') %}
    {% for value in values %}
        {% if not value.strip().isdigit() %}{{ exceptions.raise_compiler_error('PROCESS_ROOT_PIDS must be nonnegative comma-separated PIDs') }}{% endif %}
    {% endfor %}
    {{ return(values | map('trim') | join(',')) }}
{%- endmacro %}

{% macro valid_process_key(key, host) -%}
    ({{ key }} is not null and upper(trim({{ key }})) not in (
        '', 'UNKNOWN', 'NA', '-1', '0', '00000000000000000000000000000000',
        upper(md5('N`Process`' || upper(md5('Process_ID`llnl`0`' || upper(trim({{ host }})) || '`-1`')) || '`'))
    ))
{%- endmacro %}

{% macro resolve_process_parents(candidates) -%}
    with classified as (
        select *,
            {{ valid_process_key('parent_hash', 'hostname') }}
            and (parent_pid is null or parent_pid >= 0) as real_parent
        from ({{ candidates }})
    )
    select hostname, pid_hash,
        case when count(distinct parent_hash) filter(where real_parent) = 1
                  and count(distinct parent_pid) filter(where real_parent) <= 1
             then min(parent_hash) filter(where real_parent) end parent_pid_hash,
        case when count(distinct parent_hash) filter(where real_parent) = 1
                  and count(distinct parent_pid) filter(where real_parent) <= 1
             then min(parent_pid) filter(where real_parent) end parent_os_pid,
        case when count(distinct parent_hash) filter(where real_parent) > 1
                   or count(distinct parent_pid) filter(where real_parent) > 1 then 'conflicted'
             when count(*) filter(where real_parent) > 0 then 'resolved'
             when count(*) filter(where parent_hash is not null or parent_pid < 0) > 0 then 'sentinel'
             else 'missing' end parent_status
    from classified group by hostname, pid_hash
{%- endmacro %}

{% macro process_entity_files() -%}
    {% set root = env_var('WINTAP_DBT_PROCESS_ENTITY_ROOT', '').rstrip('/') %}
    {% if not root %}{{ return([]) }}{% endif %}
    {% set hosts = env_var('WINTAP_DBT_PROCESS_ENTITY_HOSTS', '').split(',') %}
    {% set namespace = env_var('WINTAP_DBT_PROCESS_ENTITY_NAMESPACE', '') %}
    {% if not namespace or not hosts or '' in hosts %}
        {{ exceptions.raise_compiler_error('Entity root requires PROCESS_ENTITY_HOSTS and PROCESS_ENTITY_NAMESPACE') }}
    {% endif %}
    {% set files = [] %}
    {% for host in hosts %}
        {% if execute %}
            {% set hash = run_query("select sha256(upper(trim('" ~ host | replace("'", "''") ~ "')))").columns[0].values()[0] %}
            {% do files.append(root ~ '/hostPK=' ~ hash ~ '/entities.parquet') %}
        {% else %}
            {% do files.append(root ~ '/hostPK=*/entities.parquet') %}
        {% endif %}
    {% endfor %}
    {{ return(files | unique | list) }}
{%- endmacro %}

{% macro processing_metrics_root() -%}
    {{ return(env_var('WINTAP_DBT_PROCESSING_METRICS_ROOT', raw_sensor_dataset_path() ~ '/processing_metrics').rstrip('/')) }}
{%- endmacro %}

{% macro processing_metrics_glob() -%}
    {{ return(processing_metrics_root() ~ '/**/*.parquet') }}
{%- endmacro %}

{% macro validate_processing_metrics(bootstrap_starts) -%}
    {% if not execute or not env_var('WINTAP_DBT_PROCESS_ENTITY_ROOT', '') %}{{ return('') }}{% endif %}
    {% if not raw_event_exists('raw_process') %}
        {{ exceptions.raise_compiler_error('Durable entity coverage requires raw_process input in the DBT window') }}
    {% endif %}
    {% set metric_glob = processing_metrics_glob() | replace("'", "''") %}
    {% set files = run_query("select count(*) from glob('" ~ metric_glob ~ "')").columns[0].values()[0] %}
    {% if files == 0 %}
        {{ exceptions.raise_compiler_error('Processing metrics are missing; durable entity coverage is stale or has not been initialized') }}
    {% endif %}
    {% set required_sql %}
        with required as (
            select distinct
                try_strptime(cast(dayPK as varchar) || lpad(cast(hourPK as varchar), 2, '0'), '%Y%m%d%H') as hour_start
            from {{ parquet_relation('raw_process') }}
            where {{ partition_filter() }}
        ), metrics as (
            select
                try_strptime(hour_start, '%Y-%m-%dT%H:00:00Z') as hour_start,
                status, metric_version, entity_mode, entity_namespace
            from read_parquet('{{ metric_glob }}', hive_partitioning=false, union_by_name=true)
        ), joined as (
            select r.hour_start,
                   count(m.hour_start) as metric_rows,
                   count(m.hour_start) filter(where m.status not in ('completed', 'completed_with_deletion_errors')) as bad_rows,
                   count(m.hour_start) filter(where m.metric_version != '1' or not m.entity_mode or m.entity_namespace != '{{ env_var('WINTAP_DBT_PROCESS_ENTITY_NAMESPACE') | replace("'", "''") }}') as invalid_rows
            from required r left join metrics m using(hour_start)
            group by r.hour_start
        )
        select
            count(*) as required_hours,
            count(*) filter(where metric_rows = 0) as missing_hours,
            count(*) filter(where metric_rows > 1) as duplicate_hours,
            coalesce(sum(bad_rows), 0) as bad_rows,
            coalesce(sum(invalid_rows), 0) as invalid_rows
        from joined
    {% endset %}
    {% set result = run_query(required_sql).rows[0] %}
    {% if result[1] != 0 or result[2] != 0 or result[3] != 0 or result[4] != 0 %}
        {{ exceptions.raise_compiler_error('Processing metrics do not prove complete durable coverage for every raw_process hour in the DBT window') }}
    {% endif %}
{%- endmacro %}

{% macro validate_process_entities() -%}
    {% if not execute or not env_var('WINTAP_DBT_PROCESS_ENTITY_ROOT', '') %}{{ return('') }}{% endif %}
    {% set files = process_entity_files() %}
    {% set hosts = env_var('WINTAP_DBT_PROCESS_ENTITY_HOSTS').split(',') %}
    {% set namespace = env_var('WINTAP_DBT_PROCESS_ENTITY_NAMESPACE') %}
    {% set schema = {
        'hostname': 'VARCHAR', 'pid_hash': 'VARCHAR', 'os_pid': 'BIGINT',
        'parent_pid_hash': 'VARCHAR', 'parent_os_pid': 'BIGINT', 'parent_status': 'VARCHAR',
        'identity_status': 'VARCHAR', 'process_name': 'VARCHAR', 'process_path': 'VARCHAR',
        'user_name': 'VARCHAR', 'args': 'VARCHAR', 'agent_id': 'VARCHAR',
        'birth_filetime': 'BIGINT', 'stop_filetime': 'BIGINT', 'lifetime_status': 'VARCHAR',
        'seen_start': 'BOOLEAN', 'seen_refresh': 'BOOLEAN', 'seen_stop': 'BOOLEAN',
        'evidence_json': 'VARCHAR', 'first_partition': 'VARCHAR', 'last_partition': 'VARCHAR'
    } %}
    {% set bootstrap_starts = [] %}
    {% for file in files %}
        {% set quoted = "'" ~ file | replace("'", "''") ~ "'" %}
        {% set actual = run_query('describe select * from read_parquet(' ~ quoted ~ ', hive_partitioning=false)') %}
        {% if actual | length != schema | length %}{{ exceptions.raise_compiler_error('Entity schema mismatch') }}{% endif %}
        {% for row in actual.rows %}
            {% if schema.get(row[0]) != row[1] %}{{ exceptions.raise_compiler_error('Entity column/type mismatch: ' ~ row[0]) }}{% endif %}
        {% endfor %}
        {% set metadata = {} %}
        {% for row in run_query('select decode(key), decode(value) from parquet_kv_metadata(' ~ quoted ~ ')').rows %}
            {% do metadata.update({row[0]: row[1]}) %}
        {% endfor %}
        {% if metadata | length != 5 or metadata.get('entity_version') != '1' or metadata.get('identity_namespace') != namespace or metadata.get('hostname') not in hosts | map('trim') | map('upper') | list or metadata.get('bootstrap_start', '') == '' or metadata.get('time_basis') != 'producer_filetime;publication_partition' %}
            {{ exceptions.raise_compiler_error('Entity metadata/namespace/host mismatch') }}
        {% endif %}
        {% do bootstrap_starts.append(metadata['bootstrap_start']) %}
        {% set hash = run_query("select sha256('" ~ metadata['hostname'] | replace("'", "''") ~ "')").columns[0].values()[0] %}
        {% if '/hostPK=' ~ hash ~ '/entities.parquet' not in file %}{{ exceptions.raise_compiler_error('Entity host path mismatch') }}{% endif %}
        {% set check %}
            select count(*) from read_parquet({{ quoted }}, hive_partitioning=false)
            where hostname is null or hostname != '{{ metadata['hostname'] | replace("'", "''") }}'
               or not {{ valid_process_key('pid_hash', 'hostname') }} or not json_valid(evidence_json)
               or first_partition is null or last_partition is null or first_partition > last_partition
               or coalesce(parent_status, '') not in ('resolved', 'missing', 'sentinel', 'conflicted')
               or coalesce(identity_status, '') not in ('recorded', 'conflicted')
               or coalesce(lifetime_status, '') not in ('observed', 'exited', 'conflicted')
        {% endset %}
        {% if run_query(check).columns[0].values()[0] != 0 %}{{ exceptions.raise_compiler_error('Invalid entity rows') }}{% endif %}
        {% set check %}
            select count(*) - count(distinct pid_hash) from read_parquet({{ quoted }}, hive_partitioning=false)
        {% endset %}
        {% if run_query(check).columns[0].values()[0] != 0 %}{{ exceptions.raise_compiler_error('Duplicate entity keys') }}{% endif %}
        {% set check %}
            select count(*) from read_parquet({{ quoted }}, hive_partitioning=false)
            where false
            {% for field in ['pids', 'parents', 'births', 'stops', 'names', 'paths', 'users', 'args', 'agents', 'activities'] %}
               or coalesce(json_type(evidence_json, '$.{{ field }}'), '') != 'ARRAY'
            {% endfor %}
               or json_array_length(evidence_json, '$.pids')=0
               or json_array_length(evidence_json, '$.activities')=0
        {% endset %}
        {% if run_query(check).columns[0].values()[0] != 0 %}{{ exceptions.raise_compiler_error('Invalid entity evidence structure') }}{% endif %}
        {% set candidates %}
            select e.hostname, e.pid_hash, json_extract_string(j.value, '$[0]') parent_hash,
                   cast(json_extract(j.value, '$[1]') as bigint) parent_pid
            from read_parquet({{ quoted }}, hive_partitioning=false) e,
                 json_each(e.evidence_json, '$.parents') j
        {% endset %}
        {% set check %}
            with expected as ({{ resolve_process_parents(candidates) }})
            select count(*) from read_parquet({{ quoted }}, hive_partitioning=false) e
            left join expected p using(hostname, pid_hash)
            where p.parent_status is null or e.parent_status is distinct from p.parent_status
               or e.parent_pid_hash is distinct from p.parent_pid_hash
               or e.parent_os_pid is distinct from p.parent_os_pid
        {% endset %}
        {% if run_query(check).columns[0].values()[0] != 0 %}{{ exceptions.raise_compiler_error('Entity parent scalars disagree with evidence') }}{% endif %}
    {% endfor %}
    {% do validate_processing_metrics(bootstrap_starts) %}
    {{ return('') }}
{%- endmacro %}
