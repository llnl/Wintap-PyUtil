{{ config(enabled=env_var('WINTAP_DBT_PROCESS_ENTITY_ROOT', '') != '') }}
-- Conflicts/missing ancestors are reported, not failed or invented. Key duplication is invalid.
select hostname, pid_hash from {{ ref('process_context') }}
group by hostname, pid_hash having count(*) != 1
union all
select hostname, pid_hash from {{ ref('process_entities') }}
group by hostname, pid_hash having count(*) != 1
