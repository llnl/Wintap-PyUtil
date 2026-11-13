{% set entity_mode = env_var is defined and env_var('WINTAP_DBT_PROCESS_ENTITY_ROOT', '') != '' %}
select * from (
    with recursive cte_process_tree (
        agent_id,
        hostname,
        pid_hash,
        os_pid,
        process_name,
        process_path,
        level,
        parent_pid_hash,
        parent_os_pid,
        seq,
        ptree,
        ptree_list,
        ptree_list_tuples
    ) as (
        select
            p.agent_id,
            p.hostname,
            p.pid_hash,
            p.os_pid,
            p.process_name,
            p.process_path,
            0,
            p.parent_pid_hash,
            p.parent_os_pid,
            1,
            cast('=' || p.process_name as varchar),
            [p.pid_hash],
            [{ 'pid_hash': p.pid_hash, 'process_name': p.process_name }]
        from {{ ref('process') }} as p
        union all
        select
            pt.agent_id,
            pt.hostname,
            pt.pid_hash,
            pt.os_pid,
            pt.process_name,
            pt.process_path,
            pt.level + 1,
            p.parent_pid_hash,
            p.parent_os_pid,
            pt.seq + 1,
            cast(pt.ptree || '->' || p.process_name as varchar),
            list_append(pt.ptree_list, p.pid_hash),
            list_append(pt.ptree_list_tuples, { 'pid_hash': p.pid_hash, 'process_name': p.process_name })
        from {{ ref('process_context') if entity_mode else ref('process') }} as p,
            cte_process_tree as pt
        where
            p.pid_hash = pt.parent_pid_hash
            and pt.parent_pid_hash != pt.pid_hash
            and not list_contains(pt.ptree_list, p.pid_hash)
            and p.hostname = {% if entity_mode %}upper(trim(pt.hostname)){% else %}pt.hostname{% endif %}
            {% if entity_mode %}
            and not coalesce(p.identity_conflict, false)
            and p.context_status != 'missing_entity'
            and (pt.parent_os_pid is null or p.os_pid=pt.parent_os_pid)
            {% endif %}
    )
    select
        *,
        max(level) over (partition by {% if entity_mode %}hostname, {% endif %}pid_hash) as max_level
    from cte_process_tree
)
where max_level = level
