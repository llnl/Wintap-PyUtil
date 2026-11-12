{# TEMPORARY validation shim; the authoritative CONN_ID generator belongs in Wintap .NET. #}
{% macro temporary_five_tuple_conn_id(protocol, local_ip, local_port, remote_ip, remote_port) -%}
case
    when {{ protocol }} is null
      or nullif(trim(cast({{ protocol }} as varchar)), '') is null
      or {{ local_ip }} is null
      or {{ local_port }} is null
      or {{ remote_ip }} is null
      or {{ remote_port }} is null
    then cast(null as varchar)
    else upper(md5(to_json(struct_pack(
        context := 'llnl',
        protocol := upper(trim(cast({{ protocol }} as varchar))),
        endpoint_a_ip := case
            when cast({{ local_ip }} as bigint) < cast({{ remote_ip }} as bigint)
              or (
                    cast({{ local_ip }} as bigint) = cast({{ remote_ip }} as bigint)
                    and cast({{ local_port }} as integer) <= cast({{ remote_port }} as integer)
                 )
            then cast({{ local_ip }} as bigint)
            else cast({{ remote_ip }} as bigint)
        end,
        endpoint_a_port := case
            when cast({{ local_ip }} as bigint) < cast({{ remote_ip }} as bigint)
              or (
                    cast({{ local_ip }} as bigint) = cast({{ remote_ip }} as bigint)
                    and cast({{ local_port }} as integer) <= cast({{ remote_port }} as integer)
                 )
            then cast({{ local_port }} as integer)
            else cast({{ remote_port }} as integer)
        end,
        endpoint_b_ip := case
            when cast({{ local_ip }} as bigint) < cast({{ remote_ip }} as bigint)
              or (
                    cast({{ local_ip }} as bigint) = cast({{ remote_ip }} as bigint)
                    and cast({{ local_port }} as integer) <= cast({{ remote_port }} as integer)
                 )
            then cast({{ remote_ip }} as bigint)
            else cast({{ local_ip }} as bigint)
        end,
        endpoint_b_port := case
            when cast({{ local_ip }} as bigint) < cast({{ remote_ip }} as bigint)
              or (
                    cast({{ local_ip }} as bigint) = cast({{ remote_ip }} as bigint)
                    and cast({{ local_port }} as integer) <= cast({{ remote_port }} as integer)
                 )
            then cast({{ remote_port }} as integer)
            else cast({{ local_port }} as integer)
        end
    ))))
end
{%- endmacro %}
