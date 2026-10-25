{% macro win32_to_epoch_expr(wts) -%}
    ({{ wts }} / 1e7 - 11644473600)
{%- endmacro %}

{% macro to_timestamp_micros_expr(epoch_seconds) -%}
    (to_timestamp(cast(floor({{ epoch_seconds }}) as bigint)) + to_microseconds(cast(floor(({{ epoch_seconds }} - floor({{ epoch_seconds }})) * 1e6) as bigint)))
{%- endmacro %}

{% macro win32_to_timestamp_expr(wts) -%}
    {# Integer FILETIME -> UTC microseconds. Floor, not truncation, before 1970.
       HUGEINT avoids overflow during epoch subtraction. Raw 100ns data is untouched. #}
    make_timestamptz(cast({{ floor_integer_div_expr('cast(' ~ wts ~ ' as hugeint) - 116444736000000000', 10) }} as bigint))
{%- endmacro %}

{% macro floor_integer_div_expr(value, divisor) -%}
    (({{ value }}) // {{ divisor }} - case when ({{ value }}) % {{ divisor }} < 0 then 1 else 0 end)
{%- endmacro %}

{% macro win32_to_minute_timestamp_expr(wts) -%}
    make_timestamptz(cast({{ floor_integer_div_expr('cast(' ~ wts ~ ' as hugeint) - 116444736000000000', 600000000) }} * 60000000 as bigint))
{%- endmacro %}

{% macro unix_or_timestamp_expr(value) -%}
    case
        when lower(typeof({{ value }})) like 'timestamp%' then cast({{ value }} as timestamp)
        else to_timestamp(cast(try_cast(cast({{ value }} as varchar) as double) as bigint))
    end
{%- endmacro %}

{% macro win32_or_timestamp_expr(value) -%}
    case
        when lower(typeof({{ value }})) = 'timestamp with time zone' then try_cast({{ value }} as timestamptz)
        when lower(typeof({{ value }})) like 'timestamp%' then cast({{ value }} as timestamp)
        else {{ win32_to_timestamp_expr("case when regexp_full_match(trim(cast(" ~ value ~ " as varchar)), '[+-]?[0-9]+') then try_cast(trim(cast(" ~ value ~ " as varchar)) as bigint) else null end") }}
    end
{%- endmacro %}
