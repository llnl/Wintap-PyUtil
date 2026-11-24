select
    *,
    {% if not raw_column_exists(['raw_process'], 'ProcessArgs') -%}
    commandline as ProcessArgs,
    {% endif -%}
    {% if not raw_column_exists(['raw_process'], 'UniqueProcessKey') -%}
    cast(null as varchar) as UniqueProcessKey,
    {% endif -%}
    {% if not raw_column_exists(['raw_process'], 'ExecutionObservationKind') -%}
    cast(null as varchar) as ExecutionObservationKind,
    {% endif -%}
    {% if not raw_column_exists(['raw_process'], 'ObservationTimeFileTimeUtc') -%}
    cast(null as bigint) as ObservationTimeFileTimeUtc,
    {% endif -%}
    {% if not raw_column_exists(['raw_process'], 'ObservationTimeMonotonicNanoseconds') -%}
    cast(null as bigint) as ObservationTimeMonotonicNanoseconds,
    {% endif -%}
    {% if not raw_column_exists(['raw_process'], 'ReceiveTime') -%}
    cast(null as bigint) as ReceiveTime,
    {% endif -%}
    count(*) as num_dups
from {{ raw_scan_for(['raw_process']) }}
where {{ partition_filter() }}
group by all
