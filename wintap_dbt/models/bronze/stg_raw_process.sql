{% set raw_process_columns = raw_columns(['raw_process']) %}

select
    *,
    {% if 'processargs' not in raw_process_columns -%}
    commandline as ProcessArgs,
    {% endif -%}
    {% if 'uniqueprocesskey' not in raw_process_columns -%}
    cast(null as varchar) as UniqueProcessKey,
    {% endif -%}
    {% if 'executionobservationkind' not in raw_process_columns -%}
    cast(null as varchar) as ExecutionObservationKind,
    {% endif -%}
    {% if 'observationtimefiletimeutc' not in raw_process_columns -%}
    cast(null as bigint) as ObservationTimeFileTimeUtc,
    {% endif -%}
    {% if 'observationtimemonotonicnanoseconds' not in raw_process_columns -%}
    cast(null as bigint) as ObservationTimeMonotonicNanoseconds,
    {% endif -%}
    {% if 'receivetime' not in raw_process_columns -%}
    cast(null as bigint) as ReceiveTime,
    {% endif -%}
    count(*) as num_dups
from {{ raw_scan_for(['raw_process']) }}
where {{ partition_filter() }}
group by all
