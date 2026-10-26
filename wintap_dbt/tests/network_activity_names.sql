-- Fail visibly on new/misspelled network activity names rather than letting
-- every directional counter become NULL. Keep this vocabulary aligned with
-- WintapMessage.ActivityTypeEnum (devtools/check_event_name_contracts.py).
select distinct ipevent
from {{ ref('stg_raw_process_conn_incr') }}
where ipevent is null or lower(replace(ipevent, '/', '')) not in (
    'tcpipaccept', 'tcpiprecv', 'tcpiptcpcopy', 'tcpipreconnect',
    'tcpipretransmit', 'tcpipdisconnect', 'tcpiparpcopy', 'tcpipdupack',
    'tcpipfullack', 'tcpippartack', 'tcpipconnect', 'tcpipsend', 'tcpipfail',
    'tcpiprecvipv6', 'tcpipsendipv6', 'udpipfail', 'udpipsend', 'udpiprecv'
)
