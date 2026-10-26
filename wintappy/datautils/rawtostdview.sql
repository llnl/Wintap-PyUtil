/*
 * Create the standard view tables from existing RAW tables.
 * SQL Dialect: DuckDB
 *
 */

/* TODO: Change any_value() to mode(), which returns the most frequent value.
   Caution! mode() is much more expensive on larger datasets! For now, sticking with any_value() */

CREATE TABLE IF NOT EXISTS host
AS
SELECT
    hostname,
    list_sort(list(distinct agentid)) agent_ids,
    any_value('windows') os_family,
    to_timestamp(min(cast(eventtime as bigint))) first_seen, -- TODO: Should lastseen for host just be dropped? It isn't accurate. Better would be to derive it from other data: network, file, etc.
    to_timestamp(max(cast(eventtime as bigint))) last_seen,
    any_value(os) os,
    count(DISTINCT os) num_os,
    any_value(CASE
        WHEN osversion = '' THEN NULL
        ELSE osversion
    END) os_version,
    count(DISTINCT osversion) num_os_version,
    any_value(CASE
        WHEN arch = '' THEN NULL
        ELSE arch
    END) arch,
    count(DISTINCT arch) num_arch,
    any_value(processorcount) processor_count,
    count(DISTINCT processorcount) num_processor_count,
    any_value(processorspeed) processor_speed,
    count(DISTINCT processorspeed) num_processor_speed,
    any_value(hasbattery) has_battery,
    count(DISTINCT hasbattery) num_has_battery,
    any_value(domain) ad_domain,
    count(DISTINCT domain) num_ad_domain,
    any_value(domainrole) domain_role,
    count(DISTINCT domainrole) num_domain_role,
    max(lastboot) last_boot,
    count(DISTINCT lastboot) num_last_boot,
    max(wintapversion) wintap_version,
    count(DISTINCT wintapversion) num_wintap_version,
    max(etlversion) etl_version,
    count(DISTINCT etlversion) num_etl_version,
--    max(collectors) collectors,
--    count(DISTINCT collectors) num_collectors,
    count(*) num_rows
FROM raw_host
GROUP BY ALL
;


CREATE TABLE IF NOT EXISTS host_ip
AS
SELECT
    agentid agent_id,
    hostname,
    any_value('windows') os_family,
    CASE
        WHEN privategateway = '' THEN NULL
        ELSE privategateway
    END private_gateway,
    int_to_ip(cast(ipaddr AS bigint)) ip_addr_no,
    CASE
        WHEN mac = '' THEN NULL
        ELSE mac
    END mac,
    ipaddr ip_addr,
    'missing?' interface,
    mtu,
    to_timestamp(min(cast(eventtime as bigint))) first_seen,
    to_timestamp(max(cast(eventtime as bigint))) last_seen,
    count(*) num_rows
FROM raw_macip
GROUP BY ALL
;

-- Summarize Process events into Process entities

CREATE TABLE IF NOT EXISTS process
AS
SELECT
    p.pidhash pid_hash, -- osfamily will eventually come back as a partition key
    any_value('windows') os_family,
    any_value(agentid) agent_id,
    count(distinct agentid) num_agent_id,
    any_value(p.hostname) hostname,
    any_value(pid) os_pid,
    any_value(CASE
        WHEN p.processname = '' THEN NULL
        ELSE p.processname
    END) process_name,
    count(DISTINCT p.processname) num_process_name,
    any_value(CASE
        WHEN p.processargs = '' THEN NULL
        ELSE p.processargs
    END) args,
    count(DISTINCT p.processargs) num_args, -- Ignore useless names
    any_value(CASE
        WHEN
            p.username = ''
            OR lower(p.username) = 'na' THEN NULL
        ELSE p.username
    END) user_name,
    count(DISTINCT p.username) num_user_name,
    any_value(CASE
        WHEN p.parentpidhash = '' THEN NULL
        ELSE p.parentpidhash
    END) parent_pid_hash,
    count(DISTINCT p.parentpidhash) num_parent_pid_hash,
    any_value(p.parentpid) parent_os_pid,
    count(DISTINCT p.parentpid) num_parent_os_pid,
    any_value(CASE
        WHEN p.processpath = '' THEN NULL
        ELSE p.processpath
    END) process_path,
    -- Add empty fields that will be set in the next step
    count(DISTINCT p.processpath) num_process_path,
    '' filename,
    '' file_id,
    any_value(CASE
        WHEN p.filemd5 = '' THEN NULL
        ELSE p.filemd5
    END) file_md5,
    count(DISTINCT p.filemd5) num_file_md5,
    any_value(CASE
        WHEN p.filesha2 = '' THEN NULL
        ELSE p.filesha2
    END) file_sha2,
    count(DISTINCT p.filesha2) num_file_sha2,
    min(CASE
        WHEN
            upper(p.activitytype) IN ('START', 'REFRESH')
            THEN win32_to_epoch(cast(p.eventtime as bigint))
        ELSE NULL
    END) process_started_seconds,
    min(CASE
        WHEN
            upper(p.activitytype) IN ('START', 'REFRESH')
            THEN to_timestamp_micros(win32_to_epoch(cast(p.eventtime as bigint)))
        ELSE NULL
    END) process_started,
    to_timestamp_micros(win32_to_epoch(min(cast(p.eventtime as bigint)))) first_seen,
    to_timestamp_micros(win32_to_epoch(max(cast(p.eventtime as bigint)))) last_seen,
    sum((CASE WHEN upper(p.activitytype) IN ('START', 'REFRESH') THEN 1 ELSE 0 END)) num_process_start,
    -- These all come from ETW Process Stop events
    max(CASE
        WHEN
            upper(p.activitytype) IN ('STOP')
            THEN win32_to_epoch(cast(p.eventtime as bigint))
        ELSE NULL
    END) process_stop_seconds,
    max(CASE
        WHEN
            upper(p.activitytype) IN ('STOP')
            THEN to_timestamp_micros(win32_to_epoch(cast(p.eventtime as bigint)))
        ELSE NULL
    END) process_term,
    max(cpucyclecount) cpu_cycle_count,
    max(cpuutilization) cpu_utilization,
    max(commitcharge) commit_charge,
    max(commitpeak) commit_peak,
    max(readoperationcount) read_operation_count,
    max(writeoperationcount) write_operation_count,
    max(readtransferkilobytes) read_transfer_kilobytes,
    max(writetransferkilobytes) write_transfer_kilobytes,
    max(hardfaultcount) hard_fault_count,
    max(tokenelevationtype) token_elevation_type,
    max(exitcode) exit_code,
    sum((CASE WHEN upper(p.activitytype) = 'STOP' THEN 1 ELSE 0 END)) num_process_stop
FROM raw_process p
GROUP BY ALL
;

--== Update process_path
-- Move to Wintap
UPDATE process
SET
    filename = CASE
        WHEN process_path IS NULL THEN process_name
        WHEN
            substring(process_path, -1) = '\\'
            THEN concat(process_path, process_name)
        ELSE process_path
    END
WHERE
    process_path IS NOT NULL
    OR process_name IS NOT NULL
;

--== Set file_id on process
-- Move to Wintap

UPDATE process
SET file_id = md5(concat_ws('||', hostname, lower(filename)))
WHERE filename IS NOT NULL
;


CREATE TABLE IF NOT EXISTS process_conn_incr
AS
SELECT
    'windows' os_family,
    agentid agent_id,
    hostname,
    pidhash pid_hash,
    processname process_name,
    connid conn_id,
    -- Calculate a time range for grouping. Starting with 1 minute.
    protocol protocol,
    -- Can't use the UDF in a view, so here's the direct version:
    --    int(((FirstSeenMs / 1e7) - 11644473600)/60)*60 incr_start_secs,
    to_timestamp_micros(
        floor(win32_to_epoch(cast(firstseenms as bigint)) / 60) * 60
    ) incr_start,
    --localiphash,
    -- Can't use a UDF in a view, so dotted-quad IPs are done in the table building.
    int_to_ip(cast(
        localipaddr
        AS bigint
    )) local_ip_addr,
    localipaddr local_ip_int,
    localport local_port,
    int_to_ip(cast(
        remoteipaddr
        AS bigint
    )) remote_ip_addr,
    remoteipaddr remote_ip_int,
    remoteport remote_port,
    sum(eventcount) total_events,
    sum(packetsize) total_size,
    count(*) num_raw_rows,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'tcpipaccept' THEN eventcount
    END) tcp_accept_count,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'tcpipconnect' THEN eventcount
    END) tcp_connect_count,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'tcpipdisconnect' THEN eventcount
    END) tcp_disconnect_count,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'tcpipreconnect' THEN eventcount
    END) tcp_reconnect_count,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) IN ('tcpiprecv', 'tcpiprecvipv6') THEN eventcount
    END) tcp_recv_count,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) IN ('tcpiprecv', 'tcpiprecvipv6') THEN packetsize
    END) tcp_recv_size,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'tcpipretransmit' THEN eventcount
    END) tcp_retransmit_count,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) IN ('tcpipsend', 'tcpipsendipv6') THEN eventcount
    END) tcp_send_count,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) IN ('tcpipsend', 'tcpipsendipv6') THEN packetsize
    END) tcp_send_size,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'tcpiptcpcopy' THEN eventcount
    END) tcp_tcpcopy_count,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'tcpiptcpcopy' THEN packetsize
    END) tcp_tcpcopy_size,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'udpiprecv' THEN eventcount
    END) udp_recv_count,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'udpiprecv' THEN packetsize
    END) udp_recv_size,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'udpipsend' THEN eventcount
    END) udp_send_count,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'udpipsend' THEN packetsize
    END) udp_send_size, -- Gather some basic stats on traffic
    -- Total events/sizes. In practice, at this level of detail, these *should* be really close the TCP/UDP stats, but exceptions like many connects/disconnects, retransmits, could throw some off.
    min(eventcount) min_10sec_eventcount,
    -- These might need to be fixed. Clarify...
    max(eventcount) max_10sec_eventcount,
    min(minpacketsize) min_size,
    max(maxpacketsize) max_size,
    -- TCP Stats - only doing send/receive for now.
    sum(packetsizesquared) sq_size,
    max(CASE
        WHEN lower(replace(ipevent, '/', '')) IN ('tcpiprecv', 'tcpiprecvipv6') THEN eventcount
    END) max_tcp_recv_count,
    min(CASE
        WHEN lower(replace(ipevent, '/', '')) IN ('tcpiprecv', 'tcpiprecvipv6') THEN minpacketsize
    END) min_tcp_recv_size,
    max(CASE
        WHEN lower(replace(ipevent, '/', '')) IN ('tcpiprecv', 'tcpiprecvipv6') THEN maxpacketsize
    END) max_tcp_recv_size,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) IN ('tcpiprecv', 'tcpiprecvipv6') THEN packetsizesquared
    END) sq_tcp_recv_size, --
    max(CASE
        WHEN lower(replace(ipevent, '/', '')) IN ('tcpipsend', 'tcpipsendipv6') THEN eventcount
    END) max_tcp_send_count,
    min(CASE
        WHEN lower(replace(ipevent, '/', '')) IN ('tcpipsend', 'tcpipsendipv6') THEN minpacketsize
    END) min_tcp_send_size,
    max(CASE
        WHEN lower(replace(ipevent, '/', '')) IN ('tcpipsend', 'tcpipsendipv6') THEN maxpacketsize
    END) max_tcp_send_size,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) IN ('tcpipsend', 'tcpipsendipv6') THEN packetsizesquared
    END) sq_tcp_send_size, -- UDP Stats
    max(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'udpiprecv' THEN eventcount
    END) max_udp_recv_count,
    min(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'udpiprecv' THEN minpacketsize
    END) min_udp_recv_size,
    max(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'udpiprecv' THEN maxpacketsize
    END) max_udp_recv_size,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'udpiprecv' THEN packetsizesquared
    END) sq_udp_recv_size, --
    max(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'udpipsend' THEN eventcount
    END) max_udp_send_count,
    min(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'udpipsend' THEN minpacketsize
    END) min_udp_send_size,
    max(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'udpipsend' THEN maxpacketsize
    END) max_udp_send_size,
    sum(CASE
        WHEN lower(replace(ipevent, '/', '')) = 'udpipsend' THEN packetsizesquared
    END) sq_udp_send_size,
    to_timestamp_micros(win32_to_epoch(min(cast(firstseenms as bigint)))) first_seen,
    to_timestamp_micros(win32_to_epoch(max(cast(lastseenms as bigint)))) last_seen
FROM raw_process_conn_incr
GROUP BY ALL
;


CREATE TABLE IF NOT EXISTS process_net_conn
AS
SELECT
    os_family,
    agent_id,
    hostname,
    pid_hash,
    process_name,
    conn_id,
    protocol,
    local_ip_addr,
    local_port,
    remote_ip_addr,
    remote_port,
    sum(total_events) total_events,
    sum(total_size) total_size,
    sum(sq_size) sq_size,
    sum(num_raw_rows) num_raw_rows,
    sum(tcp_accept_count) tcp_accept_count,
    sum(tcp_connect_count) tcp_connect_count,
    sum(tcp_disconnect_count) tcp_disconnect_count,
    sum(tcp_reconnect_count) tcp_reconnect_count,
    sum(tcp_recv_count) tcp_recv_count,
    sum(tcp_recv_size) tcp_recv_size,
    sum(sq_tcp_recv_size) sq_tcp_recv_size,
    sum(tcp_retransmit_count) tcp_retransmit_count,
    sum(tcp_send_count) tcp_send_count,
    sum(tcp_send_size) tcp_send_size,
    sum(sq_tcp_send_size) sq_tcp_send_size,
    sum(tcp_tcpcopy_count) tcp_tcpcopy_count,
    sum(tcp_tcpcopy_size) tcp_tcpcopy_size,
    sum(udp_recv_count) udp_recv_count,
    sum(udp_recv_size) udp_recv_size,
    sum(sq_udp_recv_size) sq_udp_recv_size,
    sum(udp_send_count) udp_send_count,
    sum(udp_send_size) udp_send_size,
    sum(sq_udp_send_size) sq_udp_send_size,
    min(first_seen) first_seen,
    max(last_seen) last_seen
FROM process_conn_incr
GROUP BY ALL
;

-- Summarize file activity to PID_HASH+FILE_HASH

CREATE TABLE IF NOT EXISTS process_file
AS
SELECT
    agentid agent_id,
    hostname,
    pidhash pid_hash, -- generate FileID
    processname process_name,
    md5(concat_ws('||', hostname, lower(file_path))) file_id,
    file_hash file_hash,
    file_path filename,
    activitytype activity_type,
    sum(bytesrequested) bytes_requested,
    sum(eventcount) event_count,
    count(*) num_raw_rows,
    to_timestamp_micros(win32_to_epoch(min(cast(firstseen as bigint)))) first_seen,
    to_timestamp_micros(win32_to_epoch(max(cast(lastseen as bigint)))) last_seen,
    to_timestamp(min(cast(eventtime as bigint))) min_event,
    to_timestamp(max(cast(eventtime as bigint))) max_event
FROM raw_process_file
GROUP BY ALL
;

-- Summarize registry event increments.
-- Should there be multiple levels of summary?

CREATE TABLE IF NOT EXISTS process_registry
AS
SELECT
    agentid agent_id,
    hosthame hostname,
    pidhash pid_hash,
    processname process_name,
    reg_path reg_path,
    reg_value reg_value,
    activitytype activity_type,
    reg_data reg_data,
    sum(eventcount) event_count,
    --  win32_to_epoch(min(FirstSeenMs)) first_seen_seconds,
    count(*) num_raw_rows,
    --win32_to_epoch(max(lastseenms)) last_seen_seconds
    to_timestamp_micros(win32_to_epoch(min(cast(firstseenms as bigint)))) first_seen,
    to_timestamp_micros(win32_to_epoch(max(cast(lastseenms as bigint)))) last_seen,
    to_timestamp(min(cast(eventtime as bigint))) min_event,
    to_timestamp(max(cast(eventtime as bigint))) max_event
FROM raw_process_registry
GROUP BY ALL
;

-- Summarize to PID_HASH+FILENAME

--EXPLAIN ANALYZE
CREATE TABLE IF NOT EXISTS process_image_load
AS
SELECT
    pidhash pid_hash,
    lower(filename) filename,
    any_value(agentid) agent_id,
    any_value(computername) hostname,
    any_value(processname) process_name,
    md5(concat_ws('||', computername, lower(filename))) file_id,
    any_value(md5) file_md5,
--    count(DISTINCT md5) num_file_md5,
    max(buildtime) build_time,
--    count(DISTINCT buildtime) num_uniq_build_times,
    max(imagechecksum) checksum,
--    count(DISTINCT imagechecksum) num_uniq_checksums,
    max(defaultbase) default_base,
--    count(DISTINCT defaultbase) num_default_base,
    max(imagebase) image_base,
--    count(DISTINCT imagebase) num_image_base,
    min(imagesize) min_image_size,
    max(imagesize) max_image_size,
--    count(DISTINCT imagesize) num_image_size,
    sum(if(upper(activitytype) = 'LOAD', 1, 0)) num_load,
    sum(if(upper(activitytype) = 'UNLOAD', 1, 0)) num_unload,
    to_timestamp_micros(win32_to_epoch(min(cast(eventtime as bigint)))) first_seen,
    to_timestamp_micros(win32_to_epoch(max(cast(eventtime as bigint)))) last_seen
FROM raw_imageload
GROUP BY ALL
;


CREATE OR REPLACE VIEW process_exe_file_summary
AS
SELECT
    'process'
    AS source,
    hostname,
    filename,
    file_id,
    min(process_started) min_process_started,
    max(process_term) max_process_term,
    count(*) AS process_num_rows
FROM process
GROUP BY ALL
;


CREATE OR REPLACE VIEW files_tmp_v1
AS
SELECT
    file_id,
    hostname,
    filename,
    process_num_rows,
    cast(NULL AS integer) dll_num_rows,
    cast(NULL AS integer) file_num_rows,
    min_process_started,
    max_process_term,
    cast(NULL AS timestamp) dll_first_seen,
    cast(NULL AS timestamp) dll_last_seen,
    cast(NULL AS timestamp) file_first_seen,
    cast(NULL AS timestamp) file_last_seen
FROM process_exe_file_summary
UNION
SELECT
    file_id,
    hostname,
    filename,
    NULL
    AS process_num_rows,
    count(*)
    AS dll_num_rows,
    NULL
    AS file_num_rows,
    NULL
    AS min_process_started,
    NULL
    AS max_process_term,
    min(first_seen) dll_first_seen,
    max(last_seen) dll_last_seen,
    NULL
    AS file_first_seen,
    NULL
    AS file_last_seen
FROM process_image_load
GROUP BY ALL
UNION
SELECT
    file_id,
    hostname,
    filename,
    NULL
    AS process_num_rows,
    NULL
    AS dll_num_rows,
    count(*)
    AS file_num_rows,
    NULL
    AS min_processstarted,
    NULL
    AS max_processterm,
    NULL
    AS dll_first_seen,
    NULL
    AS dll_last_seen,
    min(first_seen) file_first_seen,
    max(last_seen) file_last_seen
FROM process_file
GROUP BY ALL
;


CREATE TABLE files
AS
SELECT
    file_id,
    hostname,
    filename,
    sum(process_num_rows) process_num_rows,
    sum(dll_num_rows) dll_num_rows,
    sum(file_num_rows) file_num_rows,
    min(min_process_started) min_process_started,
    max(max_process_term) max_process_term,
    min(dll_first_seen) dll_first_seen,
    max(dll_last_seen) dll_last_seen,
    min(file_first_seen) file_first_seen,
    max(file_last_seen) file_last_seen
FROM files_tmp_v1
GROUP BY ALL
;

-- Create a set of files using just the filename.

CREATE TABLE all_files
AS
SELECT
    filename,
    count(DISTINCT hostname) num_hosts,
    sum(process_num_rows) process_num_rows,
    sum(dll_num_rows) dll_num_rows,
    sum(file_num_rows) file_num_rows
FROM files
GROUP BY ALL
;
