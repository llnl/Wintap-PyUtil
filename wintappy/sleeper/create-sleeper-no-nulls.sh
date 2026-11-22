#!/bin/bash
#
# Create a copy of the process table for sleeper. Convert nulls to something. Use a subset of fields.
# Run from inside the rolling/process dir with:
# find . -name \*parquet -exec ~/create_sleeper_no_nulls.sh {} \;

source="""
select 
	pid_hash,
	os_family,
	hostname,
	os_pid,
	process_name,
	parent_pid_hash,
	parent_os_pid,
	process_path,
	filename,
	file_id,
	cast(process_started_seconds as bigint) process_started_seconds,
	process_started,
	first_seen,
	last_seen,
	ifnull(args,'') args,
	ifnull(process_stop_seconds, -1) process_stop_seconds,
	ifnull(process_term, 'epoch'::TIMESTAMPTZ) process_term
from parquet_scan('$1') where process_name is not null
"""

target=../../sleeper/$(basename "$1")-no_nulls.parquet
copy_cmd="COPY ($source) TO '$target' (FORMAT 'parquet');"

~/apps/duckdb -s "$copy_cmd"
