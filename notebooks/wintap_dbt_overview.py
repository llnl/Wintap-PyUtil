import marimo

__generated_with = "0.23.16"
app = marimo.App(width="full")


@app.cell
def _():
    import os
    from pathlib import Path

    import altair as alt
    import duckdb
    import marimo as mo
    import pandas as pd
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    def db_path() -> str:
        if os.getenv("WINTAP_DBT_DATABASE"):
            return os.environ["WINTAP_DBT_DATABASE"]
        data_root = os.getenv("WINTAP_DATA_ROOT")
        if data_root:
            return str(Path(data_root) / "duckdb" / "wintap.duckdb")
        return "wintap.duckdb"

    def query_df(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
        try:
            return con.execute(sql).df()
        except Exception as exc:  # Keep dashboard usable when optional models are absent.
            return pd.DataFrame({"error": [str(exc)], "sql": [sql]})

    def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
        return bool(
            con.execute(
                """
                select count(*)
                from information_schema.tables
                where table_schema = 'main'
                  and lower(table_name) = lower(?)
                """,
                [table_name],
            ).fetchone()[0]
        )

    return Path, alt, db_path, duckdb, go, make_subplots, mo, query_df, table_exists


@app.cell
def _(mo):
    mo.md("""
    # Wintap/Lintap DBT QA Dashboard

    Quick overview for a DBT-built Wintap/Lintap DuckDB database. The database path is resolved from
    `WINTAP_DBT_DATABASE`, or from `WINTAP_DATA_ROOT/duckdb/wintap.duckdb` when only `WINTAP_DATA_ROOT` is set.
    """)
    return


@app.cell
def _(Path, db_path, mo):
    database_path = db_path()
    database_exists = Path(database_path).exists()
    mo.vstack(
        [
            mo.md(f"**Database:** `{database_path}`"),
            mo.md(f"**Exists:** `{database_exists}`"),
        ]
    )
    return database_exists, database_path


@app.cell
def _(database_exists, database_path, duckdb, mo):
    if not database_exists:
        mo.stop(True, mo.md("Database not found. Run `make dbt-build` first."))
    con = duckdb.connect(database_path, read_only=True)
    return (con,)


@app.cell
def _(con, mo, query_df):
    build_summary = query_df(con, "select * from build_summary order by table_name")
    event_summary = query_df(con, "select * from telemetry_event_summary order by event_type")
    mo.md("## Build and Event Summary")
    return build_summary, event_summary


@app.cell
def _(build_summary, event_summary, mo):
    mo.vstack(
        [
            mo.md("### DBT build row counts"),
            mo.ui.table(build_summary, pagination=False),
            mo.md("### Telemetry event summary"),
            mo.ui.table(event_summary, pagination=False),
        ]
    )
    return


@app.cell
def _(con, mo, query_df):
    qa_pid_hash = query_df(
        con,
        """
        with orphan_checks as (
            select 'process_conn_incr' as child_relation, count(*) as child_rows,
                   count(p.pid_hash) as matched_rows,
                   count(*) filter (where c.pid_hash is not null and p.pid_hash is null) as orphan_rows,
                   count(*) filter (where c.pid_hash is null) as null_pid_hash_rows
            from process_conn_incr c left join process p on c.pid_hash = p.pid_hash
            union all
            select 'process_net_conn', count(*), count(p.pid_hash),
                   count(*) filter (where c.pid_hash is not null and p.pid_hash is null),
                   count(*) filter (where c.pid_hash is null)
            from process_net_conn c left join process p on c.pid_hash = p.pid_hash
            union all
            select 'process_file', count(*), count(p.pid_hash),
                   count(*) filter (where c.pid_hash is not null and p.pid_hash is null),
                   count(*) filter (where c.pid_hash is null)
            from process_file c left join process p on c.pid_hash = p.pid_hash
            union all
            select 'process_registry', count(*), count(p.pid_hash),
                   count(*) filter (where c.pid_hash is not null and p.pid_hash is null),
                   count(*) filter (where c.pid_hash is null)
            from process_registry c left join process p on c.pid_hash = p.pid_hash
            union all
            select 'process_image_load', count(*), count(p.pid_hash),
                   count(*) filter (where c.pid_hash is not null and p.pid_hash is null),
                   count(*) filter (where c.pid_hash is null)
            from process_image_load c left join process p on c.pid_hash = p.pid_hash
            union all
            select 'process_summary', count(*), count(p.pid_hash),
                   count(*) filter (where c.pid_hash is not null and p.pid_hash is null),
                   count(*) filter (where c.pid_hash is null)
            from process_summary c left join process p on c.pid_hash = p.pid_hash
            union all
            select 'process_uber_summary', count(*), count(p.pid_hash),
                   count(*) filter (where c.pid_hash is not null and p.pid_hash is null),
                   count(*) filter (where c.pid_hash is null)
            from process_uber_summary c left join process p on c.pid_hash = p.pid_hash
        )
        select *,
               case when orphan_rows = 0 and null_pid_hash_rows = 0 then 'PASS' else 'FAIL' end as status
        from orphan_checks
        order by child_relation
        """,
    )
    top_pid_hash_orphans = query_df(
        con,
        """
        -- Aggregate by the missing process identity, not merely by filename:
        -- that makes the attribution gap and its data impact actionable.
        with orphan_events as (
            select
                'process_file' as child_relation,
                coalesce(c.hostname, '(unknown)') as hostname,
                coalesce(c.process_name, '(unknown)') as process_name,
                c.pid_hash,
                c.event_count as event_count,
                c.filename as affected_item,
                c.activity_type as activity,
                c.first_seen,
                c.last_seen
            from process_file c
            left join process p on c.pid_hash = p.pid_hash
            where c.pid_hash is not null and p.pid_hash is null

            union all

            select
                'process_conn_incr',
                coalesce(c.hostname, '(unknown)'),
                coalesce(c.process_name, '(unknown)'),
                c.pid_hash,
                c.total_events,
                c.protocol || ' ' || coalesce(c.remote_ip_addr, '?') || ':' || coalesce(cast(c.remote_port as varchar), '?'),
                c.protocol,
                c.first_seen,
                c.last_seen
            from process_conn_incr c
            left join process p on c.pid_hash = p.pid_hash
            where c.pid_hash is not null and p.pid_hash is null

            union all

            select
                'process_net_conn',
                coalesce(c.hostname, '(unknown)'),
                coalesce(c.process_name, '(unknown)'),
                c.pid_hash,
                c.total_events,
                c.protocol || ' ' || coalesce(c.remote_ip_addr, '?') || ':' || coalesce(cast(c.remote_port as varchar), '?'),
                c.protocol,
                c.first_seen,
                c.last_seen
            from process_net_conn c
            left join process p on c.pid_hash = p.pid_hash
            where c.pid_hash is not null and p.pid_hash is null
        )
        select
            child_relation,
            hostname,
            process_name,
            pid_hash,
            count(*) as orphan_rows,
            coalesce(sum(event_count), 0) as captured_events,
            count(distinct affected_item) as affected_items,
            string_agg(distinct activity, ', ' order by activity) as activities,
            min(first_seen) as first_seen,
            max(last_seen) as last_seen
        from orphan_events
        group by all
        order by captured_events desc, orphan_rows desc, last_seen desc
        limit 25
        """,
    )
    sentinel_pid_hash = query_df(
        con,
        """
        select 'process' as relation_name, pid_hash, count(*) as num_rows
        from process
        where lower(pid_hash) like 'fixme%' or lower(pid_hash) like 'unknown%'
        group by all
        union all
        select 'process_conn_incr', pid_hash, count(*)
        from process_conn_incr
        where lower(pid_hash) like 'fixme%' or lower(pid_hash) like 'unknown%'
        group by all
        union all
        select 'process_file', pid_hash, count(*)
        from process_file
        where lower(pid_hash) like 'fixme%' or lower(pid_hash) like 'unknown%'
        group by all
        order by relation_name, num_rows desc
        """,
    )
    mo.md("## PID Hash QA")
    return qa_pid_hash, sentinel_pid_hash, top_pid_hash_orphans


@app.cell
def _(mo, qa_pid_hash, sentinel_pid_hash, top_pid_hash_orphans):
    mo.vstack(
        [
            mo.md("### PID hash orphan checks"),
            mo.ui.table(qa_pid_hash, pagination=False),
            mo.md(
                "### Top 25 orphan process identities\n"
                "Grouped by child relation, host, reported process name, and missing PID hash; "
                "ranked by captured event impact. `affected_items` is distinct files or network endpoints."
            ),
            mo.ui.table(top_pid_hash_orphans, pagination=False),
            mo.md("### Forbidden/sentinel PID hash values"),
            mo.ui.table(sentinel_pid_hash, pagination=False),
        ]
    )
    return


@app.cell
def _(con, mo, query_df):
    host_user_summary = query_df(
        con,
        """
        select
            count(distinct hostname) as num_hosts,
            count(distinct agent_id) as num_agent_ids,
            count(distinct user_name) filter (where user_name is not null) as num_users,
            count(distinct process_name) as num_process_names,
            count(distinct os_pid) as num_os_pids,
            count(*) as num_process_instances
        from process
        """,
    )
    top_hosts = query_df(
        con,
        """
        select hostname, count(*) as process_instances, count(distinct user_name) as users,
               count(distinct process_name) as process_names
        from process
        group by hostname
        order by process_instances desc
        limit 25
        """,
    )
    top_users = query_df(
        con,
        """
        select user_name, count(*) as process_instances, count(distinct hostname) as hosts,
               count(distinct process_name) as process_names
        from process
        where user_name is not null
        group by user_name
        order by process_instances desc
        limit 25
        """,
    )
    mo.md("## Hosts, Users, Processes")
    return host_user_summary, top_hosts, top_users


@app.cell
def _(host_user_summary, mo, top_hosts, top_users):
    mo.vstack(
        [
            mo.ui.table(host_user_summary, pagination=False),
            mo.md("### Top hosts"),
            mo.ui.table(top_hosts, pagination=False),
            mo.md("### Top users"),
            mo.ui.table(top_users, pagination=False),
        ]
    )
    return


@app.cell
def _(con, mo, query_df):
    timeline = query_df(
        con,
        """
        select time_chunk, event_type, num_rows
        from process_chart
        union all
        select time_chunk, event_type, num_rows from file_chart
        union all
        select time_chunk, event_type, num_rows from network_chart
        union all
        select time_chunk, event_type, num_rows from perf_chart
        order by time_chunk, event_type
        """,
    )
    mo.md("## Timeline")
    return (timeline,)


@app.cell
def _(alt, mo, timeline):
    if timeline.empty or "error" in timeline.columns:
        timeline_chart = mo.ui.table(timeline, pagination=False)
    else:
        timeline_chart = alt.Chart(timeline).mark_bar().encode(
            x=alt.X("time_chunk:T", title="Time"),
            y=alt.Y("num_rows:Q", title="Rows / samples"),
            color="event_type:N",
            tooltip=["time_chunk:T", "event_type:N", "num_rows:Q"],
        ).properties(height=320)
    timeline_chart
    return


@app.cell
def _(con, mo, query_df):
    top_processes = query_df(
        con,
        """
        select process_name, count(*) as process_instances, count(distinct hostname) as hosts,
               count(distinct user_name) as users
        from process
        group by process_name
        order by process_instances desc
        limit 30
        """,
    )
    top_network = query_df(
        con,
        """
        select remote_ip_addr, remote_port, protocol, count(*) as connections,
               sum(total_events) as total_events, sum(total_size) as total_size
        from process_net_conn
        where remote_ip_addr is not null
        group by all
        order by total_events desc nulls last, connections desc
        limit 30
        """,
    )
    top_files = query_df(
        con,
        """
        select filename, count(*) as process_file_rows, sum(event_count) as events,
               sum(bytes_requested) as bytes_requested
        from process_file
        group by filename
        order by events desc nulls last, bytes_requested desc nulls last
        limit 30
        """,
    )
    mo.md("## Top Activity")
    return top_files, top_network, top_processes


@app.cell
def _(mo, top_files, top_network, top_processes):
    mo.vstack(
        [
            mo.md("### Top process names"),
            mo.ui.table(top_processes, pagination=False),
            mo.md("### Top network endpoints"),
            mo.ui.table(top_network, pagination=False),
            mo.md("### Top files"),
            mo.ui.table(top_files, pagination=False),
        ]
    )
    return


@app.cell
def _(con, mo, query_df):
    pidstat_summary = query_df(
        con,
        """
        select
            coalesce(sum(samples), 0) as samples,
            min(first_seen) as first_seen,
            max(last_seen) as last_seen,
            max(max_cpu_core_percent) as max_cpu_core_percent,
            max(max_mem_percent) as max_mem_percent,
            max(max_kb_read_per_sec) as max_kb_read_per_sec,
            max(max_kb_write_per_sec) as max_kb_write_per_sec,
            count(distinct hostname) as num_hosts,
            count(distinct pid) as num_pids,
            count(distinct command) as num_commands,
            count(distinct container_id) filter (where container_id is not null) as num_containers
        from pidstat_process_summary
        """,
    )
    pidstat_available = bool(
        "error" not in pidstat_summary.columns
        and not pidstat_summary.empty
        and int(pidstat_summary.loc[0, "samples"] or 0) > 0
    )
    top_pidstat = query_df(
        con,
        """
        select hostname, container_runtime, container_id, pid_ns_inode, command, pid,
               samples,
               max_cpu_core_percent,
               avg_cpu_core_percent,
               max_mem_percent,
               avg_mem_percent,
               max_kb_read_per_sec,
               avg_kb_read_per_sec,
               max_kb_write_per_sec,
               avg_kb_write_per_sec
        from pidstat_process_summary
        order by max_cpu_core_percent desc nulls last, max_mem_percent desc nulls last
        limit 30
        """,
    )
    mo.md("## Pidstat")
    return pidstat_available, pidstat_summary, top_pidstat


@app.cell
def _(mo, pidstat_available, pidstat_summary, top_pidstat):
    mo.vstack(
        [
            mo.md(f"**pidstat rows available:** `{pidstat_available}`"),
            mo.ui.table(pidstat_summary, pagination=False),
            mo.md("### Top pidstat PID/command groups"),
            mo.ui.table(top_pidstat, pagination=False),
        ]
    )
    return


@app.cell
def _(con, mo, pidstat_available, query_df):
    pidstat_hosts = query_df(
        con,
        """
        select distinct hostname
        from pidstat_process_summary
        where hostname is not null
        order by hostname
        """,
    )
    host_options = ["All hosts"]
    if not pidstat_hosts.empty and "error" not in pidstat_hosts.columns:
        host_options.extend(pidstat_hosts["hostname"].dropna().tolist())

    pidstat_metric = mo.ui.dropdown(
        ["CPU (core-summed %)", "Memory %", "Read KB/s", "Write KB/s"],
        value="CPU (core-summed %)",
        label="Pidstat time-series metric",
        full_width=True,
    )
    pidstat_aggregation = mo.ui.dropdown(
        ["PID/command grouping", "Aggregate by command"],
        value="PID/command grouping",
        label="Aggregation",
        full_width=True,
    )
    pidstat_host = mo.ui.dropdown(
        host_options,
        value="All hosts",
        label="Host filter",
        searchable=True,
        full_width=True,
    )
    pidstat_min_peak_metric = mo.ui.number(
        start=0,
        step=0.1,
        value=25.0,
        label="Minimum peak selected metric",
        full_width=True,
    )
    pidstat_command_filter = mo.ui.text(
        value="",
        placeholder="e.g. setroubleshootd",
        label="Command contains",
        full_width=True,
    )
    pidstat_max_series = mo.ui.slider(
        start=1,
        stop=30,
        step=1,
        value=12,
        show_value=True,
        include_input=True,
        label="Max series to plot",
        full_width=True,
    )
    controls = mo.hstack(
        [pidstat_metric, pidstat_aggregation, pidstat_host, pidstat_command_filter, pidstat_min_peak_metric, pidstat_max_series],
        widths=[0.16, 0.16, 0.2, 0.18, 0.14, 0.16],
    )
    if not pidstat_available:
        pidstat_controls_panel = mo.vstack([controls, mo.md("No pidstat rows matched the current build.")])
    else:
        pidstat_controls_panel = mo.vstack([controls])
    pidstat_controls_panel
    return pidstat_aggregation, pidstat_command_filter, pidstat_host, pidstat_max_series, pidstat_metric, pidstat_min_peak_metric


@app.cell
def _(con, pidstat_aggregation, pidstat_available, pidstat_command_filter, pidstat_host, pidstat_max_series, pidstat_metric, pidstat_min_peak_metric, query_df):
    pidstat_metric_columns = {
        "CPU (core-summed %)": "cpu_core_percent",
        "Memory %": "mem_percent",
        "Read KB/s": "kb_read_per_sec",
        "Write KB/s": "kb_write_per_sec",
    }
    pidstat_peak_columns = {
        "CPU (core-summed %)": "max_cpu_core_percent",
        "Memory %": "max_mem_percent",
        "Read KB/s": "max_kb_read_per_sec",
        "Write KB/s": "max_kb_write_per_sec",
    }
    pidstat_aggregation_mode = pidstat_aggregation.value or "PID/command grouping"
    pidstat_metric_label = pidstat_metric.value or "CPU (core-summed %)"
    pidstat_metric_column = pidstat_metric_columns[pidstat_metric_label]
    pidstat_peak_metric_column = pidstat_peak_columns[pidstat_metric_label]
    pidstat_peak_metric_threshold = float(pidstat_min_peak_metric.value or 0)
    pidstat_series_limit = int(pidstat_max_series.value or 12)
    pidstat_host_value = pidstat_host.value or "All hosts"
    pidstat_command_filter_value = (pidstat_command_filter.value or "").strip()
    pidstat_host_filter_sql = ""
    if pidstat_host_value != "All hosts":
        selected_pidstat_host = pidstat_host_value.replace("'", "''")
        pidstat_host_filter_sql = f"and hostname = '{selected_pidstat_host}'"
    pidstat_command_filter_sql = ""
    if pidstat_command_filter_value:
        selected_pidstat_command = pidstat_command_filter_value.replace("'", "''")
        pidstat_command_filter_sql = f"and lower(command) like lower('%{selected_pidstat_command}%')"

    if not pidstat_available:
        pidstat_timeseries = query_df(con, "select cast(null as timestamp) as time_bucket, cast(null as varchar) as process_label, cast(null as double) as metric_value, cast(null as bigint) as concurrent_pids where false")
    elif pidstat_aggregation_mode == "Aggregate by command":
        pidstat_timeseries = query_df(
            con,
            f"""
            with selected_commands as (
                select
                    hostname,
                    container_runtime,
                    container_id,
                    pid_ns_inode,
                    command,
                    max({pidstat_peak_metric_column}) as peak_metric_value,
                    sum(samples) as samples,
                    count(distinct pid) as family_pids,
                    concat_ws(
                        ' | ',
                        hostname,
                        command,
                        case
                            when container_id is not null and container_id <> '' then concat('ctr=', container_id)
                            else null
                        end
                    ) as process_label
                from pidstat_process_summary
                where {pidstat_peak_metric_column} >= {pidstat_peak_metric_threshold}
                {pidstat_host_filter_sql}
                {pidstat_command_filter_sql}
                group by all
                order by max({pidstat_peak_metric_column}) desc nulls last, sum(samples) desc, hostname, command
                limit {pidstat_series_limit}
            )
            select
                time_bucket(interval 1 minute, m.time) as time_bucket,
                s.process_label,
                sum(m.{pidstat_metric_column}) as metric_value,
                max(s.peak_metric_value) as peak_metric_value,
                count(*) as samples,
                count(distinct m.pid) as concurrent_pids
            from pidstat_metrics m
            join selected_commands s
              on m.hostname = s.hostname
             and m.command = s.command
             and m.pid_ns_inode is not distinct from s.pid_ns_inode
             and m.container_runtime is not distinct from s.container_runtime
             and m.container_id is not distinct from s.container_id
            group by all
            order by time_bucket, process_label
            """,
        )
    else:
        pidstat_timeseries = query_df(
            con,
            f"""
            with selected_processes as (
                select
                    hostname,
                    container_runtime,
                    container_id,
                    pid_ns_inode,
                    pid,
                    command,
                    {pidstat_peak_metric_column} as peak_metric_value,
                    samples,
                    concat_ws(
                        ' | ',
                        hostname,
                        command,
                        cast(pid as varchar),
                        case
                            when container_id is not null and container_id <> '' then concat('ctr=', container_id)
                            else null
                        end
                    ) as process_label
                from pidstat_process_summary
                where {pidstat_peak_metric_column} >= {pidstat_peak_metric_threshold}
                {pidstat_host_filter_sql}
                {pidstat_command_filter_sql}
                order by {pidstat_peak_metric_column} desc nulls last, samples desc, hostname, command, pid
                limit {pidstat_series_limit}
            )
            select
                time_bucket(interval 1 minute, m.time) as time_bucket,
                s.process_label,
                avg(m.{pidstat_metric_column}) as metric_value,
                max(s.peak_metric_value) as peak_metric_value,
                count(*) as samples,
                count(distinct m.pid) as concurrent_pids
            from pidstat_metrics m
            join selected_processes s
              on m.hostname = s.hostname
             and m.pid = s.pid
             and m.command = s.command
             and m.pid_ns_inode is not distinct from s.pid_ns_inode
             and m.container_runtime is not distinct from s.container_runtime
             and m.container_id is not distinct from s.container_id
            group by all
            order by time_bucket, process_label
            """,
        )
    return (
        pidstat_aggregation_mode,
        pidstat_command_filter_value,
        pidstat_host_value,
        pidstat_metric_column,
        pidstat_metric_label,
        pidstat_peak_metric_threshold,
        pidstat_series_limit,
        pidstat_timeseries,
    )


@app.cell
def _(go, mo, pidstat_aggregation_mode, pidstat_available, pidstat_command_filter_value, pidstat_host_value, pidstat_metric_label, pidstat_peak_metric_threshold, pidstat_series_limit, pidstat_timeseries):
    if not pidstat_available:
        pidstat_chart = mo.md("Pidstat time series is unavailable because the current build has no pidstat rows.")
    elif pidstat_timeseries.empty or "error" in pidstat_timeseries.columns:
        pidstat_chart = mo.ui.table(pidstat_timeseries, pagination=False)
    else:
        pidstat_figure = go.Figure()
        for process_label, process_df in pidstat_timeseries.groupby("process_label", sort=False):
            pidstat_figure.add_trace(
                go.Scatter(
                    x=process_df["time_bucket"],
                    y=process_df["metric_value"],
                    mode="lines",
                    name=process_label,
                    hovertemplate=(
                        "Series=%{fullData.name}<br>"
                        "Time=%{x}<br>"
                        f"{pidstat_metric_label}=%{{y:.2f}}<br>"
                        "Bucket samples=%{customdata[0]}<br>"
                        "Peak selected metric=%{customdata[1]:.2f}<br>"
                        "Concurrent PIDs=%{customdata[2]}<extra></extra>"
                    ),
                    customdata=process_df[["samples", "peak_metric_value", "concurrent_pids"]].to_numpy(),
                )
            )
        pidstat_figure.update_layout(
            height=520,
            margin=dict(l=50, r=20, t=30, b=40),
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
            xaxis_title="Time",
            yaxis_title=pidstat_metric_label,
        )
        pidstat_figure.update_xaxes(rangeslider_visible=True)
        pidstat_chart = pidstat_figure
    pidstat_panel = mo.vstack(
        [
            mo.md(
                f"### Pidstat time series\n"
                f"Mode: `{pidstat_aggregation_mode}`. Showing up to `{pidstat_series_limit}` series ranked by peak `{pidstat_metric_label}` with threshold `{pidstat_peak_metric_threshold}`. "
                f"Host filter: `{pidstat_host_value}`. Command filter: `{pidstat_command_filter_value or 'none'}`. Bucketed to 1 minute."
            ),
            pidstat_chart,
        ]
    )
    pidstat_panel
    return


@app.cell
def _(con, pidstat_available, pidstat_host_value, query_df):
    pidstat_event_host_filter_sql = ""
    if pidstat_host_value != "All hosts":
        selected_event_host = pidstat_host_value.replace("'", "''")
        pidstat_event_host_filter_sql = f"where hostname = '{selected_event_host}'"

    if not pidstat_available:
        pidstat_event_volume = query_df(
            con,
            "select cast(null as timestamp) as time_bucket, cast(null as varchar) as event_type, cast(null as double) as event_count where false",
        )
    else:
        pidstat_event_volume = query_df(
            con,
            f"""
            select *
            from (
                select
                    time_bucket(interval 1 minute, process_started) as time_bucket,
                    'process' as event_type,
                    sum(coalesce(num_process_start, 0) + coalesce(num_process_stop, 0)) as event_count
                from process
                {pidstat_event_host_filter_sql}
                group by all

                union all

                select
                    time_bucket(interval 1 minute, min_event) as time_bucket,
                    'file' as event_type,
                    sum(event_count) as event_count
                from process_file
                {pidstat_event_host_filter_sql}
                group by all

                union all

                select
                    time_bucket(interval 1 minute, incr_start) as time_bucket,
                    'network' as event_type,
                    sum(total_events) as event_count
                from process_conn_incr
                {pidstat_event_host_filter_sql}
                group by all

                union all

                select
                    time_bucket(interval 1 minute, min_event) as time_bucket,
                    'registry' as event_type,
                    sum(event_count) as event_count
                from process_registry
                {pidstat_event_host_filter_sql}
                group by all

                union all

                select
                    time_bucket(interval 1 minute, first_seen) as time_bucket,
                    'image_load' as event_type,
                    sum(coalesce(num_load, 0) + coalesce(num_unload, 0)) as event_count
                from process_image_load
                {pidstat_event_host_filter_sql}
                group by all
            ) event_counts
            where time_bucket is not null
            order by time_bucket, event_type
            """,
        )
    return (pidstat_event_volume,)


@app.cell
def _(go, make_subplots, mo, pidstat_available, pidstat_event_volume, pidstat_host_value):
    if not pidstat_available:
        pidstat_event_chart = mo.md("Event-volume comparison is unavailable because the current build has no pidstat rows.")
    elif pidstat_event_volume.empty or "error" in pidstat_event_volume.columns:
        pidstat_event_chart = mo.ui.table(pidstat_event_volume, pagination=False)
    else:
        event_groups = list(pidstat_event_volume.groupby("event_type", sort=False))
        pidstat_event_figure = make_subplots(
            rows=len(event_groups),
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            subplot_titles=[event_type for event_type, _ in event_groups],
        )
        for row_num, (event_type, event_df) in enumerate(event_groups, start=1):
            pidstat_event_figure.add_trace(
                go.Scatter(
                    x=event_df["time_bucket"],
                    y=event_df["event_count"],
                    mode="lines",
                    name=event_type,
                    hovertemplate=(
                        "Type=%{fullData.name}<br>"
                        "Time=%{x}<br>"
                        "Event count=%{y:.0f}<extra></extra>"
                    ),
                )
                ,
                row=row_num,
                col=1,
            )
            pidstat_event_figure.update_yaxes(title_text=event_type, row=row_num, col=1)
        pidstat_event_figure.update_layout(
            height=max(360, 220 * len(event_groups)),
            margin=dict(l=50, r=20, t=30, b=40),
            hovermode="x unified",
            showlegend=False,
        )
        pidstat_event_figure.update_xaxes(title_text="Time", rangeslider_visible=True, row=len(event_groups), col=1)
        pidstat_event_chart = pidstat_event_figure
    pidstat_event_panel = mo.vstack(
        [
            mo.md(
                f"### Event volume over time\n"
                f"One-minute bucketed event counts by type for host filter `{pidstat_host_value}`. Each event family gets its own y-scale so lower-volume types remain visible while you compare them against the pidstat chart above."
            ),
            pidstat_event_chart,
        ]
    )
    pidstat_event_panel
    return


if __name__ == "__main__":
    app.run()
