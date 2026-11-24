"""Synthetic contracts for column-specific ANY_VALUE decisions."""

import duckdb
import pytest

pytestmark = pytest.mark.contract


def test_invariant_os_pid_any_value_matches_deterministic_value():
    """Repeated observations may safely use ANY_VALUE when the field is invariant."""
    with duckdb.connect() as connection:
        connection.execute("""
            create table observations as select * from (values
                ('lifetime-a', 42, 'initial-image'),
                ('lifetime-a', 42, 'wrapper-image'),
                ('lifetime-a', 42, 'final-image')
            ) as t(pid_hash, os_pid, process_name)
        """)
        row = connection.execute("""
            select
                any_value(os_pid) as arbitrary_pid,
                min(os_pid) as deterministic_pid,
                count(distinct os_pid) as distinct_pids,
                count(distinct process_name) as distinct_names
            from observations
            group by pid_hash
        """).fetchone()

    assert row == (42, 42, 1, 3)


def test_identical_duplicates_are_not_a_conflict_and_nulls_are_ignored():
    """Duplicate identical values differ from distinct candidates semantically."""
    with duckdb.connect() as connection:
        connection.execute("""
            create table values_for_group as select * from (values
                ('same',), ('same',), (null), (null)
            ) as t(value)
        """)
        row = connection.execute("""
            select
                any_value(value) as arbitrary_value,
                min(value) as deterministic_value,
                count(distinct value) as distinct_non_null_values
            from values_for_group
        """).fetchone()

    assert row == ("same", "same", 1)


def test_all_null_any_value_group_has_documented_null_result():
    with duckdb.connect() as connection:
        row = connection.execute("""
            select any_value(value), count(distinct value)
            from (values (null), (null)) as t(value)
        """).fetchone()

    assert row == (None, 0)


def test_distinct_candidates_are_visible_instead_of_asserting_any_value_choice():
    """ANY_VALUE is not a policy when eligible values conflict."""
    with duckdb.connect() as connection:
        row = connection.execute("""
            select
                count(distinct value) as distinct_non_null_values,
                list_sort(list(distinct value)) as candidates
            from (values ('initial'), ('initial'), ('final')) as t(value)
        """).fetchone()

    assert row == (2, ["final", "initial"])


def test_name_and_path_are_selected_as_one_observation_pair():
    """The selected image tuple must be present as an input row."""
    with duckdb.connect() as connection:
        row = connection.execute("""
            with observations as (
                select * from (values
                    (1, 'start', 'shell', '/bin/shell'),
                    (2, 'exec_attempt', 'tool', '/candidate/tool'),
                    (3, 'exec_success', 'tool', '/usr/bin/tool'),
                    (4, 'refresh', 'tool', '/usr/bin/tool')
                ) as t(observation_order, evidence_kind, process_name, process_path)
            ), ranked as (
                select *, row_number() over (
                    order by observation_order desc
                ) as row_rank
                from observations
                where evidence_kind in ('start', 'exec_success', 'refresh')
            )
            select process_name, process_path
            from ranked
            where row_rank = 1
        """).fetchone()

    assert row == ("tool", "/usr/bin/tool")


def test_current_state_refresh_is_as_of_and_failed_attempt_is_ineligible():
    """A later Refresh wins only for an as-of query that can observe it."""
    with duckdb.connect() as connection:
        connection.execute("""
            create table observations as select * from (values
                (10, 'start', 'shell', '/bin/shell'),
                (11, 'exec_attempt', 'candidate', '/candidate/tool'),
                (12, 'exec_success', 'tool', '/usr/bin/tool'),
                (13, 'refresh', 'tool', '/usr/bin/tool'),
                (15, 'refresh', 'other', '/usr/bin/other')
            ) as t(observation_time, evidence_kind, process_name, process_path)
        """)
        query = """
            with eligible as (
                select *, row_number() over (
                    order by
                        case when evidence_kind = 'refresh' then 1 else 0 end desc,
                        observation_time desc
                ) as row_rank
                from observations
                where observation_time <= ?
                  and evidence_kind in ('start', 'exec_success', 'refresh')
            )
            select process_name, process_path
            from eligible
            where row_rank = 1
        """
        as_of_14 = connection.execute(query, [14]).fetchone()
        as_of_12 = connection.execute(query, [12]).fetchone()

    assert as_of_14 == ("tool", "/usr/bin/tool")
    assert as_of_12 == ("tool", "/usr/bin/tool")
