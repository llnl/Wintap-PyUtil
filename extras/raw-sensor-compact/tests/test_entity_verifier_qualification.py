"""Independent compatibility/budget/publication controls, not ownership acceptance."""

import json
from copy import deepcopy

import pytest
from test_completion_verifier import rewrite_archive
from test_entities import HOURS, observation, seed
from test_entities import stores as stores_fixture
from test_entity_operation_verifier import (
    EXECUTION,
    SUPERVISION,
    completed,
    original_observation,
    scope,
    snapshot,
    verify_entities,
    verify_inputs,
)
from test_entity_verifier_review import evolve

from raw_sensor_compact import entity_input_verifier as loader
from raw_sensor_compact import entity_operation_verifier as verifier
from raw_sensor_compact.cli import run
from raw_sensor_compact.config import CompactionError, Dataset
from raw_sensor_compact.entities import load_inputs, unknown_hash
from raw_sensor_compact.inventory import Store, discover
from raw_sensor_compact.phase_evidence import canonical, identity
from raw_sensor_compact.processing_metrics import ProcessingMetricsStore

stores = stores_fixture


def inventories(stores, hours=HOURS):
    return [discover(s, hours, ("raw_process",)) for s in stores[:2]]


@pytest.mark.parametrize("unicode", [False, True])
@pytest.mark.parametrize("overlap", [False, True])
def test_streaming_loader_matches_legacy_exclusions_and_projection_repeatedly(
    conn, stores, tmp_path, unicode, overlap
):
    args, _ = scope(conn, stores, tmp_path, multi=True)
    args.entity_host = ["HOST", "HÓST"] if unicode else ["HOST"]
    args.unknown_identity_policy = "quarantine"
    args.unknown_identity_max_ratio = 1.0
    rows = [
        observation(pidhash=None, pid=None),
        observation(pidhash=unknown_hash("HOST"), pid=-1),
        observation(hostname=None, pidhash="unknown", pid=None),
        observation(hostname="", pidhash="unknown", pid=None),
        observation(parentpidhash='quoted"\\parent', pid=-1),
    ]
    if unicode:
        rows += [
            observation(hostname=" hóSt ", pidhash="😀é", parentpidhash="λ", pid=None),
            observation(hostname="HÓST", pidhash=unknown_hash("HÓST"), pid=-1),
        ]
    for index, row in enumerate(rows):
        seed(
            conn,
            stores[0],
            row,
            hour=f"{index % 2:02}",
            filename=f"bad-{index}.parquet",
        )
        if overlap:
            seed(
                conn,
                stores[1],
                row,
                hour=f"{index % 2:02}",
                filename=f"bad-{index}.parquet",
            )
    selected = inventories(stores)
    expected = load_inputs(
        conn, selected, identity_policy="quarantine", identity_max_ratio=1.0
    )
    projected = conn.execute("SELECT * FROM entity_input ORDER BY ALL").fetchall()
    before = snapshot(tmp_path)
    for _ in range(3):
        result = loader.load_process_inputs(
            conn, selected, args, HOURS, deadline=lambda: None
        )
        assert result == expected
        assert (
            conn.execute("SELECT * FROM entity_input ORDER BY ALL").fetchall()
            == projected
        )
    assert snapshot(tmp_path) == before
    assert result["rows"] > 0 and len(result["hours"]) == 2


@pytest.mark.parametrize("raw_host", ["straße", "ﬃ", "\thost\t", "\x1chost\x1c"])
def test_streaming_loader_matches_legacy_unicode_quarantine_projection(
    conn, stores, tmp_path, raw_host
):
    from test_phase_evidence import invocation

    args = invocation(stores, tmp_path, "entities-replay")
    # This lower-level loader check preserves the writer's SQL projection, not
    # a new Python-normalization contract or supported public invocation scope.
    args.entity_host = [
        conn.execute("SELECT upper(trim(?))", [raw_host]).fetchall()[0][0]
    ]
    args.unknown_identity_policy = "quarantine"
    args.unknown_identity_max_ratio = 1.0
    seed(
        conn,
        stores[0],
        observation(hostname=raw_host, pidhash=unknown_hash(raw_host), pid=None),
        filename="sentinel.parquet",
    )
    seed(
        conn,
        stores[0],
        observation(hostname=args.entity_host[0], pidhash="valid-instance"),
    )
    selected = inventories(stores)
    expected = load_inputs(
        conn, selected, identity_policy="quarantine", identity_max_ratio=1.0
    )
    projected = conn.execute("SELECT * FROM entity_input ORDER BY ALL").fetchall()
    assert expected["rows"] == 1
    before = snapshot(tmp_path)
    assert (
        loader.load_process_inputs(conn, selected, args, HOURS, deadline=lambda: None)
        == expected
    )
    assert (
        conn.execute("SELECT * FROM entity_input ORDER BY ALL").fetchall() == projected
    )
    assert snapshot(tmp_path) == before


def test_exclusion_materialization_is_sql_owned_with_only_single_row_fetches(
    conn, stores, tmp_path
):
    args, _ = scope(conn, stores, tmp_path)
    args.unknown_identity_policy = "quarantine"
    args.unknown_identity_max_ratio = 1.0
    source = seed(conn, stores[0])
    target = source.with_name("many-exclusions.parquet")
    conn.execute(
        "COPY (SELECT * EXCLUDE (range) REPLACE ('excluded-' || range AS pidhash, "
        "NULL::BIGINT AS pid) FROM read_parquet(?) "
        "CROSS JOIN range(257)) TO ? (FORMAT PARQUET)",
        [str(source), str(target)],
    )
    selected = inventories(stores)
    expected = load_inputs(
        conn, selected, identity_policy="quarantine", identity_max_ratio=1.0
    )
    counts = {"records": 0, "encoded": 0, "deadlines": 0}

    class StreamingConnection:
        def __init__(self, connection):
            self.connection = connection
            self.query = ""

        def execute(self, query, *params):
            self.query = query.lower()
            self.connection.execute(query, *params)
            return self

        def fetchone(self):
            row = self.connection.fetchone()
            if row is not None:
                if self.query == "select * from verifier_exclusion_input":
                    assert len(row) == 8
                    counts["records"] += 1
                elif self.query.startswith("select encoded from verifier_exclusions"):
                    assert len(row) == 1
                    counts["encoded"] += 1
            return row

        def fetchall(self):
            assert "from verifier_exclusion_input" not in self.query
            assert not self.query.startswith("select encoded from verifier_exclusions")
            return self.connection.fetchall()

        def fetchmany(self, *params):
            raise AssertionError("Diagnostic batches must not materialize in Python")

        def create_function(self, *params, **kwargs):
            raise AssertionError("Verifier must not depend on Python UDF registration")

        def cursor(self):
            return StreamingConnection(self.connection.cursor())

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            self.connection.close()

    def deadline():
        counts["deadlines"] += 1

    before = snapshot(tmp_path)
    result = loader.load_process_inputs(
        StreamingConnection(conn), selected, args, HOURS, deadline=deadline
    )
    assert result == expected and result["rows"] == 257
    assert counts["records"] == 257 and counts["encoded"] == 514
    assert counts["deadlines"] >= counts["records"] + counts["encoded"]
    assert snapshot(tmp_path) == before


def test_selected_file_budget_is_shared_across_raw_and_compacted(
    conn, stores, tmp_path, monkeypatch
):
    args, anchor = scope(conn, stores, tmp_path)
    seed(conn, stores[1], observation(pidhash="compacted-instance"))
    args.max_files = 1
    seen = []
    original = verifier._object_evidence

    def observed(store, inventory, connection):
        seen.append(store.dataset.root)
        yield from original(store, inventory, connection)

    monkeypatch.setattr(verifier, "_object_evidence", observed)
    before = snapshot(tmp_path)
    with pytest.raises(CompactionError, match="selected-input file budget"):
        verify_inputs(args, anchor)
    assert seen == [stores[0].dataset.root]
    assert snapshot(tmp_path) == before


@pytest.mark.parametrize("replay", [False, True])
@pytest.mark.parametrize("historical", [False, True])
def test_nonzero_parent_conflicts_are_verified_before_historical_evolution(
    conn, stores, tmp_path, replay, historical
):
    args, anchor = scope(conn, stores, tmp_path, replay=replay)
    seed(
        conn,
        stores[0],
        observation(parentpidhash="other-parent", parentpid=8),
        filename="conflicting-parent.parquet",
    )
    original = original_observation(conn, stores, args, anchor)
    report, code = run(args)
    assert code == 0 and report["hosts"][0]["parent_conflicts"] == 1
    publication = verify_entities(args, original)
    if historical:
        evolve(conn, stores, args)
        assert (
            verify_entities(args, original, publication=publication)[
                "authority_high_water"
            ]["generation"]
            > publication["authority_high_water"]["generation"]
        )
    metrics = ProcessingMetricsStore(Store(Dataset.parse(args.processing_metrics_root)))

    def corrupt(records):
        records[2]["data"]["hosts"][0]["parent_conflicts"] = 0
        row = records[3]["data"]["rows"][0]
        assert row["entity_parent_conflicts"] == 1
        row["entity_parent_conflicts"] = 0
        records[4]["data"]["row"] = deepcopy(row)
        metrics.publish(conn, HOURS[0], row)

    rewrite_archive(args, corrupt)
    before = snapshot(tmp_path)
    message = "archive/scope mismatch" if historical else "current snapshot statistics"
    with pytest.raises(CompactionError, match=message):
        verify_entities(args, original, publication=publication)
    assert snapshot(tmp_path) == before


def test_empty_loader_preserves_empty_fingerprints_and_can_be_reused(
    conn, stores, tmp_path
):
    from test_phase_evidence import invocation

    args = invocation(stores, tmp_path, "entities-replay")
    selected = inventories(stores)
    expected = load_inputs(conn, selected)
    for _ in range(3):
        assert (
            loader.load_process_inputs(
                conn, selected, args, HOURS, deadline=lambda: None
            )
            == expected
        )
    assert expected["fingerprint"] == "" and expected["hours"] == {}


@pytest.mark.parametrize("ceiling", [0.0, 0.5])
def test_per_hour_identity_ratio_cannot_be_diluted_by_other_hours(
    conn, stores, tmp_path, ceiling
):
    args, _ = scope(conn, stores, tmp_path, multi=True)
    args.unknown_identity_policy = "quarantine"
    args.unknown_identity_max_ratio = ceiling
    for index in range(5):
        seed(
            conn,
            stores[0],
            observation(pidhash=f"valid-{index}"),
            filename=f"valid-{index}.parquet",
        )
    seed(conn, stores[0], observation(pidhash="UNKNOWN", pid=None), hour="01")
    before = snapshot(tmp_path)
    with pytest.raises(CompactionError, match="bound policy"):
        loader.load_process_inputs(
            conn, inventories(stores), args, HOURS, deadline=lambda: None
        )
    assert snapshot(tmp_path) == before


@pytest.mark.parametrize("damage", ["unknown", "missing", "negative", "lifecycle"])
def test_excluded_rows_cannot_hide_unexpected_hosts_or_lifecycle(
    conn, stores, tmp_path, damage
):
    args, anchor = scope(conn, stores, tmp_path)
    args.unknown_identity_policy = "quarantine"
    args.unknown_identity_max_ratio = 1.0
    changes = {"hostname": "UNEXPECTED-SYNTHETIC-HOST"}
    if damage == "unknown":
        changes["pidhash"] = "UNKNOWN"
    elif damage == "missing":
        changes["pid"] = None
    elif damage == "negative":
        changes["pid"] = -1
    else:
        changes = {"activitytype": "PAUSE", "pidhash": "UNKNOWN"}
    seed(conn, stores[0], observation(**changes), filename="excluded.parquet")
    before = snapshot(tmp_path)
    with pytest.raises(CompactionError, match="lifecycle or unexpected host"):
        verify_inputs(args, anchor)
    assert snapshot(tmp_path) == before


def test_exclusion_record_budget_refuses_before_python_canonicalization(
    conn, stores, tmp_path, monkeypatch
):
    args, _ = scope(conn, stores, tmp_path)
    args.unknown_identity_policy = "quarantine"
    args.unknown_identity_max_ratio = 1.0
    seed(
        conn,
        stores[0],
        observation(pidhash="x" * 20000, pid=None),
        filename="large.parquet",
    )
    calls = []

    def forbidden(*a):
        calls.append(True)
        raise AssertionError("oversized diagnostic reached Python canonicalization")

    monkeypatch.setattr(loader, "_record", forbidden)
    before = snapshot(tmp_path)
    with pytest.raises(CompactionError, match="record exceeds byte budget"):
        loader.load_process_inputs(
            conn, inventories(stores), args, HOURS, deadline=lambda: None
        )
    assert not calls and snapshot(tmp_path) == before


def test_aggregate_diagnostic_budget_refuses_instead_of_pruning(
    conn, stores, tmp_path, monkeypatch
):
    args, _ = scope(conn, stores, tmp_path, multi=True)
    args.unknown_identity_policy = "quarantine"
    args.unknown_identity_max_ratio = 1.0
    for hour in ("00", "01"):
        seed(
            conn,
            stores[0],
            observation(pid=None),
            hour=hour,
            filename="excluded.parquet",
        )
    selected = inventories(stores)
    baseline = loader.load_process_inputs(
        conn, selected, args, HOURS, deadline=lambda: None
    )
    monkeypatch.setattr(
        loader, "MAX_DIAGNOSTIC_BYTES", len(canonical(baseline).encode()) - 1
    )
    before = snapshot(tmp_path)
    with pytest.raises(CompactionError, match="report exceeds byte budget; no pruning"):
        loader.load_process_inputs(conn, selected, args, HOURS, deadline=lambda: None)
    assert snapshot(tmp_path) == before


@pytest.mark.parametrize("statistic", ["rows", "snapshot_bytes", "parent_conflicts"])
@pytest.mark.parametrize("historical", [False, True])
def test_retained_observation_rejects_consistently_rehashed_publication_statistics(
    conn, stores, tmp_path, statistic, historical
):
    args, original = completed(conn, stores, tmp_path)
    publication = verify_entities(args, original)
    if historical:
        evolve(conn, stores, args)
    metrics = ProcessingMetricsStore(Store(Dataset.parse(args.processing_metrics_root)))

    def corrupt(records):
        records[2]["data"]["hosts"][0][statistic] += 1
        for index, row in enumerate(records[3]["data"]["rows"]):
            if statistic != "snapshot_bytes":
                key = (
                    "entity_rows" if statistic == "rows" else "entity_parent_conflicts"
                )
                row[key] += 1
            records[4 + index]["data"]["row"] = deepcopy(row)
            metrics.publish(conn, HOURS[index], row)

    rewrite_archive(args, corrupt)
    before = snapshot(tmp_path)
    message = "archive/scope mismatch" if historical else "current snapshot statistics"
    with pytest.raises(CompactionError, match=message):
        verify_entities(args, original, publication=publication)
    assert snapshot(tmp_path) == before


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "extra",
        "version",
        "archive",
        "checkpoint",
        "execution",
        "supervision",
        "original",
        "inputs",
        "window",
        "run",
        "hours_bool",
        "records_bool",
        "recursive",
        "generation",
        "generation_id",
        "binding",
        "state_digest",
        "future",
    ],
)
def test_historical_publication_requires_exact_independent_original_observation(
    conn, stores, tmp_path, damage
):
    args, original = completed(conn, stores, tmp_path)
    publication = verify_entities(args, original)
    evolve(conn, stores, args)
    publication = deepcopy(publication)
    fields = {
        "archive": "archive_sha256",
        "checkpoint": "checkpoint_sha256",
        "execution": "execution_sha256",
        "supervision": "supervision_sha256",
        "original": "original_sha256",
        "inputs": "inputs_sha256",
    }
    if damage in fields:
        publication[fields[damage]] = "0" * 64
    elif damage == "missing":
        publication.pop("archive_sha256")
    elif damage == "extra":
        publication["unrecorded"] = True
    elif damage == "version":
        publication["version"] = True
    elif damage == "window":
        publication["window_end"] = "2026-01-01T02:00:00Z"
    elif damage == "run":
        publication["run_id"] = "0" * 32
    elif damage == "hours_bool":
        publication["checkpoint_hours"] = True
    elif damage == "records_bool":
        publication["archive_records"] = True
    elif damage == "recursive":
        publication["publication_sha256"] = "0" * 64
    elif damage == "state_digest":
        publication["entity_state_sha256"] = "not-a-digest"
    elif damage == "future":
        publication["authority_high_water"]["generation"] += 1
    else:
        key = "binding_sha256" if damage == "binding" else damage
        publication["authority_high_water"][key] = (
            True if key == "generation" else "0" * 64
        )
    before = snapshot(tmp_path)
    with pytest.raises(CompactionError):
        verify_entities(args, original, publication=publication)
    assert snapshot(tmp_path) == before


def arguments(args):
    result = [args.command]
    for key in (
        "source_root",
        "destination_root",
        "process_entity_root",
        "processing_metrics_root",
        "entity_namespace",
        "start_hour",
        "end_hour",
        "run_id",
        "coordinator_run_id",
        "run_evidence_directory",
    ):
        result += ["--" + key.replace("_", "-"), getattr(args, key)]
    for event in args.event_type:
        result += ["--event-type", event]
    for host in args.entity_host:
        result += ["--entity-host", host]
    return result


@pytest.mark.parametrize("replay", [False, True])
@pytest.mark.parametrize("stage", ["inputs", "completion", "historical"])
def test_genuine_canonical_request_executes_independent_verifier_and_binds_context(
    conn, stores, tmp_path, capsys, replay, stage
):
    args, anchor = scope(conn, stores, tmp_path, replay=replay)
    original = original_observation(conn, stores, args, anchor)
    publication = None
    if stage != "inputs":
        assert run(args)[1] == 0
        if stage == "historical":
            publication = verify_entities(args, original)
            evolve(conn, stores, args)
    request = {
        "version": 1,
        "operation": "entity-only-inputs"
        if stage == "inputs"
        else "entity-only-completion",
        "arguments": arguments(args),
        "execution_sha256": EXECUTION,
        "ledger_id": "a" * 32,
        "dataset_sha256": "b" * 64,
        "authority_high_water": anchor,
        "original": original,
    }
    if stage != "inputs":
        request.update(
            supervision_sha256=SUPERVISION,
            intent_sha256="c" * 64,
            publication=publication,
        )
    path = tmp_path / "request.json"
    path.write_text(canonical(request))
    before = snapshot(tmp_path)
    assert (
        verifier.main(["--request", str(path), "--request-sha256", identity(request)])
        == 0
    )
    captured = capsys.readouterr()
    assert captured.err == ""
    result = json.loads(captured.out)
    for key in ("ledger_id", "dataset_sha256", "execution_sha256"):
        assert result[key] == request[key]
    assert result["request_sha256"] == identity(request)
    assert result["operation"] == request["operation"]
    if stage == "inputs":
        assert {k: result[k] for k in verifier.ORIGINAL_FIELDS} == original
    else:
        assert result["intent_sha256"] == request["intent_sha256"]
        assert result["publication_sha256"] == (
            identity(publication) if publication else None
        )
    assert snapshot(tmp_path) == before
