"""Strict identities and single-owner retries without endpoint conditional PUTs."""

import copy
import io

import pytest
from botocore.exceptions import ClientError


def binding(tmp_path):
    from wintappy.coordination.protocol import dataset_binding

    return dataset_binding(
        "synthetic-dataset",
        {
            "raw": "s3://synthetic-bucket/dataset/raw_sensor/",
            "compacted": str(tmp_path / "compacted"),
        },
    )


def request(tmp_path, job_id="a" * 32):
    from wintappy.coordination.protocol import make_request

    return make_request(
        binding(tmp_path),
        job_id,
        "dbt-build",
        "2026-01-01T00:00:00Z",
        "2026-01-02T00:00:00Z",
        "2026-01-02T00:01:00Z",
    )


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.puts = []
        self.failure = ""
        self.on_put = lambda: None

    def get_object(self, Bucket, Key):
        if self.failure == "get":
            raise OSError("injected GET failure")
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kwargs):
        assert "IfNoneMatch" not in kwargs
        self.on_put()
        self.puts.append((Key, Body))
        self.objects[Key] = Body
        if self.failure == "ack":
            raise OSError("injected lost PUT acknowledgment")

    def list_objects_v2(self, **kwargs):
        if self.failure == "list":
            raise OSError("injected LIST failure")
        keys = sorted(k for k in self.objects if k.startswith(kwargs["Prefix"]))
        offset = int(kwargs.get("ContinuationToken", "0"))
        selected = keys[offset : offset + 1]
        more = offset + 1 < len(keys)
        return {
            "Contents": [{"Key": k} for k in selected],
            "IsTruncated": more,
            **({"NextContinuationToken": str(offset + 1)} if more else {}),
        }


def test_binding_normalizes_roots_and_ignores_no_active_fields(tmp_path):
    from wintappy.coordination.protocol import dataset_binding

    a = binding(tmp_path)
    b = dataset_binding(
        "synthetic-dataset",
        {
            "compacted": str(tmp_path / "nested/../compacted"),
            "raw": "s3://synthetic-bucket/dataset/raw_sensor",
        },
    )
    assert a == b and a["entity"] is None


@pytest.mark.parametrize(
    "root",
    [
        "s3://user@bucket/key",
        "s3://bucket/key?x=1",
        "s3://bucket/a/../b",
        "https://bucket/key",
        "s3://bucket/a/*",
        "s3://bucket//key",
    ],
)
def test_ambiguous_roots_fail(tmp_path, root):
    from wintappy.coordination.protocol import CoordinationError, dataset_binding

    with pytest.raises(CoordinationError):
        dataset_binding("synthetic-dataset", {"raw": root, "compacted": str(tmp_path)})


def test_active_entity_binding_is_complete_and_roots_do_not_overlap(tmp_path):
    from wintappy.coordination.protocol import CoordinationError, dataset_binding

    roots = {
        "raw": "/synthetic/raw",
        "compacted": "/synthetic/compacted",
        "entity": "/synthetic/entities",
        "metrics": "/synthetic/metrics",
    }
    entity = {
        "namespace": "synthetic-pid-v2",
        "hosts": ["synthetic-host"],
        "identity_policy": "reject",
        "identity_max_ratio": 0.05,
    }
    bound = dataset_binding("synthetic-dataset", roots, entity)
    assert bound["entity"]["hosts"] == ["SYNTHETIC-HOST"]
    for changed_roots, changed_entity in [
        (roots, None),
        ({**roots, "metrics": "/synthetic/entities/metrics"}, entity),
    ]:
        with pytest.raises(CoordinationError):
            dataset_binding("synthetic-dataset", changed_roots, changed_entity)


@pytest.mark.parametrize("damage", ["digest", "job", "version", "unknown", "window"])
def test_request_corruption_rejected(tmp_path, damage):
    from wintappy.coordination.protocol import CoordinationError, validate_message

    value = request(tmp_path)
    key, changed = {
        "digest": ("request_sha256", "b" * 64),
        "job": ("job_id", "../bad"),
        "version": ("protocol_version", True),
        "unknown": ("extra", 1),
        "window": ("end", "2025-01-01T00:00:00Z"),
    }[damage]
    value[key] = changed
    with pytest.raises(CoordinationError):
        validate_message(value)


def test_duplicate_json_and_oversized_messages_fail(tmp_path):
    from wintappy.coordination.protocol import (
        CoordinationError,
        decode_message,
        encode_message,
    )

    value = encode_message(request(tmp_path))
    with pytest.raises(CoordinationError):
        decode_message(b'{"job_id":"ambiguous",' + value[1:])
    with pytest.raises(CoordinationError):
        decode_message(b" " * (1024 * 1024 + 1))


def test_same_key_retry_after_lost_ack_is_identical_and_conflict_never_overwrites(
    tmp_path,
):
    from wintappy.coordination.protocol import CoordinationError, MessageStore

    client = FakeS3()
    store = MessageStore("s3://synthetic-bucket/coordination", client)
    value = request(tmp_path)
    client.failure = "ack"
    with pytest.raises(CoordinationError):
        store.publish(value)
    original = copy.deepcopy(client.objects)
    client.failure = ""
    store.publish(value)
    assert client.objects == original and len(client.puts) == 1
    changed = request(tmp_path)
    changed["created_at"] = "2026-01-02T00:02:00Z"
    from wintappy.coordination.protocol import request_digest

    changed["request_sha256"] = request_digest(changed)
    with pytest.raises(CoordinationError, match="conflict"):
        store.publish(changed)
    assert client.objects == original


def test_paginated_listing_and_api_errors_are_not_empty_inputs(tmp_path):
    from wintappy.coordination.protocol import CoordinationError, MessageStore

    client = FakeS3()
    store = MessageStore("s3://synthetic-bucket/coordination", client)
    for job_id in ["a" * 32, "b" * 32]:
        store.publish(request(tmp_path, job_id))
    assert len(store.messages("request")) == 2
    for failure in ["list", "get"]:
        client.failure = failure
        with pytest.raises(CoordinationError):
            store.messages("request")


@pytest.mark.parametrize("field", ["action", "identity_policy"])
def test_unhashable_field_is_a_protocol_error(tmp_path, field):
    from wintappy.coordination.protocol import (
        CoordinationError,
        dataset_binding,
        request_digest,
        validate_message,
    )

    with pytest.raises(CoordinationError):
        if field == "action":
            value = request(tmp_path)
            value[field] = []
            value["request_sha256"] = request_digest(value)
            validate_message(value)
        else:
            dataset_binding(
                "synthetic-dataset",
                {
                    "raw": "/synthetic/raw",
                    "compacted": "/synthetic/compacted",
                    "entity": "/synthetic/entities",
                    "metrics": "/synthetic/metrics",
                },
                {
                    "namespace": "synthetic-v2",
                    "hosts": ["SYNTHETIC"],
                    "identity_policy": [],
                    "identity_max_ratio": 0.05,
                },
            )


def test_stream_transfer_failure_is_not_missing_or_a_raw_exception(tmp_path):
    from wintappy.coordination.protocol import CoordinationError, MessageStore

    class BrokenBody:
        closed = False

        def read(self, size):
            raise OSError("injected body transfer failure")

        def close(self):
            self.closed = True

    body = BrokenBody()
    client = FakeS3()
    client.get_object = lambda **kwargs: {"Body": body}
    store = MessageStore("s3://synthetic-bucket/coordination", client)
    with pytest.raises(CoordinationError, match="GET"):
        store.read("request", "a" * 32)
    assert body.closed


def test_aggregate_listing_budget_fails_without_truncating_jobs(tmp_path, monkeypatch):
    from wintappy.coordination import protocol

    monkeypatch.setattr(protocol, "MAX_LIST_BYTES", 1, raising=False)
    client = FakeS3()
    store = protocol.MessageStore("s3://synthetic-bucket/coordination", client)
    store.publish(request(tmp_path))
    with pytest.raises(protocol.CoordinationError, match="budget"):
        store.messages("request")


def test_cycle_of_continuation_tokens_is_rejected(tmp_path):
    from wintappy.coordination.protocol import CoordinationError, MessageStore

    client = FakeS3()
    tokens = iter(["a", "b", "a"])

    def cycling(**kwargs):
        try:
            token = next(tokens)
        except StopIteration:
            pytest.fail("Cyclic listing was not rejected within three pages")
        return {"Contents": [], "IsTruncated": True, "NextContinuationToken": token}

    client.list_objects_v2 = cycling
    with pytest.raises(CoordinationError, match="token"):
        MessageStore("s3://synthetic-bucket/coordination", client).messages("request")


def test_deep_json_is_a_bounded_protocol_failure():
    from wintappy.coordination.protocol import CoordinationError, decode_message

    with pytest.raises(CoordinationError):
        decode_message(b"[" * 2000 + b"0" + b"]" * 2000)


def test_short_stream_reads_preserve_complete_bytes_and_whitespace_counts_in_budget(
    tmp_path, monkeypatch
):
    from wintappy.coordination import protocol

    client = FakeS3()
    store = protocol.MessageStore("s3://synthetic-bucket/coordination", client)
    value = request(tmp_path)
    encoded = protocol.encode_message(value) + b" " * 100
    client.objects[store.key("request", value["job_id"])] = encoded

    class ShortBody(io.BytesIO):
        def read(self, size):
            return super().read(min(size, 17))

    client.get_object = lambda Bucket, Key: {"Body": ShortBody(client.objects[Key])}
    assert store.read("request", value["job_id"]) == value
    monkeypatch.setattr(protocol, "MAX_LIST_BYTES", len(encoded) - 1)
    with pytest.raises(protocol.CoordinationError, match="budget"):
        store.messages("request")
