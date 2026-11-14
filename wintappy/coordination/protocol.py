"""Version-one single-owner messages; no conditional-write/expiry assumptions."""

import hashlib
import json
import logging
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit

from botocore.exceptions import BotoCoreError, ClientError

MAX_MESSAGE_BYTES = 1024 * 1024
MAX_MESSAGES = 10_000
MAX_LIST_BYTES = 64 * 1024 * 1024
MAX_LIST_PAGES = 10_000
LOG = logging.getLogger(__name__)
PREFIXES = {
    "request": "requests",
    "grant": "grants",
    "release": "releases",
    "cancellation": "cancellations",
}
COMMON = {"protocol_version", "kind", "job_id", "request_sha256", "binding"}
FIELDS = {
    "request": {"action", "start", "end", "created_at"},
    "grant": {"grant_id", "granted_at", "mode"},
    "release": {"grant_id", "completed_at", "exit_code", "children_stopped"},
    "cancellation": {"cancelled_at", "waiting_stopped"},
}


class CoordinationError(RuntimeError):
    """Ownership is not inferred from failed access or uncertain publication."""


def canonical(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError, RecursionError):
        raise CoordinationError("Invalid canonical JSON value") from None


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def identifier(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{32}", value):
        raise CoordinationError("Identity must be 32 lowercase hexadecimal characters")
    return value


def timestamp(value, *, hour=False):
    if not isinstance(value, str):
        raise CoordinationError("Timestamp must be canonical UTC")
    try:
        result = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        raise CoordinationError("Timestamp must be canonical UTC") from None
    if (
        result.strftime("%Y-%m-%dT%H:%M:%SZ") != value
        or hour
        and (result.minute or result.second)
    ):
        raise CoordinationError("Timestamp must be canonical UTC hour")
    return result


def canonical_root(value):
    if not isinstance(value, str) or not value or len(value) > 4096:
        raise CoordinationError("Input root must be a bounded path")
    if any(ord(c) < 32 for c in value) or any(c in value for c in "*?[]#"):
        raise CoordinationError("Ambiguous input root")
    if value.startswith("s3://"):
        uri = urlsplit(value)
        path = uri.path.rstrip("/")
        if (
            not re.fullmatch(r"[a-z0-9][a-z0-9.-]*", uri.netloc)
            or uri.query
            or uri.fragment
            or "%" in value
            or "\\" in value
            or any(c.isspace() for c in value)
            or "//" in uri.path
            or any(p in {".", ".."} for p in path.split("/"))
        ):
            raise CoordinationError("Ambiguous S3 input root")
        return "s3://" + uri.netloc + path
    if "://" in value:
        raise CoordinationError("Unsupported input root scheme")
    return str(Path(value).expanduser().resolve())


def dataset_binding(dataset_id, roots, entity=None):
    if not isinstance(dataset_id, str) or not re.fullmatch(
        r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}", dataset_id
    ):
        raise CoordinationError("Invalid dataset identity")
    if not isinstance(roots, dict) or set(roots) not in (
        {"raw", "compacted"},
        {"raw", "compacted", "entity", "metrics"},
    ):
        raise CoordinationError("Binding requires exactly the active input roots")
    resolved = {key: canonical_root(value) for key, value in sorted(roots.items())}
    values = list(resolved.values())
    for i, first in enumerate(values):
        for second in values[i + 1 :]:
            a, b = first.rstrip("/") + "/", second.rstrip("/") + "/"
            if a.startswith(b) or b.startswith(a):
                raise CoordinationError("Input roots must not overlap")
    if "entity" in roots:
        if not isinstance(entity, dict) or set(entity) != {
            "namespace",
            "hosts",
            "identity_policy",
            "identity_max_ratio",
        }:
            raise CoordinationError("Active entity binding must be complete")
        namespace, hosts = entity["namespace"], entity["hosts"]
        ratio = entity["identity_max_ratio"]
        if (
            not isinstance(namespace, str)
            or not namespace
            or len(namespace) > 256
            or any(ord(c) < 32 for c in namespace)
            or not isinstance(hosts, list)
            or not 0 < len(hosts) <= 1024
            or any(
                not isinstance(h, str)
                or not h.strip()
                or len(h) > 256
                or any(ord(c) < 32 for c in h)
                for h in hosts
            )
            or not isinstance(entity["identity_policy"], str)
            or entity["identity_policy"] not in {"reject", "quarantine"}
            or type(ratio) not in (int, float)
            or not math.isfinite(ratio)
            or not 0 <= ratio <= 1
        ):
            raise CoordinationError("Invalid entity identity policy/hosts/namespace")
        entity = {
            **entity,
            "hosts": sorted({h.strip().upper() for h in hosts}),
            "identity_max_ratio": float(ratio),
        }
    elif entity is not None:
        raise CoordinationError("Disabled entities cannot bind unused entity settings")
    return {"dataset_id": dataset_id, "roots": resolved, "entity": entity}


def validate_binding(value):
    if not isinstance(value, dict) or set(value) != {"dataset_id", "roots", "entity"}:
        raise CoordinationError("Invalid dataset binding shape")
    expected = dataset_binding(value["dataset_id"], value["roots"], value["entity"])
    if canonical(value) != canonical(expected):
        raise CoordinationError("Dataset binding must be canonical")
    return value


def separate_transport_root(root, binding):
    root = canonical_root(root).rstrip("/") + "/"
    validate_binding(binding)
    for value in binding["roots"].values():
        value = value.rstrip("/") + "/"
        if root.startswith(value) or value.startswith(root):
            raise CoordinationError(
                "Coordination transport root must not overlap data scan roots"
            )


def request_digest(value):
    return digest({k: v for k, v in value.items() if k != "request_sha256"})


def validate_message(value):
    if not isinstance(value, dict) or not isinstance(value.get("kind"), str):
        raise CoordinationError("Invalid message shape")
    kind = value["kind"]
    if kind not in FIELDS or set(value) != COMMON | FIELDS[kind]:
        raise CoordinationError("Invalid message fields")
    if type(value["protocol_version"]) is not int or value["protocol_version"] != 1:
        raise CoordinationError("Unsupported protocol version")
    identifier(value["job_id"])
    validate_binding(value["binding"])
    if not isinstance(value["request_sha256"], str) or not re.fullmatch(
        r"[a-f0-9]{64}", value["request_sha256"]
    ):
        raise CoordinationError("Invalid request digest")
    if kind == "request":
        if not isinstance(value["action"], str) or value["action"] not in {
            "dbt-build",
            "dbt-source",
            "dbt-test",
            "dbt-docs",
            "shared-command",
        }:
            raise CoordinationError("Unsupported reader action")
        start, end = timestamp(value["start"]), timestamp(value["end"])
        timestamp(value["created_at"])
        if start >= end or value["request_sha256"] != request_digest(value):
            raise CoordinationError("Request digest/window mismatch")
    elif kind == "grant":
        identifier(value["grant_id"])
        timestamp(value["granted_at"])
        if value["mode"] != "read":
            raise CoordinationError("Unsupported grant mode")
    elif kind == "release":
        identifier(value["grant_id"])
        timestamp(value["completed_at"])
        if (
            value["children_stopped"] is not True
            or type(value["exit_code"]) is not int
            or not -255 <= value["exit_code"] <= 255
        ):
            raise CoordinationError("Release requires stopped children and exit status")
    else:
        timestamp(value["cancelled_at"])
        if value["waiting_stopped"] is not True:
            raise CoordinationError("Cancellation must forbid acting on a late grant")
    if len(canonical(value).encode()) > MAX_MESSAGE_BYTES:
        raise CoordinationError("Message exceeds byte budget")
    return value


def make_request(binding, job_id, action, start, end, created_at):
    value = {
        "protocol_version": 1,
        "kind": "request",
        "job_id": job_id,
        "binding": binding,
        "action": action,
        "start": start,
        "end": end,
        "created_at": created_at,
    }
    value["request_sha256"] = request_digest(value)
    return validate_message(value)


def _common(value, kind):
    validate_message(value)
    return {**{k: value[k] for k in COMMON}, "kind": kind}


def make_grant(request, grant_id, granted_at):
    if request.get("kind") != "request":
        raise CoordinationError("Grant requires a request")
    return validate_message(
        {
            **_common(request, "grant"),
            "grant_id": grant_id,
            "granted_at": granted_at,
            "mode": "read",
        }
    )


def make_release(grant, completed_at, exit_code):
    if grant.get("kind") != "grant":
        raise CoordinationError("Release requires a grant")
    return validate_message(
        {
            **_common(grant, "release"),
            "grant_id": grant["grant_id"],
            "completed_at": completed_at,
            "exit_code": exit_code,
            "children_stopped": True,
        }
    )


def make_cancellation(request, cancelled_at):
    if request.get("kind") != "request":
        raise CoordinationError("Cancellation requires a request")
    return validate_message(
        {
            **_common(request, "cancellation"),
            "cancelled_at": cancelled_at,
            "waiting_stopped": True,
        }
    )


def encode_message(value):
    return canonical(validate_message(value)).encode()


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise CoordinationError("Duplicate JSON key")
        value[key] = item
    return value


def decode_message(encoded):
    if not isinstance(encoded, bytes) or len(encoded) > MAX_MESSAGE_BYTES:
        raise CoordinationError("Message exceeds byte budget")
    try:
        value = json.loads(encoded, object_pairs_hook=_unique_object)
    except (ValueError, UnicodeError, RecursionError):
        raise CoordinationError("Invalid message JSON") from None
    return validate_message(value)


class MessageStore:
    """Bounded S3 transport. Callers own each key; ledger owns permissions."""

    def __init__(self, root, client):
        self.root = canonical_root(root)
        if not self.root.startswith("s3://"):
            raise CoordinationError("Coordination transport must use S3")
        uri = urlsplit(self.root)
        self.bucket, self.prefix, self.client = uri.netloc, uri.path.lstrip("/"), client

    def key(self, kind, job_id):
        if kind not in PREFIXES:
            raise CoordinationError("Unknown message kind")
        return "/".join(
            filter(None, [self.prefix, PREFIXES[kind], identifier(job_id) + ".json"])
        )

    def _read(self, key):
        try:
            result = self.client.get_object(Bucket=self.bucket, Key=key)
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") in {
                "NoSuchKey",
                "NotFound",
                "404",
            }:
                return None
            raise CoordinationError("Coordination GET failed") from None
        except (BotoCoreError, OSError):
            raise CoordinationError("Coordination GET failed") from None
        body = None
        try:
            body = result["Body"]
            encoded = bytearray()
            while len(encoded) <= MAX_MESSAGE_BYTES:
                chunk = body.read(min(64 * 1024, MAX_MESSAGE_BYTES + 1 - len(encoded)))
                if not chunk:
                    break
                if not isinstance(chunk, bytes):
                    raise CoordinationError("Coordination GET did not return bytes")
                encoded.extend(chunk)
            body.close()
            body = None
        except (
            BotoCoreError,
            OSError,
            TypeError,
            KeyError,
            AttributeError,
            CoordinationError,
        ):
            raise CoordinationError("Coordination GET body transfer failed") from None
        finally:
            if body is not None:
                try:
                    body.close()
                except (BotoCoreError, OSError, AttributeError):
                    LOG.warning("Coordination GET body cleanup failed")
        if len(encoded) > MAX_MESSAGE_BYTES:
            raise CoordinationError("Message exceeds byte budget")
        return bytes(encoded)

    def read(self, kind, job_id):
        encoded = self._read(self.key(kind, job_id))
        if encoded is None:
            return None
        return self._decode_key(encoded, kind, job_id)

    @staticmethod
    def _decode_key(encoded, kind, job_id):
        value = decode_message(encoded)
        if value["kind"] != kind or value["job_id"] != job_id:
            raise CoordinationError("Message key/identity mismatch")
        return value

    def publish(self, value):
        encoded = encode_message(value)
        key = self.key(value["kind"], value["job_id"])
        old = self._read(key)
        if old is not None:
            if old != encoded:
                raise CoordinationError("Immutable coordination message conflict")
            return
        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=encoded,
                ContentType="application/json",
            )
            if self._read(key) != encoded:
                raise CoordinationError("Coordination PUT content verification failed")
        except (BotoCoreError, ClientError, OSError, CoordinationError):
            raise CoordinationError(
                "Coordination publication uncertain; retain ownership"
            ) from None

    def messages(self, kind):
        if kind not in PREFIXES:
            raise CoordinationError("Unknown message kind")
        prefix = "/".join(filter(None, [self.prefix, PREFIXES[kind]])) + "/"
        messages, token, seen, tokens = [], None, set(), set()
        pages = total_bytes = 0
        while True:
            try:
                pages += 1
                if pages > MAX_LIST_PAGES:
                    raise CoordinationError("Message listing exceeds page budget")
                response = self.client.list_objects_v2(
                    Bucket=self.bucket,
                    Prefix=prefix,
                    **({"ContinuationToken": token} if token is not None else {}),
                )
                for item in response.get("Contents", []):
                    key = item["Key"]
                    job_id = key.removeprefix(prefix).removesuffix(".json")
                    if key != self.key(kind, job_id) or key in seen:
                        raise CoordinationError(
                            "Message listing key/duplicate mismatch"
                        )
                    seen.add(key)
                    if len(seen) > MAX_MESSAGES:
                        raise CoordinationError("Message listing exceeds count budget")
                    encoded = self._read(key)
                    if encoded is None:
                        raise CoordinationError("Listed message disappeared")
                    total_bytes += len(encoded)
                    if total_bytes > MAX_LIST_BYTES:
                        raise CoordinationError(
                            "Message listing exceeds aggregate byte budget"
                        )
                    messages.append(self._decode_key(encoded, kind, job_id))
                if response.get("IsTruncated") is False:
                    return messages
                if response.get("IsTruncated") is not True:
                    raise CoordinationError("Invalid listing truncation indicator")
                next_token = response.get("NextContinuationToken")
                if (
                    not isinstance(next_token, str)
                    or not next_token
                    or len(next_token) > 4096
                    or next_token in tokens
                ):
                    raise CoordinationError("Invalid continuation token")
                tokens.add(next_token)
                token = next_token
            except CoordinationError:
                raise
            except (
                BotoCoreError,
                ClientError,
                OSError,
                TypeError,
                KeyError,
                AttributeError,
            ):
                raise CoordinationError("Coordination LIST/read failed") from None
