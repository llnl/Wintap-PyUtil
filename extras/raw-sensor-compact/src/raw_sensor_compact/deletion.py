"""Explicit post-validation deletion of a closed UTC event/hour prefix."""

from pathlib import Path
from time import sleep

from .config import CompactionError, Leaf


class DeletionError(CompactionError):
    """A deletion failed after the source/output safety checks."""

    def __init__(self, message, *, details=None):
        super().__init__(message)
        self.details = details or {}


def _response_metadata(response):
    metadata = response.get("ResponseMetadata", {}) if response else {}
    return {
        key: metadata[key]
        for key in ("HTTPStatusCode", "RequestId", "HostId")
        if metadata.get(key) is not None
    }


def _list_keys(store, prefix, *, limit=None):
    """List exact-prefix keys, optionally stopping after ``limit`` keys."""
    if not store.dataset.bucket:
        directory = Path(store.dataset.file_uri(prefix))
        if not directory.exists():
            return []
        return [str(path) for path in directory.rglob("*") if path.is_file()]

    keys = []
    continuation = None
    while True:
        store.check_deadline()
        kwargs = {
            "Bucket": store.dataset.bucket,
            "Prefix": prefix,
            "MaxKeys": min(limit, 1000) if limit is not None else 1000,
        }
        if continuation:
            kwargs["ContinuationToken"] = continuation
        response = store.client.list_objects_v2(**kwargs)
        for item in response.get("Contents", []):
            key = item["Key"]
            if not key.startswith(prefix):
                raise DeletionError("S3 returned an out-of-scope deletion object")
            keys.append(key)
            if limit is not None and len(keys) >= limit:
                return keys
        if not response.get("IsTruncated"):
            return keys
        continuation = response.get("NextContinuationToken")
        if not continuation:
            raise DeletionError("S3 deletion listing omitted its continuation token")


def delete_hour(
    store,
    parent: Leaf,
    *,
    confirm_attempts=5,
    confirm_initial_delay_seconds=1.0,
    report=None,
    sleep_fn=sleep,
) -> int:
    """Delete the entire exact hour, including markers; failures need manual repair."""
    if type(confirm_attempts) is not int or not 1 <= confirm_attempts <= 10:
        raise CompactionError("Deletion confirmation attempts must be 1–10")
    if (
        type(confirm_initial_delay_seconds) not in (int, float)
        or not 0 <= confirm_initial_delay_seconds <= 60
    ):
        raise CompactionError("Deletion confirmation delay must be 0–60 seconds")
    if parent.protocol:
        raise CompactionError("Source deletion requires an event/hour parent")
    store.check_deadline()
    if not store.dataset.bucket:
        # Preflight all paths before unlinking; never follow symlinks.
        directory = Path(store.dataset.file_uri(parent.relative))
        if not store.exists(parent.relative):
            return 0
        paths = list(directory.rglob("*"))
        if any(p.is_symlink() for p in paths):
            raise CompactionError("Symlinked deletion paths are unsupported")
        count = 0
        try:
            for path in sorted(paths, key=lambda p: len(p.parts), reverse=True):
                store.check_deadline()
                if path.is_dir():
                    path.rmdir()
                else:
                    path.unlink()
                    count += 1
            directory.rmdir()
        except CompactionError:
            raise
        except Exception:
            remaining = [str(path) for path in directory.rglob("*") if path.is_file()]
            raise DeletionError(
                "Source prefix deletion failed; manual repair required",
                details={"remaining_keys": remaining[:100]},
            ) from None
        return count

    prefix = store.dataset.object_key(parent.relative)
    # Complete listing before deletion so mutation cannot invalidate pagination.
    keys = set()
    responses = []
    try:
        for page in store.client.get_paginator("list_objects_v2").paginate(
            Bucket=store.dataset.bucket, Prefix=prefix
        ):
            store.check_deadline()
            for item in page.get("Contents", []):
                key = item["Key"]
                if not key.startswith(prefix):
                    raise CompactionError("S3 returned an out-of-scope deletion object")
                keys.add(key)
        ordered = sorted(keys)
        for offset in range(0, len(ordered), 1000):
            store.check_deadline()
            result = store.client.delete_objects(
                Bucket=store.dataset.bucket,
                Delete={
                    "Objects": [{"Key": k} for k in ordered[offset : offset + 1000]],
                    "Quiet": True,
                },
            )
            responses.append(_response_metadata(result))
            if result.get("Errors"):
                details = {
                    "deleted_key_count": len(keys),
                    "delete_responses": responses,
                    "delete_errors": result.get("Errors", []),
                    "remaining_keys": _list_keys(store, prefix, limit=100),
                }
                raise DeletionError(
                    "Source prefix deletion partially failed; manual repair required",
                    details=details,
                )
        for attempt in range(1, confirm_attempts + 1):
            remaining = _list_keys(store, prefix, limit=1)
            if not remaining:
                details = {
                    "deleted_key_count": len(keys),
                    "confirmation_attempts": attempt,
                    "remaining_key_count": 0,
                    "remaining_keys": [],
                    "delete_responses": responses,
                }
                if report is not None:
                    report.update(details)
                return len(keys)
            if attempt < confirm_attempts:
                store.check_deadline()
                sleep_fn(confirm_initial_delay_seconds * (2 ** (attempt - 1)))
        remaining = _list_keys(store, prefix, limit=100)
        details = {
            "deleted_key_count": len(keys),
            "confirmation_attempts": confirm_attempts,
            "remaining_key_count": len(remaining),
            "remaining_keys": remaining,
            "delete_responses": responses,
        }
        raise DeletionError(
            "Source prefix remains after deletion; manual repair required",
            details=details,
        )
    except DeletionError:
        raise
    except CompactionError:
        raise
    except Exception:
        raise DeletionError(
            "Source prefix deletion failed; manual repair required",
            details={"delete_responses": responses},
        ) from None
