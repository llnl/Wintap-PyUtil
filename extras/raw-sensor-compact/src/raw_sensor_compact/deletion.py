"""Explicit post-validation deletion of a closed UTC event/hour prefix."""

from pathlib import Path

from .config import CompactionError, Leaf


def delete_hour(store, parent: Leaf) -> int:
    """Delete the entire exact hour, including markers; failures need manual repair."""
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
            raise CompactionError(
                "Source prefix deletion failed; manual repair required"
            ) from None
        return count

    prefix = store.dataset.object_key(parent.relative)
    # Complete listing before deletion so mutation cannot invalidate pagination.
    keys = set()
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
            if result.get("Errors"):
                raise CompactionError(
                    "Source prefix deletion partially failed; manual repair required"
                )
        if store.exists(parent.relative):
            raise CompactionError(
                "Source prefix remains after deletion; manual repair required"
            )
        return len(keys)
    except CompactionError:
        raise
    except Exception:
        raise CompactionError(
            "Source prefix deletion failed; manual repair required"
        ) from None
