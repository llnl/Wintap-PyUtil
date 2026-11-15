"""Native-Mac feasibility only, NOT a coordinated reader or shutdown certificate.

Runs fixed local synthetic controls, never the repository's DBT project or S3.
The bootstrapped experiment uses Apple's deprecated sandbox_init and CPython's
private tracker/finalizer seams. Neither dependency is selected for production.
Timeout/uncertainty is failure; this script cannot publish a grant or release.
"""

import argparse
import ctypes
import errno
import json
import os
import select
import subprocess
import sys
import tempfile
from pathlib import Path

PROFILE = "(version 1)(allow default)(deny process-fork)"


def _restricted_child(bootstrap, project):
    from multiprocessing import resource_tracker, util

    tracker = resource_tracker._resource_tracker
    failed_semaphores = []
    original_register = resource_tracker.register

    def register(name, kind):
        try:
            return original_register(name, kind)
        except BaseException:
            if kind == "semaphore":
                failed_semaphores.append(name)
            raise

    resource_tracker.register = register
    if bootstrap:
        tracker.ensure_running()
    library = ctypes.CDLL("/usr/lib/libsandbox.dylib", use_errno=True)
    library.sandbox_init.argtypes = (
        ctypes.c_char_p,
        ctypes.c_uint64,
        ctypes.POINTER(ctypes.c_char_p),
    )
    library.sandbox_init.restype = ctypes.c_int
    library.sandbox_free_error.argtypes = (ctypes.c_char_p,)
    error = ctypes.c_char_p()
    try:
        code = library.sandbox_init(PROFILE.encode(), 0, ctypes.byref(error))
        if code != 0:
            return {"sandbox_applied": False, "error": "sandbox_init_refused"}
        result = {"sandbox_applied": True, "bootstrap": bootstrap}
        for name, operation in (
            ("fork", os.fork),
            ("spawn", lambda: subprocess.run(["/usr/bin/true"], timeout=2, check=False).returncode),
        ):
            try:
                pid = operation()
                if name == "fork":
                    if pid == 0:
                        os._exit(0)
                    os.waitpid(pid, 0)
                result[name + "_denied"] = False
            except OSError as exc:
                result[name + "_denied"] = exc.errno == errno.EPERM
        from dbt.contracts.graph.manifest import Manifest

        try:
            manifest = Manifest()
            result["manifest"] = "ok"
            del manifest
        except OSError as exc:
            result["manifest"] = "denied" if exc.errno == errno.EPERM else "error"
        if bootstrap and result["manifest"] == "ok":
            from dbt.cli.main import dbtRunner

            for action in ("build", "test"):
                invocation = dbtRunner().invoke(
                    [action, "--project-dir", project, "--profiles-dir", project]
                )
                result[action] = bool(invocation.success)
        return result
    finally:
        if error.value:
            library.sandbox_free_error(error)
        # Synthetic-only cleanup, before tracker EOF. This is not a backend's
        # interruption/descendant/wrapper-death qualification.
        util._run_finalizers()
        tracker._stop()
        from _multiprocessing import sem_unlink

        for name in failed_semaphores:
            sem_unlink(name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--child", choices=("plain", "bootstrap"))
    parser.add_argument("--project")
    args = parser.parse_args()
    if sys.platform != "darwin":
        parser.error("Requires actual native macOS")
    if args.child:
        print(json.dumps(_restricted_child(args.child == "bootstrap", args.project)))
        return 0
    queue = select.kqueue()
    try:
        # Darwin NOTE_TRACK=0x1, explicitly unsupported. Register only ourselves;
        # never infer complete descendant tracking from fork notifications.
        try:
            queue.control(
                [
                    select.kevent(
                        os.getpid(), filter=select.KQ_FILTER_PROC, flags=select.KQ_EV_ADD, fflags=1
                    )
                ],
                0,
                0,
            )
            track = "unexpectedly_supported"
        except OSError as exc:
            track = "unsupported" if exc.errno == errno.ENOTSUP else "other_error"
    finally:
        queue.close()
    env = {key: os.environ[key] for key in ("HOME", "PATH", "TMPDIR") if key in os.environ}
    env["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    outcomes = {"note_track": track, "native_shutdown_qualified": False}
    with tempfile.TemporaryDirectory(prefix="dcc-mac-restriction-") as directory:
        project = Path(directory)
        (project / "models").mkdir()
        (project / "dbt_project.yml").write_text(
            "name: synthetic_restriction_probe\nversion: '1.0'\nconfig-version: 2\nprofile: synthetic_restriction_probe\n"
        )
        (project / "profiles.yml").write_text(
            "synthetic_restriction_probe:\n  target: test\n  outputs:\n    test:\n      type: duckdb\n"
            + "      path: "
            + json.dumps(str(project / "probe.duckdb"))
            + "\n      threads: 1\n"
        )
        (project / "models" / "probe.sql").write_text("select 1 as id\n")
        (project / "models" / "schema.yml").write_text(
            "version: 2\nmodels:\n  - name: probe\n    columns:\n      - name: id\n        data_tests: [not_null, unique]\n"
        )
        for case in ("plain", "bootstrap"):
            try:
                invocation = subprocess.run(
                    [
                        sys.executable,
                        "-B",
                        str(Path(__file__).resolve()),
                        "--child",
                        case,
                        "--project",
                        directory,
                    ],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=45,
                    check=False,
                )
                # Fixed tiny project/log scope only. Preserve stdout/stderr in
                # the caller's private report rather than silently suppressing it.
                print(invocation.stdout, end="")
                print(invocation.stderr, file=sys.stderr, end="")
                outcomes[case] = {"exit_code": invocation.returncode}
                if invocation.returncode == 0:
                    outcomes[case].update(json.loads(invocation.stdout.splitlines()[-1]))
            except (subprocess.TimeoutExpired, ValueError, IndexError):
                outcomes[case] = {"error": "bounded_probe_inconclusive"}
    print(json.dumps(outcomes, sort_keys=True))
    expected = (
        outcomes["plain"].get("manifest") == "denied"
        and outcomes["bootstrap"].get("build") is True
        and outcomes["bootstrap"].get("test") is True
        and all(
            outcomes[case].get(key) is True
            for case in ("plain", "bootstrap")
            for key in ("sandbox_applied", "fork_denied", "spawn_denied")
        )
    )
    return 0 if expected else 1


if __name__ == "__main__":
    raise SystemExit(main())
