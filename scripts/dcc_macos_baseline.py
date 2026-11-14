"""Actual-Mac, offline regression baseline; never reader-shutdown acceptance."""

import argparse
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

MAIN_TESTS = [
    "tests/test_coordination_authority.py",
    "tests/test_coordination_completion.py",
    "tests/test_coordination_completion_controls.py",
    "tests/test_coordination_ledger.py",
    "tests/test_coordination_execution.py",
    "tests/test_coordination_verification.py",
    "tests/test_coordination_tick.py",
    "tests/test_coordination_protocol.py",
    "tests/test_coordination_supervisor.py",
]
STANDALONE_TESTS = [
    "tests/test_completion_verifier.py",
    "tests/test_entity_integrity.py",
    "tests/test_phase_evidence.py",
]


def validate_checkout(root, manifest, commit):
    """Check committed source identity without running platform tests."""
    if manifest.get("version") != 2:
        raise ValueError("Unsupported source manifest")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("Supply the full qualification commit from the Mac runbook")
    head = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True
    ).strip()
    if head != commit:
        raise ValueError("Use the exact qualification commit in a disposable checkout")
    ancestor = subprocess.run(
        [
            "git",
            "merge-base",
            "--is-ancestor",
            manifest["baseline_parent_commit"],
            head,
        ],
        cwd=root,
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if ancestor.returncode:
        raise ValueError("Qualification commit does not contain the reviewed baseline")
    dirty = subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=no"], cwd=root, text=True
    )
    if dirty:
        raise ValueError("Tracked changes are not an exact-revision baseline")
    for name, expected in manifest["files"].items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Source manifest must name checkout-relative files")
        path = root / relative
        if path.is_symlink() or not path.is_file():
            raise ValueError("Required feature source is missing or aliased")
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError(
                "Feature source/lockfile mismatch; do not alter the manifest"
            )
        committed = subprocess.run(
            ["git", "cat-file", "blob", f"{commit}:{relative.as_posix()}"],
            cwd=root,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        if committed.returncode:
            raise ValueError(
                "Required feature source/lockfile is absent from qualification commit"
            )
        if hashlib.sha256(committed.stdout).hexdigest() != expected:
            raise ValueError(
                "Committed source/lockfile mismatch; do not alter the manifest"
            )
    return head


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    if sys.platform != "darwin":
        parser.error(
            "Run this baseline on an actual Mac; emulation is not qualification"
        )
    root = args.root.resolve(strict=True)
    manifest_bytes = args.manifest.read_bytes()
    manifest = json.loads(manifest_bytes)
    try:
        commit = validate_checkout(root, manifest, args.commit)
    except ValueError as error:
        parser.error(str(error))
    output = args.output.resolve()
    if output == root or root in output.parents:
        parser.error("Keep private reports outside the source checkout")
    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    summary = {
        "version": 2,
        "scope": "actual-macos-offline-regression-baseline-only",
        "reader_backend_qualified": False,
        "macos_version": platform.mac_ver()[0],
        "architecture": platform.machine(),
        "commit": commit,
        "source_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "stages": [],
    }
    with tempfile.TemporaryDirectory(prefix="dcc-macos-", dir=output) as scratch:
        scratch = Path(scratch)
        (scratch / "home").mkdir()
        environment = {
            key: os.environ[key]
            for key in ("PATH", "LANG", "LC_ALL")
            if key in os.environ
        }
        environment.update(
            HOME=str(scratch / "home"),
            TMPDIR=str(scratch),
            PYTHONDONTWRITEBYTECODE="1",
            AWS_EC2_METADATA_DISABLED="true",
        )
        standalone = root / "extras/raw-sensor-compact"
        for name, project, tests in (
            ("main", root, MAIN_TESTS),
            ("standalone", standalone, STANDALONE_TESTS),
        ):
            environment["PYTHONPATH"] = str(root if name == "main" else project / "src")
            command = [
                str(project / ".venv/bin/python"),
                "-B",
                "-m",
                "pytest",
                "-q",
                "-r",
                "s",
                "-p",
                "no:cacheprovider",
                "--basetemp",
                str(scratch / name),
                *[str(project / target) for target in tests],
            ]
            start = time.monotonic()
            try:
                result = subprocess.run(
                    command,
                    cwd=scratch,
                    env=environment,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    timeout=1200,
                    check=False,
                )
                code, captured, timed_out = result.returncode, result.stdout, False
            except subprocess.TimeoutExpired as error:
                code, captured, timed_out = 124, error.stdout or b"", True
            except OSError as error:
                code, captured, timed_out = 127, str(error).encode(), False
            # Local raw reports may contain account paths. Never copy them into the wiki.
            with (output / (name + ".txt")).open("xb") as report:
                os.chmod(report.name, 0o600)
                report.write(captured)
            text = captured.decode("utf-8", errors="replace")
            counts = {
                kind: int(matches[-1]) if matches else 0
                for kind in ("passed", "failed", "skipped", "error", "deselected")
                for matches in [
                    re.findall(
                        r"(\d+) " + kind + (r"s?\b" if kind == "error" else r"\b"), text
                    )
                ]
            }
            summary["stages"].append(
                {
                    "runtime": name,
                    "returncode": code,
                    "timed_out": timed_out,
                    "elapsed_seconds": round(time.monotonic() - start, 2),
                    "pytest_counts": counts,
                }
            )
            # A timeout has no certified descendant-shutdown contract: stop here.
            if timed_out:
                break
    summary["baseline_passed"] = len(summary["stages"]) == 2 and all(
        stage["returncode"] == 0 for stage in summary["stages"]
    )
    encoded = json.dumps(summary, sort_keys=True, indent=2) + "\n"
    with (output / "summary.json").open("x") as report:
        os.chmod(report.name, 0o600)
        report.write(encoded)
    print(encoded, end="")
    return 0 if summary["baseline_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
