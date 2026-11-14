"""Harness identity/refusal controls only, not actual-Mac qualification."""

import contextlib
import hashlib
import io
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from subprocess import CompletedProcess
from unittest.mock import patch

import dcc_macos_baseline as baseline

COMMIT = "a" * 40
PARENT = "b" * 40


class CheckoutTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        (self.root / "source.py").write_bytes(b"source\n")
        self.manifest = {
            "version": 2,
            "baseline_parent_commit": PARENT,
            "files": {"source.py": hashlib.sha256(b"source\n").hexdigest()},
        }

    def validate(self, *, head=COMMIT, dirty="", ancestor=0, commit=COMMIT):
        with (
            patch.object(
                baseline.subprocess, "check_output", side_effect=[head, dirty]
            ),
            patch.object(
                baseline.subprocess,
                "run",
                return_value=CompletedProcess([], ancestor),
            ),
        ):
            return baseline.validate_checkout(self.root, self.manifest, commit)

    def test_committed_revision_passes_not_parent_head(self):
        self.assertEqual(self.validate(), COMMIT)

    def test_obsolete_parent_head_refuses(self):
        with self.assertRaisesRegex(ValueError, "exact qualification commit"):
            self.validate(head=PARENT)

    def test_wrong_commit_refuses(self):
        with self.assertRaisesRegex(ValueError, "exact qualification commit"):
            self.validate(head="c" * 40)

    def test_abbreviated_commit_refuses(self):
        with self.assertRaisesRegex(ValueError, "full qualification commit"):
            self.validate(commit="a" * 7)

    def test_nonancestor_refuses(self):
        with self.assertRaisesRegex(ValueError, "reviewed baseline"):
            self.validate(ancestor=1)

    def test_tracked_changes_refuse(self):
        with self.assertRaisesRegex(ValueError, "Tracked changes"):
            self.validate(dirty=" M source.py\n")

    def test_changed_source_refuses(self):
        (self.root / "source.py").write_bytes(b"changed\n")
        with self.assertRaisesRegex(ValueError, "mismatch"):
            self.validate()

    def test_missing_source_refuses(self):
        (self.root / "source.py").unlink()
        with self.assertRaisesRegex(ValueError, "missing or aliased"):
            self.validate()

    def test_symlink_source_refuses(self):
        (self.root / "source.py").rename(self.root / "target.py")
        (self.root / "source.py").symlink_to("target.py")
        with self.assertRaisesRegex(ValueError, "missing or aliased"):
            self.validate()

    def test_parent_path_refuses(self):
        self.manifest["files"] = {"../source.py": "unused"}
        with self.assertRaisesRegex(ValueError, "checkout-relative"):
            self.validate()

    def test_obsolete_manifest_refuses(self):
        self.manifest["version"] = 1
        with self.assertRaisesRegex(ValueError, "Unsupported source manifest"):
            self.validate()

    def test_linux_refuses_before_source_output_or_process_access(self):
        output = self.root / "must-not-exist"
        with (
            patch.object(baseline.sys, "platform", "linux"),
            patch.object(baseline, "validate_checkout") as validate,
            contextlib.redirect_stderr(io.StringIO()),
            self.assertRaises(SystemExit) as error,
        ):
            baseline.main(
                [
                    "--root",
                    str(self.root),
                    "--manifest",
                    str(self.root / "missing.json"),
                    "--commit",
                    COMMIT,
                    "--output",
                    str(output),
                ]
            )
        self.assertEqual(error.exception.code, 2)
        validate.assert_not_called()
        self.assertFalse(output.exists())


class ReportTests(unittest.TestCase):
    def run_harness(self, outcomes):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "checkout"
            root.mkdir()
            manifest = root / "manifest.json"
            manifest.write_text("{}")
            output = parent / "private-reports"
            with (
                patch.object(baseline.sys, "platform", "darwin"),
                patch.object(
                    baseline.platform, "mac_ver", return_value=("synthetic", (), "")
                ),
                patch.object(baseline.platform, "machine", return_value="synthetic"),
                patch.object(baseline, "validate_checkout", return_value=COMMIT),
                patch.object(baseline.subprocess, "run", side_effect=outcomes) as run,
                patch.dict(os.environ, {"AWS_ACCESS_KEY_ID": "synthetic-secret"}),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                code = baseline.main(
                    [
                        "--root",
                        str(root),
                        "--manifest",
                        str(manifest),
                        "--commit",
                        COMMIT,
                        "--output",
                        str(output),
                    ]
                )
            summary = json.loads((output / "summary.json").read_text())
            self.assertNotIn(directory, json.dumps(summary))
            self.assertNotIn("synthetic-secret", json.dumps(summary))
            self.assertEqual((output / "summary.json").stat().st_mode & 0o777, 0o600)
            for call in run.call_args_list:
                self.assertNotIn("AWS_ACCESS_KEY_ID", call.kwargs["env"])
            return code, summary, run.call_count

    def test_failure_retained_and_other_runtime_still_runs(self):
        code, summary, calls = self.run_harness(
            [
                CompletedProcess([], 1, b"1 failed, 2 errors, 3 skipped"),
                CompletedProcess([], 0, b"4 passed, 5 skipped"),
            ]
        )
        self.assertEqual((code, calls), (1, 2))
        self.assertFalse(summary["baseline_passed"])
        self.assertFalse(summary["reader_backend_qualified"])
        self.assertEqual(summary["stages"][0]["pytest_counts"]["error"], 2)
        self.assertEqual(summary["stages"][1]["pytest_counts"]["skipped"], 5)

    def test_timeout_retained_without_starting_other_runtime(self):
        code, summary, calls = self.run_harness(
            [subprocess.TimeoutExpired([], 1200, output=b"1 passed")]
        )
        self.assertEqual((code, calls), (1, 1))
        self.assertTrue(summary["stages"][0]["timed_out"])
        self.assertEqual(summary["stages"][0]["returncode"], 124)

    def test_missing_interpreters_still_leave_summary(self):
        code, summary, calls = self.run_harness(
            [FileNotFoundError(), FileNotFoundError()]
        )
        self.assertEqual((code, calls), (1, 2))
        self.assertEqual(
            [stage["returncode"] for stage in summary["stages"]], [127, 127]
        )


if __name__ == "__main__":
    unittest.main()
