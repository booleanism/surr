"""``lib.version``: the node's build id.

The node reports the same string the Makefile stamps into the Go binaries, so
these tests pin the contract rather than a particular checkout's value: the
result is always a non-empty string, and a checkout where git cannot answer
degrades to ``UNKNOWN`` instead of raising on the way to startup.

The git-fallback tests pass an explicit empty ``env`` so that the ambient
environment cannot decide their outcome -- the injected-version tests below are
what cover ``SURR_VERSION``.
"""

from __future__ import annotations

import pathlib
import subprocess
import tempfile
import unittest
import unittest.mock

from lib import version

class ResolveVersionTests(unittest.TestCase):
    def test_the_repo_root_is_the_directory_holding_the_makefile(self):
        self.assertTrue((version._REPO_ROOT / "Makefile").is_file())
        self.assertTrue((version._REPO_ROOT / "go.mod").is_file())

    def test_this_checkout_resolves_to_a_non_empty_string(self):
        # Not asserted against `git describe` directly: the suite must pass on a
        # source archive with no git, and on a dirty tree.
        self.assertIsInstance(version.VERSION, str)
        self.assertNotEqual(version.VERSION, "")

    def test_a_directory_that_is_not_a_git_checkout_is_unknown(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(
                version.resolve_version(pathlib.Path(tmp), env={}), version.UNKNOWN
            )

    def test_a_missing_git_binary_is_unknown_not_an_exception(self):
        with unittest.mock.patch.object(
            version.subprocess, "run", side_effect=FileNotFoundError("git")
        ):
            self.assertEqual(version.resolve_version(env={}), version.UNKNOWN)

    def test_a_git_timeout_is_unknown_not_an_exception(self):
        # Startup must not depend on git answering promptly.
        with unittest.mock.patch.object(
            version.subprocess,
            "run",
            side_effect=subprocess.TimeoutExpired(cmd="git", timeout=5),
        ):
            self.assertEqual(version.resolve_version(env={}), version.UNKNOWN)

    def test_a_failing_git_is_unknown_not_an_exception(self):
        completed = subprocess.CompletedProcess(args=["git"], returncode=128, stdout="", stderr="")
        with unittest.mock.patch.object(version.subprocess, "run", return_value=completed):
            self.assertEqual(version.resolve_version(env={}), version.UNKNOWN)

    def test_a_failing_git_is_unknown_even_if_it_printed_something(self):
        # The returncode is what decides, not whether stdout is empty. A failing
        # `git describe` (no commits, not a repository) is not a build id just
        # because it wrote to stdout on the way out, and the empty-stdout case
        # above would pass even with the returncode check deleted.
        completed = subprocess.CompletedProcess(
            args=["git"], returncode=128, stdout="fatal: not a git repository\n", stderr=""
        )
        with unittest.mock.patch.object(version.subprocess, "run", return_value=completed):
            self.assertEqual(version.resolve_version(env={}), version.UNKNOWN)

    def test_empty_git_output_is_unknown(self):
        # `git describe` in a repo with no commits prints nothing and exits 128,
        # but an empty stdout with status 0 must not produce an empty version.
        completed = subprocess.CompletedProcess(args=["git"], returncode=0, stdout="\n", stderr="")
        with unittest.mock.patch.object(version.subprocess, "run", return_value=completed):
            self.assertEqual(version.resolve_version(env={}), version.UNKNOWN)

    def test_the_output_is_stripped(self):
        completed = subprocess.CompletedProcess(
            args=["git"], returncode=0, stdout="v1.2.3-dirty\n", stderr=""
        )
        with unittest.mock.patch.object(version.subprocess, "run", return_value=completed):
            self.assertEqual(version.resolve_version(env={}), "v1.2.3-dirty")

    def test_it_asks_git_describe_with_the_makefile_flags(self):
        # The Makefile uses exactly this invocation; a drift here would make the
        # two languages report different strings for the same checkout.
        completed = subprocess.CompletedProcess(args=["git"], returncode=0, stdout="abc\n", stderr="")
        with unittest.mock.patch.object(
            version.subprocess, "run", return_value=completed
        ) as run:
            version.resolve_version(env={})

        argv = run.call_args.args[0]
        self.assertEqual(argv, ["git", "describe", "--tags", "--always", "--dirty"])
        self.assertEqual(run.call_args.kwargs["cwd"], version._REPO_ROOT)

class InjectedVersionTests(unittest.TestCase):
    """SURR_VERSION, the override a container image uses.

    The image excludes .git, so git inside a container can only ever answer
    UNKNOWN; the build id the Go binaries were compiled with is handed over in
    the environment instead. Git stays the fallback so a working checkout needs
    no environment at all.
    """

    def test_an_injected_version_wins_over_git(self):
        with unittest.mock.patch.object(
            version.subprocess, "run", side_effect=AssertionError("git should not be asked")
        ):
            self.assertEqual(
                version.resolve_version(env={version.VERSION_ENV: "v9.9.9"}), "v9.9.9"
            )

    def test_the_injected_version_is_stripped(self):
        self.assertEqual(
            version.resolve_version(env={version.VERSION_ENV: "  v9.9.9\n"}), "v9.9.9"
        )

    def test_an_empty_injected_version_falls_back_to_git(self):
        completed = subprocess.CompletedProcess(
            args=["git"], returncode=0, stdout="from-git\n", stderr=""
        )
        with unittest.mock.patch.object(version.subprocess, "run", return_value=completed):
            self.assertEqual(
                version.resolve_version(env={version.VERSION_ENV: ""}), "from-git"
            )

    def test_a_whitespace_only_injected_version_falls_back_to_git(self):
        # A blank value is not a build id; treating it as one would print
        # `version=` with nothing after it, which is worse than UNKNOWN.
        completed = subprocess.CompletedProcess(
            args=["git"], returncode=0, stdout="from-git\n", stderr=""
        )
        with unittest.mock.patch.object(version.subprocess, "run", return_value=completed):
            self.assertEqual(
                version.resolve_version(env={version.VERSION_ENV: "   "}), "from-git"
            )
