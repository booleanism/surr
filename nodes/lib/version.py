"""The node's build id, resolved the same way the Makefile resolves VERSION.

The Go services have their build id injected at link time
(``-X main.Version=...``). A Python process has no equivalent, so the node asks
git at startup instead. The point is that both sides report the SAME string for
the same checkout, which is what makes a mixed deployment comparable across the
two languages: an operator reading ``version=`` on a Go log line and on a node
log line is comparing the same thing.

``git describe --tags --always --dirty`` is the source of truth: the nearest
tag, or the abbreviated commit when the tree has no tags, with ``-dirty``
appended when there are uncommitted changes. It returns "unknown" when git is
missing or the code is running from an archive with no ``.git`` directory,
which is the honest answer there -- a source tarball has no build id.

``SURR_VERSION`` takes precedence over git when it is set to something
non-empty. That is what a packaged copy needs: an archive or an extracted tree
has no ``.git``, so asking git there could only ever answer "unknown", while
whatever assembled the package knows the build id and can hand it over in the
environment. Git remains the fallback so a checkout run directly from a working
tree -- the development case -- needs no environment at all.
"""

from __future__ import annotations

import os
import pathlib
import subprocess
import typing

# nodes/lib/version.py -> nodes/lib -> nodes -> <repo root>
_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent

UNKNOWN = "unknown"

# The environment variable a container image uses to inject the build id.
VERSION_ENV = "SURR_VERSION"

def resolve_version(
    repo_root: pathlib.Path = _REPO_ROOT,
    env: typing.Optional[typing.Mapping[str, str]] = None,
) -> str:
    """Return the build id for this checkout, or UNKNOWN if nothing can say.

    An injected ``SURR_VERSION`` wins; otherwise git is asked.
    """
    environ = os.environ if env is None else env
    injected = environ.get(VERSION_ENV, "").strip()
    if injected:
        return injected

    try:
        out = subprocess.run(
            ["git", "describe", "--tags", "--always", "--dirty"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return UNKNOWN

    if out.returncode != 0:
        return UNKNOWN

    return out.stdout.strip() or UNKNOWN

VERSION = resolve_version()
