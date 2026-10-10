"""Tests for the nodes runtime.

Run with ``make test-nodes`` from the repo root, or directly::

    cd nodes && .venv/bin/python -m unittest discover -s tests -t .

These suites deliberately drive error paths (unknown commands, malformed
trackIDs, retried RPCs), so the root logger is turned down: the warnings are
the point of the tests, not output anyone needs to read. ``assertLogs`` sets
its own level and handler, so it still captures what it asserts on.
"""

import logging

logging.getLogger().setLevel(logging.CRITICAL)
