"""Root logging setup for the nodes runtime.

The Python side stays on stdlib ``logging`` (DESIGN.md §13): level
semantics match the Go services' zerolog policy, output formats do not
need to. ``setup_logging`` configures the root logger once from the CLI
(--log-level on the entrypoint) and, where the node id is known, stamps
every record with it so log lines carry the same context the Go side
logs (node_id=...).

This module is node-type-agnostic: it knows nothing about AI models,
databases or pipelines.
"""

from __future__ import annotations

import logging
import sys
import typing

_LEVELS: dict[str, int] = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


class _NodeIDFilter(logging.Filter):
    """Stamp every record with the node id (or "-"), like a zerolog field."""

    def __init__(self, node_id: typing.Optional[str]) -> None:
        super().__init__()
        self.node_id = node_id or "-"

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "node_id"):
            record.node_id = self.node_id
        return True


def setup_logging(level: str = "info", node_id: typing.Optional[str] = None) -> None:
    """Configure the root logger: timestamp, level, node id, name, message."""
    root = logging.getLogger()
    root.setLevel(_LEVELS.get(level.lower(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s [node_id=%(node_id)s] %(name)s: %(message)s"
        )
    )
    handler.addFilter(_NodeIDFilter(node_id))

    root.handlers.clear()
    root.addHandler(handler)
