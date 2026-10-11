"""``lib.core.logging_setup``: the root logger configuration.

The node stamps every record with its node id, the same context the Go
services carry as a zerolog field (DESIGN.md 13). ``setup_logging`` is called
once from the entrypoint, so it has to be idempotent rather than additive.
"""

from __future__ import annotations

import contextlib
import io
import logging
import unittest

from lib.core import logging_setup

class SetupLoggingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = logging.getLogger()
        self.original_handlers = self.root.handlers[:]
        self.original_level = self.root.level

    def tearDown(self) -> None:
        self.root.handlers[:] = self.original_handlers
        self.root.setLevel(self.original_level)

    def test_known_levels_are_mapped(self):
        cases = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warn": logging.WARNING,
            "warning": logging.WARNING,
            "error": logging.ERROR,
        }
        for level, expected in cases.items():
            with self.subTest(level=level):
                logging_setup.setup_logging(level)
                self.assertEqual(self.root.level, expected)

    def test_level_names_are_case_insensitive(self):
        logging_setup.setup_logging("DEBUG")
        self.assertEqual(self.root.level, logging.DEBUG)

    def test_an_unknown_level_falls_back_to_info(self):
        logging_setup.setup_logging("chatty")
        self.assertEqual(self.root.level, logging.INFO)

    def test_repeated_calls_do_not_stack_handlers(self):
        logging_setup.setup_logging("info", "a")
        logging_setup.setup_logging("info", "b")
        self.assertEqual(len(self.root.handlers), 1)

    def test_records_go_to_stdout_with_the_node_id(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            logging_setup.setup_logging("info", "node-abc")
            logging.getLogger("bustle").info("hello")

        out = buf.getvalue()
        self.assertIn("[node_id=node-abc]", out)
        self.assertIn("bustle: hello", out)
        self.assertIn("INFO", out)

    def test_the_node_id_defaults_to_a_placeholder(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            logging_setup.setup_logging("info")
            logging.getLogger("bustle").info("hello")

        self.assertIn("[node_id=-]", buf.getvalue())

    def test_an_explicit_node_id_field_wins(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            logging_setup.setup_logging("info", "node-abc")
            logging.getLogger("bustle").info("hello", extra={"node_id": "explicit"})

        self.assertIn("[node_id=explicit]", buf.getvalue())

    def test_a_uuid_node_id_is_rendered(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            logging_setup.setup_logging("info", "50154422-fd3b-4cf0-a761-908fcbe98a1e")
            logging.getLogger("bustle").info("hello")

        self.assertIn("[node_id=50154422-fd3b-4cf0-a761-908fcbe98a1e]", buf.getvalue())

    def test_the_configured_level_filters_output(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            logging_setup.setup_logging("error", "node-abc")
            logging.getLogger("bustle").info("not shown")
            logging.getLogger("bustle").error("shown")

        out = buf.getvalue()
        self.assertNotIn("not shown", out)
        self.assertIn("shown", out)

if __name__ == "__main__":
    unittest.main()
