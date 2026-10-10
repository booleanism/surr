"""``core.siblings``: the registry that resolves a command to a service.

``core.rpc._call_once`` calls ``get_name`` and then tries *every* returned
name, because a sibling that restarted under a new name leaves a stale entry
behind. ``get_name`` returning an empty list rather than None is therefore
load-bearing: ``if not names`` is what produces the "no sibling yet" error.
"""

from __future__ import annotations

import json
import unittest

from core import siblings

def registered(sibs: siblings.Siblings, name: str) -> siblings.Sibling:
    """``Siblings.get`` with the missing case turned into a failure."""
    sib = sibs.get(name)
    if sib is None:
        raise AssertionError(f"no sibling registered as {name}")
    return sib

class SiblingTests(unittest.TestCase):
    def test_wire_shape_is_exactly_commands(self):
        self.assertEqual(json.loads(siblings.Sibling(commands=["a", "b"]).to_json()), {"commands": ["a", "b"]})

    def test_round_trip(self):
        sib = siblings.Sibling.from_json(siblings.Sibling(commands=["nodes.add"]).to_json())
        self.assertEqual(sib.commands, ["nodes.add"])

    def test_a_missing_commands_key_is_an_empty_list(self):
        self.assertEqual(siblings.Sibling.from_json(b"{}").commands, [])

    def test_undecodable_body_raises(self):
        with self.assertRaises(json.JSONDecodeError):
            siblings.Sibling.from_json(b"not json")

class SiblingsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.sibs = siblings.Siblings()

    def test_get_name_returns_none_when_nobody_serves_the_command(self):
        self.sibs.set("hub0", siblings.Sibling(commands=["nodes.add"]))
        self.assertIsNone(self.sibs.get_name("subscriptions.nodes.get"))

    def test_get_name_returns_every_candidate(self):
        self.sibs.set("hub0", siblings.Sibling(commands=["nodes.add"]))
        self.sibs.set("hub1", siblings.Sibling(commands=["nodes.add", "nodes.remove"]))
        self.sibs.set("rx0", siblings.Sibling(commands=["nodes.remove"]))
        self.assertEqual(sorted(self.sibs.get_name("nodes.remove") or []), ["hub1", "rx0"])

    def test_get_returns_the_stored_sibling(self):
        self.sibs.set("hub0", siblings.Sibling(commands=["nodes.add"]))
        self.assertEqual(registered(self.sibs, "hub0").commands, ["nodes.add"])
        self.assertIsNone(self.sibs.get("hub1"))

    def test_set_overwrites_a_restarted_sibling(self):
        # introduce is broadcast on a ticker, so the same name is re-set often.
        self.sibs.set("hub0", siblings.Sibling(commands=["nodes.add"]))
        self.sibs.set("hub0", siblings.Sibling(commands=["nodes.add", "nodes.remove"]))
        self.assertEqual(registered(self.sibs, "hub0").commands, ["nodes.add", "nodes.remove"])
        self.assertEqual(len(self.sibs._d), 1)

if __name__ == "__main__":
    unittest.main()
