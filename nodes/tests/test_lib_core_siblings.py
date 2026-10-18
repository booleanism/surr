"""``lib.core.siblings``: the registry that resolves a command to a service.

``lib.core.rpc._call_once`` calls ``get_name`` and then tries *every* returned
name, because a sibling that restarted under a new name leaves a stale entry
behind. ``get_name`` returning an empty list rather than None is therefore
load-bearing: ``if not names`` is what produces the "no sibling yet" error.
"""

from __future__ import annotations

import json
import unittest

from lib.core import siblings

def registered(sibs: siblings.Siblings, name: str) -> siblings.Sibling:
    """``Siblings.get`` with the missing case turned into a failure."""
    sib = sibs.get(name)
    if sib is None:
        raise AssertionError(f"no sibling registered as {name}")
    return sib

class SiblingTests(unittest.TestCase):
    def test_the_wire_carries_commands_revision_and_version(self):
        body = json.loads(
            siblings.Sibling(
                commands=["a", "b"], protocol_revision=1, version="abc123"
            ).to_json()
        )
        self.assertEqual(
            body,
            {"commands": ["a", "b"], "protocol_revision": 1, "version": "abc123"},
        )

    def test_the_defaults_are_the_old_peer_shape(self):
        # A Sibling built without the new fields must encode as revision 0, so
        # it is indistinguishable from a peer that predates the field -- which
        # is what makes the mismatch warning fire during a rolling upgrade.
        body = json.loads(siblings.Sibling(commands=["a"]).to_json())
        self.assertEqual(body, {"commands": ["a"], "protocol_revision": 0, "version": ""})

    def test_round_trip(self):
        sib = siblings.Sibling.from_json(
            siblings.Sibling(
                commands=["nodes.add"], protocol_revision=1, version="abc123"
            ).to_json()
        )
        self.assertEqual(sib.commands, ["nodes.add"])
        self.assertEqual(sib.protocol_revision, 1)
        self.assertEqual(sib.version, "abc123")

    def test_a_missing_commands_key_is_an_empty_list(self):
        self.assertEqual(siblings.Sibling.from_json(b"{}").commands, [])

    def test_a_body_without_a_revision_decodes_as_zero(self):
        # The pre-revision wire shape. Decoding it as 0 rather than refusing it
        # is deliberate: the peer is registered and warned about, so a mixed
        # deployment keeps working while the operator is told.
        sib = siblings.Sibling.from_json(b'{"commands": ["nodes.add"]}')
        self.assertEqual(sib.protocol_revision, 0)
        self.assertEqual(sib.version, "")

    def test_the_revision_is_a_positive_int(self):
        self.assertIsInstance(siblings.PROTOCOL_REVISION, int)
        self.assertGreater(siblings.PROTOCOL_REVISION, 0)

    def test_undecodable_body_raises(self):
        with self.assertRaises(json.JSONDecodeError):
            siblings.Sibling.from_json(b"not json")

class SiblingsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now = 0.0
        self.sibs = siblings.Siblings(clock=self.clock)

    def clock(self) -> float:
        """A hand-wound monotonic clock, so expiry costs no wall time."""
        return self.now

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

    def test_a_sibling_that_stopped_introducing_expires(self):
        # TODO.md F5: a node killed without a graceful nodes.remove used to stay
        # resolvable for the life of the process, so rpc._call_once kept trying
        # a dead address first.
        self.sibs.set("node0", siblings.Sibling(commands=["n0.sink.start"]))
        self.now = siblings.SIBLING_TTL_SECONDS + 1
        self.assertIsNone(self.sibs.get_name("n0.sink.start"))
        self.assertIsNone(self.sibs.get("node0"))

    def test_expiry_drops_the_entry_rather_than_hiding_it(self):
        self.sibs.set("node0", siblings.Sibling(commands=["n0.sink.start"]))
        self.now = siblings.SIBLING_TTL_SECONDS + 1
        self.assertIsNone(self.sibs.get("node0"))
        # A registry that kept the entry would grow one dead key per node for
        # the life of the process.
        self.assertEqual(self.sibs._d, {})
        self.assertEqual(self.sibs._seen, {})

    def test_a_fresh_sibling_is_still_resolvable(self):
        # The other half: the TTL must not make a live peer unreachable.
        self.sibs.set("node0", siblings.Sibling(commands=["n0.sink.start"]))
        self.now = siblings.SIBLING_TTL_SECONDS - 0.001
        self.assertEqual(self.sibs.get_name("n0.sink.start"), ["node0"])
        self.assertIsNotNone(self.sibs.get("node0"))

    def test_re_introducing_refreshes_the_deadline(self):
        # The TTL is measured from the last introduce, not the first: a
        # long-running node re-introduces every second and must never expire.
        self.sibs.set("node0", siblings.Sibling(commands=["n0.sink.start"]))
        self.now = siblings.SIBLING_TTL_SECONDS - 1
        self.sibs.set("node0", siblings.Sibling(commands=["n0.sink.start"]))
        self.now = siblings.SIBLING_TTL_SECONDS * 2 - 1
        self.assertEqual(self.sibs.get_name("n0.sink.start"), ["node0"])

    def test_expiry_is_per_sibling(self):
        self.sibs.set("dead", siblings.Sibling(commands=["sink.start"]))
        self.now = siblings.SIBLING_TTL_SECONDS + 1
        self.sibs.set("alive", siblings.Sibling(commands=["sink.start"]))
        self.assertEqual(self.sibs.get_name("sink.start"), ["alive"])

    def test_the_ttl_mirrors_the_go_side(self):
        # internal/protocols/siblings.go SiblingTTL. A Go service and a node
        # disagreeing about when a peer is dead is a behaviour difference, not
        # a wire one, so nothing else would catch it.
        self.assertEqual(siblings.SIBLING_TTL_SECONDS, 15.0)

if __name__ == "__main__":
    unittest.main()
