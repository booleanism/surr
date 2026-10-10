"""``core.rpc.call``: sibling resolution, retries and candidate failover.

The node calls this at boot before the hub has necessarily introduced itself
(``nodes.add`` retries until the hub shows up), so "no sibling yet" has to be
a retryable failure rather than a fatal one, while a real reply with a
non-zero code has to keep walking the candidate list.
"""

from __future__ import annotations

import asyncio
import unittest

from core import handlers, rpc, siblings

from . import support

def reply(code: int = 0, result=None) -> handlers.RouterResponse:
    return handlers.RouterResponse(code, result)

class CallTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.sibs = siblings.Siblings()
        self.sibs.set("hub0", siblings.Sibling(commands=["nodes.add"]))

    async def test_returns_the_reply_of_the_serving_sibling(self):
        stub = support.ScriptedRouter(lambda *_: reply(0, "ok"))
        resp = await rpc.call(stub, self.sibs, "nodes.add", b"{}", retry_interval=0)
        self.assertEqual(resp.code, 0)
        self.assertEqual(resp.result, "ok")
        self.assertEqual(stub.sent, [("nodes.add", "hub0")])

    async def test_passes_the_timeout_to_recv(self):
        stub = support.ScriptedRouter(lambda *_: reply())
        await rpc.call(stub, self.sibs, "nodes.add", b"{}", timeout=1.5, retry_interval=0)
        self.assertEqual(stub.timeouts, [1.5])

    async def test_forwards_the_payload_unchanged(self):
        seen: list[bytes] = []

        def respond(_command, payload, _name):
            seen.append(payload)
            return reply()

        stub = support.ScriptedRouter(respond)
        await rpc.call(stub, self.sibs, "nodes.add", b'[{"id":"n"}]', retry_interval=0)
        self.assertEqual(seen, [b'[{"id":"n"}]'])

    async def test_retries_until_the_sibling_appears(self):
        # "no sibling with nodes.add command yet" is the boot case.
        attempts = 0

        def respond(*_):
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                return RuntimeError("no sibling with nodes.add command yet")
            return reply()

        stub = support.ScriptedRouter(respond)
        resp = await rpc.call(stub, self.sibs, "nodes.add", b"{}", retry_interval=0)
        self.assertEqual(resp.code, 0)
        self.assertEqual(attempts, 3)

    async def test_retries_are_bounded_when_asked(self):
        stub = support.ScriptedRouter(lambda *_: RuntimeError("nope"))
        with self.assertRaises(RuntimeError):
            await rpc.call(stub, self.sibs, "nodes.add", b"{}", retry_interval=0, retries=2)
        self.assertEqual(len(stub.sent), 3)  # the first attempt plus two retries

    async def test_retries_zero_means_a_single_attempt(self):
        # This is the shutdown path: nodes.remove with retries=0.
        stub = support.ScriptedRouter(lambda *_: RuntimeError("hub is gone"))
        with self.assertRaises(RuntimeError):
            await rpc.call(stub, self.sibs, "nodes.add", b"{}", retry_interval=0, retries=0)
        self.assertEqual(len(stub.sent), 1)

    async def test_an_unserved_command_is_retried(self):
        stub = support.ScriptedRouter(lambda *_: reply())
        with self.assertRaises(RuntimeError):
            await rpc.call(stub, self.sibs, "subscriptions.nodes.get", b"{}", retry_interval=0, retries=1)
        self.assertEqual(stub.sent, [])  # never sent: no sibling to send to

    async def test_fails_over_to_the_next_candidate(self):
        # A stale registry entry must not stop the call: hub1 has restarted
        # and hub0 lingers.
        self.sibs.set("hub1", siblings.Sibling(commands=["nodes.add"]))

        def respond(_command, _payload, name):
            if name == "hub0":
                return TimeoutError("stale entry")
            return reply(0, "from-hub1")

        stub = support.ScriptedRouter(respond)
        resp = await rpc.call(stub, self.sibs, "nodes.add", b"{}", retry_interval=0)
        self.assertEqual(resp.result, "from-hub1")
        self.assertEqual(stub.sent, [("nodes.add", "hub0"), ("nodes.add", "hub1")])

    async def test_a_non_zero_code_fails_over_too(self):
        self.sibs.set("hub1", siblings.Sibling(commands=["nodes.add"]))

        def respond(_command, _payload, name):
            if name == "hub0":
                return reply(-2, "domain error")
            return reply(0, "from-hub1")

        stub = support.ScriptedRouter(respond)
        resp = await rpc.call(stub, self.sibs, "nodes.add", b"{}", retry_interval=0)
        self.assertEqual(resp.result, "from-hub1")

    async def test_the_last_error_names_the_code_and_the_sibling(self):
        def respond(_command, _payload, name):
            return reply(-4, "subscription not found")

        stub = support.ScriptedRouter(respond)
        with self.assertRaises(RuntimeError) as ctx:
            await rpc.call(stub, self.sibs, "nodes.add", b"{}", retry_interval=0, retries=0)
        message = str(ctx.exception)
        self.assertIn("hub0", message)
        self.assertIn("-4", message)
        self.assertIn("subscription not found", message)

    async def test_every_candidate_rejecting_reports_the_last_one(self):
        self.sibs.set("hub1", siblings.Sibling(commands=["nodes.add"]))
        stub = support.ScriptedRouter(lambda _c, _p, name: reply(-2, f"rejected by {name}"))
        with self.assertRaises(RuntimeError) as ctx:
            await rpc.call(stub, self.sibs, "nodes.add", b"{}", retry_interval=0, retries=0)
        self.assertIn("rejected by hub1", str(ctx.exception))

    async def test_cancellation_is_not_retried(self):
        stub = support.ScriptedRouter(lambda *_: asyncio.CancelledError())
        with self.assertRaises(asyncio.CancelledError):
            await rpc.call(stub, self.sibs, "nodes.add", b"{}", retry_interval=0)
        self.assertEqual(len(stub.sent), 1)

    async def test_retry_interval_is_awaited_between_attempts(self):
        attempts = 0

        def respond(*_):
            nonlocal attempts
            attempts += 1
            return RuntimeError("again")

        stub = support.ScriptedRouter(respond)
        loop = asyncio.get_running_loop()
        start = loop.time()
        with self.assertRaises(RuntimeError):
            await rpc.call(stub, self.sibs, "nodes.add", b"{}", retry_interval=0.01, retries=2)
        self.assertGreaterEqual(loop.time() - start, 0.02)

if __name__ == "__main__":
    unittest.main()
