"""``lib.core.router``: the sibling RPC protocol on the Python side.

These tests pin the behaviours the Go router (``internal/protocols/rabbitmq``)
relies on, and the ones that were repaired in E8:

* a reply that arrives after ``recv`` gave up must be dropped, not mis-read as
  an inbound request (its trackID is no longer pending, so it would fall
  through to the handler lookup and log a spurious error);
* a handler task must be retained so its exception is retrieved rather than
  surfacing as "Task exception was never retrieved" at shutdown;
* a failed ``send`` must not leak the pending trackID.
"""

from __future__ import annotations

import asyncio
import json
import unittest
import unittest.mock
import uuid

from aio_pika import abc

from lib.core import handlers, router, siblings

from . import support

class SetupRouterTests(unittest.IsolatedAsyncioTestCase):
    async def test_declares_a_durable_topic_exchange(self):
        channel = support.FakeChannel()
        await support.setup(channel)
        self.assertEqual(channel.exchange_name, "surr0")
        self.assertEqual(channel.exchange_type, abc.ExchangeType.TOPIC)
        self.assertTrue(channel.durable)

    async def test_declares_an_exclusive_auto_delete_queue(self):
        channel = support.FakeChannel()
        await support.setup(channel)
        self.assertEqual(channel.queue_name, "")
        self.assertEqual(channel.queue_kwargs, {"exclusive": True, "auto_delete": True})

    async def test_binds_the_direct_key_and_the_broadcast_key(self):
        channel = support.FakeChannel()
        _, _, direct_rk = await support.setup(channel)
        self.assertEqual(channel.queue.bound, [direct_rk, router.BROADCAST_ROUTING_KEY])

    async def test_returns_the_direct_routing_key(self):
        channel = support.FakeChannel()
        _, exchange, direct_rk = await support.setup(channel)
        self.assertEqual(direct_rk, "rpc.direct.bustle0")
        self.assertIs(exchange, channel.exchange)

    def test_direct_routing_key_shape(self):
        self.assertEqual(router.direct_routing_key("hub0"), "rpc.direct.hub0")

class RouterTestCase(unittest.IsolatedAsyncioTestCase):
    """Builds a router wired to fakes in every test."""

    def setUp(self) -> None:
        self.exchange = support.FakeExchange()
        self.queue = support.FakeQueue()
        self.handlers = handlers.Handlers()
        self.sibs = siblings.Siblings()
        self.router = support.make_router(self.handlers, self.sibs, self.exchange, self.queue)

    def peer(self, name: str = "hub0", commands: list[str] | None = None) -> None:
        self.sibs.set(name, siblings.Sibling(commands=commands or ["nodes.add"]))

class IntroduceTests(RouterTestCase):
    async def test_advertises_the_registered_commands(self):
        async def handler(track_id, req, rep):  # pragma: no cover - never called
            pass

        self.handlers.set("b.sink.start", handler)
        await self.router.introduce()

        message, routing_key = self.exchange.last
        self.assertEqual(routing_key, router.BROADCAST_ROUTING_KEY)
        self.assertEqual(message.type, router.INTRODUCE_COMMAND)
        self.assertEqual(message.reply_to, "rpc.direct.bustle0")
        self.assertEqual(json.loads(message.body), {"commands": ["b.sink.start"]})

    async def test_advertises_nothing_before_any_handler_is_set(self):
        await self.router.introduce()
        self.assertEqual(json.loads(self.exchange.last[0].body), {"commands": []})

class SendTests(RouterTestCase):
    async def test_sends_to_the_sibling_name_and_records_the_track_id(self):
        self.peer()
        track_id = await self.router.send("nodes.add", b"[]", "hub0")

        message, routing_key = self.exchange.last
        self.assertEqual(routing_key, "hub0")
        self.assertEqual(message.type, "nodes.add")
        self.assertEqual(message.correlation_id, str(track_id))
        self.assertEqual(message.reply_to, "rpc.direct.bustle0")
        self.assertEqual(message.content_type, "application/json")
        self.assertEqual(message.body, b"[]")
        self.assertIn(track_id, self.router._known_track_id)

    async def test_an_unknown_sibling_raises_before_publishing(self):
        with self.assertRaises(ValueError):
            await self.router.send("nodes.add", b"[]", "nobody")
        self.assertEqual(self.exchange.published, [])
        self.assertEqual(self.router._known_track_id, {})

    async def test_a_failed_publish_does_not_leak_the_track_id(self):
        self.peer()
        self.router = support.make_router(
            self.handlers, self.sibs, support.FakeExchange(fail=True), self.queue
        )
        with self.assertRaises(RuntimeError):
            await self.router.send("nodes.add", b"[]", "hub0")
        self.assertEqual(self.router._known_track_id, {})

class RecvTests(RouterTestCase):
    async def test_an_unknown_track_id_raises(self):
        with self.assertRaises(KeyError):
            await self.router.recv(uuid.uuid4())

    async def test_a_timeout_abandons_the_track_id(self):
        self.peer()
        track_id = await self.router.send("nodes.add", b"[]", "hub0")

        with self.assertRaises(asyncio.TimeoutError):
            await self.router.recv(track_id, timeout=0.01)

        self.assertNotIn(track_id, self.router._known_track_id)
        self.assertIn(track_id, self.router._abandoned)

    async def test_a_delivered_reply_resolves_the_future(self):
        self.peer()
        track_id = await self.router.send("nodes.add", b"[]", "hub0")

        message = support.FakeMessage(
            body=b'{"code":0,"result":"ok"}',
            correlation_id=str(track_id),
        )
        await support.deliver(self.router, message)

        resp = await self.router.recv(track_id, timeout=1)
        self.assertEqual(resp.code, 0)
        self.assertEqual(resp.result, "ok")
        self.assertEqual(message.acks, 1)

class DeliveryTests(RouterTestCase):
    async def test_a_malformed_track_id_is_discarded(self):
        for correlation_id in ("not-a-uuid", None, ""):
            with self.subTest(correlation_id=correlation_id):
                message = support.FakeMessage(type="nodes.add", correlation_id=correlation_id)
                await support.deliver(self.router, message)
                self.assertEqual(message.nacks, [False])
                self.assertEqual(message.acks, 0)

    async def test_an_introduce_without_reply_to_is_discarded(self):
        message = support.FakeMessage(body=b'{"commands":[]}', type=router.INTRODUCE_COMMAND)
        await support.deliver(self.router, message)
        self.assertEqual(message.nacks, [False])
        self.assertEqual(self.sibs.get_name("nodes.add"), None)

    async def test_an_undecodable_introduce_is_discarded(self):
        message = support.FakeMessage(
            body=b"not json", type=router.INTRODUCE_COMMAND, reply_to="rpc.direct.hub0"
        )
        await support.deliver(self.router, message)
        self.assertEqual(message.nacks, [False])
        self.assertIsNone(self.sibs.get("rpc.direct.hub0"))

    async def test_an_introduce_registers_the_sibling(self):
        message = support.FakeMessage(
            body=b'{"commands":["nodes.add"]}',
            type=router.INTRODUCE_COMMAND,
            reply_to="rpc.direct.hub0",
        )
        await support.deliver(self.router, message)

        self.assertEqual(message.acks, 1)
        self.assertEqual(self.sibs.get_name("nodes.add"), ["rpc.direct.hub0"])

    async def test_an_unknown_command_is_nacked_without_dispatch(self):
        message = support.FakeMessage(
            body=b"{}", type="nodes.nope", correlation_id=str(uuid.uuid4()), reply_to="rpc.direct.rx0"
        )
        await support.deliver(self.router, message)
        await support.drain(self.router)

        self.assertEqual(message.nacks, [False])
        self.assertEqual(message.acks, 0)
        self.assertEqual(self.exchange.published, [])

    async def test_a_message_without_a_type_is_nacked(self):
        message = support.FakeMessage(body=b"{}", correlation_id=str(uuid.uuid4()))
        await support.deliver(self.router, message)
        self.assertEqual(message.nacks, [False])

    async def test_a_known_command_is_dispatched_and_replied_to(self):
        calls: list[tuple[uuid.UUID, bytes]] = []

        async def handler(track_id, req, rep):
            calls.append((track_id, req.payload))
            await rep(handlers.RouterResponse(0, "done"))

        self.handlers.set("ping", handler)
        track_id = uuid.uuid4()
        message = support.FakeMessage(
            body=b'{"a":1}', type="ping", correlation_id=str(track_id), reply_to="rpc.direct.rx0"
        )
        await support.deliver(self.router, message)
        await support.drain(self.router)

        self.assertEqual(message.acks, 1)
        self.assertEqual(message.nacks, [])
        self.assertEqual(calls, [(track_id, b'{"a":1}')])

        reply_message, routing_key = self.exchange.last
        self.assertEqual(routing_key, "rpc.direct.rx0")
        self.assertEqual(reply_message.correlation_id, str(track_id))
        self.assertEqual(reply_message.content_type, "application/json")
        self.assertEqual(json.loads(reply_message.body), {"code": 0, "result": "done"})

    async def test_replying_to_a_request_without_reply_to_raises(self):
        async def handler(track_id, req, rep):
            await rep(handlers.RouterResponse(0, "done"))

        self.handlers.set("ping", handler)
        message = support.FakeMessage(body=b"{}", type="ping", correlation_id=str(uuid.uuid4()))

        with self.assertLogs("lib.core.router", level="ERROR") as captured:
            await support.deliver(self.router, message)
            await support.drain(self.router)

        self.assertEqual(self.exchange.published, [])
        self.assertTrue(any("handler ping failed" in line for line in captured.output))

    async def test_a_reply_with_the_wrong_content_type_is_nacked(self):
        self.peer()
        track_id = await self.router.send("nodes.add", b"[]", "hub0")
        message = support.FakeMessage(
            body=b'{"code":0,"result":null}',
            correlation_id=str(track_id),
            content_type="text/plain",
        )
        await support.deliver(self.router, message)

        self.assertEqual(message.nacks, [False])
        self.assertFalse(self.router._known_track_id[track_id].done())

    async def test_an_undecodable_reply_is_nacked(self):
        self.peer()
        track_id = await self.router.send("nodes.add", b"[]", "hub0")
        message = support.FakeMessage(body=b"not json", correlation_id=str(track_id))
        await support.deliver(self.router, message)

        self.assertEqual(message.nacks, [False])
        self.assertFalse(self.router._known_track_id[track_id].done())

    async def test_a_second_reply_for_the_same_track_id_is_still_acked(self):
        self.peer()
        track_id = await self.router.send("nodes.add", b"[]", "hub0")
        first = support.FakeMessage(body=b'{"code":0,"result":"one"}', correlation_id=str(track_id))
        second = support.FakeMessage(body=b'{"code":0,"result":"two"}', correlation_id=str(track_id))

        await support.deliver(self.router, first)
        await support.deliver(self.router, second)

        self.assertEqual(second.acks, 1)
        resp = await self.router.recv(track_id, timeout=1)
        self.assertEqual(resp.result, "one")

class AbandonedTrackIDTests(RouterTestCase):
    async def _abandon_one(self) -> uuid.UUID:
        self.peer()
        track_id = await self.router.send("nodes.add", b"[]", "hub0")
        with self.assertRaises(asyncio.TimeoutError):
            await self.router.recv(track_id, timeout=0.01)
        return track_id

    async def test_a_late_reply_is_dropped_not_dispatched(self):
        dispatched: list[uuid.UUID] = []

        async def handler(track_id, req, rep):  # pragma: no cover - must not run
            dispatched.append(track_id)

        self.handlers.set("nodes.add", handler)
        track_id = await self._abandon_one()
        self.exchange.published.clear()  # forget the request send() published

        message = support.FakeMessage(
            body=b'{"code":0,"result":"late"}',
            type="nodes.add",
            correlation_id=str(track_id),
            reply_to="rpc.direct.hub0",
        )
        await support.deliver(self.router, message)
        await support.drain(self.router)

        self.assertEqual(dispatched, [])
        self.assertEqual(message.acks, 1)
        self.assertEqual(message.nacks, [])
        self.assertEqual(self.exchange.published, [])
        self.assertNotIn(track_id, self.router._abandoned)

    async def test_the_same_message_is_dispatched_when_not_abandoned(self):
        # The control for the test above: only the abandonment makes the
        # difference, not the message shape.
        dispatched: list[uuid.UUID] = []

        async def handler(track_id, req, rep):
            dispatched.append(track_id)
            await rep(handlers.RouterResponse(0, "ok"))

        self.handlers.set("nodes.add", handler)
        track_id = uuid.uuid4()
        message = support.FakeMessage(
            body=b'{"code":0,"result":"late"}',
            type="nodes.add",
            correlation_id=str(track_id),
            reply_to="rpc.direct.hub0",
        )
        await support.deliver(self.router, message)
        await support.drain(self.router)

        self.assertEqual(dispatched, [track_id])

    async def test_stale_entries_are_pruned_by_ttl(self):
        with unittest.mock.patch.object(router, "ABANDONED_MAX", 4):
            self.router._abandoned = {uuid.uuid4(): 0.0 for _ in range(4)}
            fresh = uuid.uuid4()
            self.router._abandon(fresh)

            self.assertEqual(list(self.router._abandoned), [fresh])

    async def test_the_map_is_capped_when_nothing_is_stale(self):
        with unittest.mock.patch.object(router, "ABANDONED_MAX", 4):
            now = asyncio.get_running_loop().time()
            self.router._abandoned = {uuid.uuid4(): now - i for i in range(4)}
            fresh = uuid.uuid4()
            self.router._abandon(fresh)

            self.assertIn(fresh, self.router._abandoned)
            self.assertLess(len(self.router._abandoned), 4)

class HandlerTaskTests(RouterTestCase):
    async def test_a_raising_handler_is_logged_and_untracked(self):
        async def handler(track_id, req, rep):
            raise RuntimeError("boom")

        self.handlers.set("ping", handler)
        message = support.FakeMessage(
            body=b"{}", type="ping", correlation_id=str(uuid.uuid4()), reply_to="rpc.direct.rx0"
        )

        with self.assertLogs("lib.core.router", level="ERROR") as captured:
            await support.deliver(self.router, message)
            await support.drain(self.router)

        self.assertEqual(self.router._inflight, set())
        self.assertTrue(any("handler ping failed: boom" in line for line in captured.output))
        # The request was still acked before the handler ran, so the caller
        # times out rather than the message being redelivered forever.
        self.assertEqual(message.acks, 1)

class ListenTests(RouterTestCase):
    async def test_listen_registers_and_cancels_the_consumer(self):
        task = asyncio.create_task(self.router.listen())
        await support.settle()

        self.assertEqual(self.queue.consumer, self.router._handle_delivery)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertEqual(self.queue.cancelled, [self.queue.consumer_tag])

if __name__ == "__main__":
    unittest.main()
