"""Tests for ``lib.core.node`` -- the node runtime.

The lifecycle is the thing under test, so the fakes record the order they were
called in. What can go wrong on shutdown is a race: a task cancelled before it
has finished writing, a ``nodes.remove`` sent after the listener is gone, a
stop hook that never ran. None of those show up as an exception, which is why
the assertions are about sequence rather than about return values.
"""

from __future__ import annotations

import asyncio
import json
import typing
import unittest
import unittest.mock
import uuid

from lib.core import handlers, node, siblings

from . import support

NODE_ID = uuid.UUID("50154422-fd3b-4cf0-a761-908fcbe98a1e")

class RecordingWorker(node.Worker):
    """A worker that writes what happened into a shared list."""

    def __init__(self, rec: list[str], register_fails: bool = False) -> None:
        self.rec = rec
        self.attach_calls: list[handlers.Handlers] = []
        self.registered_calls: list[tuple[typing.Any, typing.Any]] = []
        self._registered_error = register_fails

    def attach(self, hs: handlers.Handlers) -> None:
        self.rec.append("attach")
        self.attach_calls.append(hs)

    def start(self) -> list[asyncio.Task[typing.Any]]:
        self.rec.append("start")
        return [asyncio.create_task(self._background())]

    async def _background(self) -> None:
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.rec.append("background cancelled")
            raise

    async def registered(self, r: typing.Any, sibs: typing.Any) -> None:
        self.rec.append("post-registration hook")
        self.registered_calls.append((r, sibs))

        if self._registered_error:
            raise RuntimeError("the hook failed")

    async def stop(self) -> None:
        self.rec.append("stop")

class FakeRouter:
    """A ``Router`` stand-in that records every call in the shared list.

    ``serve`` builds the real router, so the tests patch the class; this stands
    in for it and behaves like one from the lifecycle's point of view: ``listen``
    never returns on its own, ``send``/``recv`` answer a round trip.
    """

    def __init__(
        self,
        rec: list[str],
        listen_error: typing.Optional[BaseException] = None,
        failing: typing.Optional[set[str]] = None,
    ) -> None:
        self.rec = rec
        self.listen_error = listen_error
        self.failing = failing or set()
        self._pending: tuple[str, bytes, str] = ("", b"", "")

    async def listen(self) -> None:
        self.rec.append("listen started")

        if self.listen_error is not None:
            self.rec.append("listen failed")
            raise self.listen_error

        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.rec.append("listen cancelled")
            raise

    async def introduce(self, version: str) -> None:
        self.rec.append(f"introduce {version}")

    async def send(
        self, command: handlers.Command, payload: bytes, sibling_name: siblings.SiblingName
    ) -> uuid.UUID:
        self.rec.append(f"send {command}")
        self._pending = (command, payload, sibling_name)
        return uuid.uuid4()

    async def recv(
        self, track_id: uuid.UUID, timeout: typing.Optional[float] = None
    ) -> handlers.RouterResponse:
        command, _, _ = self._pending

        if command in self.failing:
            raise RuntimeError(f"{command} failed")

        return handlers.RouterResponse(0, "")

def make_node(
    rec: list[str],
    worker: node.Worker,
    *,
    intro_interval: float = 0.01,
    remove_timeout: float = 0.01,
    rpc_timeout: float = 0.05,
) -> node.Node:
    return node.Node(
        node_id=NODE_ID,
        name="bustle0",
        family="surr0",
        broker="amqp://guest:guest@localhost/",
        version="test-version",
        register_payload=node.register_payload(NODE_ID, bitrate=300, width=1280, height=720),
        worker=worker,
        intro_interval=intro_interval,
        rpc_timeout=rpc_timeout,
        remove_timeout=remove_timeout,
    )

def register_hub(n: node.Node) -> None:
    """Make the hub resolvable so ``nodes.add`` can be dispatched."""
    # The registry is built by the node, and a real one is populated by the
    # hub's own introduce broadcast. Seeding it is the shortest way to the state
    # that broadcast produces.
    n._siblings.set(  # noqa: SLF001
        "rpc.direct.hub0",
        siblings.Sibling(commands=["nodes.add", "nodes.remove"]),
    )

async def serve(
    n: node.Node,
    rec: list[str],
    stop_event: asyncio.Event,
    listen_error: typing.Optional[BaseException] = None,
    failing: typing.Optional[set[str]] = None,
) -> None:
    """Run ``serve`` against a fake channel and a fake router."""
    channel = support.FakeChannel()
    router = FakeRouter(rec, listen_error, failing)

    await _serve_with(n, channel, router, stop_event)

async def _serve_with(
    n: node.Node,
    channel: support.FakeChannel,
    router: FakeRouter,
    stop_event: asyncio.Event,
) -> None:
    with unittest.mock.patch.object(node.core_router, "Router", lambda *a, **kw: router):
        await n.serve(  # pyright: ignore[reportArgumentType]
            typing.cast(typing.Any, channel), stop_event
        )

class ShutdownOrderTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_worker_is_attached_before_the_listener_starts(self) -> None:
        rec: list[str] = []
        n = make_node(rec, RecordingWorker(rec))
        register_hub(n)

        stop_event = asyncio.Event()
        stop_event.set()

        await serve(n, rec, stop_event)

        self.assertIn("attach", rec)
        self.assertLess(rec.index("attach"), rec.index("listen started"))

    async def test_shutdown_runs_in_the_documented_order(self) -> None:
        rec: list[str] = []
        worker = RecordingWorker(rec)
        n = make_node(rec, worker)
        register_hub(n)

        stop_event = asyncio.Event()
        stop_event.set()

        await serve(n, rec, stop_event)

        # The frame source stops before the tasks reading from it are cancelled,
        # nodes.remove is sent while the listener is still up, and only then is
        # the listener cancelled.
        self.assertEqual(
            [step for step in rec if step in {"stop", "background cancelled", "send nodes.remove", "listen cancelled"}],
            ["stop", "background cancelled", "send nodes.remove", "listen cancelled"],
            rec,
        )

    async def test_a_node_that_never_registered_does_not_deregister(self) -> None:
        rec: list[str] = []
        n = make_node(rec, RecordingWorker(rec))

        # No hub in the registry: nodes.add can never be dispatched, so the
        # registration task keeps retrying and the event is never set.
        stop_event = asyncio.Event()
        stop_event.set()

        await serve(n, rec, stop_event)

        self.assertNotIn("send nodes.remove", rec)
        self.assertIn("listen cancelled", rec)

    async def test_a_node_whose_registration_failed_does_not_deregister(self) -> None:
        rec: list[str] = []
        n = make_node(rec, RecordingWorker(rec))

        # The hub is reachable -- nodes.remove could be dispatched -- but
        # nodes.add fails, so the node never registered and the hub has no row
        # to remove. Sending it anyway is how a restarting node deregisters the
        # row its own restart just created.
        register_hub(n)

        stop_event = asyncio.Event()
        stop_event.set()

        await serve(n, rec, stop_event, failing={"nodes.add"})

        self.assertNotIn("send nodes.remove", rec)

    async def test_the_listener_is_stopped_even_when_registration_never_completed(self) -> None:
        rec: list[str] = []
        n = make_node(rec, RecordingWorker(rec))

        stop_event = asyncio.Event()
        stop_event.set()

        await serve(n, rec, stop_event)

        # Otherwise the consumer stays parked on its future while the caller
        # closes the channel underneath it.
        self.assertIn("listen cancelled", rec)

class FailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_failed_listener_is_reported(self) -> None:
        rec: list[str] = []
        n = make_node(rec, RecordingWorker(rec))
        register_hub(n)

        stop_event = asyncio.Event()

        with self.assertRaises(RuntimeError):
            await serve(n, rec, stop_event, listen_error=RuntimeError("broker gone"))

        # Reported, not swallowed: a node whose listener died is not running,
        # and a clean exit is what makes a supervisor leave it up.
        self.assertIn("listen failed", rec)
        self.assertIn("stop", rec)

    async def test_a_failing_post_registration_hook_does_not_stop_the_node(self) -> None:
        rec: list[str] = []
        worker = RecordingWorker(rec, register_fails=True)
        n = make_node(rec, worker)
        register_hub(n)

        stop_event = asyncio.Event()

        with self.assertLogs("lib.core.node", level="ERROR") as logs:
            serving = asyncio.create_task(serve(n, rec, stop_event))

            # Wait for the hook to run and fail, then let the node keep going.
            for _ in range(200):
                if "post-registration hook" in rec:
                    break
                await asyncio.sleep(0.01)

            self.assertIn("post-registration hook", rec)
            self.assertFalse(serving.done(), "the node stopped over a failed hook")

            stop_event.set()
            await asyncio.wait_for(serving, timeout=5.0)

        self.assertTrue(any("post-registration hook failed" in line for line in logs.output))

    async def test_a_failing_deregistration_is_only_a_warning(self) -> None:
        rec: list[str] = []
        n = make_node(rec, RecordingWorker(rec))
        register_hub(n)

        stop_event = asyncio.Event()
        stop_event.set()

        channel = support.FakeChannel()
        router = FakeRouter(rec)

        original_send = router.send

        async def refuse_remove(
            command: handlers.Command, payload: bytes, sibling_name: siblings.SiblingName
        ) -> uuid.UUID:
            if command == "nodes.remove":
                raise RuntimeError("hub is gone")

            return await original_send(command, payload, sibling_name)

        router.send = refuse_remove  # type: ignore[method-assign]

        with self.assertLogs("lib.core.node", level="WARNING") as logs:
            await _serve_with(n, channel, router, stop_event)

        # A stale row is a nuisance; refusing to shut down over one is worse.
        self.assertTrue(any("nodes.remove failed" in line for line in logs.output))
        self.assertIn("listen cancelled", rec)

class RegistrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_post_registration_hook_gets_the_router_and_registry(self) -> None:
        rec: list[str] = []
        worker = RecordingWorker(rec)
        n = make_node(rec, worker)
        register_hub(n)

        stop_event = asyncio.Event()

        serving = asyncio.create_task(serve(n, rec, stop_event))

        for _ in range(200):
            if worker.registered_calls:
                break
            await asyncio.sleep(0.01)

        stop_event.set()
        await asyncio.wait_for(serving, timeout=5.0)

        self.assertEqual(len(worker.registered_calls), 1)

        r, sibs = worker.registered_calls[0]
        self.assertTrue(hasattr(r, "send"), "the hook needs a router to publish with")
        self.assertTrue(hasattr(sibs, "get_name"), "the hook needs the registry to resolve with")

class PayloadTests(unittest.TestCase):
    def test_the_register_payload_is_a_one_element_batch(self) -> None:
        # The hub takes a list, which is easy to get wrong because there is
        # exactly one node in it.
        body = json.loads(node.register_payload(NODE_ID, bitrate=300, width=1280, height=720))

        self.assertIsInstance(body, list)
        self.assertEqual(
            body,
            [{"id": str(NODE_ID), "bitrate": 300, "width": 1280, "height": 720}],
        )

    def test_the_register_payload_carries_only_the_fields_it_was_given(self) -> None:
        body = json.loads(node.register_payload(NODE_ID))

        self.assertEqual(body, [{"id": str(NODE_ID)}])

class IntroduceTests(unittest.IsolatedAsyncioTestCase):
    async def test_introduce_broadcasts_the_version_each_tick(self) -> None:
        rec: list[str] = []
        router = FakeRouter(rec)

        task = asyncio.create_task(
            node.introduce(0.01, typing.cast(typing.Any, router), "build-123")
        )

        for _ in range(200):
            if len([step for step in rec if step.startswith("introduce")]) >= 2:
                break
            await asyncio.sleep(0.01)

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        self.assertIn("introduce build-123", rec)
