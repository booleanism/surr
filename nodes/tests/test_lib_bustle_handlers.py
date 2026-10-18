"""``lib.bustle.handlers``: the two commands a bustle node serves.

There was no test file for this module at all until the class was de-singletoned,
which is why the defect it fixes went unnoticed: every dependency lived on the
*class*, so a second instance replaced the first's pipeline and node id without
raising anything. The first node then served its own commands with the second's
pipeline and counted the wrong frames.

The class below is therefore as much about the wiring as about the replies: the
names, the payload field and the reply codes are the wire contract the Go side
sends (``<node-id>.sink.start`` with ``{"source_address": ...}``), and the
per-instance checks are what pin the de-singletoning.

Nothing here touches GStreamer or the detector: the streamer is a fake that
records start/stop, and ``processing.frame_worker`` is patched with one that
blocks until the test releases it, so "a task was created" and "a task was
cancelled" can be asserted without a pipeline or a thread that outlives the
test.
"""

from __future__ import annotations

import asyncio
import json
import threading
import unittest
import uuid
from unittest import mock

from lib.core import handlers
from lib.bustle import handlers as bustle_handlers
from lib.bustle import processing

NODE_ID = uuid.UUID("50154422-fd3b-4cf0-a761-908fcbe98a1e")
OTHER_NODE_ID = uuid.UUID("74d16bbc-f350-432e-9d2b-3b712d84a332")

class FakeStreamer:
    """Stands in for SRTStreamer: records the calls, never builds a pipeline.

    The real one constructs GStreamer elements, so a test using it would need an
    element registry and the plugins the CI runner does not install. What this
    module does with the streamer is call start/stop and hand it to the worker,
    which is exactly what is recorded here.
    """

    def __init__(self) -> None:
        self.started: list[str] = []
        self.stopped = 0

    def start(self, source_address: str) -> None:
        self.started.append(source_address)

    def stop(self) -> None:
        self.stopped += 1

class FakeDetector:
    """Only ``names`` is read by anything reachable from here."""

    @property
    def names(self) -> dict[int, str]:
        return {0: "person", 1: "bicycle"}

class Recorder:
    """Collects replies, so a handler can be awaited without a broker."""

    def __init__(self) -> None:
        self.responses: list[handlers.RouterResponse] = []

    async def reply(self, response: handlers.RouterResponse) -> None:
        self.responses.append(response)

    @property
    def last(self) -> handlers.RouterResponse:
        return self.responses[-1]

class WorkerGate:
    """A ``frame_worker`` that blocks until released.

    A task wrapping a real ``frame_worker`` finishes immediately against a fake
    streamer (there is no ``running`` flag to loop on), which would make
    "the task is still alive" untestable. Blocking on an event instead makes the
    task's state exactly what the test decides, and ``release`` in teardown stops
    ``asyncio.run``'s executor shutdown from waiting on a thread forever.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []
        self.entered = threading.Event()
        self.release = threading.Event()

    def __call__(self, *args: object) -> None:
        self.calls.append(args)
        self.entered.set()
        self.release.wait(timeout=10)

    async def wait_entered(self) -> None:
        # The task wrapping this runs in an executor thread, so the call lands
        # one loop iteration and one thread start after sink.start returns.
        # Waiting for the event is what makes the assertions below about the
        # worker's arguments rather than about a race.
        await asyncio.to_thread(self.entered.wait, 10)
        self.release.set()

def build(
    gate: WorkerGate,
    node_id: uuid.UUID = NODE_ID,
    streamer: FakeStreamer | None = None,
) -> tuple[bustle_handlers.BustleHandlers, FakeStreamer, handlers.Handlers]:
    streamer = streamer if streamer is not None else FakeStreamer()
    hs = handlers.Handlers()
    bh = bustle_handlers.BustleHandlers(
        node_id,
        streamer,  # type: ignore[arg-type]  # the fake implements the two calls used
        FakeDetector(),  # type: ignore[arg-type]
        [0],
        asyncio.Queue(),
    )
    bh.register(hs)

    return bh, streamer, hs

def request(source_address: str) -> handlers.RouterRequest:
    return handlers.RouterRequest(json.dumps({"source_address": source_address}).encode())

class HandlerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.gate = WorkerGate()
        self.patch = mock.patch.object(processing, "frame_worker", self.gate)
        self.patch.start()

    def tearDown(self) -> None:
        self.gate.release.set()
        self.patch.stop()

class RegistrationTests(HandlerTestCase):
    def test_the_two_commands_are_the_node_id_plus_the_verb(self):
        # The names embed the node id because the hub addresses one specific
        # node; a name without the prefix would answer on a shared topic and the
        # reply would reach whoever asked first.
        _, _, hs = build(self.gate)
        self.assertEqual(
            sorted(hs.available_commands()),
            sorted([f"{NODE_ID}.sink.start", f"{NODE_ID}.sink.stop"]),
        )

    def test_no_analytics_command_is_registered(self):
        # Reading the timeseries belongs to the analytics service; a node that
        # still answered a chart command would be a second reader with a second
        # copy of the aggregation.
        _, _, hs = build(self.gate)
        self.assertFalse([c for c in hs.available_commands() if "analytics" in c])

    def test_two_instances_keep_their_own_node_ids(self):
        # The defect this class exists for: with class attributes the second
        # registration replaced the first's dependencies, so the first node's
        # commands ran against the second node's pipeline.
        first, first_streamer, first_hs = build(self.gate, NODE_ID)
        second, second_streamer, second_hs = build(self.gate, OTHER_NODE_ID)

        self.assertIn(f"{NODE_ID}.sink.start", first_hs.available_commands())
        self.assertIn(f"{OTHER_NODE_ID}.sink.start", second_hs.available_commands())
        self.assertIsNot(first, second)

        start_first = first_hs.get(f"{NODE_ID}.sink.start")
        start_second = second_hs.get(f"{OTHER_NODE_ID}.sink.start")
        # Both are bound methods of different instances; the registry stores
        # whatever was registered, so this is what proves the two commands do
        # not share one object.
        self.assertIsNot(
            getattr(start_first, "__self__", None),
            getattr(start_second, "__self__", None),
        )

        async def drive() -> None:
            await start_first(uuid.uuid4(), request("srt://rx:9001"), Recorder().reply)
            first._worker_task.cancel()  # type: ignore[union-attr]
            self.gate.release.set()

        asyncio.run(drive())
        self.assertEqual(first_streamer.started, ["srt://rx:9001"])
        self.assertEqual(second_streamer.started, [])

class StartSinkTests(HandlerTestCase):
    def test_a_valid_source_starts_the_pipeline_and_replies_zero(self):
        bh, streamer, hs = build(self.gate)
        recorder = Recorder()

        async def drive() -> None:
            await hs.get(f"{NODE_ID}.sink.start")(
                uuid.uuid4(), request("srt://rx:9001"), recorder.reply
            )
            self.gate.release.set()

        asyncio.run(drive())
        self.assertEqual(streamer.started, ["srt://rx:9001"])
        self.assertEqual(recorder.last.code, 0)
        self.assertIsNotNone(bh._worker_task)

    def test_the_worker_gets_the_dependencies_this_node_was_built_with(self):
        # The de-singletoning is only worth anything if the right objects reach
        # the worker: a shared class attribute passed whichever was assigned
        # last, and the frames were counted with the wrong detector.
        bh, streamer, hs = build(self.gate)

        async def drive() -> None:
            await hs.get(f"{NODE_ID}.sink.start")(
                uuid.uuid4(), request("srt://rx:9001"), Recorder().reply
            )
            await self.gate.wait_entered()

        asyncio.run(drive())

        self.assertEqual(len(self.gate.calls), 1)
        receiver, detector, chosen_classes, _, async_q = self.gate.calls[0]
        self.assertIs(receiver, streamer)
        self.assertIsInstance(detector, FakeDetector)
        self.assertEqual(chosen_classes, [0])
        self.assertIs(async_q, bh._async_q)

    def test_an_empty_source_is_refused_without_touching_the_pipeline(self):
        # The code is -1, which the Go adapters turn into a failure: a node that
        # refuses a command must not look like a started sink.
        bh, streamer, hs = build(self.gate)
        recorder = Recorder()

        asyncio.run(hs.get(f"{NODE_ID}.sink.start")(uuid.uuid4(), request(""), recorder.reply))

        self.assertEqual(recorder.last.code, -1)
        self.assertEqual(recorder.last.result, "invalid source address")
        self.assertEqual(streamer.started, [])
        self.assertIsNone(bh._worker_task)
        self.assertEqual(self.gate.calls, [])

    def test_a_restart_stops_the_previous_pipeline_first(self):
        # Not just the worker: a leaked srtsrc/appsink keeps its callback
        # feeding the queue alongside the new one, so two pipelines would be
        # counted into the same numbers.
        _, streamer, hs = build(self.gate)
        start = hs.get(f"{NODE_ID}.sink.start")

        async def drive() -> None:
            await start(uuid.uuid4(), request("srt://rx:9001"), Recorder().reply)
            await start(uuid.uuid4(), request("srt://rx:9002"), Recorder().reply)
            self.gate.release.set()

        asyncio.run(drive())
        self.assertEqual(streamer.started, ["srt://rx:9001", "srt://rx:9002"])
        # One stop per start, and before it: the sequence is stop, start, stop,
        # start, which the two counters alone cannot show.
        self.assertEqual(streamer.stopped, 2)

class StopSinkTests(HandlerTestCase):
    def test_stop_without_a_start_is_harmless(self):
        # The hub may send sink.stop to a node that never started, and the
        # shutdown path does the same; a None worker task must not raise.
        _, streamer, hs = build(self.gate)
        recorder = Recorder()

        asyncio.run(
            hs.get(f"{NODE_ID}.sink.stop")(uuid.uuid4(), handlers.RouterRequest(b""), recorder.reply)
        )

        self.assertEqual(streamer.stopped, 1)
        self.assertEqual(recorder.last.code, 0)

    def test_stop_cancels_the_worker_and_clears_it(self):
        bh, _, hs = build(self.gate)
        recorder = Recorder()

        async def drive() -> None:
            await hs.get(f"{NODE_ID}.sink.start")(
                uuid.uuid4(), request("srt://rx:9001"), recorder.reply
            )
            task = bh._worker_task
            self.assertIsNotNone(task)
            self.assertFalse(task.done())  # type: ignore[union-attr]

            await hs.get(f"{NODE_ID}.sink.stop")(
                uuid.uuid4(), handlers.RouterRequest(b""), recorder.reply
            )
            self.assertTrue(task.done())  # type: ignore[union-attr]
            self.assertTrue(task.cancelled())  # type: ignore[union-attr]
            self.assertIsNone(bh._worker_task)
            self.gate.release.set()

        asyncio.run(drive())
        self.assertEqual(recorder.last.code, 0)

    def test_one_nodes_stop_leaves_the_others_worker_alone(self):
        # The shared class attribute meant a stop on either instance cancelled
        # whichever worker had been assigned last.
        first, _, first_hs = build(self.gate, NODE_ID)
        second, _, second_hs = build(self.gate, OTHER_NODE_ID)

        async def drive() -> None:
            recorder = Recorder()
            await first_hs.get(f"{NODE_ID}.sink.start")(
                uuid.uuid4(), request("srt://rx:9001"), recorder.reply
            )
            await second_hs.get(f"{OTHER_NODE_ID}.sink.start")(
                uuid.uuid4(), request("srt://rx:9002"), recorder.reply
            )

            await first_hs.get(f"{NODE_ID}.sink.stop")(
                uuid.uuid4(), handlers.RouterRequest(b""), recorder.reply
            )

            self.assertIsNone(first._worker_task)
            self.assertIsNotNone(second._worker_task)
            self.assertFalse(second._worker_task.done())  # type: ignore[union-attr]
            # ``done()`` alone does not catch a shared class attribute: the
            # first node's stop cancels the second node's task and leaves the
            # *class* holding it, so the second still looks alive until the
            # cancellation lands. ``cancelled()`` is the assertion that fails.
            self.assertFalse(second._worker_task.cancelled())  # type: ignore[union-attr]
            self.gate.release.set()

        asyncio.run(drive())
