import asyncio
from dataclasses import dataclass
import json
import typing
import uuid

from lib.core import handlers
from lib.core import pipeline
from lib.detector import Detector
from . import processing

@dataclass
class NodeSource:
    source_address: str

class BustleHandlers:
    """The two commands a bustle node serves, as an object.

    An instance, not a set of class attributes: two of these used to clobber
    each other silently, because ``setup()`` wrote the pipeline, detector and
    node id onto the *class*. A second node -- or a test running two in one
    process -- replaced the first's dependencies and the first then served its
    own commands with the second's pipeline. Nothing raised; the wrong frames
    were just counted.

    The dependencies arrive at construction and are never reassigned. The one
    piece of mutable state is ``_worker_task``, which is per-sink: ``sink.start``
    replaces it and ``sink.stop`` cancels it, and a shared class attribute would
    let one node's stop cancel another's worker.
    """

    def __init__(
        self,
        node_id: uuid.UUID,
        streamer: pipeline.SRTStreamer,
        detector: Detector,
        chosen_classes: list[int],
        async_q: asyncio.Queue[dict[str, int]],
    ) -> None:
        self._node_id = node_id
        self._pipeline = streamer
        self._detector = detector
        self._chosen_classes = chosen_classes
        self._async_q = async_q
        self._worker_task: typing.Optional[asyncio.Task[None]] = None

    def register(self, handlers_mgr: handlers.Handlers) -> None:
        # No analytics command is registered any more. Reading the timeseries is
        # the analytics service's job now: the node publishes its queries as a
        # manifest at boot and never serves a chart, so it needs no read handle
        # on its own collection -- only the write one the normalizer holds.
        handlers_mgr.set(f"{self._node_id}.sink.start", self._start_sink)
        handlers_mgr.set(f"{self._node_id}.sink.stop", self._stop_sink)

    async def _stop_worker_task(self) -> None:
        if self._worker_task is not None and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        self._worker_task = None

    async def _stop_sink(
        self, _trackID: uuid.UUID, _req: handlers.RouterRequest, rep: handlers.ReplyFunc
    ) -> None:
        # set_state(NULL) on an srtsrc can block for seconds (poll-timeout), so
        # it must not run on the event loop: no introduce, no replies meanwhile.
        await asyncio.to_thread(self._pipeline.stop)
        await self._stop_worker_task()
        await rep(handlers.RouterResponse(0, ""))

    async def _start_sink(
        self, _: uuid.UUID, req: handlers.RouterRequest, rep: handlers.ReplyFunc
    ) -> None:
        src = NodeSource(**json.loads(req.payload))
        if src.source_address == "":
            await rep(handlers.RouterResponse(-1, "invalid source address"))
            return

        # Stop the previous pipeline, not just its worker: otherwise the old
        # srtsrc/appsink is leaked and its appsink callback keeps feeding the
        # queue alongside the new one.
        await asyncio.to_thread(self._pipeline.stop)
        await self._stop_worker_task()

        await asyncio.to_thread(self._pipeline.start, src.source_address)

        loop = asyncio.get_running_loop()
        self._worker_task = asyncio.create_task(
            asyncio.to_thread(
                processing.frame_worker,
                self._pipeline,
                self._detector,
                self._chosen_classes,
                loop,
                self._async_q,
            )
        )

        await rep(handlers.RouterResponse(0, ""))
