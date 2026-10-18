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
    _pipeline: pipeline.SRTStreamer
    _detector: Detector
    _chosen_classes: list[int]
    _async_q: asyncio.Queue[dict[str, int]]
    _node_id: uuid.UUID
    _worker_task: typing.Optional[asyncio.Task[None]] = None

    @classmethod
    def setup(
        cls,
        node_id: uuid.UUID,
        handlers_mgr: handlers.Handlers,
        pipeline: typing.Any,
        detector: Detector,
        chosen_classes: list[int],
        async_q: asyncio.Queue[dict[str, int]],
    ) -> None:
        cls._pipeline = pipeline
        cls._detector = detector
        cls._chosen_classes = chosen_classes
        cls._async_q = async_q
        cls._node_id = node_id

        # No analytics command is registered any more. Reading the timeseries is
        # the analytics service's job now: the node publishes its queries as a
        # manifest at boot and never serves a chart, so it needs no read handle
        # on its own collection -- only the write one the normalizer holds.
        handlers_mgr.set(f"{node_id}.sink.start", cls._start_sink)
        handlers_mgr.set(f"{node_id}.sink.stop", cls._stop_sink)

    @classmethod
    async def _stop_worker_task(cls) -> None:
        if cls._worker_task is not None and not cls._worker_task.done():
            cls._worker_task.cancel()
            try:
                await cls._worker_task
            except asyncio.CancelledError:
                pass
        cls._worker_task = None

    @classmethod
    async def _stop_sink(
        cls, _trackID: uuid.UUID, _req: handlers.RouterRequest, rep: handlers.ReplyFunc
    ) -> None:
        # set_state(NULL) on an srtsrc can block for seconds (poll-timeout), so
        # it must not run on the event loop: no introduce, no replies meanwhile.
        await asyncio.to_thread(cls._pipeline.stop)
        await cls._stop_worker_task()
        await rep(handlers.RouterResponse(0, ""))

    @classmethod
    async def _start_sink(
        cls, _: uuid.UUID, req: handlers.RouterRequest, rep: handlers.ReplyFunc
    ) -> None:
        src = NodeSource(**json.loads(req.payload))
        if src.source_address == "":
            await rep(handlers.RouterResponse(-1, "invalid source address"))
            return

        # Stop the previous pipeline, not just its worker: otherwise the old
        # srtsrc/appsink is leaked and its appsink callback keeps feeding the
        # queue alongside the new one.
        await asyncio.to_thread(cls._pipeline.stop)
        await cls._stop_worker_task()

        await asyncio.to_thread(cls._pipeline.start, src.source_address)

        loop = asyncio.get_running_loop()
        cls._worker_task = asyncio.create_task(
            asyncio.to_thread(
                processing.frame_worker,
                cls._pipeline,
                cls._detector,
                cls._chosen_classes,
                loop,
                cls._async_q,
            )
        )

        await rep(handlers.RouterResponse(0, ""))
