import asyncio
from dataclasses import dataclass
import json
import typing
import uuid

from ultralytics.models import YOLO
from core import handlers
from core import pipeline
from . import processing

class Pipeline(typing.Protocol):
    def stop(self) -> None:
        ...

    def start(self, srt_address: str) -> None:
        ...

@dataclass
class NodeSource:
    source_address: str

class BustleHandlers:
    _pipeline: pipeline.SRTStreamer
    _model: YOLO
    _chosen_classes: list[int]
    _async_q: asyncio.Queue[dict[str, int]]
    _worker_task: typing.Optional[asyncio.Task[None]] = None

    @classmethod
    def setup(
        cls,
        node_id: uuid.UUID,
        handlers_mgr: handlers.Handlers,
        pipeline: typing.Any,
        model: YOLO,
        chosen_classes: list[int],
        async_q: asyncio.Queue[dict[str, int]],
    ) -> None:
        cls._pipeline = pipeline
        cls._model = model
        cls._chosen_classes = chosen_classes
        cls._async_q = async_q

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
        cls._pipeline.stop()
        await cls._stop_worker_task()
        await rep(handlers.RouterResponse(0, ""))

    @classmethod
    async def _start_sink(
        cls, _: uuid.UUID, req: handlers.RouterRequest, rep: handlers.ReplyFunc
    ) -> None:
        src = NodeSource(**json.loads(req.payload))
        if src.source_address == "":
            rep(handlers.RouterResponse(-1, "invalid source address"))
            return

        await cls._stop_worker_task()
        cls._pipeline.start(src.source_address)

        loop = asyncio.get_running_loop()
        cls._worker_task = asyncio.create_task(
            asyncio.to_thread(
                processing.frame_worker,
                cls._pipeline,
                cls._model,
                cls._chosen_classes,
                loop,
                cls._async_q,
            )
        )

        await rep(handlers.RouterResponse(0, ""))
