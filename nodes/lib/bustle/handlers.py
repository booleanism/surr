import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import typing
import uuid

from pymongo import collection
from ultralytics.models import YOLO
from core import handlers
from core import pipeline
from lib.bustle.analytics import detail_line_chart_pipeline, summary_bar_chart_pipeline
from . import processing

@dataclass
class AnalyticsParams:
    time_cluster: str

@dataclass
class NodeSource:
    source_address: str

class BustleHandlers:
    _pipeline: pipeline.SRTStreamer
    _model: YOLO
    _chosen_classes: list[int]
    _async_q: asyncio.Queue[dict[str, int]]
    _coll: collection.Collection[dict[str, typing.Any]]
    _node_id: uuid.UUID
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
        coll: collection.Collection[dict[str, typing.Any]]
    ) -> None:
        cls._pipeline = pipeline
        cls._model = model
        cls._chosen_classes = chosen_classes
        cls._async_q = async_q
        cls._coll = coll
        cls._node_id = node_id

        handlers_mgr.set(f"{node_id}.sink.start", cls._start_sink)
        handlers_mgr.set(f"{node_id}.sink.stop", cls._stop_sink)
        handlers_mgr.set(f"{node_id}.sink.analytics", cls._analytics)

    @classmethod
    async def _analytics(
            cls, _trackID: uuid.UUID, req: handlers.RouterRequest, rep: handlers.ReplyFunc
    ) -> None:
        try:
            p = AnalyticsParams(**json.loads(req.payload))
            match p.time_cluster:
                case "minutes":
                    start_time = datetime.now(timezone.utc) - timedelta(minutes=5)
                    unit_time = "second"
                case "hours":
                    start_time = datetime.now(timezone.utc) - timedelta(hours=5)
                    unit_time = "minute"
                case "dailies":
                    start_time = datetime.now(timezone.utc) - timedelta(days=5)
                    unit_time = "hour"
                case _:
                    start_time = datetime.now(timezone.utc) - timedelta(weeks=5)
                    unit_time = "day"

            # A five-week timeseries aggregation can block for a long time; on
            # the event loop it would stall every introduce and every reply.
            summary_bar_chart, detail_line_chart = await asyncio.to_thread(
                cls._aggregate, start_time, unit_time
            )

            await rep(handlers.RouterResponse(0, json.dumps([summary_bar_chart, detail_line_chart], default=str).encode()))
        except Exception as e:
            await rep(handlers.RouterResponse(-2, str(e)))
            raise
        return None

    @classmethod
    def _aggregate(
        cls, start_time: datetime, unit_time: str
    ) -> tuple[list[typing.Any], list[typing.Any]]:
        summary_bar_chart = list(cls._coll.aggregate(summary_bar_chart_pipeline(cls._node_id, start_time)))
        detail_line_chart = list(cls._coll.aggregate(detail_line_chart_pipeline(cls._node_id, start_time, unit_time)))
        return summary_bar_chart, detail_line_chart

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
                cls._model,
                cls._chosen_classes,
                loop,
                cls._async_q,
            )
        )

        await rep(handlers.RouterResponse(0, ""))
