import asyncio
import typing
import uuid

import aio_pika
import pymongo
from pymongo import collection
from ultralytics.models import YOLO

from core import handlers as core_handlers
from core import pipeline as core_pipeline
from core import router as core_router
from core import siblings as core_siblings
from core import ticker as core_ticker
from lib.bustle import handlers as bustle_handlers
from lib.bustle import processing as bustle_processing

import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst

NODE_ID = uuid.UUID("50154422-fd3b-4cf0-a761-908fcbe98a1e")
NODE_NAME = "bustle0"
EXCHANGE_NAME = "surr0"
INTRO_INTERVAL = 1
DRAIN_INTERVAL_SECONDS = 2

CHOSEN_CLASSES = [0, 1, 2, 3]


async def introduce(
    interval: int,
    intro: typing.Callable[[], typing.Coroutine[typing.Any, typing.Any, None]],
) -> None:
    ticker_gen = core_ticker.ticker(interval)
    async for _ in ticker_gen:
        await intro()


def init_db() -> collection.Collection:
    mongo_client = pymongo.MongoClient("mongodb://root:root@127.0.0.1:27017/")
    nodes_db = mongo_client.get_database("nodes")

    id = str(NODE_ID).replace("-", "")
    if id not in nodes_db.list_collection_names():
        nodes_db.create_collection(
            id,
            timeseries={
                "timeField": "timestamp",
                "metaField": "node",
                "granularity": "seconds",
            },
        )

    return nodes_db.get_collection(id)


async def main():
    Gst.init(None)

    try:
        coll = await asyncio.to_thread(init_db)
    except Exception as e:
        print(f"ERRO: error opening database connection: {e}")
        return

    handlers = core_handlers.Handlers()
    siblings = core_siblings.Siblings()
    streamer = core_pipeline.SRTStreamer(NODE_ID)
    async_q: asyncio.Queue[dict[str, int]] = asyncio.Queue()
    model = YOLO("yolo26s.pt")

    bustle_handlers.BustleHandlers.setup(NODE_ID, handlers, streamer, model, CHOSEN_CLASSES, async_q, coll)

    tasks: list[asyncio.Task] = []
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    async with connection:
        try:
            channel = await connection.channel()
            queue, exchange, routing_id = await core_router.setup_router(
                channel, NODE_NAME, EXCHANGE_NAME
            )
            router = core_router.Router(
                channel, queue, exchange, routing_id, handlers, siblings
            )

            listen_task = asyncio.create_task(router.listen())
            print("INFO: starting router listen")

            intro_task = asyncio.create_task(
                introduce(INTRO_INTERVAL, router.introduce)
            )

            normalizer_task = asyncio.create_task(
                bustle_processing.normalizer(
                    async_q, DRAIN_INTERVAL_SECONDS, NODE_ID, coll
                )
            )
            print("INFO: starting normalizer")

            tasks.extend([listen_task, intro_task, normalizer_task])
            done, _ = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_EXCEPTION
            )

            for task in done:
                exc = task.exception()
                if exc is not None:
                    raise exc

        except asyncio.CancelledError:
            print("INFO: shutting down bustle node...")
        finally:
            streamer.stop()
            for task in tasks:
                if not task.done():
                    task.cancel()

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("ERRO: process terminated by user.")
