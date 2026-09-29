import argparse
import asyncio
import json
import os
import signal
import typing
import uuid

import aio_pika
import pymongo
from pymongo import collection
from ultralytics.models import YOLO

from core import handlers as core_handlers
from core import pipeline as core_pipeline
from core import rpc as core_rpc
from core import router as core_router
from core import siblings as core_siblings
from core import ticker as core_ticker
from lib.bustle import handlers as bustle_handlers
from lib.bustle import processing as bustle_processing

import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst # pyright: ignore[reportMissingModuleSource]

DEFAULT_NODE_ID = "50154422-fd3b-4cf0-a761-908fcbe98a1e"
DEFAULT_NODE_NAME = "bustle0"
DEFAULT_NODE_TYPE = "bustle"
DEFAULT_FAMILY = "surr0"
DEFAULT_BROKER = "amqp://guest:guest@localhost/"
DEFAULT_MONGO_URL = "mongodb://root:root@127.0.0.1:27017/"
DEFAULT_MONGO_DB = "nodes"
DEFAULT_MODEL = "yolo26s.pt"
DEFAULT_CHOSEN_CLASSES = "0,1,2,3"
DEFAULT_INTRO_INTERVAL = 1.0
DEFAULT_DRAIN_INTERVAL = 2.0
# Matches tx's DefaultStreamSpec
DEFAULT_WIDTH = 1280
DEFAULT_HEIGHT = 720
DEFAULT_BITRATE = 300
DEFAULT_RPC_TIMEOUT = 5.0


def parse_classes(s: str) -> typing.List[int]:
    try:
        return [int(c) for c in s.split(",") if c != ""]
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"invalid class list: {s!r}") from e


def parse_args(argv: typing.Optional[typing.List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bustle",
        description="Bustle AI node: object counter worker",
    )
    parser.add_argument(
        "--node-id",
        type=uuid.UUID,
        default=DEFAULT_NODE_ID,
        help="stable uuid identifying this node (must match <node-id>.sink.* commands)",
    )
    parser.add_argument(
        "-n", "--name",
        dest="node_name",
        default=DEFAULT_NODE_NAME,
        help="sibling/service name, unique per running instance",
    )
    parser.add_argument(
        "-t", "--type",
        dest="node_type",
        default=DEFAULT_NODE_TYPE,
        help="AI worker type recorded with analytics",
    )
    parser.add_argument(
        "-f", "--family",
        dest="family",
        default=DEFAULT_FAMILY,
        help="broker exchange/family name, must match the other services",
    )
    parser.add_argument(
        "-b", "--broker",
        dest="broker",
        default=os.environ.get("RABBITMQ_URL", DEFAULT_BROKER),
        help="rabbitmq url (env RABBITMQ_URL)",
    )
    parser.add_argument(
        "--mongo-url",
        dest="mongo_url",
        default=os.environ.get("MONGO_URL", DEFAULT_MONGO_URL),
        help="mongodb url (env MONGO_URL)",
    )
    parser.add_argument(
        "--mongo-db",
        dest="mongo_db",
        default=os.environ.get("MONGO_DB", DEFAULT_MONGO_DB),
        help="mongodb database for timeseries collections (env MONGO_DB)",
    )
    parser.add_argument(
        "-m", "--model",
        dest="model",
        default=DEFAULT_MODEL,
        help="path to the YOLO weights used for inference",
    )
    parser.add_argument(
        "-c", "--chosen-classes",
        dest="chosen_classes",
        type=parse_classes,
        default=DEFAULT_CHOSEN_CLASSES,
        help="comma separated class ids to count (see YOLO model.names)",
    )
    parser.add_argument(
        "--intro-interval",
        dest="intro_interval",
        type=float,
        default=DEFAULT_INTRO_INTERVAL,
        help="seconds between introduce broadcasts",
    )
    parser.add_argument(
        "--drain-interval",
        dest="drain_interval",
        type=float,
        default=DEFAULT_DRAIN_INTERVAL,
        help="seconds between draining the frame-count queue into mongodb",
    )
    parser.add_argument(
        "--width",
        dest="width",
        type=int,
        default=DEFAULT_WIDTH,
        help="stream width advertised to the hub on nodes.add",
    )
    parser.add_argument(
        "--height",
        dest="height",
        type=int,
        default=DEFAULT_HEIGHT,
        help="stream height advertised to the hub on nodes.add",
    )
    parser.add_argument(
        "--bitrate",
        dest="bitrate",
        type=int,
        default=DEFAULT_BITRATE,
        help="stream bitrate advertised to the hub on nodes.add",
    )
    return parser.parse_args(argv)


async def introduce(
    interval: float,
    intro: typing.Callable[[], typing.Coroutine[typing.Any, typing.Any, None]],
) -> None:
    ticker_gen = core_ticker.ticker(interval)
    async for _ in ticker_gen:
        await intro()


def install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass


def init_db(mongo_url: str, mongo_db: str, node_id: uuid.UUID) -> collection.Collection:
    mongo_client = pymongo.MongoClient(mongo_url)
    nodes_db = mongo_client.get_database(mongo_db)

    id = str(node_id).replace("-", "")
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
    args = parse_args()

    Gst.init(None) # pyright: ignore[reportAttributeAccessIssue]

    try:
        coll = await asyncio.to_thread(
            init_db, args.mongo_url, args.mongo_db, args.node_id
        )
    except Exception as e:
        print(f"ERRO: error opening database connection: {e}")
        return

    handlers = core_handlers.Handlers()
    siblings = core_siblings.Siblings()
    streamer = core_pipeline.SRTStreamer(args.node_id)
    async_q: asyncio.Queue[dict[str, int]] = asyncio.Queue()
    model = YOLO(args.model)

    bustle_handlers.BustleHandlers.setup(
        args.node_id,
        handlers,
        streamer,
        model,
        args.chosen_classes,
        async_q,
        coll,
    )

    tasks: list[asyncio.Task] = []
    router: typing.Optional[core_router.Router] = None
    listen_task: typing.Optional[asyncio.Task] = None
    stop_waiter: typing.Optional[asyncio.Task] = None

    reg_payload = json.dumps(
        [
            {
                "id": str(args.node_id),
                "bitrate": args.bitrate,
                "width": args.width,
                "height": args.height,
            }
        ]
    ).encode()

    registered_event = asyncio.Event()

    async def register(r: core_router.Router) -> None:
        """Register the node once, retrying until the hub is discovered.

        Retries (rather than failing outright) because the node may start
        before the hub has introduced itself.
        """
        await core_rpc.call(
            r,
            siblings,
            "nodes.add",
            reg_payload,
            timeout=DEFAULT_RPC_TIMEOUT,
            retry_interval=args.intro_interval,
        )
        print("INFO: node registered via nodes.add")
        registered_event.set()

    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)

    connection = await aio_pika.connect_robust(args.broker)
    async with connection:
        try:
            channel = await connection.channel()
            queue, exchange, routing_id = await core_router.setup_router(
                channel, args.node_name, args.family
            )
            router = core_router.Router(
                channel, queue, exchange, routing_id, handlers, siblings
            )

            listen_task = asyncio.create_task(router.listen())
            print("INFO: starting router listen")

            intro_task = asyncio.create_task(
                introduce(args.intro_interval, router.introduce)
            )

            normalizer_task = asyncio.create_task(
                bustle_processing.normalizer(
                    async_q,
                    args.drain_interval,
                    args.node_id,
                    args.node_type,
                    coll,
                )
            )
            print("INFO: starting normalizer")

            register_task = asyncio.create_task(register(router))
            print("INFO: starting registration")

            stop_waiter = asyncio.create_task(stop_event.wait())

            tasks.extend([listen_task, intro_task, normalizer_task, register_task])

            # listen/intro/normalizer never complete on their own, so any
            # completion here means shutdown or a crashed task. register_task
            # is deliberately excluded: it finishes once nodes.add succeeds
            # and the node keeps running afterwards.
            done, _ = await asyncio.wait(
                [listen_task, intro_task, normalizer_task, stop_waiter],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if stop_waiter in done:
                print("INFO: shutdown signal received, stopping bustle node...")
            else:
                for task in done:
                    exc = task.exception()
                    if exc is not None:
                        raise exc

        except asyncio.CancelledError:
            print("INFO: shutting down bustle node...")
        finally:
            streamer.stop()

            # stop background work first; listen stays alive so the
            # nodes.remove reply can still be received
            background = [t for t in tasks if t is not listen_task]
            for task in background:
                if not task.done():
                    task.cancel()

            if background:
                await asyncio.gather(*background, return_exceptions=True)

            if (
                registered_event.is_set()
                and listen_task is not None
                and not listen_task.done()
                and router is not None
            ):
                try:
                    await asyncio.wait_for(
                        core_rpc.call(
                            router,
                            siblings,
                            "nodes.remove",
                            reg_payload,
                            timeout=2.0,
                            retry_interval=0.0,
                            retries=0,
                        ),
                        timeout=DEFAULT_RPC_TIMEOUT,
                    )
                    print("INFO: node deregistered via nodes.remove")
                except Exception as e:
                    print(f"WARN: nodes.remove failed (stale row may remain in hub): {e}")

                listen_task.cancel()
                await asyncio.gather(listen_task, return_exceptions=True)

            if stop_waiter is not None and not stop_waiter.done():
                stop_waiter.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("ERRO: process terminated by user.")
