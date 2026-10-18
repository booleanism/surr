import argparse
import asyncio
import logging
import os
import sys
import typing
import uuid

import pymongo
from pymongo import collection
from pymongo.errors import CollectionInvalid

from lib import detector as lib_detector
from lib import version as lib_version
from lib.bustle import analytics as bustle_analytics
from lib.bustle import handlers as bustle_handlers
from lib.bustle import processing as bustle_processing
from lib.core import handlers as core_handlers
from lib.core import logging_setup
from lib.core import node as core_node
from lib.core import pipeline as core_pipeline
from lib.core import rpc as core_rpc
from lib.core import siblings as core_siblings

logger = logging.getLogger("bustle")

import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst

DEFAULT_NODE_ID = "50154422-fd3b-4cf0-a761-908fcbe98a1e"
DEFAULT_NODE_NAME = "bustle0"
DEFAULT_NODE_TYPE = "bustle"
DEFAULT_FAMILY = "surr0"
DEFAULT_BROKER = "amqp://guest:guest@localhost/"
DEFAULT_MONGO_URL = "mongodb://root:root@127.0.0.1:27017/"
DEFAULT_MONGO_DB = "nodes"
DEFAULT_MODEL = lib_detector.DEFAULT_WEIGHTS
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


def redact_url(raw: str) -> str:
    """Reduce a credential-bearing URI to scheme://host (never-log rule)."""
    if "://" not in raw:
        return "unparseable-url"
    scheme, rest = raw.split("://", 1)
    host = rest.split("@", 1)[-1].split("/", 1)[0].split("?", 1)[0]
    return f"{scheme}://{host}"


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
        help="detector weights: a path, or a bare name resolved under ./weights and downloaded on first use",
    )
    parser.add_argument(
        "--conf",
        dest="confidence",
        type=float,
        default=lib_detector.DEFAULT_CONFIDENCE,
        help="drop detections below this score; the decoder applies no NMS, so low-confidence boxes are counted too",
    )
    parser.add_argument(
        "-c", "--chosen-classes",
        dest="chosen_classes",
        type=parse_classes,
        default=DEFAULT_CHOSEN_CLASSES,
        help="comma separated class ids to count (see the detector's COCO class names)",
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
    parser.add_argument(
        "--decode-chain",
        dest="decode_chain",
        default=core_pipeline.DEFAULT_DECODE_CHAIN,
        help=(
            "GStreamer elements between the SRT source and the appsink "
            "(default: the h264-in-MPEG-TS chain every node receives)"
        ),
    )
    parser.add_argument(
        "--log-level",
        dest="log_level",
        default="info",
        help="log level (debug|info|warn|error)",
    )
    return parser.parse_args(argv)


class BustleWorker(core_node.Worker):
    """The bustle node type: three commands and one drain loop.

    Everything about broker membership -- the router, the introduce ticker,
    registration and the shutdown ordering -- belongs to the runtime, so it is
    not here. What is here is what only an object counter has.
    """

    def __init__(
        self,
        node_id: uuid.UUID,
        streamer: core_pipeline.SRTStreamer,
        detector: lib_detector.Detector,
        chosen_classes: typing.List[int],
        async_q: asyncio.Queue[dict[str, int]],
        coll: collection.Collection,
        drain_interval: float,
        node_type: str,
    ) -> None:
        self._node_id = node_id
        self._streamer = streamer
        self._detector = detector
        self._chosen_classes = chosen_classes
        self._async_q = async_q
        self._coll = coll
        self._drain_interval = drain_interval
        self._node_type = node_type
        # What the analytics declaration is about. Resolved once at construction
        # rather than at declare time, because the answer depends only on the
        # detector's class table and the chosen classes, both of which are fixed
        # for the life of the process.
        self._subject = bustle_analytics.resolve_subject(detector.names, chosen_classes)
        self._utc_offset = bustle_analytics.local_utc_offset()

    def attach(self, hs: core_handlers.Handlers) -> None:
        bustle_handlers.BustleHandlers.setup(
            self._node_id,
            hs,
            self._streamer,
            self._detector,
            self._chosen_classes,
            self._async_q,
        )

    def start(self) -> list[asyncio.Task[typing.Any]]:
        task = asyncio.create_task(
            bustle_processing.normalizer(
                self._async_q,
                self._drain_interval,
                self._node_id,
                self._node_type,
                self._coll,
            )
        )
        logger.info("starting normalizer")
        return [task]

    async def stop(self) -> None:
        # set_state(NULL) blocks, so keep it off the event loop.
        await asyncio.to_thread(self._streamer.stop)

    async def registered(
        self, r: core_rpc.RouterLike, sibs: core_siblings.Siblings
    ) -> None:
        # Declared once the hub has the node, which is also the first moment the
        # analytics service has any reason to know this collection exists. It
        # retries while the analytics sibling is still booting; giving up only
        # costs the queries until the node restarts, since nothing can ask a
        # running node to declare again.
        await bustle_analytics.declare(
            r,
            sibs,
            self._node_id,
            self._coll.name,
            subject=self._subject,
            utc_offset=self._utc_offset,
        )

def init_db(mongo_url: str, mongo_db: str, node_id: uuid.UUID) -> tuple[pymongo.MongoClient, collection.Collection]:
    # serverSelectionTimeoutMS keeps a dead or unreachable mongod from parking
    # startup in the driver's 30s default; the failure surfaces immediately.
    mongo_client = pymongo.MongoClient(mongo_url, serverSelectionTimeoutMS=5000)
    nodes_db = mongo_client.get_database(mongo_db)

    id = str(node_id).replace("-", "")

    # create_collection is not idempotent, so the old list_collection_names
    # check-then-create raced with a second node starting on the same database.
    # Creating and tolerating CollectionInvalid is atomic.
    try:
        nodes_db.create_collection(
            id,
            timeseries={
                "timeField": "timestamp",
                "metaField": "node",
                "granularity": "seconds",
            },
        )
    except CollectionInvalid:
        pass

    return mongo_client, nodes_db.get_collection(id)

async def main() -> int:
    args = parse_args()

    logging_setup.setup_logging(args.log_level, str(args.node_id))

    logger.info(
        "bustle node starting name=%s node_id=%s version=%s family=%s broker=%s node=%s",
        args.node_name,
        args.node_id,
        lib_version.VERSION,
        args.family,
        redact_url(args.broker),
        args.node_type,
    )

    Gst.init(None) # pyright: ignore[reportAttributeAccessIssue]

    try:
        mongo_client, coll = await asyncio.to_thread(
            init_db, args.mongo_url, args.mongo_db, args.node_id
        )
    except Exception as e:
        # Exiting 0 here used to make a node that never connected look like a
        # clean run to whatever supervises it, so it was never restarted.
        logger.error("error opening database connection node_id=%s: %s", args.node_id, e)
        return 1

    worker = BustleWorker(
        args.node_id,
        core_pipeline.SRTStreamer(args.node_id, args.decode_chain),
        lib_detector.LibreYOLODetector(args.model, args.confidence),
        args.chosen_classes,
        asyncio.Queue(),
        coll,
        args.drain_interval,
        args.node_type,
    )

    stop_event = asyncio.Event()
    core_node.install_signal_handlers(stop_event)

    n = core_node.Node(
        node_id=args.node_id,
        name=args.node_name,
        family=args.family,
        broker=args.broker,
        version=lib_version.VERSION,
        register_payload=core_node.register_payload(
            args.node_id,
            bitrate=args.bitrate,
            width=args.width,
            height=args.height,
        ),
        worker=worker,
        intro_interval=args.intro_interval,
    )

    await n.run(stop_event)

    # Closed after the connection so a late insert from a cancelled normalizer
    # cannot use a client that is already torn down.
    mongo_client.close()

    return 0

if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except KeyboardInterrupt:
        # Only reachable where add_signal_handler is unsupported (Windows); on
        # POSIX the signal is handled inside the loop and main returns normally.
        logging.getLogger("bustle").info("process terminated by user")
        sys.exit(130)
