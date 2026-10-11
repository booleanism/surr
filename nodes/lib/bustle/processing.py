import asyncio
from collections import Counter
from datetime import datetime, timezone
import logging
import math
import typing
import uuid

import pandas as pd
from pymongo import collection
from lib.core import pipeline
from lib.core import ticker
from lib.detector import Detector

logger = logging.getLogger(__name__)

# How long frame_worker blocks waiting for a frame before re-checking whether
# it should stop. Blocking is what keeps the loop off the CPU; the timeout is
# what keeps a stop responsive.
FRAME_WAIT_SECONDS = 0.5

async def normalizer(
    q: asyncio.Queue[dict[str, int]], 
    interval: float, 
    node_id: uuid.UUID, 
    node_type: str,
    coll: collection.Collection[dict[str, typing.Any]]
):
    ticker_gen = ticker.ticker(interval)
    async for _ in ticker_gen:
        batch: list[dict[str, int]] = []
        while not q.empty():
            batch.append(q.get_nowait())
            q.task_done()

        if not batch:
            continue

        res_df = pd.DataFrame(batch)
        counts: dict[str, int] = {}
        res: dict[str, typing.Any] = {
            "node": {
                "id": str(node_id),
                "type": node_type
            },
            # The timeseries timeField is read back as UTC by the aggregation
            # pipelines, so a naive local timestamp shifted every bucket by the
            # host's UTC offset.
            "timestamp": datetime.now(timezone.utc),
            "counts": counts
        }

        for col, series in res_df.items():
            if not isinstance(col, str):
                continue

            val = series.median()
            try:
                f_val = float(val)
                if math.isnan(f_val):
                    continue

                counts[col] = round(f_val)
            except (ValueError, TypeError):
                continue

        # A failed insert used to propagate out of the task and take the whole
        # normalizer down, silently ending all persistence for the node.
        try:
            await asyncio.to_thread(coll.insert_one, res)
        except Exception as e:
            logger.error("inserting counts for node %s: %s", node_id, e, exc_info=e)


def frame_worker(
    receiver: pipeline.SRTStreamer,
    detector: Detector,
    chosen_classes: list[int],
    loop: asyncio.AbstractEventLoop,
    async_q: asyncio.Queue[dict[str, int]],
):
    # Read once: the mapping only changes when the model does.
    names = detector.names

    while receiver.running:
        # Blocking get with a timeout: a non-blocking poll here spun at 100%
        # CPU for every frame interval that produced no frame at all.
        frame = receiver.get_frame(timeout=FRAME_WAIT_SECONDS)
        if frame is None:
            continue

        # One bad frame (or a transient inference failure) must not kill the
        # worker: it used to exit the thread while running stayed true, so the
        # node looked healthy but counted nothing.
        try:
            detections = detector.detect(frame)

            counts: Counter[int] = Counter(d.class_id for d in detections)
            frame_data: dict[str, int] = {
                names[c]: counts.get(c, 0) for c in chosen_classes if c in names
            }
        except Exception as e:
            logger.error("processing frame: %s", e, exc_info=e)
            continue

        loop.call_soon_threadsafe(async_q.put_nowait, frame_data)
