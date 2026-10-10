import asyncio
from collections import Counter
from datetime import datetime, timezone
import logging
import math
import typing
import uuid

import pandas as pd
from pymongo import collection
import torch
from ultralytics.models import YOLO
from core import ticker
from core import pipeline

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
    coll: collection.Collection
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
    model: YOLO,
    chosen_classes: list[int],
    loop: asyncio.AbstractEventLoop,
    async_q: asyncio.Queue[dict[str, int]],
):
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
            res = model.track(frame, verbose=False)
            if not res or len(res) < 1:
                continue

            boxes = res[0].boxes
            if boxes is None or boxes.cls is None:
                continue

            if isinstance(boxes.cls, torch.Tensor):
                class_ids = boxes.cls.cpu().numpy().astype(int)
            else:
                class_ids = boxes.cls

            counts: Counter[int] = Counter(class_ids)
            frame_data: dict[str, int] = {
                model.names[c]: counts.get(c, 0) for c in chosen_classes if c in model.names
            }
        except Exception as e:
            logger.error("processing frame: %s", e, exc_info=e)
            continue

        loop.call_soon_threadsafe(async_q.put_nowait, frame_data)
