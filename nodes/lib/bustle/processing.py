import asyncio
from collections import Counter
from datetime import datetime
import math
import typing
import uuid

import pandas as pd
from pymongo import collection
import torch
from ultralytics.models import YOLO
from core import ticker
from core import pipeline

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
            "timestamp": datetime.now(),
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

        await asyncio.to_thread(coll.insert_one, res)


def frame_worker(
    receiver: pipeline.SRTStreamer,
    model: YOLO,
    chosen_classes: list[int],
    loop: asyncio.AbstractEventLoop,
    async_q: asyncio.Queue[dict[str, int]],
):
    while receiver.running:
        frame = receiver.get_frame()
        if frame is None:
            continue

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

        loop.call_soon_threadsafe(async_q.put_nowait, frame_data)
