"""What a bustle node stores, and the two questions it knows how to answer.

The node owns its collection, so it owns the pipelines that read it: the
analytics service executes a declared query but cannot invent one. This module
is the single source of that declaration: the manifest published on
``analytics.declare``, and the pipelines it points at. Nothing here runs a
query -- the service does, on request.

Two rules the pipelines follow, both of which the service enforces on declare:

  * read-only stages only. ``$out``, ``$merge``, ``$unionWith``, ``$lookup``
    and ``$graphLookup`` are not on the service's allowlist, so a manifest that
    used one would be refused at declare time rather than at the first query.
  * generic chart data, not a chart library's option object. The old pipelines
    ended by projecting ``xAxis``/``yAxis``/``legend``/``series.type``/
    ``showSymbol``, which meant the shape of a specific renderer was baked into
    stored data. What comes out now is names, values and buckets; whatever
    draws it decides how to draw it.

The two queries return different shapes on purpose, because their data has
different shapes:

  * ``counts_detail`` returns one document per class, ``{"name": ...,
    "points": [[bucket, value], ...]}``. It does NOT share a category axis:
    a bucket can be missing for one class and not another (a class added to
    ``--chosen-classes`` mid-run has no earlier buckets), and padding it with
    zeros would be a lie. The consumer takes the union of the points it gets.
  * ``counts_summary`` returns one document, ``{"categories": [class, ...],
    "series": [{"name": "min", "values": [...]}, ...]}``. Its axis is the class
    set and all three series are built by the same ``$group``, so alignment is
    structural rather than assumed.
"""

from __future__ import annotations

import json
import logging
import typing
import uuid

from lib.core import rpc as core_rpc
from lib.core import siblings as core_siblings

logger = logging.getLogger(__name__)

# Parameters are substituted textually by the service, which replaces a string
# that is exactly "${name}" with the JSON encoding of the value. Writing the
# placeholders into the pipeline is what makes one declaration answer every
# window: the node does not know which range the caller will ask for.
START_TIME_PARAM = "${start_time}"
UNIT_TIME_PARAM = "${unit_time}"

# ISO-8601 rather than a number of milliseconds: `$toDate` parses it, a reader
# can tell what it means, and the caller does not have to know which epoch the
# service's driver uses.
PARAMS_SCHEMA_DETAIL: dict[str, str] = {"start_time": "string", "unit_time": "string"}
PARAMS_SCHEMA_SUMMARY: dict[str, str] = {"start_time": "string"}
PARAMS_SCHEMA_TOTALS: dict[str, str] = {"start_time": "string"}

# The window and the granularity are the caller's; the node id is not. Baking
# it into the pipeline is what keeps a declaration from reading another node's
# collection even if a caller asks for one -- the declaration is resolved by
# node id in the first place, and the pipeline agrees with it.
def _window(node_id: uuid.UUID, start_time: str) -> dict[str, typing.Any]:
    # $toDate because the parameter arrives as a string and `timestamp` is a
    # BSON date: comparing the two directly compares a string against a date,
    # and BSON orders every string below every date, so the filter would match
    # nothing and the chart would come back empty rather than wrong.
    return {
        "$match": {
            "node.id": str(node_id),
            "$expr": {"$gte": ["$timestamp", {"$toDate": start_time}]},
        }
    }

def detail_pipeline(
    node_id: uuid.UUID,
    start_time: str = START_TIME_PARAM,
    unit_time: str = UNIT_TIME_PARAM,
) -> list[dict[str, typing.Any]]:
    """Average count per class per time bucket, one document per class."""
    return [
        _window(node_id, start_time),
        {
            "$project": {
                "bucketTime": {"$dateTrunc": {"date": "$timestamp", "unit": unit_time}},
                "countsArray": {"$objectToArray": "$counts"},
            }
        },
        {"$unwind": "$countsArray"},
        {
            "$group": {
                "_id": {"bucketTime": "$bucketTime", "key": "$countsArray.k"},
                "avgValue": {"$avg": "$countsArray.v"},
            }
        },
        {"$sort": {"_id.bucketTime": 1}},
        {
            "$project": {
                "_id": 0,
                "key": "$_id.key",
                "point": ["$_id.bucketTime", {"$round": ["$avgValue", 2]}],
            }
        },
        {"$group": {"_id": "$key", "points": {"$push": "$point"}}},
        {"$project": {"_id": 0, "name": "$_id", "points": 1}},
    ]

def summary_pipeline(
    node_id: uuid.UUID,
    start_time: str = START_TIME_PARAM,
) -> list[dict[str, typing.Any]]:
    """Min, average and max count per class over the whole window."""
    return [
        _window(node_id, start_time),
        {"$project": {"countsArray": {"$objectToArray": "$counts"}}},
        {"$unwind": "$countsArray"},
        {
            "$group": {
                "_id": "$countsArray.k",
                "minVal": {"$min": "$countsArray.v"},
                "avgVal": {"$avg": "$countsArray.v"},
                "maxVal": {"$max": "$countsArray.v"},
            }
        },
        # Sorted here so the category axis and all three value lists come out in
        # the same class order; sorting afterwards would only order the axis.
        {"$sort": {"_id": 1}},
        {
            "$group": {
                "_id": None,
                "categories": {"$push": "$_id"},
                "minList": {"$push": "$minVal"},
                "avgList": {"$push": {"$round": ["$avgVal", 2]}},
                "maxList": {"$push": "$maxVal"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "categories": 1,
                "series": [
                    {"name": "min", "values": "$minList"},
                    {"name": "average", "values": "$avgList"},
                    {"name": "max", "values": "$maxList"},
                ],
            }
        },
    ]

def totals_pipeline(
    node_id: uuid.UUID,
    start_time: str = START_TIME_PARAM,
) -> list[dict[str, typing.Any]]:
    """How much activity each class accounts for over the window.

    One document, ``{"categories": [class, ...], "values": [total, ...]}``: the
    numbers a share-of-activity chart is drawn from. Totals rather than averages
    because the two answer different questions -- the summary says how busy a
    class usually is, this says how much of the work it did.

    It takes no granularity: a total is over the whole window by definition, so
    declaring ``unit_time`` here would be asking the caller for a parameter this
    query would ignore.
    """
    return [
        _window(node_id, start_time),
        {"$project": {"countsArray": {"$objectToArray": "$counts"}}},
        {"$unwind": "$countsArray"},
        {
            "$group": {
                "_id": "$countsArray.k",
                "total": {"$sum": "$countsArray.v"},
            }
        },
        # Sorted before the group so the axis and the values share one class
        # order; the caller pairs them positionally.
        {"$sort": {"_id": 1}},
        {
            "$group": {
                "_id": None,
                "categories": {"$push": "$_id"},
                "values": {"$push": "$total"},
            }
        },
        {"$project": {"_id": 0, "categories": 1, "values": 1}},
    ]

def manifest(node_id: uuid.UUID, collection: str) -> dict[str, typing.Any]:
    """The declaration published on ``analytics.declare``."""
    return {
        "node_id": str(node_id),
        "collection": collection,
        "queries": [
            {
                "name": "counts_detail",
                "description": (
                    "average count per class per time bucket; one document per "
                    "class as {name, points: [[bucket, value], ...]}"
                ),
                "params_schema": PARAMS_SCHEMA_DETAIL,
                "pipeline": detail_pipeline(node_id),
            },
            {
                "name": "counts_summary",
                "description": (
                    "min, average and max count per class over the window; one "
                    "document as {categories: [class], series: [{name, values}]}"
                ),
                "params_schema": PARAMS_SCHEMA_SUMMARY,
                "pipeline": summary_pipeline(node_id),
            },
            {
                "name": "counts_totals",
                "description": (
                    "total count per class over the window; one document as "
                    "{categories: [class], values: [total]}"
                ),
                "params_schema": PARAMS_SCHEMA_TOTALS,
                "pipeline": totals_pipeline(node_id),
            },
        ],
    }

# How long to keep trying before giving up on the analytics service. A node may
# start before it (or without it), and one attempt per introduce interval is
# enough because the registry is rebuilt from the same broadcasts. Bounded
# rather than forever: a node that can never declare would otherwise log a
# warning every second for the life of the process, and the declaration is
# re-published on the next start anyway.
DECLARE_RETRIES = 30

async def declare(
    r: core_rpc.RouterLike,
    sibs: core_siblings.Siblings,
    node_id: uuid.UUID,
    collection: str,
    *,
    timeout: float = 5.0,
    retry_interval: float = 1.0,
    retries: typing.Optional[int] = DECLARE_RETRIES,
) -> None:
    """Publish this node's query manifest to the analytics service.

    Called from the node runtime's post-registration hook, so the broker is
    already up by the time it runs; what it waits for is the analytics service,
    which may not have introduced itself yet.
    """
    decl = manifest(node_id, collection)
    payload = json.dumps(decl).encode()

    await core_rpc.call(
        r,
        sibs,
        "analytics.declare",
        payload,
        timeout=timeout,
        retry_interval=retry_interval,
        retries=retries,
    )

    logger.info(
        "declared analytics queries node_id=%s collection=%s queries=%d",
        node_id,
        collection,
        len(decl["queries"]),
    )
