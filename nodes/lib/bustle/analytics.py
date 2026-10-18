"""What a bustle node stores, and every question it knows how to answer.

The node owns its collection, so it owns the pipelines that read it: the
analytics service executes a declared query but cannot invent one. This module
is the single source of that declaration: the manifest published on
``analytics.declare``, and the pipelines it points at. Nothing here runs a
query -- the service does, on request.

A bustle node counts the classes it was told to count, once per drain interval.
That is one number per class per tick, and every insight below is derived from
it -- so the queries are grouped by the *question* they answer rather than by
the class they happen to read, and each group is one pipeline with one ``$facet``
so a report costs one collection scan instead of one per chart.

The four groups:

  * ``activity_by_class`` -- how much each class contributes (total, share), how
    steady it is (min/average/max/spread), when it peaked, and whether the data
    behind the other three can be trusted at all (coverage). Takes no subject,
    so it answers for a node that counts cars as readily as one that counts
    people.
  * ``occupancy`` -- the subject class alone: its timeline, the distribution of
    how full the scene was (crowd pressure), the busiest moments, whether the
    window is filling or emptying, how long the scene stayed continuously
    occupied, and how volatile it is.
  * ``rhythm`` -- the subject's hour-of-day and day-of-week profile: what a
    social space looks like across a day and across a week, in the node's own
    timezone rather than in UTC.
  * ``class_affinity`` -- which other classes appear in the same buckets as the
    subject, which is the closest a counts-only dataset gets to "what
    accompanies people here".

Three rules the pipelines follow, all of them enforced by the service at
declare time:

  * read-only stages only. ``$out``, ``$merge``, ``$unionWith``, ``$lookup``
    and ``$graphLookup`` are not on the service's allowlist, so a manifest that
    used one would be refused at declare time rather than at the first query.
  * generic chart data, not a chart library's option object. What comes out is
    names, values, buckets and shares; whatever draws it decides how to draw it.
  * bounded output. One report per query, one array per insight: the service
    caps a result at 2000 documents and 1 MiB, and a shape that is one document
    with named arrays cannot grow past that as history accumulates.

Two design choices worth knowing before editing a pipeline:

  * per-bucket averages, not per-document averages. The drain loop inserts one
    document per interval, but the interval is not guaranteed -- a restart or a
    stalled frame source leaves a gap. Averaging documents would let a densely
    sampled minute outvote a sparsely sampled hour; collapsing to buckets first
    makes every bucket one vote.
  * ``$toDate`` on the window parameter. ``timestamp`` is a BSON date and the
    parameter arrives as a string, and BSON orders every string below every date
    -- so comparing them directly matches nothing and the report comes back
    empty rather than wrong, which is the harder failure to notice.
"""

from __future__ import annotations

import datetime
import json
import logging
import re
import typing
import uuid

from lib.core import rpc as core_rpc
from lib.core import siblings as core_siblings

logger = logging.getLogger(__name__)

# The class these insights are about. A bustle node counts whatever it was told
# to count, but the questions worth asking -- how busy was it, when, with what --
# are about the thing people are: the subject. Only "person" gets the
# subject-relative queries, and a node that does not count people declares the
# class-agnostic one alone rather than a report full of zeroes.
# The node type, which is what a fleet is keyed by in the analytics registry.
# Sent with every manifest so a node that declares nothing fleet can still be a
# member of one: the fleet manifest belongs to the type, not to the node that
# happened to publish it last.
NODE_TYPE = "bustle"

SUBJECT_CLASS = "person"

# `$dayOfWeek` order, so index n-1 names the day it returns.
DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]

# The subject is spliced into a field path, so it has to be a path and nothing
# else. The names come from the detector's COCO table rather than from a
# request, but a name with a dot or a `$` in it would silently address a nested
# field or an operator, so the check is here rather than assumed upstream.
_SAFE_FIELD = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9 _-]*$")

# Parameters are substituted textually by the service, which replaces a string
# that is exactly "${name}" with the JSON encoding of the value. Writing the
# placeholders into the pipeline is what makes one declaration answer every
# window: the node does not know which range the caller will ask for.
START_TIME_PARAM = "${start_time}"
UNIT_TIME_PARAM = "${unit_time}"

PARAMS_SCHEMA_ACTIVITY: dict[str, str] = {"start_time": "string", "unit_time": "string"}
PARAMS_SCHEMA_OCCUPANCY: dict[str, str] = {"start_time": "string", "unit_time": "string"}
PARAMS_SCHEMA_RHYTHM: dict[str, str] = {"start_time": "string"}
PARAMS_SCHEMA_AFFINITY: dict[str, str] = {"start_time": "string", "unit_time": "string"}
PARAMS_SCHEMA_FLEET: dict[str, str] = {"start_time": "string", "unit_time": "string"}

# How full the scene was, in people. Fixed boundaries rather than a caller
# parameter because they are a judgement about what "crowded" means, not about
# the window; a venue that seats six and a station concourse disagree, and the
# node type is the right place for that disagreement to live.
CROWD_BANDS: list[tuple[str, float]] = [
    ("empty", 0.0),
    ("quiet", 1.0),
    ("busy", 3.0),
    ("crowded", 6.0),
    ("packed", 11.0),
]

# How many busiest buckets `occupancy` reports. Ten is a leaderboard, not a
# timeline: the timeline is already in the same document.
TOP_MOMENTS = 10

# A change smaller than this many people per bucket is noise, not a trend.
TREND_THRESHOLD = 0.5


def _default_coverage() -> dict[str, typing.Any]:
    """The coverage of a window with no documents in it."""
    return {"samples": 0, "first": None, "last": None, "span_seconds": 0}


def _default_timeline() -> dict[str, typing.Any]:
    return {"buckets": [], "average": [], "peak": [], "samples": []}


def _default_profile() -> dict[str, typing.Any]:
    return {"keys": [], "average": [], "peak": [], "buckets": []}


def _default_trend() -> dict[str, typing.Any]:
    return {"first_half": None, "second_half": None, "change": 0, "direction": "steady"}


def _default_streaks() -> dict[str, typing.Any]:
    return {"busiest_streak": 0, "quietest_streak": 0, "buckets": 0}


def _default_volatility() -> dict[str, typing.Any]:
    return {"stddev": 0, "average": 0, "peak": 0, "buckets": 0}


def _default_fleet() -> dict[str, typing.Any]:
    return {"buckets": [], "average": 0, "peak": 0}


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


def _bucket(unit_time: str) -> dict[str, typing.Any]:
    return {"$dateTrunc": {"date": "$timestamp", "unit": unit_time}}


def _label(axis: str, labels: typing.Optional[list[str]]) -> typing.Any:
    """Rename a numeric axis from a 1-based lookup, or pass it through."""
    if labels is None:
        return axis

    return {
        "$map": {
            "input": axis,
            "as": "k",
            "in": {"$arrayElemAt": [labels, {"$subtract": ["$$k", 1]}]},
        }
    }


def _iso(expr: typing.Any) -> dict[str, typing.Any]:
    """Render a date expression as an ISO 8601 UTC string.

    A bucket label leaves this service as JSON, and the driver marshals a BSON
    date as ``{"$date": ...}`` -- an object a chart cannot label an axis with.
    Truncation still groups on the real date; only the label is a string.
    """
    return {
        "$dateToString": {
            "date": expr,
            "format": "%Y-%m-%dT%H:%M:%SZ",
            "timezone": "UTC",
        }
    }


def _subject_value(subject: str) -> dict[str, typing.Any]:
    """The subject's count in this document, or 0 when the class is absent.

    ``$ifNull`` rather than a bare path: a node restarted with a different
    ``--chosen-classes`` has documents from before the change, and a missing
    field would drop the bucket out of every average instead of counting as the
    zero people who were there.
    """
    return {"$ifNull": [f"$counts.{subject}", 0]}


def _counts_array() -> dict[str, typing.Any]:
    return {"$objectToArray": "$counts"}


def _round(value: typing.Any, digits: int = 2) -> dict[str, typing.Any]:
    return {"$round": [value, digits]}


def _longest_run(flag: typing.Any) -> dict[str, typing.Any]:
    """The longest streak of consecutive 1s in an array of 0/1 flags.

    ``flag`` is an aggregation expression rather than a field name, because the
    caller inverts the array for the quiet streak instead of running a second
    pass over the same buckets.

    Streaks are the one thing a counts-only dataset can say about dwell: the
    scene was continuously occupied for N consecutive buckets, or empty for N.
    ``$reduce`` is what makes it a single pass -- there is no ``$setWindowFields``
    on the service's allowlist, and threading state through an array is the
    read-only equivalent.
    """
    return {
        "$let": {
            "vars": {
                "run": {
                    "$reduce": {
                        "input": flag,
                        "initialValue": {"best": 0, "current": 0},
                        "in": {
                            "best": {
                                "$cond": [
                                    {"$eq": ["$$this", 1]},
                                    {
                                        "$max": [
                                            "$$value.best",
                                            {"$add": ["$$value.current", 1]},
                                        ]
                                    },
                                    "$$value.best",
                                ]
                            },
                            "current": {
                                "$cond": [
                                    {"$eq": ["$$this", 1]},
                                    {"$add": ["$$value.current", 1]},
                                    0,
                                ]
                            },
                        },
                    }
                }
            },
            "in": "$$run.best",
        }
    }


# --- activity_by_class -------------------------------------------------------


def activity_pipeline(
    node_id: uuid.UUID,
    start_time: str = START_TIME_PARAM,
    unit_time: str = UNIT_TIME_PARAM,
) -> list[dict[str, typing.Any]]:
    """Per-class totals, spread, peak moment, share, and the data's coverage.

    ``classes`` is one array rather than one document per class, so the report
    is one document however many classes a node counts -- and so a consumer
    cannot accidentally pair the total of one class with the spread of another.

    The spread is computed over per-bucket averages (see the module docstring),
    which is also what makes ``stddev`` mean "how much the scene changed between
    buckets" rather than "how noisy the sampling was".
    """
    return [
        _window(node_id, start_time),
        {
            "$project": {
                "timestamp": 1,
                "bucketTime": _bucket(unit_time),
                "countsArray": _counts_array(),
            }
        },
        {
            "$facet": {
                "classes": [
                    {"$unwind": "$countsArray"},
                    # One value per class per bucket, so an irregular drain
                    # interval cannot weight a class more heavily than its
                    # neighbours in time.
                    {
                        "$group": {
                            "_id": {
                                "name": "$countsArray.k",
                                "bucketTime": "$bucketTime",
                            },
                            "value": {"$avg": "$countsArray.v"},
                        }
                    },
                    # Descending on the value, so the first document of each
                    # class group below is that class's busiest bucket. The
                    # class key is ascending so the output order is stable.
                    {"$sort": {"_id.name": 1, "value": -1}},
                    {
                        "$group": {
                            "_id": "$_id.name",
                            "peak": {
                                "$first": {
                                    "bucket": _iso("$_id.bucketTime"),
                                    "value": "$value",
                                }
                            },
                            "total": {"$sum": "$value"},
                            "average": {"$avg": "$value"},
                            "min": {"$min": "$value"},
                            "max": {"$max": "$value"},
                            "stddev": {"$stdDevPop": "$value"},
                            "buckets": {"$sum": 1},
                        }
                    },
                    {"$sort": {"_id": 1}},
                    {
                        "$group": {
                            "_id": None,
                            "classes": {
                                "$push": {
                                    "name": "$_id",
                                    "total": "$total",
                                    "average": "$average",
                                    "min": "$min",
                                    "max": "$max",
                                    "stddev": "$stddev",
                                    "buckets": "$buckets",
                                    "peak": "$peak",
                                }
                            },
                        }
                    },
                    # The grand total is what turns a total into a share. It is
                    # computed once here rather than per class, and the guard
                    # against a zero denominator keeps an empty window from
                    # dividing by zero.
                    {"$set": {"grand": {"$sum": "$classes.total"}}},
                    {
                        "$project": {
                            "_id": 0,
                            "classes": {
                                "$map": {
                                    "input": "$classes",
                                    "as": "c",
                                    "in": {
                                        "name": "$$c.name",
                                        "total": _round("$$c.total"),
                                        "average": _round("$$c.average"),
                                        "min": _round("$$c.min"),
                                        "max": _round("$$c.max"),
                                        "stddev": _round(
                                            {"$ifNull": ["$$c.stddev", 0]}
                                        ),
                                        "buckets": "$$c.buckets",
                                        "peak": {
                                            "bucket": "$$c.peak.bucket",
                                            "value": _round("$$c.peak.value"),
                                        },
                                        "share": _round(
                                            {
                                                "$cond": [
                                                    {"$gt": ["$grand", 0]},
                                                    {
                                                        "$divide": [
                                                            "$$c.total",
                                                            "$grand",
                                                        ]
                                                    },
                                                    0,
                                                ]
                                            },
                                            4,
                                        ),
                                    },
                                }
                            },
                        }
                    },
                ],
                # Coverage is not a chart; it is the answer to "should I believe
                # the other three". A node that spent half the window unable to
                # pull frames produces a perfectly well-formed report about a
                # scene it was not watching, and only the sample count and the
                # first/last timestamps show it.
                "coverage": [
                    {
                        "$group": {
                            "_id": None,
                            "samples": {"$sum": 1},
                            "first": {"$min": "$timestamp"},
                            "last": {"$max": "$timestamp"},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "samples": 1,
                            "first": 1,
                            "last": 1,
                            "span_seconds": _round(
                                {"$divide": [{"$subtract": ["$last", "$first"]}, 1000]},
                                1,
                            ),
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "samples": 1,
                            "span_seconds": 1,
                            "first": _iso("$first"),
                            "last": _iso("$last"),
                        }
                    },
                ],
            }
        },
        {
            "$replaceWith": {
                "$mergeObjects": [
                    {"classes": [], "coverage": _default_coverage()},
                    # Each facet result goes under its own key. Merging the
                    # facet document itself would splice its fields into the
                    # report root -- `samples` at the top level next to a
                    # `coverage` that stayed empty.
                    {
                        "coverage": {
                            "$ifNull": [
                                {"$arrayElemAt": ["$coverage", 0]},
                                _default_coverage(),
                            ]
                        }
                    },
                    {
                        "classes": {
                            "$ifNull": [{"$arrayElemAt": ["$classes.classes", 0]}, []]
                        }
                    },
                ]
            }
        },
    ]


# --- occupancy ---------------------------------------------------------------


def occupancy_pipeline(
    node_id: uuid.UUID,
    subject: str = SUBJECT_CLASS,
    start_time: str = START_TIME_PARAM,
    unit_time: str = UNIT_TIME_PARAM,
) -> list[dict[str, typing.Any]]:
    """Everything the subject's own time series can say about the scene.

    Six views of one series, so one scan:

    ``timeline``
        average and peak subject count per bucket -- the shape of the window.
    ``crowd``
        how many buckets fell into each occupancy band. This is the
        distribution, which a timeline cannot show: two windows with the same
        average look identical there and completely different here.
    ``moments``
        the busiest buckets, for "when was it busiest" without reading a chart.
    ``trend``
        the average of the first half of the buckets against the second half:
        is the scene filling up or emptying out over the window.
    ``streaks``
        the longest run of consecutive non-empty and empty buckets -- the only
        dwell measure available without per-track identity.
    ``volatility``
        the spread of the per-bucket averages: a steady trickle and a burst
        have the same total and different stddev.
    """
    return [
        _window(node_id, start_time),
        {
            "$project": {
                "bucketTime": _bucket(unit_time),
                "value": _subject_value(subject),
            }
        },
        {
            "$facet": {
                "timeline": [
                    {
                        "$group": {
                            "_id": "$bucketTime",
                            "average": {"$avg": "$value"},
                            "peak": {"$max": "$value"},
                            "samples": {"$sum": 1},
                        }
                    },
                    {"$sort": {"_id": 1}},
                    {
                        "$group": {
                            "_id": None,
                            "buckets": {"$push": _iso("$_id")},
                            "average": {"$push": _round("$average")},
                            "peak": {"$push": "$peak"},
                            "samples": {"$push": "$samples"},
                        }
                    },
                    {"$project": {"_id": 0}},
                ],
                "crowd": [
                    {
                        "$group": {
                            "_id": "$bucketTime",
                            "average": {"$avg": "$value"},
                        }
                    },
                    {"$project": {"band": _crowd_band()}},
                    {
                        "$group": {
                            "_id": "$band.name",
                            "order": {"$first": "$band.order"},
                            "buckets": {"$sum": 1},
                        }
                    },
                    # Sorted by the band's own order, so the histogram reads
                    # empty -> packed whatever the class order would have been.
                    {"$sort": {"order": 1}},
                    {
                        "$group": {
                            "_id": None,
                            "bands": {
                                "$push": {
                                    "name": "$_id",
                                    "order": "$order",
                                    "buckets": "$buckets",
                                }
                            },
                        }
                    },
                    {"$project": {"_id": 0}},
                ],
                "moments": [
                    {
                        "$group": {
                            "_id": "$bucketTime",
                            "average": {"$avg": "$value"},
                        }
                    },
                    # An empty bucket is not a moment. Without this the top of a
                    # quiet window would be ten rows of zero.
                    {"$match": {"average": {"$gt": 0}}},
                    {"$sort": {"average": -1, "_id": 1}},
                    {"$limit": TOP_MOMENTS},
                    {
                        "$group": {
                            "_id": None,
                            "moments": {
                                "$push": {
                                    "bucket": _iso("$_id"),
                                    "value": _round("$average"),
                                }
                            },
                        }
                    },
                    {"$project": {"_id": 0}},
                ],
                "trend": [
                    {
                        "$group": {
                            "_id": "$bucketTime",
                            "average": {"$avg": "$value"},
                        }
                    },
                    {"$sort": {"_id": 1}},
                    {
                        "$group": {
                            "_id": None,
                            "values": {"$push": "$average"},
                            "buckets": {"$sum": 1},
                        }
                    },
                    {"$project": {"_id": 0, "values": 1, "buckets": 1, "half": _half()}},
                    {"$project": _trend_shape()},
                ],
                "streaks": [
                    {
                        "$group": {
                            "_id": "$bucketTime",
                            "average": {"$avg": "$value"},
                        }
                    },
                    {"$sort": {"_id": 1}},
                    {
                        "$group": {
                            "_id": None,
                            "present": {
                                "$push": {
                                    "$cond": [{"$gt": ["$average", 0]}, 1, 0]
                                }
                            },
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "busiest_streak": _longest_run("$present"),
                            "quietest_streak": _longest_run(
                                {
                                    "$map": {
                                        "input": "$present",
                                        "as": "f",
                                        "in": {"$subtract": [1, "$$f"]},
                                    }
                                }
                            ),
                            "buckets": {"$size": "$present"},
                        }
                    },
                ],
                "volatility": [
                    {
                        "$group": {
                            "_id": "$bucketTime",
                            "average": {"$avg": "$value"},
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "stddev": {"$stdDevPop": "$average"},
                            "average": {"$avg": "$average"},
                            "peak": {"$max": "$average"},
                            "buckets": {"$sum": 1},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "stddev": _round({"$ifNull": ["$stddev", 0]}),
                            "average": _round("$average"),
                            "peak": _round("$peak"),
                            "buckets": 1,
                        }
                    },
                ],
            }
        },
        {
            "$replaceWith": {
                "$mergeObjects": [
                    {
                        "timeline": _default_timeline(),
                        "crowd": [],
                        "moments": [],
                        "trend": _default_trend(),
                        "streaks": _default_streaks(),
                        "volatility": _default_volatility(),
                    },
                    # Each view goes under its own key: merging the facet
                    # document itself would splice `buckets`, `average` and
                    # `peak` into the report root and leave `timeline` empty.
                    {
                        "timeline": {
                            "$ifNull": [
                                {"$arrayElemAt": ["$timeline", 0]},
                                _default_timeline(),
                            ]
                        }
                    },
                    {"crowd": {"$ifNull": [{"$arrayElemAt": ["$crowd.bands", 0]}, []]}},
                    {
                        "moments": {
                            "$ifNull": [{"$arrayElemAt": ["$moments.moments", 0]}, []]
                        }
                    },
                    {
                        "trend": {
                            "$ifNull": [
                                {"$arrayElemAt": ["$trend", 0]},
                                _default_trend(),
                            ]
                        }
                    },
                    {
                        "streaks": {
                            "$ifNull": [
                                {"$arrayElemAt": ["$streaks", 0]},
                                _default_streaks(),
                            ]
                        }
                    },
                    {
                        "volatility": {
                            "$ifNull": [
                                {"$arrayElemAt": ["$volatility", 0]},
                                _default_volatility(),
                            ]
                        }
                    },
                ]
            }
        },
    ]


def _crowd_band() -> dict[str, typing.Any]:
    """Map one bucket's average occupancy onto a named band."""
    branches = [
        {"case": {"$lt": ["$average", CROWD_BANDS[i + 1][1]]}, "then": _band(i)}
        for i in range(len(CROWD_BANDS) - 1)
    ]

    return {
        "$switch": {
            "branches": branches,
            "default": _band(len(CROWD_BANDS) - 1),
        }
    }


def _band(index: int) -> dict[str, typing.Any]:
    return {"name": CROWD_BANDS[index][0], "order": index}


def _half() -> dict[str, typing.Any]:
    return {"$floor": {"$divide": ["$buckets", 2]}}


def _trend_shape() -> dict[str, typing.Any]:
    """First-half and second-half averages, and the verdict between them.

    ``change`` is second minus first, so a positive number means the scene is
    filling. Both halves are computed here from ``values`` and ``half`` rather
    than read from fields that do not exist yet -- the slice boundaries are what
    make the two halves partition the window for an odd bucket count as well as
    an even one.

    ``$ifNull`` on the averages is load-bearing: ``$avg`` of an empty array is
    null, and a null would serialise as a hole in the report rather than as the
    zero it means.
    """
    first = {"$avg": {"$slice": ["$values", "$half"]}}
    second = {
        "$avg": {
            "$slice": ["$values", "$half", {"$subtract": ["$buckets", "$half"]}]
        }
    }

    change = {"$subtract": [second, first]}

    return {
        "_id": 0,
        "buckets": 1,
        "first_half": _round({"$ifNull": [first, 0]}),
        "second_half": _round({"$ifNull": [second, 0]}),
        "change": _round({"$ifNull": [change, 0]}),
        "direction": {
            "$cond": [
                # One bucket has no halves to compare, so it cannot be a trend
                # in either direction whatever the difference works out to.
                {"$lt": ["$buckets", 2]},
                "steady",
                {
                    "$switch": {
                        "branches": [
                            {"case": {"$gt": [change, TREND_THRESHOLD]}, "then": "rising"},
                            {"case": {"$lt": [change, -TREND_THRESHOLD]}, "then": "falling"},
                        ],
                        "default": "steady",
                    }
                },
            ]
        },
    }


# --- rhythm ------------------------------------------------------------------


def rhythm_pipeline(
    node_id: uuid.UUID,
    subject: str = SUBJECT_CLASS,
    start_time: str = START_TIME_PARAM,
    utc_offset: str = "+00:00",
) -> list[dict[str, typing.Any]]:
    """The subject's profile by hour of day and by day of week.

    This is the query that answers "when is this place busy" rather than "how
    busy was it", and it is the one that has to be local: bucketing by UTC hour
    shifts every peak by the node's offset, which for a node at +07:00 turns an
    evening venue into an afternoon one.

    ``utc_offset`` is baked in at declare time from the node's own clock. A
    fixed offset rather than an Olson zone because the node has no reliable way
    to learn its zone name, and a wrong-by-an-hour profile during a DST
    transition is a smaller error than a profile that is wrong by the offset all
    year.
    """
    tz = {"timezone": utc_offset}

    return [
        _window(node_id, start_time),
        {
            "$project": {
                "hour": {"$hour": {"date": "$timestamp", **tz}},
                "day": {"$dayOfWeek": {"date": "$timestamp", **tz}},
                "value": _subject_value(subject),
            }
        },
        {
            "$facet": {
                "hourly": _profile("hour", "hours"),
                # $dayOfWeek is 1..7 with 1 = Sunday, so the axis is renamed
                # here rather than left for every client to map.
                "daily": _profile("day", "days", DAYS),
            }
        },
        {
            "$replaceWith": {
                "$mergeObjects": [
                    {"hourly": _default_profile(), "daily": _default_profile()},
                    {
                        "hourly": {
                            "$ifNull": [
                                {"$arrayElemAt": ["$hourly", 0]},
                                _default_profile(),
                            ]
                        }
                    },
                    {
                        "daily": {
                            "$ifNull": [
                                {"$arrayElemAt": ["$daily", 0]},
                                _default_profile(),
                            ]
                        }
                    },
                ]
            }
        },
    ]


def _profile(
    key: str,
    name: str,
    labels: typing.Optional[list[str]] = None,
) -> list[dict[str, typing.Any]]:
    """Collapse documents to one row per key, then to one document of arrays.

    ``keys`` rather than a dense 0..23 axis: an hour the window never covered
    has no average, and emitting a zero there would claim the place was empty at
    a time nobody looked.

    ``labels`` names a numeric key: ``$dayOfWeek`` is 1..7 and a chart axis
    reading "3" instead of "Tue" is a puzzle rather than an insight.
    """
    return [
        {
            "$group": {
                "_id": f"${key}",
                "average": {"$avg": "$value"},
                "peak": {"$max": "$value"},
                "buckets": {"$sum": 1},
            }
        },
        {"$sort": {"_id": 1}},
        {
            "$group": {
                "_id": None,
                name: {"$push": "$_id"},
                "average": {"$push": _round("$average")},
                "peak": {"$push": "$peak"},
                "buckets": {"$push": "$buckets"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "keys": _label(f"${name}", labels),
                "average": 1,
                "peak": 1,
                "buckets": 1,
            }
        },
    ]


# --- class_affinity ----------------------------------------------------------


def affinity_pipeline(
    node_id: uuid.UUID,
    subject: str = SUBJECT_CLASS,
    start_time: str = START_TIME_PARAM,
    unit_time: str = UNIT_TIME_PARAM,
) -> list[dict[str, typing.Any]]:
    """How often each class shares a bucket with the subject.

    ``together`` over ``buckets`` is the share of the subject's occupied buckets
    in which this class was also present -- the closest thing to co-presence a
    counts-only dataset can express. It is deliberately per bucket and not per
    document: two classes seen in the same second are not the same claim as two
    classes seen in the same hour.

    The grouping is wrapped in a ``$facet`` for the same reason the other three
    are: a bare ``$group`` over an empty window emits no document at all, and a
    query that returns nothing is indistinguishable from one that failed.
    """
    return [
        _window(node_id, start_time),
        {
            "$project": {
                "bucketTime": _bucket(unit_time),
                "subject": _subject_value(subject),
                "countsArray": _counts_array(),
            }
        },
        {
            "$facet": {
                # The denominator every class is compared against: the buckets
                # the subject was present in. Computed once here because a
                # per-class group cannot see how many buckets the subject
                # filled.
                "occupied": [
                    {"$match": {"subject": {"$gt": 0}}},
                    {"$group": {"_id": "$bucketTime"}},
                    {"$count": "buckets"},
                ],
                "classes": [
                    {"$unwind": "$countsArray"},
                    # Collapse to one row per class per bucket first; `subject`
                    # rides along as the max of a constant, which is that
                    # bucket's subject count.
                    {
                        "$group": {
                            "_id": {
                                "name": "$countsArray.k",
                                "bucketTime": "$bucketTime",
                            },
                            "value": {"$avg": "$countsArray.v"},
                            "subject": {"$max": "$subject"},
                        }
                    },
                    {
                        "$group": {
                            "_id": "$_id.name",
                            # `buckets` counts the buckets the class was
                            # actually seen in. Counting every bucket in the
                            # window would make it equal for every class and
                            # turn `share` into a constant.
                            "buckets": {
                                "$sum": {"$cond": [{"$gt": ["$value", 0]}, 1, 0]}
                            },
                            "together": {
                                "$sum": {
                                    "$cond": [
                                        {
                                            "$and": [
                                                {"$gt": ["$value", 0]},
                                                {"$gt": ["$subject", 0]},
                                            ]
                                        },
                                        1,
                                        0,
                                    ]
                                }
                            },
                            "average": {"$avg": "$value"},
                        }
                    },
                    # The subject against itself is always 1.0, which says
                    # nothing and would head the list of companions with a
                    # constant.
                    {"$match": {"_id": {"$ne": subject}}},
                    {"$sort": {"_id": 1}},
                    {
                        "$group": {
                            "_id": None,
                            "classes": {
                                "$push": {
                                    "name": "$_id",
                                    "buckets": "$buckets",
                                    "together": "$together",
                                    "average": "$average",
                                }
                            },
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "classes": {
                                "$map": {
                                    "input": "$classes",
                                    "as": "c",
                                    "in": {
                                        "name": "$$c.name",
                                        "buckets": "$$c.buckets",
                                        "together": "$$c.together",
                                        "average": _round("$$c.average"),
                                    },
                                }
                            },
                        }
                    },
                ],
            }
        },
        {
            "$replaceWith": {
                "$mergeObjects": [
                    {"classes": [], "occupied": 0},
                    {
                        "occupied": {
                            "$ifNull": [{"$arrayElemAt": ["$occupied.buckets", 0]}, 0]
                        }
                    },
                    {
                        "classes": {
                            "$map": {
                                "input": {
                                    "$ifNull": [
                                        {"$arrayElemAt": ["$classes.classes", 0]},
                                        [],
                                    ]
                                },
                                "as": "c",
                                "in": {
                                    "name": "$$c.name",
                                    "buckets": "$$c.buckets",
                                    "together": "$$c.together",
                                    "average": "$$c.average",
                                    # Share of the subject's occupied buckets
                                    # that this class shared, so the number
                                    # means the same thing for every class.
                                    "share": _round(
                                        {
                                            "$cond": [
                                                {
                                                    "$gt": [
                                                        {
                                                            "$ifNull": [
                                                                {
                                                                    "$arrayElemAt": [
                                                                        "$occupied.buckets",
                                                                        0,
                                                                    ]
                                                                },
                                                                0,
                                                            ]
                                                        },
                                                        0,
                                                    ]
                                                },
                                                {
                                                    "$divide": [
                                                        "$$c.together",
                                                        {
                                                            "$arrayElemAt": [
                                                                "$occupied.buckets",
                                                                0,
                                                            ]
                                                        },
                                                    ]
                                                },
                                                0,
                                            ]
                                        },
                                        4,
                                    ),
                                },
                            }
                        }
                    },
                ]
            }
        },
    ]


# --- fleet -------------------------------------------------------------------

def fleet_pipeline(
    subject: str = SUBJECT_CLASS,
    start_time: str = START_TIME_PARAM,
    unit_time: str = UNIT_TIME_PARAM,
) -> list[dict[str, typing.Any]]:
    """The same window, but across every node of this type.

    This is the one query whose pipeline does not match on ``node.id``. The
    service unions the caller's collections before this pipeline runs, so a node
    filter here would narrow the union straight back to one camera -- the query
    would answer with the fleet's shape and one node's numbers.

    Two views of one scan:

    ``fleet``
        the summed subject count per bucket across the whole fleet -- what the
        deployment saw, which no single node's report can show.
    ``nodes``
        one row per contributing node, ordered by average, so "which camera is
        busiest" is a reading rather than a comparison of N separate reports.

    The arithmetic is two-staged and the order is load-bearing. First each node's
    own value for a bucket is the **average** of the documents it wrote there --
    a bucket holds several drain intervals, and summing them would report a
    camera that sampled twice as often as seeing twice as many people. Only then
    are the nodes' values for that bucket **summed**, because the fleet total is
    what the deployment saw at that moment. Summing first overstates occupancy by
    the sample rate; averaging the per-node averages afterwards would give a node
    that reported twice the weight of one that reported once, so a camera
    restarted mid-window would count for less than one that was not.
    """
    bucket = _bucket(unit_time)
    value = _subject_value(subject)

    # Shared first stage: one value per node per bucket.
    per_node_bucket = [
        {
            "$group": {
                "_id": {"node": "$node.id", "bucket": bucket},
                "value": {"$avg": value},
            }
        },
    ]

    per_bucket = per_node_bucket + [
        {"$group": {"_id": "$_id.bucket", "value": {"$sum": "$value"}}},
        {"$sort": {"_id": 1}},
    ]

    return [
        # No node.id filter: the union is the membership. $toDate for the same
        # reason as everywhere else -- the parameter is a string and `timestamp`
        # is a BSON date, and a string orders below every date.
        {"$match": {"$expr": {"$gte": ["$timestamp", {"$toDate": start_time}]}}},
        {
            "$facet": {
                "fleet": per_bucket
                + [
                    {
                        "$group": {
                            "_id": None,
                            "buckets": {
                                "$push": {"bucket": _iso("$_id"), "value": "$value"}
                            },
                            "average": {"$avg": "$value"},
                            "peak": {"$max": "$value"},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "buckets": 1,
                            "average": _round("$average"),
                            "peak": _round("$peak"),
                        }
                    },
                ],
                "nodes": per_node_bucket
                + [
                    {
                        "$group": {
                            "_id": "$_id.node",
                            "values": {"$push": "$value"},
                        }
                    },
                    {
                        "$project": {
                            "_id": 0,
                            "id": "$_id",
                            "buckets": {"$size": "$values"},
                            "average": _round({"$avg": "$values"}),
                            "peak": {"$max": "$values"},
                        }
                    },
                    {"$sort": {"average": -1, "id": 1}},
                ],
            }
        },
        {
            "$replaceWith": {
                "$mergeObjects": [
                    {"fleet": _default_fleet(), "nodes": []},
                    # The same tail as every other query: each facet result under
                    # its own key, never the facet document itself. A fleet
                    # report with `average` at the top level next to an empty
                    # `fleet` is what the other form produces.
                    {
                        "fleet": {
                            "$ifNull": [
                                {"$arrayElemAt": ["$fleet", 0]},
                                _default_fleet(),
                            ]
                        }
                    },
                    {"nodes": {"$ifNull": ["$nodes", []]}},
                ]
            }
        },
    ]

# --- manifest ----------------------------------------------------------------


def resolve_subject(
    names: typing.Mapping[int, str],
    chosen_classes: typing.Sequence[int],
) -> typing.Optional[str]:
    """The subject class this node counts, or None when it does not count one.

    Returning None is what makes the manifest honest: a node configured for
    vehicles gets the class-agnostic query instead of three queries about a class
    it never counts.
    """
    if SUBJECT_CLASS not in {names.get(c) for c in chosen_classes}:
        return None

    if not _SAFE_FIELD.match(SUBJECT_CLASS):
        logger.warning("subject class %r is not a usable field name", SUBJECT_CLASS)
        return None

    return SUBJECT_CLASS


def local_utc_offset(now: typing.Optional[datetime.datetime] = None) -> str:
    """The node's UTC offset as a MongoDB timezone string, e.g. ``"+07:00"``.

    Read from the node's own clock rather than passed in as a flag: the offset
    is a property of where the process runs, and a flag that defaults to ``+00:00``
    would make every profile silently UTC for anyone who did not know to set it.

    A fixed offset and not an Olson zone name, because the node has no reliable
    way to learn its zone name -- and a profile that is off by an hour across a
    DST transition is a smaller error than one that is off by the offset all
    year.
    """
    moment = now if now is not None else datetime.datetime.now().astimezone()
    offset = moment.utcoffset()
    seconds = int(offset.total_seconds()) if offset is not None else 0
    sign = "+" if seconds >= 0 else "-"
    seconds = abs(seconds)

    return f"{sign}{seconds // 3600:02d}:{(seconds % 3600) // 60:02d}"


def manifest(
    node_id: uuid.UUID,
    collection: str,
    subject: typing.Optional[str] = SUBJECT_CLASS,
    utc_offset: str = "+00:00",
) -> dict[str, typing.Any]:
    """The declaration published on ``analytics.declare``.

    A node with no subject declares only ``activity_by_class``: the other three
    are questions about people, and answering them with a class the node does
    not count would produce a report of zeroes that looks like an empty venue.
    """
    queries: list[dict[str, typing.Any]] = [
        {
            "name": "activity_by_class",
            "description": (
                "per-class totals, share of activity, spread and busiest bucket, "
                "plus the coverage of the window itself; one document as "
                "{classes: [{name, total, share, average, min, max, stddev, "
                "buckets, peak: {bucket, value}}], coverage: {samples, first, "
                "last, span_seconds}}"
            ),
            "params_schema": PARAMS_SCHEMA_ACTIVITY,
            "pipeline": activity_pipeline(node_id),
        }
    ]

    if subject is None:
        return {
            "node_id": str(node_id),
            "node_type": NODE_TYPE,
            "collection": collection,
            "queries": queries,
        }

    queries.extend(
        [
            {
                "name": "occupancy",
                "description": (
                    f"how full the scene was, from the {subject} count alone: "
                    "timeline, occupancy distribution, busiest moments, "
                    "filling-or-emptying trend, longest occupied and empty "
                    "streaks, volatility; one document as {timeline: {buckets, "
                    "average, peak, samples}, crowd: [{name, order, buckets}], "
                    "moments: [{bucket, value}], trend: {first_half, "
                    "second_half, change, direction}, streaks: {busiest_streak, "
                    "quietest_streak, buckets}, volatility: {stddev, average, "
                    "peak, buckets}}"
                ),
                "params_schema": PARAMS_SCHEMA_OCCUPANCY,
                "pipeline": occupancy_pipeline(node_id, subject),
            },
            {
                "name": "rhythm",
                "description": (
                    f"the {subject} count by hour of day and day of week, in the "
                    f"node's timezone (UTC{utc_offset}); one document as "
                    "{hourly: {keys, average, peak, buckets}, daily: {keys, "
                    "average, peak, buckets}} -- a key is an hour (0-23) or a "
                    "weekday (1 = Sunday)"
                ),
                "params_schema": PARAMS_SCHEMA_RHYTHM,
                "pipeline": rhythm_pipeline(node_id, subject, utc_offset=utc_offset),
            },
            {
                "name": "class_affinity",
                "description": (
                    f"how often each class shares a time bucket with the "
                    f"{subject} count; one document as "
                    "{classes: [{name, buckets, together, share, average}]}"
                ),
                "params_schema": PARAMS_SCHEMA_AFFINITY,
                "pipeline": affinity_pipeline(node_id, subject),
            },
            {
                "name": "fleet_activity",
                "description": (
                    f"the {subject} count summed across every node of this type "
                    "the caller may read, so one report describes the whole "
                    "deployment; one document as {fleet: {buckets: [{bucket, "
                    "value}], average, peak}, nodes: [{id, buckets, average, "
                    "peak}]} -- nodes is ordered busiest first"
                ),
                "params_schema": PARAMS_SCHEMA_FLEET,
                # The one query that reads across collections. Its scope is what
                # the service unions the caller's nodes for; the pipeline itself
                # deliberately has no node.id filter, or the union would be
                # narrowed back to one camera.
                "scope": "fleet",
                "pipeline": fleet_pipeline(subject),
            },
        ]
    )

    return {
        "node_id": str(node_id),
        "node_type": NODE_TYPE,
        "collection": collection,
        "queries": queries,
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
    subject: typing.Optional[str] = SUBJECT_CLASS,
    utc_offset: str = "+00:00",
    timeout: float = 5.0,
    retry_interval: float = 1.0,
    retries: typing.Optional[int] = DECLARE_RETRIES,
) -> None:
    """Publish this node's query manifest to the analytics service.

    Called from the node runtime's post-registration hook, so the broker is
    already up by the time it runs; what it waits for is the analytics service,
    which may not have introduced itself yet.
    """
    decl = manifest(node_id, collection, subject=subject, utc_offset=utc_offset)
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
        "declared analytics queries node_id=%s collection=%s queries=%d subject=%s",
        node_id,
        collection,
        len(decl["queries"]),
        subject,
    )
