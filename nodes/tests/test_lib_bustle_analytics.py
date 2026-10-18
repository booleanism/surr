"""``lib.bustle.analytics``: the query manifest a bustle node declares.

The analytics service executes a pipeline it is given, so the pipelines are part
of the node's contract with it: read-only stages only, one document per query,
and generic chart data rather than a chart library's option object. All three
are asserted here because none is checked anywhere else -- the service validates
structure (allowlist, caps) but cannot tell a sensible insight from a nonsense
one.

The allowlist below mirrors ``allowedStages`` in
``analytics/internal/usecases/query.go``. It is duplicated rather than fetched
on purpose: the point is to fail here, with a message naming the Go file, when a
manifest starts using a stage the service will refuse.
"""

from __future__ import annotations

import asyncio
import datetime
import json
import re
import typing
import unittest
import uuid

from lib.bustle import analytics
from lib.core import handlers, siblings

NODE_ID = uuid.UUID("50154422-fd3b-4cf0-a761-908fcbe98a1e")
COLLECTION = "50154422fd3b4cf0a761908fcbe98a1e"

# analytics/internal/usecases/query.go: allowedStages
ALLOWED_STAGES = {
    "$addFields", "$bucket", "$bucketAuto", "$count", "$facet", "$group",
    "$limit", "$match", "$project", "$replaceRoot", "$replaceWith", "$sample",
    "$set", "$skip", "$sort", "$sortByCount", "$unset", "$unwind",
}

# Stages that write, or that read a second collection. None of them is on the
# allowlist; naming them separately keeps the failure message specific.
WRITING_STAGES = {"$out", "$merge", "$unionWith", "$lookup", "$graphLookup"}

# The service caps a pipeline at 24 stages and `$facet` nesting at 2 levels.
MAX_STAGES = 24
MAX_FACET_DEPTH = 2


def node_scoped_pipelines() -> list[list[dict[str, typing.Any]]]:
    """The pipelines that read this node's own collection."""
    return [
        analytics.activity_pipeline(NODE_ID),
        analytics.occupancy_pipeline(NODE_ID),
        analytics.rhythm_pipeline(NODE_ID),
        analytics.affinity_pipeline(NODE_ID),
    ]


def pipelines() -> list[list[dict[str, typing.Any]]]:
    """Every pipeline a fully featured node declares."""
    return node_scoped_pipelines() + [analytics.fleet_pipeline()]


def stages(pipeline: list[dict[str, typing.Any]]) -> typing.Iterator[dict[str, typing.Any]]:
    """Every stage in a pipeline, descending into ``$facet`` sub-pipelines.

    Walking the facets matters: a nested stage is executed by the same service
    and is subject to the same allowlist, so checking only the top level would
    let a ``$lookup`` hide one level down.
    """
    for stage in pipeline:
        yield stage
        if "$facet" in stage:
            for sub in stage["$facet"].values():
                yield from stages(sub)


def keys(node: typing.Any) -> typing.Iterator[str]:
    """Every dictionary key anywhere in a pipeline, at any depth.

    Walking keys rather than searching the serialised text is what tells an
    operator apart from an expression that merely starts with its name --
    ``$mergeObjects`` is not ``$merge``.
    """
    if isinstance(node, dict):
        for key, value in node.items():
            yield key
            yield from keys(value)
    elif isinstance(node, list):
        for item in node:
            yield from keys(item)


def facet_depth(pipeline: list[dict[str, typing.Any]]) -> int:
    depth = 0
    for stage in pipeline:
        if "$facet" in stage:
            inner = 1 + max(
                (facet_depth(sub) for sub in stage["$facet"].values()), default=0
            )
            depth = max(depth, inner)
    return depth


class StageTests(unittest.TestCase):
    def test_every_stage_is_one_the_service_allows(self):
        for pipeline in pipelines():
            for stage in stages(pipeline):
                self.assertEqual(len(stage), 1, f"stage has several operators: {stage}")
                operator = next(iter(stage))
                self.assertIn(
                    operator,
                    ALLOWED_STAGES,
                    f"{operator} is not in allowedStages (analytics/internal/usecases/query.go)",
                )

    def test_no_pipeline_uses_a_writing_or_joining_stage(self):
        # Checked by key rather than by substring: `$mergeObjects` contains
        # `$merge`, and a substring check would refuse a legitimate expression
        # while a stage-key check refuses exactly the operators meant here.
        for pipeline in pipelines():
            for key in keys(pipeline):
                self.assertNotIn(
                    key, WRITING_STAGES, f"manifest uses {key} as an operator"
                )

    def test_no_pipeline_exceeds_the_stage_cap(self):
        for pipeline in pipelines():
            self.assertLessEqual(len(pipeline), MAX_STAGES)

    def test_no_pipeline_nests_facets_past_the_cap(self):
        # $facet is how each query stays one document, so it is load-bearing
        # here; the service refuses a third level, and the refusal would arrive
        # as a failed query in the report rather than as a failed declaration.
        for pipeline in pipelines():
            self.assertLessEqual(facet_depth(pipeline), MAX_FACET_DEPTH)

    def test_the_node_id_is_baked_in_rather_than_passed(self):
        # A declaration is resolved by node id, so the pipeline agreeing with it
        # is what stops a query reading a collection it was not declared for.
        #
        # The fleet pipeline is the one exception, and deliberately so: it runs
        # over a union the service built, so a node filter would narrow that
        # union back to a single camera. FleetShapeTests pins that it has none.
        for pipeline in node_scoped_pipelines():
            self.assertEqual(pipeline[0]["$match"]["node.id"], str(NODE_ID))

    def test_every_query_returns_exactly_one_document(self):
        # The service caps a result at 2000 documents and 1 MiB. A shape that is
        # one document of named arrays cannot grow past that as history
        # accumulates, which is what makes an unbounded window safe to offer.
        for pipeline in pipelines():
            last = pipeline[-1]
            self.assertIn("$replaceWith", last)
            # Either a merge of the facet views into one document, or a dict that
            # nests them under their own keys -- both are one document, and the
            # nesting form is the one that cannot splice a view's fields into the
            # report root.
            self.assertIsInstance(last["$replaceWith"], dict)
            self.assertGreaterEqual(len(last["$replaceWith"]), 1)

    def test_the_pipelines_carry_no_chart_library_keys(self):
        # These are what made the stored shape belong to ECharts, and they were
        # written as $literal expressions, so the check is on the KEY: matching
        # the rendered value would have let the original stages through.
        # "series" is deliberately not checked: it is generic, and a query may
        # legitimately emit it as a field of its own result.
        for pipeline in pipelines():
            text = json.dumps(pipeline)
            for key in ('"xAxis"', '"yAxis"', '"legend"', '"showSymbol"', '"type"'):
                self.assertNotIn(key, text)


class ActivityShapeTests(unittest.TestCase):
    def test_the_classes_are_one_array_rather_than_one_document_each(self):
        # One array means a consumer cannot pair the total of one class with the
        # spread of another, and the report stays one document however many
        # classes a node counts.
        pipeline = analytics.activity_pipeline(NODE_ID)
        classes = pipeline[-2]["$facet"]["classes"]
        final = classes[-1]["$project"]
        self.assertEqual(final["_id"], 0)
        self.assertEqual(final["classes"]["$map"]["input"], "$classes")

    def test_a_share_is_divided_by_the_grand_total(self):
        # The grand total is what turns a total into a share, and it is computed
        # once for the whole array rather than per class.
        pipeline = analytics.activity_pipeline(NODE_ID)
        classes = pipeline[-2]["$facet"]["classes"]
        set_stage = next(s for s in classes if "$set" in s)
        self.assertEqual(set_stage["$set"]["grand"], {"$sum": "$classes.total"})
        share = classes[-1]["$project"]["classes"]["$map"]["in"]["share"]
        self.assertEqual(share["$round"][1], 4)
        self.assertIn("$grand", json.dumps(share))

    def test_a_zero_denominator_cannot_be_divided_by(self):
        # An empty window has no classes at all, but the guard is what stops a
        # window whose counts are all zero from producing a division error
        # instead of a report.
        pipeline = analytics.activity_pipeline(NODE_ID)
        share = pipeline[-2]["$facet"]["classes"][-1]["$project"]["classes"]["$map"]["in"]["share"]
        guard = share["$round"][0]["$cond"]
        self.assertEqual(guard[0], {"$gt": ["$grand", 0]})
        self.assertEqual(guard[2], 0)

    def test_coverage_reports_the_window_the_data_actually_covers(self):
        # A node that spent half the window unable to pull frames produces a
        # well-formed report about a scene it was not watching; the sample count
        # and the first/last timestamps are the only things that show it.
        pipeline = analytics.activity_pipeline(NODE_ID)
        coverage = pipeline[-2]["$facet"]["coverage"]
        group = coverage[0]["$group"]
        self.assertEqual(group["samples"], {"$sum": 1})
        self.assertEqual(group["first"], {"$min": "$timestamp"})
        self.assertEqual(group["last"], {"$max": "$timestamp"})

    def test_the_empty_window_still_has_the_full_shape(self):
        # The service decodes the result into whatever the description promised;
        # a missing key would make an empty window look like a broken query.
        pipeline = analytics.activity_pipeline(NODE_ID)
        defaults = pipeline[-1]["$replaceWith"]["$mergeObjects"][0]
        self.assertEqual(defaults["classes"], [])
        self.assertEqual(
            set(defaults["coverage"]), {"samples", "first", "last", "span_seconds"}
        )


class OccupancyShapeTests(unittest.TestCase):
    def facet(self, name: str) -> list[dict[str, typing.Any]]:
        pipeline = analytics.occupancy_pipeline(NODE_ID)
        return pipeline[2]["$facet"][name]

    def test_the_six_views_are_all_present(self):
        pipeline = analytics.occupancy_pipeline(NODE_ID)
        self.assertEqual(
            set(pipeline[2]["$facet"]),
            {"timeline", "crowd", "moments", "trend", "streaks", "volatility"},
        )

    def test_the_trend_halves_are_computed_rather_than_referenced(self):
        # A first draft read $first_half/$second_half without ever computing
        # them, which Mongo reports as a missing field and returns null for --
        # a report that decodes cleanly and says the scene never changed.
        shape = self.facet("trend")[-1]["$project"]
        self.assertEqual(
            shape["first_half"]["$round"][0]["$ifNull"][0],
            {"$avg": {"$slice": ["$values", "$half"]}},
        )
        second = shape["second_half"]["$round"][0]["$ifNull"][0]
        self.assertEqual(second["$avg"]["$slice"][0], "$values")
        self.assertEqual(second["$avg"]["$slice"][1], "$half")
        self.assertEqual(
            second["$avg"]["$slice"][2], {"$subtract": ["$buckets", "$half"]}
        )

    def test_the_halves_partition_the_window_for_an_odd_bucket_count(self):
        # $floor(buckets/2) for the first half and buckets-half for the second
        # is what makes 9 buckets split 4/5 instead of 4/4 with one dropped.
        half = self.facet("trend")[3]["$project"]["half"]
        self.assertEqual(half, {"$floor": {"$divide": ["$buckets", 2]}})

    def test_one_bucket_cannot_be_a_trend(self):
        shape = self.facet("trend")[-1]["$project"]
        direction = shape["direction"]
        self.assertEqual(direction["$cond"][0], {"$lt": ["$buckets", 2]})
        self.assertEqual(direction["$cond"][1], "steady")

    def test_a_change_within_the_threshold_is_steady(self):
        # The bands are what stop noise from being reported as a trend: a
        # window that drifts by a quarter of a person per bucket is steady.
        shape = self.facet("trend")[-1]["$project"]
        branches = shape["direction"]["$cond"][2]["$switch"]["branches"]
        self.assertEqual(
            [b["case"] for b in branches],
            [
                {"$gt": [shape["change"]["$round"][0]["$ifNull"][0], analytics.TREND_THRESHOLD]},
                {"$lt": [shape["change"]["$round"][0]["$ifNull"][0], -analytics.TREND_THRESHOLD]},
            ],
        )
        self.assertEqual([b["then"] for b in branches], ["rising", "falling"])

    def test_the_crowd_bands_are_ordered_rather_than_alphabetical(self):
        # The order is what makes the histogram read empty -> packed; sorting by
        # name would put "busy" before "crowded" and "empty" last.
        crowd = self.facet("crowd")
        self.assertIn({"$sort": {"order": 1}}, crowd)
        self.assertLess(
            crowd.index({"$sort": {"order": 1}}),
            len(crowd) - 1,
            "the bands are collected after they are ordered",
        )
        self.assertEqual(
            [band[0] for band in analytics.CROWD_BANDS],
            ["empty", "quiet", "busy", "crowded", "packed"],
        )

    def test_each_band_is_bounded_by_the_next_one(self):
        # A band that is not closed by the next boundary would leave a gap the
        # $switch default swallows into "packed".
        crowd = self.facet("crowd")
        switch = crowd[1]["$project"]["band"]["$switch"]
        self.assertEqual(
            [b["case"] for b in switch["branches"]],
            [
                {"$lt": ["$average", analytics.CROWD_BANDS[i + 1][1]]}
                for i in range(len(analytics.CROWD_BANDS) - 1)
            ],
        )
        self.assertEqual(
            switch["default"], {"name": analytics.CROWD_BANDS[-1][0], "order": len(analytics.CROWD_BANDS) - 1}
        )

    def test_an_empty_bucket_is_not_one_of_the_busiest_moments(self):
        # Without the filter the top of a quiet window would be ten rows of
        # zero, which reads as ten separate moments of nobody.
        moments = self.facet("moments")
        self.assertEqual(moments[1], {"$match": {"average": {"$gt": 0}}})
        self.assertEqual(moments[2], {"$sort": {"average": -1, "_id": 1}})
        self.assertEqual(moments[3], {"$limit": analytics.TOP_MOMENTS})

    def test_a_streak_is_computed_in_one_pass(self):
        # There is no $setWindowFields on the allowlist, so a run length has to
        # be threaded through a $reduce -- which is what keeps it one stage and
        # one document rather than one group per candidate run.
        streaks = self.facet("streaks")
        project = streaks[-1]["$project"]
        self.assertIn("$reduce", json.dumps(project["busiest_streak"]))
        self.assertIn("$reduce", json.dumps(project["quietest_streak"]))

    def test_the_quiet_streak_is_the_inverse_of_the_busy_one(self):
        # The same $reduce over the inverted flags, so the two numbers cannot
        # disagree about how many buckets there were.
        streaks = self.facet("streaks")
        quiet = streaks[-1]["$project"]["quietest_streak"]
        invert = quiet["$let"]["vars"]["run"]["$reduce"]["input"]
        self.assertEqual(invert, {"$map": {"input": "$present", "as": "f", "in": {"$subtract": [1, "$$f"]}}})

    def test_the_timeline_averages_per_bucket_rather_than_per_document(self):
        # One document per drain interval, and the interval is not guaranteed.
        # Averaging documents would let a densely sampled minute outvote a
        # sparsely sampled hour.
        timeline = self.facet("timeline")
        group = timeline[0]["$group"]
        self.assertEqual(group["_id"], "$bucketTime")
        self.assertEqual(group["average"], {"$avg": "$value"})
        self.assertEqual(group["samples"], {"$sum": 1})


class RhythmShapeTests(unittest.TestCase):
    def test_the_profile_is_taken_in_the_nodes_own_timezone(self):
        # Bucketing by UTC hour shifts every peak by the node's offset, which
        # for a node at +07:00 turns an evening venue into an afternoon one.
        pipeline = analytics.rhythm_pipeline(NODE_ID, utc_offset="+07:00")
        project = pipeline[1]["$project"]
        self.assertEqual(project["hour"]["$hour"]["timezone"], "+07:00")
        self.assertEqual(project["day"]["$dayOfWeek"]["timezone"], "+07:00")

    def test_the_offset_is_a_literal_rather_than_a_parameter(self):
        # The hub supplies start_time and unit_time and nothing else, so an
        # offset written as a placeholder would be refused by the service as an
        # unknown parameter.
        text = json.dumps(analytics.rhythm_pipeline(NODE_ID, utc_offset="+07:00"))
        self.assertNotIn("${", text.replace(analytics.START_TIME_PARAM, ""))

    def test_an_hour_the_window_never_covered_is_absent(self):
        # A dense 0..23 axis would claim the place was empty at a time nobody
        # looked; the keys travel with the values instead.
        pipeline = analytics.rhythm_pipeline(NODE_ID)
        for facet, axis in (("hourly", "hours"), ("daily", "days")):
            stages_of = pipeline[2]["$facet"][facet]
            # The axis is named in the $group that builds it and read back by
            # the $project: a mismatch between the two would push an array and
            # project a missing field, which Mongo reports as null rather than
            # as an error.
            self.assertEqual(stages_of[2]["$group"][axis], {"$push": "$_id"})
            project = stages_of[3]["$project"]
            self.assertEqual(project["average"], 1)
            self.assertEqual(project["peak"], 1)
            self.assertEqual(project["buckets"], 1)
            self.assertIn(f"${axis}", json.dumps(project["keys"]), facet)

    def test_the_days_are_named_rather_than_numbered(self):
        # $dayOfWeek is 1..7 with 1 = Sunday, and an axis reading "3" is a
        # puzzle rather than an insight.
        daily = analytics.rhythm_pipeline(NODE_ID)[2]["$facet"]["daily"][3]["$project"]
        self.assertEqual(
            daily["keys"],
            {
                "$map": {
                    "input": "$days",
                    "as": "k",
                    "in": {"$arrayElemAt": [analytics.DAYS, {"$subtract": ["$$k", 1]}]},
                }
            },
        )
        self.assertEqual(analytics.DAYS[0], "Sun")
        self.assertEqual(len(analytics.DAYS), 7)

    def test_the_hours_are_left_as_numbers(self):
        # 0..23 needs no lookup, and renaming it would be a table to keep in
        # sync with nothing.
        hourly = analytics.rhythm_pipeline(NODE_ID)[2]["$facet"]["hourly"][3]["$project"]
        self.assertEqual(hourly["keys"], "$hours")

    def test_both_profiles_come_from_the_same_documents(self):
        # One $facet over one scan: the hourly and daily views are the same
        # window, so they cannot disagree about which buckets existed.
        pipeline = analytics.rhythm_pipeline(NODE_ID)
        self.assertEqual(set(pipeline[2]["$facet"]), {"hourly", "daily"})
        self.assertEqual(len(pipeline), 4)


class AffinityShapeTests(unittest.TestCase):
    def test_the_subject_is_not_its_own_companion(self):
        # Its share is always 1.0, which says nothing and would head the list.
        pipeline = analytics.affinity_pipeline(NODE_ID)
        self.assertIn(
            {"$match": {"_id": {"$ne": analytics.SUBJECT_CLASS}}},
            pipeline[-2]["$facet"]["classes"],
        )

    def test_together_is_counted_per_bucket(self):
        # Two classes seen in the same second are not the same claim as two
        # classes seen in the same hour, so the flag is per collapsed bucket.
        # Both conditions matter: a bucket the subject was absent from is not
        # co-presence, and neither is one the class itself was absent from.
        pipeline = analytics.affinity_pipeline(NODE_ID)
        group = next(
            s
            for s in pipeline[-2]["$facet"]["classes"]
            if "$group" in s and "together" in s["$group"]
        )
        self.assertEqual(
            group["$group"]["together"],
            {
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
        )

    def test_buckets_counts_the_buckets_a_class_was_seen_in(self):
        # Not every bucket in the window: the class key is present in every
        # document, so counting rows would give the same number for every class
        # and make the share a constant.
        pipeline = analytics.affinity_pipeline(NODE_ID)
        group = next(
            s
            for s in pipeline[-2]["$facet"]["classes"]
            if "$group" in s and "together" in s["$group"]
        )
        self.assertEqual(
            group["$group"]["buckets"],
            {"$sum": {"$cond": [{"$gt": ["$value", 0]}, 1, 0]}},
        )

    def test_the_denominator_is_the_subjects_occupied_buckets(self):
        # A per-class group cannot see how many buckets the subject filled, so
        # the denominator is its own facet view and the share is divided by it
        # rather than by the class's own count.
        pipeline = analytics.affinity_pipeline(NODE_ID)
        occupied = pipeline[-2]["$facet"]["occupied"]
        self.assertEqual(
            occupied,
            [
                {"$match": {"subject": {"$gt": 0}}},
                {"$group": {"_id": "$bucketTime"}},
                {"$count": "buckets"},
            ],
        )
        share = pipeline[-1]["$replaceWith"]["$mergeObjects"][-1]["classes"]["$map"]["in"]["share"]
        self.assertEqual(
            share["$round"][0]["$cond"][0]["$gt"][0],
            {"$ifNull": [{"$arrayElemAt": ["$occupied.buckets", 0]}, 0]},
        )
        self.assertEqual(
            share["$round"][0]["$cond"][1]["$divide"][1],
            {"$arrayElemAt": ["$occupied.buckets", 0]},
        )

    def test_the_share_is_guarded_against_a_zero_denominator(self):
        # An empty window has no occupied buckets, and a division by zero would
        # fail the whole query rather than report zero co-presence.
        pipeline = analytics.affinity_pipeline(NODE_ID)
        share = pipeline[-1]["$replaceWith"]["$mergeObjects"][-1]["classes"]["$map"]["in"]["share"]
        denominator = share["$round"][0]["$cond"][0]["$gt"][0]
        self.assertEqual(denominator["$ifNull"][1], 0)
        self.assertEqual(share["$round"][0]["$cond"][2], 0)

    def test_the_empty_window_still_has_the_full_shape(self):
        # A bare $group over an empty window emits no document at all, and a
        # query that returns nothing is indistinguishable from one that failed.
        pipeline = analytics.affinity_pipeline(NODE_ID)
        self.assertEqual(set(pipeline[-2]["$facet"]), {"classes", "occupied"})
        defaults = pipeline[-1]["$replaceWith"]["$mergeObjects"][0]
        self.assertEqual(defaults, {"classes": [], "occupied": 0})


class ReportShapeTests(unittest.TestCase):
    """What the service actually returns: one document, one key per view.

    Every query is a `$facet` whose result is merged into a single document, so
    the merge is the one place where a facet's output can be spliced into the
    report root instead of nested under its own key -- which produces a report
    that looks empty next to a pile of unexplained top-level fields, and does so
    without any error.
    """

    def test_every_facet_view_has_a_default_in_the_report(self):
        for pipeline in pipelines():
            facet = next(s for s in pipeline if "$facet" in s)["$facet"]
            defaults = pipeline[-1]["$replaceWith"]["$mergeObjects"][0]
            self.assertEqual(
                set(defaults),
                set(facet),
                "a view with no default reports nothing when its window is empty",
            )

    def test_each_view_is_nested_under_its_own_key(self):
        for pipeline in pipelines():
            facet = next(s for s in pipeline if "$facet" in s)["$facet"]
            for operand in pipeline[-1]["$replaceWith"]["$mergeObjects"][1:]:
                self.assertEqual(len(operand), 1, operand)
                key = next(iter(operand))
                self.assertIn(key, facet, f"{key} is not a view of this query")
                self.assertIsInstance(operand[key], dict, operand)


class DateLabelTests(unittest.TestCase):
    """A bucket label is a string by the time it leaves the service.

    The service marshals documents as relaxed extended JSON, so a BSON date
    arrives as ``{"$date": ...}`` -- an object no chart can label an axis with.
    Truncation still groups on the real date; only the label is formatted.
    """

    FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    def assertIso(self, expr: typing.Any) -> None:
        self.assertEqual(
            expr,
            {
                "$dateToString": {
                    "date": expr["$dateToString"]["date"],
                    "format": self.FORMAT,
                    "timezone": "UTC",
                }
            },
        )

    def test_the_activity_peak_bucket_is_a_string(self):
        pipeline = analytics.activity_pipeline(NODE_ID)
        classes = next(s for s in pipeline if "$facet" in s)["$facet"]["classes"]
        peak = next(
            s for s in classes if "$group" in s and "peak" in s["$group"]
        )["$group"]["peak"]
        self.assertIso(peak["$first"]["bucket"])

    def test_the_coverage_timestamps_are_strings(self):
        pipeline = analytics.activity_pipeline(NODE_ID)
        coverage = next(s for s in pipeline if "$facet" in s)["$facet"]["coverage"]
        self.assertIso(coverage[-1]["$project"]["first"])
        self.assertIso(coverage[-1]["$project"]["last"])
        # ... and the span is still computed from the dates, not the strings.
        self.assertEqual(
            coverage[1]["$project"]["span_seconds"]["$round"][0]["$divide"][0],
            {"$subtract": ["$last", "$first"]},
        )

    def test_the_timeline_axis_is_strings(self):
        pipeline = analytics.occupancy_pipeline(NODE_ID)
        timeline = next(s for s in pipeline if "$facet" in s)["$facet"]["timeline"]
        pushed = next(
            s for s in timeline if "$group" in s and "buckets" in s["$group"]
        )["$group"]["buckets"]
        self.assertIso(pushed["$push"])

    def test_the_moments_are_strings(self):
        pipeline = analytics.occupancy_pipeline(NODE_ID)
        moments = next(s for s in pipeline if "$facet" in s)["$facet"]["moments"]
        pushed = next(
            s for s in moments if "$group" in s and "moments" in s["$group"]
        )["$group"]["moments"]
        self.assertIso(pushed["$push"]["bucket"])


class ParameterTests(unittest.TestCase):
    def test_the_window_is_a_placeholder(self):
        pipeline = analytics.activity_pipeline(NODE_ID)
        self.assertEqual(
            pipeline[0]["$match"]["$expr"],
            {"$gte": ["$timestamp", {"$toDate": analytics.START_TIME_PARAM}]},
        )

    def test_the_window_is_compared_as_a_date(self):
        # `timestamp` is a BSON date and the parameter arrives as a string, and
        # BSON orders every string below every date -- so comparing them
        # directly matches nothing and the report comes back empty rather than
        # wrong, which is the harder failure to notice.
        for pipeline in pipelines():
            self.assertEqual(
                pipeline[0]["$match"]["$expr"]["$gte"][1],
                {"$toDate": analytics.START_TIME_PARAM},
            )

    def test_the_granularity_is_a_placeholder_where_it_is_used(self):
        for pipeline in (
            analytics.activity_pipeline(NODE_ID),
            analytics.occupancy_pipeline(NODE_ID),
            analytics.affinity_pipeline(NODE_ID),
        ):
            text = json.dumps(pipeline)
            self.assertIn(analytics.UNIT_TIME_PARAM, text)

    def test_a_query_without_a_bucket_width_declares_none(self):
        # A parameter the query asks for and then ignores is refused by the
        # service, and would make the endpoint supply a value for nothing.
        text = json.dumps(analytics.rhythm_pipeline(NODE_ID))
        self.assertNotIn(analytics.UNIT_TIME_PARAM, text)
        self.assertEqual(set(analytics.PARAMS_SCHEMA_RHYTHM), {"start_time"})

    def test_a_concrete_start_time_replaces_the_placeholder(self):
        # The builders take a concrete timestamp as well as the placeholder, so
        # the argument has to work as both -- a pipeline that only builds with a
        # placeholder could not be inspected against real data.
        pipeline = analytics.activity_pipeline(
            NODE_ID, "2026-10-12T00:00:00+00:00", "hour"
        )
        self.assertEqual(
            pipeline[0]["$match"]["$expr"]["$gte"][1],
            {"$toDate": "2026-10-12T00:00:00+00:00"},
        )
        self.assertEqual(
            pipeline[1]["$project"]["bucketTime"]["$dateTrunc"]["unit"], "hour"
        )

    def test_every_placeholder_is_declared_in_the_schema(self):
        # Both directions matter and neither is free: the service refuses a
        # pipeline that uses an undeclared parameter (ErrMissingParam) and one
        # that declares a parameter it never uses (ErrUnknownParam).
        decl = analytics.manifest(NODE_ID, COLLECTION)
        for query in decl["queries"]:
            text = json.dumps(query["pipeline"])
            used = set(re.findall(r"\$\{([a-z_]+)\}", text))
            self.assertEqual(used, set(query["params_schema"]), query["name"])
            for kind in query["params_schema"].values():
                self.assertEqual(kind, "string")


class SubjectTests(unittest.TestCase):
    NAMES = {0: "person", 1: "bicycle", 2: "car", 3: "motorcycle"}

    def test_a_node_that_counts_people_has_a_subject(self):
        self.assertEqual(analytics.resolve_subject(self.NAMES, [0, 1, 2, 3]), "person")

    def test_a_node_that_does_not_count_people_has_none(self):
        # Reporting three charts of zeroes about a class the node never counts
        # looks exactly like an empty venue, so the honest answer is to declare
        # the class-agnostic query alone.
        self.assertIsNone(analytics.resolve_subject(self.NAMES, [2, 3]))

    def test_an_unknown_class_id_is_not_a_subject(self):
        self.assertIsNone(analytics.resolve_subject(self.NAMES, [99]))

    def test_a_subject_that_is_not_a_field_path_is_refused(self):
        # The subject is spliced into a field path, so a dot or a `$` would
        # silently address a nested field or an operator instead of a class.
        for bad in ("counts.person", "$person", "person$", ""):
            self.assertIsNone(analytics._SAFE_FIELD.match(bad), bad)

    def test_an_ordinary_class_name_is_a_field_path(self):
        for good in ("person", "traffic light", "person_2", "0"):
            self.assertIsNotNone(analytics._SAFE_FIELD.match(good), good)


class UtcOffsetTests(unittest.TestCase):
    def test_a_positive_offset_is_formatted_as_mongodb_expects(self):
        moment = datetime.datetime(2026, 10, 12, 9, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=7)))
        self.assertEqual(analytics.local_utc_offset(moment), "+07:00")

    def test_a_half_hour_offset_keeps_its_minutes(self):
        moment = datetime.datetime(2026, 10, 12, 9, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
        self.assertEqual(analytics.local_utc_offset(moment), "+05:30")

    def test_a_negative_offset_keeps_its_sign(self):
        moment = datetime.datetime(2026, 10, 12, 9, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=-3)))
        self.assertEqual(analytics.local_utc_offset(moment), "-03:00")

    def test_utc_is_written_as_an_offset_rather_than_as_z(self):
        # MongoDB accepts an offset string as a timezone; "Z" is not one, so a
        # naive "just use UTC" shortcut would be refused by the server.
        moment = datetime.datetime(2026, 10, 12, 9, 0, tzinfo=datetime.timezone.utc)
        self.assertEqual(analytics.local_utc_offset(moment), "+00:00")


class FleetShapeTests(unittest.TestCase):
    """The one query that reads across collections.

    Two of its properties cannot be seen by a structural test at all, so they are
    asserted by name here: it must not filter on ``node.id`` (the service unions
    the collections before the pipeline runs, so a node filter would narrow the
    union back to one camera), and the two views must be nested under their own
    keys rather than spliced into the report root.
    """

    def test_the_fleet_pipeline_does_not_filter_on_a_node(self):
        serialised = json.dumps(analytics.fleet_pipeline())
        self.assertNotIn("node.id\": \"", serialised)
        self.assertNotIn("$node_id", serialised)
        self.assertNotIn("node_id", serialised)

    def test_the_fleet_pipeline_opens_with_the_window(self):
        # The only filter is the window, and it is on the raw timestamp: a string
        # compared against a BSON date matches nothing.
        pipeline = analytics.fleet_pipeline()
        self.assertEqual(set(pipeline[0]), {"$match"})
        self.assertEqual(
            pipeline[0]["$match"],
            {
                "$expr": {
                    "$gte": [
                        "$timestamp",
                        {"$toDate": analytics.START_TIME_PARAM},
                    ]
                }
            },
        )

    def test_the_fleet_pipeline_has_two_views_in_one_facet(self):
        facet = next(stage["$facet"] for stage in analytics.fleet_pipeline() if "$facet" in stage)
        self.assertEqual(set(facet), {"fleet", "nodes"})

    def test_the_default_matches_the_view_it_stands_in_for(self):
        # The fallback and the real shape have to agree, or an empty window
        # decodes into a different shape than a populated one.
        default = analytics._default_fleet()
        self.assertEqual(set(default), {"buckets", "average", "peak"})

        tail = analytics.fleet_pipeline()[-1]["$replaceWith"]["$mergeObjects"]
        self.assertEqual(tail[0]["fleet"], default)
        self.assertEqual(tail[1]["fleet"]["$ifNull"][1], default)

    def test_the_fleet_buckets_are_labelled_as_strings(self):
        facet = next(stage["$facet"] for stage in analytics.fleet_pipeline() if "$facet" in stage)
        push = json.dumps(facet["fleet"])
        self.assertIn("$dateToString", push)

    def test_the_nodes_view_is_ordered_busiest_first(self):
        facet = next(stage["$facet"] for stage in analytics.fleet_pipeline() if "$facet" in stage)
        self.assertEqual(facet["nodes"][-1], {"$sort": {"average": -1, "id": 1}})

    def test_each_nodes_value_for_a_bucket_is_an_average(self):
        # A bucket holds several drain intervals. Summing them would report a
        # camera that sampled twice as often as seeing twice as many people, so
        # the first stage is a per-node average. A live probe is what found this:
        # six 10s documents of four people in one minute summed to 24.
        facet = next(stage["$facet"] for stage in analytics.fleet_pipeline() if "$facet" in stage)
        first_group = facet["fleet"][0]["$group"]
        self.assertEqual(
            first_group["value"],
            {"$avg": {"$ifNull": ["$counts.person", 0]}},
        )
        self.assertEqual(
            first_group["_id"],
            {
                "node": "$node.id",
                "bucket": {"$dateTrunc": {"date": "$timestamp", "unit": "${unit_time}"}},
            },
        )

    def test_the_fleet_then_sums_the_nodes_for_a_bucket(self):
        # Summing the per-node averages is what makes the fleet total the number
        # of people in front of the deployment at that moment.
        facet = next(stage["$facet"] for stage in analytics.fleet_pipeline() if "$facet" in stage)
        self.assertEqual(facet["fleet"][1]["$group"]["value"], {"$sum": "$value"})
        self.assertEqual(facet["fleet"][1]["$group"]["_id"], "$_id.bucket")

    def test_both_views_share_the_per_node_stage(self):
        # The two views answer from one scan: a second copy of the per-node group
        # would be a second opportunity for them to disagree about what a node
        # reported in a bucket.
        facet = next(stage["$facet"] for stage in analytics.fleet_pipeline() if "$facet" in stage)
        self.assertEqual(facet["nodes"][0], facet["fleet"][0])
        self.assertEqual(facet["nodes"][1]["$group"]["_id"], "$_id.node")


class ScopeTests(unittest.TestCase):
    def test_only_the_fleet_query_is_fleet_scoped(self):
        queries = analytics.manifest(NODE_ID, COLLECTION)["queries"]
        scopes = {q["name"]: q.get("scope") for q in queries}
        self.assertEqual(scopes["fleet_activity"], "fleet")
        self.assertEqual(
            [n for n, scope in scopes.items() if scope is not None],
            ["fleet_activity"],
        )

    def test_the_manifest_names_its_node_type(self):
        # The type is what a fleet is keyed by, and it cannot be derived from the
        # collection name -- that is the node id with its dashes stripped.
        self.assertEqual(analytics.manifest(NODE_ID, COLLECTION)["node_type"], "bustle")
        self.assertEqual(
            analytics.manifest(NODE_ID, COLLECTION, subject=None)["node_type"],
            "bustle",
        )

    def test_a_node_without_a_subject_declares_no_fleet_query(self):
        # A fleet query sums the subject's count; without a subject there is
        # nothing to sum, and a chart of zeroes reads as an empty deployment.
        decl = analytics.manifest(NODE_ID, COLLECTION, subject=None)
        self.assertEqual([q["name"] for q in decl["queries"]], ["activity_by_class"])


class ManifestTests(unittest.TestCase):
    def test_the_manifest_names_the_collection_and_the_node(self):
        decl = analytics.manifest(NODE_ID, COLLECTION)
        self.assertEqual(decl["node_id"], str(NODE_ID))
        self.assertEqual(decl["collection"], COLLECTION)

    def test_a_subject_node_declares_every_query(self):
        # The endpoint runs whatever is declared, so this list is the whole of
        # what a caller can be shown. A query added here appears in the report
        # without a change anywhere else.
        names = [q["name"] for q in analytics.manifest(NODE_ID, COLLECTION)["queries"]]
        self.assertEqual(
            names,
            [
                "activity_by_class",
                "occupancy",
                "rhythm",
                "class_affinity",
                "fleet_activity",
            ],
        )

    def test_a_node_without_a_subject_declares_the_class_agnostic_query_alone(self):
        decl = analytics.manifest(NODE_ID, COLLECTION, subject=None)
        self.assertEqual([q["name"] for q in decl["queries"]], ["activity_by_class"])

    def test_every_query_describes_itself(self):
        for query in analytics.manifest(NODE_ID, COLLECTION)["queries"]:
            self.assertTrue(query["description"])
            self.assertIsInstance(query["pipeline"], list)

    def test_the_rhythm_query_names_the_timezone_it_used(self):
        # The description is the only place a caller can learn that a key is a
        # local hour rather than a UTC one.
        queries = {
            q["name"]: q for q in analytics.manifest(NODE_ID, COLLECTION, utc_offset="+07:00")["queries"]
        }
        self.assertIn("+07:00", queries["rhythm"]["description"])

    def test_the_manifest_is_json_serializable(self):
        # It crosses the broker as JSON, and a uuid or a datetime in a pipeline
        # would only fail at declare time on the far side.
        json.dumps(analytics.manifest(NODE_ID, COLLECTION))


class FakeRouter:
    """Records what was sent and answers every request the same way."""

    def __init__(self, code: int = 0) -> None:
        self.code = code
        self.sent: list[tuple[str, bytes, str]] = []

    async def send(self, command: str, payload: bytes, sibling_name: str) -> uuid.UUID:
        self.sent.append((command, payload, sibling_name))
        return uuid.uuid4()

    async def recv(self, track_id: uuid.UUID, timeout=None) -> handlers.RouterResponse:
        return handlers.RouterResponse(self.code, "")


def registry(*commands: str) -> siblings.Siblings:
    sibs = siblings.Siblings()
    sibs.set("rpc.direct.analytics0", siblings.Sibling(commands=list(commands)))
    return sibs


class DeclareTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_manifest_is_sent_to_the_analytics_service(self):
        router = FakeRouter()
        await analytics.declare(
            router, registry("analytics.declare"), NODE_ID, COLLECTION, retry_interval=0.0
        )

        self.assertEqual(len(router.sent), 1)
        command, payload, name = router.sent[0]
        self.assertEqual(command, "analytics.declare")
        self.assertEqual(name, "rpc.direct.analytics0")
        self.assertEqual(json.loads(payload), analytics.manifest(NODE_ID, COLLECTION))

    async def test_the_subject_and_offset_reach_the_declaration(self):
        # Both are resolved by the node from its own detector and clock, so a
        # caller that forgot to pass them would declare a UTC report about a
        # class it may not count.
        router = FakeRouter()
        await analytics.declare(
            router,
            registry("analytics.declare"),
            NODE_ID,
            COLLECTION,
            subject=None,
            utc_offset="+07:00",
            retry_interval=0.0,
        )

        sent = json.loads(router.sent[0][1])
        self.assertEqual([q["name"] for q in sent["queries"]], ["activity_by_class"])

    async def test_a_service_that_is_not_up_yet_is_retried(self):
        # The node starts before the analytics service may have introduced
        # itself, which is the same situation nodes.add already handles.
        router = FakeRouter()
        sibs = siblings.Siblings()

        async def publish_soon() -> None:
            await asyncio.sleep(0.01)
            sibs.set("rpc.direct.analytics0", siblings.Sibling(commands=["analytics.declare"]))

        task = asyncio.ensure_future(publish_soon())
        await analytics.declare(router, sibs, NODE_ID, COLLECTION, retry_interval=0.005)
        await task

        self.assertEqual(len(router.sent), 1)

    async def test_giving_up_raises_so_the_runtime_can_log_it(self):
        router = FakeRouter()
        with self.assertRaises(RuntimeError):
            await analytics.declare(
                router, siblings.Siblings(), NODE_ID, COLLECTION,
                retry_interval=0.0, retries=2,
            )
        self.assertEqual(router.sent, [])

    async def test_a_refused_declaration_raises(self):
        router = FakeRouter(code=-1)
        with self.assertRaises(RuntimeError):
            await analytics.declare(
                router, registry("analytics.declare"), NODE_ID, COLLECTION,
                retry_interval=0.0, retries=1,
            )


if __name__ == "__main__":
    unittest.main()
