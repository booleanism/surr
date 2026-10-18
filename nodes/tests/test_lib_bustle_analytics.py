"""``lib.bustle.analytics``: the query manifest a bustle node declares.

The analytics service executes a pipeline it is given, so the pipelines are
part of the node's contract with it: read-only stages only, and generic chart
data rather than a chart library's option object. Both are asserted here
because neither is checked anywhere else -- the service validates structure
(allowlist, caps) but cannot tell a sensible chart from a nonsense one, and the
legacy handler still reshapes the results, which would hide a regression.

The allowlist below mirrors ``allowedStages`` in
``analytics/internal/usecases/query.go``. It is duplicated rather than fetched
on purpose: the point is to fail here, with a message naming the Go file, when
a manifest starts using a stage the service will refuse.
"""

from __future__ import annotations

import asyncio
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

# The service caps a pipeline at 24 stages.
MAX_STAGES = 24

def pipelines() -> list[list[dict[str, typing.Any]]]:
    return [
        analytics.detail_pipeline(NODE_ID),
        analytics.summary_pipeline(NODE_ID),
        analytics.totals_pipeline(NODE_ID),
    ]

class StageTests(unittest.TestCase):
    def test_every_stage_is_one_the_service_allows(self):
        for pipeline in pipelines():
            for stage in pipeline:
                self.assertEqual(len(stage), 1, f"stage has several operators: {stage}")
                operator = next(iter(stage))
                self.assertIn(
                    operator,
                    ALLOWED_STAGES,
                    f"{operator} is not in allowedStages (analytics/internal/usecases/query.go)",
                )

    def test_no_pipeline_uses_a_writing_or_joining_stage(self):
        for pipeline in pipelines():
            text = json.dumps(pipeline)
            for operator in WRITING_STAGES:
                self.assertNotIn(operator, text, f"manifest uses {operator}")

    def test_no_pipeline_exceeds_the_stage_cap(self):
        for pipeline in pipelines():
            self.assertLessEqual(len(pipeline), MAX_STAGES)

    def test_the_node_id_is_baked_in_rather_than_passed(self):
        # A declaration is resolved by node id, so the pipeline agreeing with it
        # is what stops a query reading a collection it was not declared for.
        for pipeline in pipelines():
            self.assertEqual(pipeline[0]["$match"]["node.id"], str(NODE_ID))

class ShapeTests(unittest.TestCase):
    def test_the_pipelines_carry_no_chart_library_keys(self):
        # These are what made the stored shape belong to ECharts, and they were
        # written as $literal expressions, so the check is on the KEY: matching
        # the rendered value would have let the original stages through.
        # "series" is deliberately not checked: it is generic, and the summary
        # query legitimately emits it as a field of its own result.
        for pipeline in pipelines():
            text = json.dumps(pipeline)
            for key in ('"xAxis"', '"yAxis"', '"legend"', '"showSymbol"', '"type"'):
                self.assertNotIn(key, text)

    def test_the_detail_pipeline_emits_one_document_per_class(self):
        pipeline = analytics.detail_pipeline(NODE_ID)
        final = pipeline[-1]["$project"]
        self.assertEqual(final["_id"], 0)
        self.assertEqual(final["name"], "$_id")
        self.assertEqual(final["points"], 1)

    def test_the_summary_pipeline_emits_one_document_with_an_axis(self):
        pipeline = analytics.summary_pipeline(NODE_ID)
        final = pipeline[-1]["$project"]
        self.assertEqual(final["_id"], 0)
        self.assertEqual(final["categories"], 1)
        names = [series["name"] for series in final["series"]]
        self.assertEqual(names, ["min", "average", "max"])
        # Every series must read from a list built by the same $group, or the
        # values would not line up with the categories. The mapping is pinned
        # exactly, because "starts with a $" would accept any of the three.
        self.assertEqual(
            [series["values"] for series in final["series"]],
            ["$minList", "$avgList", "$maxList"],
        )

    def test_the_totals_pipeline_emits_one_document_with_an_axis(self):
        final = analytics.totals_pipeline(NODE_ID)[-1]["$project"]
        self.assertEqual(final["_id"], 0)
        self.assertEqual(final["categories"], 1)
        self.assertEqual(final["values"], 1)

    def test_the_totals_pipeline_takes_no_granularity(self):
        # A total is over the whole window, so a unit_time here would be a
        # parameter the query asks for and then ignores -- and the endpoint
        # would have to supply it for nothing.
        text = json.dumps(analytics.totals_pipeline(NODE_ID))
        self.assertNotIn(analytics.UNIT_TIME_PARAM, text)
        self.assertEqual(set(analytics.PARAMS_SCHEMA_TOTALS), {"start_time"})

    def test_the_axes_are_ascending(self):
        # A descending sort would still line up with its own category list, so
        # nothing downstream would look broken -- it would just be backwards.
        # Ascending time is the contract the reader assumes.
        self.assertEqual(analytics.detail_pipeline(NODE_ID)[4], {"$sort": {"_id.bucketTime": 1}})
        self.assertEqual(analytics.summary_pipeline(NODE_ID)[4], {"$sort": {"_id": 1}})
        self.assertEqual(analytics.totals_pipeline(NODE_ID)[4], {"$sort": {"_id": 1}})

class ParameterTests(unittest.TestCase):
    def test_the_window_is_a_placeholder(self):
        pipeline = analytics.detail_pipeline(NODE_ID)
        self.assertEqual(
            pipeline[0]["$match"]["$expr"],
            {"$gte": ["$timestamp", {"$toDate": analytics.START_TIME_PARAM}]},
        )

    def test_the_granularity_is_a_placeholder(self):
        pipeline = analytics.detail_pipeline(NODE_ID)
        self.assertEqual(
            pipeline[1]["$project"]["bucketTime"],
            {"$dateTrunc": {"date": "$timestamp", "unit": analytics.UNIT_TIME_PARAM}},
        )

    def test_a_concrete_start_time_replaces_the_placeholder(self):
        # The builders take a concrete timestamp as well as the placeholder, so
        # the argument has to work as both -- a pipeline that only builds with a
        # placeholder could not be inspected against real data.
        pipeline = analytics.detail_pipeline(NODE_ID, "2026-10-12T00:00:00+00:00", "hour")
        self.assertEqual(pipeline[0]["$match"]["$expr"]["$gte"][1], {"$toDate": "2026-10-12T00:00:00+00:00"})
        self.assertEqual(pipeline[1]["$project"]["bucketTime"]["$dateTrunc"]["unit"], "hour")

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

class ManifestTests(unittest.TestCase):
    def test_the_manifest_names_the_collection_and_the_node(self):
        decl = analytics.manifest(NODE_ID, COLLECTION)
        self.assertEqual(decl["node_id"], str(NODE_ID))
        self.assertEqual(decl["collection"], COLLECTION)

    def test_the_manifest_declares_every_query(self):
        # The endpoint runs whatever is declared, so this list is the whole of
        # what a caller can be shown. A query added here appears in the report
        # without a change anywhere else.
        names = [q["name"] for q in analytics.manifest(NODE_ID, COLLECTION)["queries"]]
        self.assertEqual(names, ["counts_detail", "counts_summary", "counts_totals"])

    def test_every_query_describes_itself(self):
        for query in analytics.manifest(NODE_ID, COLLECTION)["queries"]:
            self.assertTrue(query["description"])
            self.assertIsInstance(query["pipeline"], list)

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
