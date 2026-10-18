# TODO

Execution tracker for the designed-but-unbuilt efforts. Design context:
ARCHITECTURE.md "Planned evolution", DESIGN.md §11 (analytics service),
§12 (nodes refactor) and §13 (logging). Principles: whoever writes the data
also writes the query; nodes/lib/core stays node-type-agnostic; the wire stays
Go/Python compatible at every stage.

Status of the three workstreams: **C (logging) is done**; A and B are open.
Neither A nor B exists on `main`. Prior art for both lives outside it: the
abandoned `feat/analytics-service` branch (surviving only as dangling commits
and `refs/stash`) contains a first `analytics/` module, and
`refs/remotes/gitea/main` commit `66e308b "nodes: pythonic refactor"`
implements roughly section A (its runtime package
`nodes/core/{cli,config,detector,registry,service,storage,stream,worker}.py`
— the same modules section A now targets under `nodes/lib/core/` — plus
`nodes/node.py`, `nodes/workers/bustle/*`, deleting `nodes/lib/bustle/*`).
Treat both as prior art to review, not as landed code.

**Section E is different**: it tracks defects found by reading the code during
a full review pass, then re-checked against the working tree. Its checkboxes
are **fix status** (`[x]` = fixed), not "verified" — every item, fixed or not,
was reproduced from the source. **All of E0–E11 is fixed and committed**
(`beaa467`..`1911b36`). Line numbers inside `[x]` items refer to the revision
they were found in, not to the fixed tree.

**Section F is different again**: it tracks defects found by *running* the
system, on a `docker compose` stack built from a single commit and driven by
`deploy/smoke.sh`. Every one of them needs a live process to be reachable — a
node that dies between two broker calls, a duplicate row left in a database by
a previous run, a reply code nothing consumed. All six are fixed. F6 (dead
nodes leaving rows behind) was found by the same sweep run again after the
analytics rewrite.

## A. Nodes refactor (type-agnostic runtime) — DESIGN.md §12

Ordered stages; each lands independently and is wire-compatible.

- [x] A0. Safety net (before touching anything)
  - [x] pyproject.toml at the repo root: pyright config (standard, exclude
        .venv, extraPaths nodes), proper dependency pins (requirements.txt was
        a venv dump, and is gone; uv.lock carries the transitive graph)
  - [x] Unit tests for nodes/lib/core (pure python, fakes, no broker):
    - [x] handlers: RouterResponse to_json/from_json round-trip incl.
          base64-bytes convention (guards the Go wire contract)
    - [x] siblings: get_name returns all candidates / None
    - [x] rpc: retry-until-success; try-every-candidate on stale entries
    - [x] ticker: spacing holds under simulated delay
  - [x] Golden-file test: one captured to_json() output asserted byte-for-byte
        so accidental wire changes fail CI
- [x] A1. De-singleton BustleHandlers (lib/bustle/handlers.py)
  - [x] Class attributes + setup() classmethod → `__init__` instance deps
        (node_id, pipeline, model, chosen_classes, async_q, coll). `setup()`
        is gone; `BustleWorker.__init__` builds the object and `attach()` calls
        `register(hs)`. The object is built in `__init__` rather than `attach()`
        so the node owns its lifetime — the registry stores bound methods, and
        an object created and dropped inside `attach()` would be kept alive
        only by that reference.
  - [x] register(self, handlers_mgr) registers bound handlers
  - [x] Command names and payloads stay byte-identical
  - [x] `nodes/tests/test_lib_bustle_handlers.py` (NEW, 10 tests) — there was
        no test file for this module at all, which is why the defect survived:
        every dependency lived on the class, so a second instance silently
        replaced the first's pipeline and node id and the first node counted
        the wrong frames. The test patches `processing.frame_worker` with one
        that blocks on an event, so "a task was created" and "a task was
        cancelled" are assertable without a pipeline, a detector or a thread
        that outlives the test. Mutation-checked: writing the dependencies back
        onto the class fails `test_two_instances_keep_their_own_node_ids` and
        `test_one_nodes_stop_leaves_the_others_worker_alone`.
  - Note on the second mutation result: `assertFalse(task.done())` alone did
    NOT catch the shared class attribute, because the first node's stop cancels
    the second node's task and leaves the *class* holding it, so the second
    still looks alive until the cancellation lands. The assertion that fails is
    `assertFalse(task.cancelled())`.
- [x] A2. Extract lib/core/node.py (the real work)
  - [x] `Node(node_id, name, family, broker, version, register_payload,
        worker, intro_interval, rpc_timeout, remove_timeout)`: the runtime owns
        the handlers registry, the siblings registry, `run()` (connect_robust)
        and `serve(channel, stop_event)`. The channel is a parameter rather
        than opened inline because every part of the lifecycle can be driven
        against a fake channel, and the parts that go wrong on shutdown are
        exactly the parts a live broker would hide.
  - [x] `Worker` base class, all four methods defaulted: `attach(hs)` (the
        command names embed the node id, so they can only be registered at
        startup), `start() -> list[asyncio.Task]`, `registered(router, sibs)`
        (the post-registration hook B4 needs) and `stop()`.
  - [x] `Node.serve()` owns: setup_router, router.listen, the introduce
        ticker, `nodes.add` with retry-until-sibling, and the shutdown
        choreography — `Worker.stop()` → cancel+await background tasks keeping
        listen alive → `nodes.remove` → cancel listen. The ordering is
        documented in the module docstring and pinned by tests that record the
        call order, because none of those failures show up as an exception.
  - [x] `register_payload(node_id, **fields)` in `lib/core/node.py`: builds the
        hub's one-element list (`[{"id": ..., "bitrate": ..., "width": ...,
        "height": ...}]`). It is a helper rather than an inline `json.dumps`
        because the batch-list shape is the easy thing to get wrong when there
        is exactly one node in it.
  - [x] `bustle.py` shrank from 461 to 336 lines and is now: parse args → open
        mongodb → build `BustleWorker` (streamer, detector, chosen classes,
        queue, collection) → `Node(...).run(stop_event)`. `init_db` stays,
        because a timeseries collection is bustle's own storage decision.
  - [x] Verify: pyright clean, 161 Python tests OK (12 new in
        `nodes/tests/test_lib_core_node.py`), `make` exit 0, and the behaviour
        is unchanged — the same log lines, the same `nodes.add`/`nodes.remove`
        payloads, the same `nodes.remove` skipped when registration never
        completed. Mutation-checked: moving `Worker.stop()` after the
        background gather, dropping the `registered_event` guard, dropping the
        hook call, never cancelling the listener, and swallowing a failed
        listener are each caught.
  - Note: a `Worker.registered()` failure is logged, not raised. The node is
        registered and serving by then, and what the hook publishes is
        recoverable while the node's own commands are not.
- [x] A3. Pipeline/worker hygiene
  - [x] SRTStreamer: decode chain as parameter (default current
        "tsdemux ! h264parse ! avdec_h264 ! videoconvert"); BGR frames stay
        the contract. `DEFAULT_DECODE_CHAIN` + `SRTStreamer(node_id,
        decode_chain)` + `pipeline_string()`, exposed as `--decode-chain` on
        bustle. An empty chain is a `ValueError` at construction rather than a
        launch line with a doubled `!`. Tests assert the default, the splice
        and that the BGR caps do not follow the chain; asserted on the launch
        line, not on a `parse_launch` result, because the CI runner installs
        plugins-base only and has neither `tsdemux` (plugins-bad) nor
        `avdec_h264` (libav).
  - [x] Fix frame_worker busy-loop (lib/bustle/processing.py:67-69): add
        get_frame(timeout) via queue.get(block=True, timeout=...) — already
        done: `get_frame(timeout=...)` with `timeout=None` kept non-blocking
        for the old `get_nowait()` callers, and `frame_worker` passes
        `FRAME_WAIT_SECONDS`. Pinned by
        `test_get_frame_without_a_timeout_does_not_block` (run off-thread so a
        regression fails instead of hanging the suite).
  - [x] Thread-safety: guard start()/stop() against the appsink callback
        thread (lock or event) — already done: `self._lock` guards
        `pipeline`/`running`, `start()` publishes the fully wired pipeline
        under it, and `stop()` clears it under the lock but calls
        `set_state(NULL)` *outside*, because that call blocks until the
        streaming thread stops and the streaming thread takes the same lock in
        `_on_new_sample` — taking it there would deadlock.
- [ ] A4. Lifecycle hardening (opt-in only)
  - [ ] Periodic re-registration (--reregister flag, default off; hub upsert
        is idempotent so this is safe)
- [x] A5. Packaging/docs
  - [ ] pyproject entry point (bustle = bustle:main) — **deliberately not done**.
        A console script needs an installable project, so `[tool.uv] package`
        would flip to true and a wheel would install `lib/` as a *top-level*
        package into `.venv`. `lib` is a generic name to squat on, and it
        creates exactly the hazard the four Go `replace` directives already
        taught: an installed copy shadowing the edited source tree, silently.
        `uv run python nodes/bustle.py` and the container's
        `ENTRYPOINT ["python", "nodes/bustle.py"]` both already work, so the
        entry point buys an alias at the cost of a second import path.
  - [x] nodes/README: "add a node type" recipe — decode chain + processing
        loop + document shape + query manifest; everything else inherited.
        Also records what the runtime owns, the shutdown order and why it is
        that order, the executor's stage allowlist / size caps / facet-nesting
        rules, the flag table, and a checklist.

Out of scope for A (deliberate): sibling-registry TTL/sweep (cross-language,
Go + Python, its own branch); renames of lib/ layout; async rewrite of the
GStreamer/inference path.

## B. Analytics service (writer-owned queries) — DESIGN.md §11

New Go service analytics/ modeled on the subscriptions/ template: broker
router + SQLite registry + read-only Mongo executor. Nodes become write-only;
the service becomes the sole reader. Builds on A2 (the Node runtime hosts the
declare step).

**Status:** B1–B5 are implemented, tested, deployed and on the read path
(`deploy/analytics.Dockerfile`, the `analytics` and `migrate-analytics` compose
services). A node publishes its query manifest at boot, `GET /nodes/analytics`
asks the service what that node declared and runs every query in it, and the
node no longer reads its own collection. B7 (the rewrite of what the bustle
queries actually say) is also done, as is B6 (fleet-wide rollups). The only
remaining item is versioned query names, and it
is optional.

- [x] B1. analytics service skeleton
  - [x] analytics/ module: cmdline (broker, family, name, mongo url, dsn),
        main wiring, introduce loop, restart loops — near copy of
        subscriptions/ skeleton. Flags: `-b/--broker`, `-d/--database`
        (default `./analytics.db`), `--mongo-url` (env `MONGO_URL`),
        `--mongo-db`, `-f/--family`, `-n/--name`, plus the logging pair. The
        Mongo client is built before any command is served, so a bad URL is a
        startup failure rather than the first failed query.
  - [x] SQLite registry schema: declarations (node_id, collection, queries)
        with idempotent upsert on analytics.declare. One row per declared
        query, primary key `(node_id, name)`; the upsert is delete-then-insert
        inside one transaction, because a manifest is a set and a re-declare
        that drops a query has to remove it.
- [x] B2. Commands
  - [x] analytics.declare: {node_id, collection, queries: [{name,
        params_schema?, pipeline}]} → upsert manifest. Validated here, at
        declare time, so a broken manifest is refused where it can still be
        fixed rather than at the first query.
  - [x] analytics.query: {node_id, query_name, params} → substitute params,
        append time-range $match, execute read-only, return JSON.
        Substitution happens on the raw bytes: decoding the pipeline into a
        Go map would lose `$sort` operand order, which changes the query's
        meaning. The time-range `$match` is the caller's, passed as a
        parameter; the service appends the document cap.
  - [x] analytics.queries.list: {node_id} → declared query names
- [x] B3. Executor hardening (the one security surface)
  - [x] Operator whitelist: allow $match/$group/$project/$sort/$limit/
        $unwind/...; deny $out/$merge/$unionWith. An **allowlist** of 18
        read-only stages, not a blocklist: `$facet` can nest a writing stage
        inside a pipeline that looks read-only from the outside, and the
        contents of a `$facet` are validated recursively for the same reason.
  - [x] Stage count cap (24) and result-size cap (2000 documents / 1 MiB).
        `$limit` is **appended** rather than merged into whatever the manifest
        already had, so a manifest without one cannot return a whole
        collection.
  - [x] Output-shape check: declared pipelines must return generic chart
        data (axes/categories, series names, values) — reject ECharts-style
        option structures (xAxis/legend/series.type) at declare time where
        feasible, or at minimum enforce it in review/convention. **Deferred to
        B4/B5 on purpose**: the service cannot recognise a chart shape it has
        never seen, and the ECharts `$project` stages are still in
        `lib/bustle/analytics.py`. They die with B4, which is the commit that
        makes the shape checkable; until then the service validates structure
        (read-only operators, caps) and not semantics.
        **Closed by B4 rather than by the service**: the pipelines were the
        only ECharts-shaped thing, and they now emit generic chart data. The
        check that they keep doing so lives where the shape is authored
        (`nodes/tests/test_lib_bustle_analytics.py` rejects `xAxis`, `yAxis`,
        `legend`, `showSymbol` and `type` keys), because a service-side
        heuristic would be guessing at a shape it cannot see the renderer for.
- [x] B4. Node side (A2 is done; the hook exists)
  - [x] Node runtime: `Worker.registered(router, sibs)` runs once `nodes.add`
        succeeded and is where a declaration is published. A failure is logged
        rather than raised — the node is registered and serving by then.
  - [x] bustle: publish the manifest from `BustleWorker.registered()`, with
        retry-until-sibling like `nodes.add` — `bustle_analytics.declare()`
        wraps `core_rpc.call(..., "analytics.declare", ...)`, so it inherits
        the same resolve-and-retry ladder and fails on a non-zero reply code.
        Bounded at `DECLARE_RETRIES = 30` rather than forever: a deployment
        with no analytics service logs one warning a second for half a minute
        and then stops, instead of logging for the life of the process. The
        failure is swallowed by the runtime's hook guard, so the node keeps
        serving its own commands — the declaration is re-published on the next
        boot anyway, and nothing can ask a running node to declare again.
        Verified live: `lib.bustle.analytics: declared analytics queries
        node_id=... collection=... queries=2` 9ms after `node registered`.
  - [x] bustle: convert lib/bustle/analytics.py's two pipelines into a query
        manifest (params: time_cluster window + granularity via placeholders)
        — dropping the ECharts-shaped final $project stages (xAxis/yAxis/
        legend/series.type/showSymbol) in favor of generic chart data:
        categories + named series of values; the presentation layer maps
        these onto whatever chart library it uses.
        As built: `counts_detail` → one document per class,
        `{name, points: [[bucket, value], ...]}`; `counts_summary` → one
        document, `{categories: [...], series: [{name, values}]}`. Parameters
        are `${start_time}` (ISO-8601) and `${unit_time}`; the window wraps the
        parameter in `$toDate` because a string compared against a BSON date
        orders below every date and would match nothing. The node id is baked
        into the pipeline rather than passed, so a declaration cannot read a
        collection it was not declared for. At B4 the ECharts mapping still
        survived as `to_echarts()`, called only by the legacy handler and
        verified live to answer
        `[xAxis, yAxis, legend, series(min/bar, average/bar, max/bar)]`; B5
        deleted both. A third query, `counts_totals`, was added at B5.
        `nodes/tests/test_lib_bustle_analytics.py` asserts the allowlist, the
        stage cap, the absence of chart-library keys, the sort directions, the
        placeholder/schema agreement in both directions, and the declare
        round trip; mutation-checked against 9 edits (dropping `$toDate`,
        adding `$out`, an undeclared and an unused parameter, a hardcoded
        granularity, a wrong series source, a flipped sort, a missing ECharts
        key).
- [x] B5. Cutover (the one wire-changing step — coordinated commit, Go +
        Python together) — **done, and it ended up wider than planned.**
  - [x] Hub: the node-analytics adapter now asks the service for the node's
        declaration (`analytics.queries.list`) and runs every query in it
        (`analytics.query`). No query name is baked into the hub: the charts
        belong to the node type, so a detector with a different story to tell
        declares its own and appears in the report with no hub change. The
        response shape did change — from a fixed `{summary, detail}` pair to
        `{window, queries: [{name, description, data | error}]}` — because a
        fixed pair is exactly the coupling the cutover exists to remove. Each
        query is sent only the parameters its own schema declares (sending
        `unit_time` to a query that does not take it is a payload error at the
        service; withholding it from one that does is a missing parameter), and
        one failing query is reported per entry rather than failing the report,
        which fails outright only when nothing could be answered. The
        `time_cluster` → window/bucket mapping moved here from the node, since
        how much history to show is the caller's question.
  - [x] Nodes: `sink.analytics`, `AnalyticsParams`, `_aggregate` and
        `to_echarts` are deleted; `BustleHandlers.setup` no longer takes a
        collection and the node holds no read handle on its own data. The
        manifest gained `counts_totals` (`{categories, values}`) to prove the
        endpoint is not tied to two names — it takes only `start_time`, so it
        also exercises the per-query parameter selection end to end.
  - [x] Docs: ARCHITECTURE.md command catalog + data flow step 7, DESIGN.md
        §8/§9/§11, this file, AGENTS.md invariants.
  - [x] Mongo credential split: `deploy/mongo-init.js` creates `surr-writer`
        (`readWrite` on the nodes database) and `surr-reader` (`read`), as a
        one-shot `mongo-init` compose service rather than a
        `/docker-entrypoint-initdb.d` script — those only fire on an empty data
        directory, so an existing `mongo-data` volume would never get the users.
        bustle uses the writer, analytics the reader: the split is enforced by
        credentials, not convention.
- [x] B7. Bustle analytics rewrite — the queries now say something, and they
        landed before B6 (which is optional) rather than after it.
  - [x] Replaced the three demo-shaped queries with four that answer questions
        about a scene: `activity_by_class` (class-agnostic: per-class total,
        average, min, max, stddev, peak bucket, share of all detections, plus a
        `coverage` block), `occupancy` (six views of one scan: `timeline`,
        `crowd` band histogram, `moments`, `trend` first-half vs second-half,
        `streaks`, `volatility`), `rhythm` (average and peak by hour of day and
        day of week) and `class_affinity` (how often each other class shares a
        bucket with the subject). The user's instruction was explicitly "don't
        keep existing", so `counts_detail`/`counts_summary`/`counts_totals` are
        gone rather than kept alongside.
  - [x] Every query ends as ONE `$facet` document. The service caps a result at
        2000 documents and 1 MiB and appends `$limit` to the pipeline, so a
        query returning one document per bucket would truncate on a long window
        and would cost a pass over the collection per chart; one document per
        query means one scan per query and a report whose size depends on the
        number of views, not the length of the window.
  - [x] The subject is resolved, not assumed: `resolve_subject()` returns
        `person` only when the node's `--chosen-classes` include it, and the
        manifest then declares `activity_by_class` alone. A vehicle node
        reporting three empty occupancy charts would be indistinguishable from
        an empty venue.
  - [x] `rhythm` buckets in the node's OWN offset (`local_utc_offset()`,
        formatted `±HH:MM` from the node's clock rather than a flag that would
        default to UTC and silently shift every peak). Day names replace
        `$dayOfWeek`'s 1..7 so the axis is labelable.
  - [x] Bucket labels leave as ISO strings via `_iso()`: the service marshals
        relaxed extended JSON, so a BSON date reaches the client as
        `{"$date": ...}` and no chart can label an axis with it.
  - [x] Averages are per bucket, never per document — the drain interval is not
        constant, so a densely sampled minute would otherwise outvote a sparsely
        sampled hour.
  - [x] `nodes/bustle.py` wires `subject` and `utc_offset` into
        `bustle_analytics.declare` from `BustleWorker.__init__`; both are fixed
        for the process (they depend only on the detector's class table and the
        wall clock).
  - [x] Seven defects found by writing the tests and by probing real MongoDB
        (`tmp_probe_analytics.py`, 4800 seeded documents over three hours with a
        deliberate 10-minute gap): `$case` instead of the bare `case` key;
        `$mergeObjects` matching a `$merge` substring check; `affinity_pipeline`
        emitting no document at all on an empty window (no `$facet`, so a bare
        `$group` over nothing produces nothing); the facet results being spliced
        into the report root instead of nested under their own keys (the
        `$arrayElemAt` merge); `rhythm`'s facet views spliced the same way;
        `class_affinity`'s `share` counting every bucket instead of the subject's
        occupied ones; and BSON dates reaching the client as objects. Only the
        live probe found the last three — the unit tests asserted shapes, not
        values, which is why `tmp_probe_analytics.py` was worth writing.
  - [x] `nodes/tests/test_lib_bustle_analytics.py` rewritten (228 Python tests
        total, was 183): a `stages()` generator recurses into `$facet`
        sub-pipelines and `keys()` walks every dict key, so `$mergeObjects` is
        not mistaken for `$merge` and the stage count is checked through the
        nesting rather than at the top level.
  - [x] Docs: DESIGN.md §8/§11, ARCHITECTURE.md, this file, deploy/README.md,
        and `deploy/smoke.sh` (four names plus a bucket-label-is-a-string check).
- [x] B6. Fleet declarations (done; versioned query names still deferred)
  - [x] A declaration carries `node_type` and `scope` (`node` | `fleet`), added
        by migration `000002_fleet_scope` with a `(node_type, scope)` index.
        `scope` defaults to `'node'`, so the rows that predate it keep their
        meaning with no backfill.
  - [x] The fleet manifest is owned by the **node type**, not the node:
        `UpsertDeclaration` deletes the type's `scope='fleet'` rows and inserts
        the incoming ones in the same transaction, so a node that declares no
        fleet query leaves the type's manifest alone while a node that does
        replaces it whole. The last declaring node of a type owns the rollup.
  - [x] `fleet_activity` is declared by the bustle node next to the four
        per-node queries, and only when the node counts a subject (a rollup of a
        class nobody counts is three empty charts).
  - [x] The hub passes the caller's **own** node set with every query, because
        the scope belongs to the node type's manifest and the hub knows no query
        names. The service intersects that set with the type's declarations to
        get the member collections, so a fleet query can only ever read
        collections the caller owns.
  - [x] The executor prepends one `$unionWith` per member beyond the first. The
        member's own collection stays the aggregate target, so a one-node fleet
        is the plain query with no union at all.
  - [x] No members is `models.ErrEmptyFleet` rather than an empty answer: a
        report of empty arrays reads as "the fleet saw nothing".
  - [x] `$unionWith` stays **out** of the node pipeline allowlist. It is
        injected by the executor after validation, so a manifest still cannot
        name a collection it does not own — and `$lookup`/`$graphLookup` remain
        refused for the same reason.
  - [x] Two-stage arithmetic, and the order is load-bearing: per node per bucket
        the value is the **average** of that node's documents (a bucket holds
        several drain intervals; summing them reports a camera that sampled
        twice as often as seeing twice as many people), and only then are the
        nodes' values **summed** (the fleet total at that moment). A live probe
        against two seeded collections is what found the first version summing
        inside the bucket -- six 10s documents of four people summed to 24 per
        minute.
  - [x] Tests: 250 Python (the fleet arithmetic, both views sharing one per-node
        stage, the default shape), and Go coverage of the scope round-trip, the
        type-owned replace, the member intersection, the union prepend, the
        empty fleet and the hub's fleet payload.
  - [ ] Versioned query names (counts_by_minute@2) if rolling deploys mix
        versions

Sequencing: B1–B7 are done, so the analytics service is the whole read path,
the node-side aggregation code is gone, and a report can answer across a fleet.
What is left in B is one optional hardening item: versioned query names, which
only matters once two builds of the same node type run at once.
Logging (C) is Go-only and independent of A — it can proceed in parallel,
but must land before B1 so the analytics service is born with structured
logging instead of being migrated after.

## C. Structured logging (zerolog, Go services) — DESIGN.md §13

~1–1.5 days total. Mechanical per service, but field names and levels follow
the DESIGN.md §13 policy exactly — logs must carry context values
(command/track_id/sub_id/node_id/peer/...), not just messages.

- [x] C1. Shared logging package (root module)
  - [x] internal/logging: zerolog setup — --log-level (default info) and
        --log-format=console|json (default console) flags, root logger with
        service=<sibling> family=<exchange> fields
  - [x] Add zerolog to root go.mod (shared by hub/rx/subscriptions)
  - [x] Convention: main injects the root logger into ctx; packages pull
        zerolog.Ctx(ctx); ctx-less components (broadcaster) hold a logger
        field from construction
- [x] C2. internal/ shared packages
  - [x] router.go: introduce broadcast (debug), sibling registry set (debug:
        sibling, command count), inbound dispatch (debug: command, track_id,
        reply_to), reply matched (debug: track_id, code), malformed/nack
        (warn: reason, track_id), handler lookup failure (error: command, err)
- [x] C3. rx
  - [x] Lifecycle (info): startup config summary (broker, family, sibling,
        ingress/egress addrs, worker + goroutine counts), shutdown, beforeExit
        results, restart-loop re-entry
  - [x] SRT (info/warn): accept (peer, streamid, sub_id/node_id), reject
        (peer, streamid, reason), publisher start/stop (sub_id, peer, node
        count), subscriber subscribe/unsubscribe (sub_id, node_id)
  - [x] Slow-consumer drops: warn, sampled/countered (first drop + every Nth
        with dropped count) — never per-chunk logging on the media path
  - [x] RPC adapters: sink.start/stop dispatched (info: node_id, command),
        membership queries (debug), failures (error: command, err)
- [x] C4. hub
  - [x] HTTP middleware (status-capturing wrapper or hlog): method, path,
        status, duration_ms, remote; 2xx debug, 4xx warn, 5xx error
  - [x] Auth: signin success (info: user_id), failure (warn: email — never
        the password), refresh rotation + logout (info: user_id)
  - [x] Broker handlers: nodes.add/remove (info: node count), non-zero reply
        codes (warn: command, code)
- [x] C5. subscriptions — handler invocations (debug: command), failures
      (error: command, err), lifecycle as in rx
- [x] C6. tx (standalone module — its own zerolog dep)
  - [x] Pipeline setup (info: stream name, downstream/recording specs, sink
        host), state changes, bus errors, DASH register/unregister
  - [x] Source URI may embed rtsp credentials — log scheme://host only
- [x] C7. Remove all remaining fmt.Printf INFO:/ERRO:/WARN: prints across
      mains and internals — verified with go build, go vet, gofumpt and
      golangci-lint (make lint: 0 issues in every module)
- [x] C8. bustle.py print()s → stdlib logging, matching the level semantics of
      lib/core/ modules (Python side keeps stdlib logging). **Correction:** this did
      NOT fold into A2 — it shipped as a standalone module,
      `nodes/lib/core/logging_setup.py` (imported at `bustle.py:16`, called at
      `bustle.py:208`), while A2 is still open. `lib/core/*.py` still use
      module-level loggers rather than the configured root logger.

## D. Standalone fixes (unrelated to A/B, small)

- [x] ECharts-shaped analytics output (nodes/lib/bustle/analytics.py) — fixed
      across B4 and B5. The pipelines now emit generic chart data only, the
      `to_echarts` shim died with the handler that used it, and `GET
      /nodes/analytics` returns named rows with no renderer's vocabulary
      anywhere. The regression guard is
      `nodes/tests/test_lib_bustle_analytics.py`, which checks the *keys* —
      the old stages wrote `{"$literal": "line"}`, so matching the rendered
      text would let them straight back in
- [x] tx --recording flag wrote parsed specs into DownstreamSpecs instead of
      RecordingSpecs (tx/cmd/tx/cmdline.go, recording flag Action) — fixed
      and committed (e3ca2db)
- [x] Replace the `restart = restart || !restart` idiom — it always
      evaluated to true after the first pass, i.e. it was an obfuscated
      `restart = true`. Exact sites were (verified): `rx/cmd/rx/main.go:198`
      (a single `listen()` helper — the four old rx sites were consolidated by
      commit 4efe154), `hub/cmd/hub/main.go:157,178,233`,
      `subscriptions/cmd/subscriptions/main.go:132`. **Fixed** (E4): the idiom
      is gone from the tree (`grep -rn 'restart || '` returns nothing); every
      long-running routine now runs inside `restart.Loop`, which owns the
      backoff and the retry logging (`hub/cmd/hub/main.go:195,196,240`,
      `rx/cmd/rx/main.go:136-139`,
      `subscriptions/cmd/subscriptions/main.go:135,136`).
- [x] POST /subscriptions/remove used to return 501 — implemented
      (`hub/internal/protocols/http/handlers/subscriptions.go:75`: decodes the
      subscription id, calls the remove usecase over the broker; 400 on nil id).
      Ownership is now enforced too (E9): the `DELETE` binds
      `(id = ? AND user_id = ?)` and a refusal comes back as
      `protocols.CodeNotFound`, which the handler answers with 403.
- [x] Detector licensing and coupling. The node inferred through
      `ultralytics` (**AGPL-3.0**) while this repository is MIT, and
      ultralytics' `Results` object leaked into the counting code
      (`nodes/lib/bustle/processing.py`: `boxes.cls`,
      `isinstance(boxes.cls, torch.Tensor)`, `model.names`), so the licence was
      not the only obstacle to replacing it. **Fixed**: `nodes/lib/detector.py`
      defines a narrow `Detector` Protocol (`names: Mapping[int, str]`,
      `detect(frame) -> list[Detection]`) with `Detection` a frozen dataclass of
      `class_id`/`confidence`/`xyxy`; `LibreYOLODetector` implements it over
      **LibreYOLO** (MIT code, no `py.typed`) with **`LibreDEIMm.pt`** weights
      (**Apache-2.0**, 55.4 mAP50-95 at 19.6 M params, 640 px). DEIM v2 at
      S/M/L/X was rejected because those checkpoints add Meta's DINOv3 licence,
      which is not OSI-approved and forbids military/ITAR uses; DEIM v1 is a
      HGNetv2 backbone and carries none of that. `ultralytics` and `lap` are out
      of `pyproject.toml`, `libreyolo==1.6.0` is in. Two behaviour changes:
      DEIM decodes top-K with **no NMS**, so `--conf` (default `0.25`) is the
      only filter between raw output and the count (overlapping low-confidence
      boxes are counted separately, unlike ultralytics' NMS); and tracking was
      dropped — `model.track()` was called but only `boxes.cls` was ever read,
      and LibreYOLO's `predict` reports no ids. Cost: ~546 ms/frame on CPU at
      1280x720. Covered by `nodes/tests/test_lib_detector.py` (17 tests against
      a faked `libreyolo`).
- [x] Go test suite. **Done**: `make test` runs `go test ./...` in every module
      and covers the RPC reply layer
      (`internal/protocols/handlers_test.go`, the `hub` and `subscriptions`
      `handlers` packages), `internal/backoff`, the sibling wire shape and
      introduce warning (`internal/protocols/siblings_test.go`,
      `internal/protocols/rabbitmq/router_test.go`), the SQLite user store
      (`hub/internal/db/sqlite/user_test.go`), signup validation
      (`hub/internal/protocols/http/handlers/users_test.go`), the rx data plane
      (`rx/internal/protocols/srt/broadcast_test.go`, `pending_test.go`,
      `rx/internal/adapter/rabbitmq/nodes_test.go`), the rx CLI address parser
      (`rx/cmd/rx/cmdline_test.go`) and the tx shutdown verdict
      (`tx/cmd/tx/main_test.go`, `tx/internal/core/coder/element_test.go`). The
      two `handlers` tests enumerate the commands their service advertises and
      fail when one has no case. The Python side is covered as well:
      `nodes/tests/` (137 tests, stdlib `unittest`) exercises the
      `nodes/lib/core` runtime, the detector seam and the version resolver, and
      runs from `make test-nodes`, wired into `make test`, with
      `make typecheck` gating it with pyright. What is still untested is the
      wiring rather than the logic: no test drives a real SRT socket or a real
      GStreamer pipeline — those are exercised by the compose stack.
- [ ] PostgreSQL support (README TODO)
- [ ] Analytics presentation layer (README TODO)

## E. Verified defects (full code review pass) — all fixed

Every item below was reproduced by reading the code (two of them empirically:
E0 with a scratch SQLite/Go test, E1 with a `json.Marshal` probe; E6 and the
unbounded rx reconnect loop with a live container stack). These are described in
DESIGN.md "Known limitations" §A/§B and summarised in AGENTS.md "Invariants".
E0 and E1 blocked the whole media path and the whole error-reporting path
respectively; both are fixed, and the code each fix landed in now has a test.
The items are kept as written — the historical defect text with the fix
appended — because the reasoning is what stops a regression, not the checkbox.

### ✅ Compile regression (fixed): the tree did not build

- [x] `rabbitmq.NewRouter` gained a fifth parameter `run RunTask`
      (`internal/protocols/rabbitmq/router.go:84`, used at `:199-205` to send
      deliveries through the goroutines pool instead of an unbounded `go`),
      and the three call sites were left behind. All three now pass a pool:
      `hub/cmd/hub/main.go:103`, `rx/cmd/rx/main.go:90`,
      `subscriptions/cmd/subscriptions/main.go:88`. Two follow-on edits were
      needed: rx builds the pool *before* the router (`rx/cmd/rx/main.go:89`),
      and subscriptions had no pool at all, so it grew a `--goroutines` flag
      (`subscriptions/cmd/subscriptions/cmdline.go:35-41`,
      `subscriptions/cmd/subscriptions/main.go:87`). `go build ./...` is now
      clean in all five modules.

**Status of the E0–E5 fixes**: E0–E5 are fixed and committed. The E1
regression tests (`internal/protocols/handlers_test.go`, the two
handler-package tests, `internal/backoff/backoff_test.go`) are the Go-side
coverage, and `nodes/tests/` covers the Python mirror of the RPC plumbing plus
the whole E8 runtime. The two handler-package tests enumerate the commands each
service advertises and fail when one is untested, so the reply layer cannot
regress silently. The gap named here originally — "the Go-side storage,
data-plane and tx paths have no test" — was closed by the E6/E7/E11 pass:
`hub/internal/db/sqlite/user_test.go`, `rx/internal/protocols/srt/`,
`rx/internal/adapter/rabbitmq/nodes_test.go`, `rx/cmd/rx/cmdline_test.go` and
`tx/cmd/tx/main_test.go` now exist.

### E0. Subscriptions UUID storage-class mismatch — data plane dead (CRITICAL) — fixed

- [x] `subscriptions/internal/db/sqlite/subscription.go:60,64` bind
      `uuid.UUID` (→ TEXT) on insert; `:123`, `:157`, `:168` bind `ID[:]`
      (→ BLOB) on select. SQLite compares storage classes → zero rows.
- [x] Same class at `hub/internal/db/sqlite/node.go:68,71`
      (`SelectAllNodesByID` binds `n.ID` TEXT against BLOB rows).
- [x] Effect: rx ingress rejects every publisher ("no subscribed nodes"),
      rx egress rejects every node ("no unique subscription"), hub analytics
      ownership check fails.
- [x] Fix: bind `uuid[:]` on both sides (BLOBs are the documented intent,
      and DESIGN invariant 10 now records this). Applied across the package —
      `:29` (`SelectSubscriptionsByUser`, now `userID[:]`), `:60,64` (insert),
      `:113,116` (`DeleteSubscriptions`), `:123`, `:168` all bind `ID[:]`.
      No regression test was added for the UUID binding — the reply-layer tests
      do not touch SQLite, so a future edit can silently reintroduce the
      mismatch. (Checking it needs a real SQLite connection: the storage class
      is a property of the executed bind, not of the Go value.)

### E1. RPC error replies are never published (CRITICAL) — fixed

- [x] `RouterResponse.Result` is `json.RawMessage`; handlers assign
      `[]byte(err.Error())`, so `json.Marshal` fails with
      `invalid character 's' looking for beginning of value` and the error is
      discarded (`_ = rep(...)`). Callers time out instead of getting -1/-2/-3.
- [x] Sites: `hub/internal/protocols/rabbitmq/handlers/node.go:72,81,88` and
      ~18 in `subscriptions/internal/protocols/rabbitmq/handlers/subscription.go`.
- [x] Consequence: `nodes.add` failures become a permanent 1 Hz retry loop
      (`nodes/lib/core/rpc.py`, `retries=None`), with no diagnostic on either side.
- [x] Fix: build the error `Result` as valid JSON (e.g.
      `json.RawMessage(strconv.Quote(msg))`). **Applied and verified** — all 27
      `Result:` assignments across `hub/internal/protocols/rabbitmq/handlers/node.go`
      (4) and `subscriptions/internal/protocols/rabbitmq/handlers/subscription.go`
      (23) are now either `json.RawMessage(strconv.Quote(err.Error()))` or a
      marshalled success payload; no `Result: []byte(...)` remains anywhere.
      **Fixed** (E1.4): the regression coverage lives in
      `internal/protocols/handlers_test.go` (`TestErrorResponseIsValidJSON` for
      all three failure codes plus quoting/newlines) and in the handler tables,
      where every reply goes through `assertWire`, a real
      `json.Marshal` → `json.Unmarshal` round trip that also asserts a non-zero
      reply's `Result` is `json.Valid`.
      `hub/internal/protocols/rabbitmq/handlers/node_test.go` and
      `subscriptions/internal/protocols/rabbitmq/handlers/subscription_test.go`
      now each call `assertEveryCommandCovered`, which enumerates the commands
      the service actually advertises (via `AvailableCommand`) and fails when
      one has no case in the table — so a newly registered command cannot ship
      untested. Both packages also cover the `-4` `CodeNotFound` path and a
      message containing quotes and a newline. Verified by reverting
      `ErrorResponse` to `Result: []byte(msg)`: all 12 non-zero-code cases fail
      with `json: error calling MarshalJSON ... invalid character 'u' looking
      for beginning of value`, and pass again once restored.

### E2. RPC plumbing (HIGH) — fixed

- [x] `call()` double-send on a cap-1 channel: `res <- err` then
      unconditional `res <- nil` → the closure blocks forever, `done` never
      closes, `call()` blocks until ctx cancellation and leaks a pool worker.
      `hub/internal/adapter/rabbitmq/nodes.go:99-106`,
      `hub/internal/adapter/rabbitmq/subscription.go:151-158`,
      `rx/internal/adapter/rabbitmq/subscriptions.go:109-131`.
      **Fixed**: each closure now `return`s after `res <- err` and declares
      `err :=` locally, so the second send is gone. The unbuffered-channel
      deadlock that the first attempt introduced is also gone —
      `hub/internal/adapter/rabbitmq/subscription.go:106` is back to
      `make(chan error, 1)`, matching `hub/internal/adapter/rabbitmq/nodes.go:57`
      and `rx/internal/adapter/rabbitmq/subscriptions.go:66`.
- [x] Same closures race on the captured `err`/`resp` variables — fixed: the
      closures now declare `err :=` locally, shadowing the outer variable. The
      early `return` on the Recv-error path also skipped `close(done)`, so
      `call()` waited for ctx cancellation instead of returning the real error.
      **Resolved** by removing the `done` channel outright: `call()` now
      `select`s on an internal `recvErr := make(chan error, 1)` and
      `ctx.Done()`, so the Recv error is returned as-is.
- [x] `hub/internal/adapter/rabbitmq/subscription.go:91-137`: **fixed.** The
      shared `res` channel before the sibling loop is gone — a channel is now
      created per sibling — so a straggler reply can no longer be read as the
      next sibling's result. A non-zero reply code `continue`s to the next
      candidate, as does a payload-unmarshal failure, and so do the two
      hard-error paths (a `call` error and a receive error): every failure is
      appended to an `errs` slice and the loop ends with
      `return errors.Join(errs...)`. That is what "try every candidate" means
      in `nodes/lib/core/rpc.py`. The same rewrite landed in
      `rx/internal/adapter/rabbitmq/subscriptions.go`.
- [x] `internal/protocols/rabbitmq/router.go`: **fixed.** The `track` struct
      and its `done` flag are gone. `knownTrackID` is now
      `map[uuid.UUID]chan protocols.RouterResponse` with a **buffered** channel
      (cap 1), so `handleDelivery` hands the reply over without needing a
      receiver, and `Recv` does `defer r.Forget(trackID)`, which releases the
      entry on the success, timeout and cancellation paths alike. The `if c.done`
      branch that returned without `Ack`/`Nack` is gone; a reply that arrives
      after the caller gave up is `Nack`ed without requeue and logged. The same
      change removed the leak where every successful RPC left an entry behind.
- [x] `hub/internal/protocols/rabbitmq/handlers/node.go:85-96` is missing a
      `return` after the -3 reply → two replies per request (second one
      `Code: 0`).
- [x] `internal/protocols/handlers.go:88-104` `AvailableCommand` reads the map
      without `l.mu`, racing the 1s introduce loop against a late `Set`.
- [x] `internal/protocols/handlers.go:30` `GetBytes()` had no call sites; the
      base64 convention survived only because `json.Unmarshal` into `*[]byte`
      happens to decode Python's JSON string. **Fixed** by making the contract
      explicit and giving failures their own accessor: reply codes are now
      named constants (`CodeSuccess`, `CodeDecodePayload`, `CodeDomain`,
      `CodeMarshalReply`), replies are built by `ResultResponse`/
      `ErrorResponse` instead of 27 hand-written `RawMessage(strconv.Quote(...))`
      sites, and the three adapters read failures through the new
      `RouterResponse.ErrorMessage()` — `GetBytes()` on a short ASCII message
      such as `internal` returns base64 garbage, which is exactly what the
      adapters used to log. (The helper itself was deleted in the E11 pass
      below, once nothing depended on it.)
- [x] Ack semantics diverge: Go `Nack(false, true)` requeues on shutdown,
      Python always `nack(requeue=False)`.

### E3. Shutdown / lifecycle (HIGH) — fixed

- [x] `internal/logging/cleanups.go:28-46`: `Run` copies `pending` and sets it
      nil before executing, so a `Register` concurrent with the drain is
      silently dropped — the doc comment claims the opposite.
- [x] `rx/cmd/rx/main.go:98-104,181-198`: **fixed.** Both sockets are
      registered with `cleanups`, so SIGTERM closes them, and the blocking
      `Listen`/`Accept` loop is now cancellable: `rx/internal/protocols/srt/listen.go`
      runs `sock.Listen` and the accept loop on their own goroutine and closes
      the socket when ctx is cancelled — closing it is the only thing that
      wakes `srt_epoll_wait`, which parks with an infinite timeout. Both
      `ingress.Listen` and `egress.Listen` are now one call to that helper. The
      old `routine`/`startRoutines`/`listen` restart helpers in
      `rx/cmd/rx/main.go` are gone, replaced by `internal/restart` (see E4).
- [x] `subscriptions/cmd/subscriptions/main.go:73-76` + `:58`: `run()` returns
      on a fatal setup error without cancelling ctx, so the `<-cleanupDone` at
      `:58` blocks forever (hub's `main` does call `cancel()`).
- [x] `nodes/bustle.py:351-379`: **fixed.** `listen_task` is cancelled in the
      `finally` unconditionally, after the `nodes.remove` attempt, so the
      consumer is stopped even when registration never completed — otherwise
      it stayed parked on `await asyncio.Future()` while
      `async with connection:` closed the channel under it.
- [x] `nodes/bustle.py:323-326`: **fixed, with a correction.** `intro_task`
      *is* in the `asyncio.wait` set (a crashed introduce loop is re-raised),
      and `register_task` is deliberately excluded because its completion is
      the success path — but its failure was invisible, so it now has a
      done-callback that logs `node registration failed`. The shutdown
      `gather(..., return_exceptions=True)` no longer swallows: each result is
      inspected and a non-`CancelledError` exception is logged with its
      traceback.

### E4. Restart loops / reconnect (HIGH) — fixed

- [x] No backoff and no reconnect anywhere: **fixed.** Three new pieces,
      wired into all three broker services:
      - `internal/backoff` — `Backoff` with `DefaultBase = 250ms`,
        `DefaultMax = 30s`, `Next`/`Reset`/`Wait(ctx)`. The jitter is
        deterministic (a prime multiplier), so no `math/rand` and no gosec
        exemption; it is clamped back under the maximum.
      - `internal/restart.Loop(ctx, name, f)` — the one restart loop all
        routines use now. It logs each failure, waits with backoff before
        re-entering, and resets the backoff when an attempt stayed up for at
        least `DefaultMax`, so a routine that survived a while does not inherit
        the previous outage's delay. This replaced three hand-rolled spins in
        `hub/cmd/hub/main.go` and rx's `startRoutines`/`listen` helpers, and
        subscriptions' router loop.
      - `internal/protocols/rabbitmq/supervisor.go` — `Supervisor.Watch(ctx)`
        blocks on `conn.NotifyClose(...)` and, when the broker drops the
        connection, redials with backoff, declares a fresh queue and installs
        it via the new `router.ReplaceChannel`. `router`'s channel is now
        behind a `RWMutex` with `channel()`/`queue()` accessors so every
        publish and consume uses the current one. Broker URIs in these logs go
        through `logging.RedactURL`.
      Also fixed on the way: subscriptions never started its goroutines pool
      (`goPool.Listen` was called only by hub and rx), so every inbound RPC sat
      in the pool's queue, unexecuted, until the channel filled.
- [x] `internal/pools/goroutines.go:32` `for range p.n - 1` underflows when
      `n == 0` (uint32) → ~4.29B goroutines with `--goroutines 0`.

### E5. SQLite / storage (HIGH) — fixed

- [x] `subscriptions/internal/db/sqlite/subscription.go:45-82` +
      `internal/db/db.go:37-67`: **fixed.** `BeginTx` now takes a
      single-slot gate (`t.gate`) before swapping `DB`, so two overlapping
      callers each get their own `*sql.Tx` instead of the second one executing
      on the first one's transaction; the returned release function is
      idempotent (`sync.Once`) and gives the gate back, so a caller that
      forgets it blocks the next transaction rather than corrupting it.
- [x] `DeleteSubscriptions` (`:107-120`): **fixed.** It deletes from
      `subscribed_nodes` first and then from `subscriptions`, in one
      transaction, because the schema's `ON DELETE CASCADE` only fires when
      `PRAGMA foreign_keys` is enabled on the connection and the services never
      set it. The empty-list panic (`subsID[:len(subsID)-1]`) is gone: an empty
      list is now rejected up front with `ErrNothingToWrite`.
- [x] Empty node lists produce invalid SQL (`:67-77`): **fixed** — the second
      statement is skipped when a subscription has no nodes, since a
      subscription with zero nodes is legitimate.
- [x] `scanRows` silently truncates at `cap(*buf)` (`:67-77`): **fixed** — it
      now returns `ErrBufferFull` naming the row count, so a truncated read can
      no longer be mistaken for a small result set.
- [x] `hub/internal/db/sqlite/node.go:63-71` `SelectAllNodesByID`: **fixed by
      removal** — the function is gone (it was unused, and its replacement is
      the `SelectAllNodes` that does not take an ID list at all).
- [x] `internal/db/db.go:15-17`: **fixed.** The `TX` interface declares
      `BeginTx(ctx, opts) (func(), error)`, matching `*Transaction.BeginTx`.
- [x] `hub/internal/usecases/auth/refresh.go:37-62`: **fixed.**
      `SaveRefreshToken` sweeps expired entries at most once per
      `sweepInterval` (1m), and `ValidateRefreshToken` still deletes the token
      it just rejected; the store no longer grows without bound.
- [x] The migrations are applied out-of-band with the golang-migrate CLI, so a
      fresh checkout has no `hub.db`/`subscriptions.db` schema and nothing in
      the startup path enforced it. **Fixed by failing fast**:
      `internal/db/schema.go` adds `VerifySchema(ctx, db, migrationsPath, tables...)`,
      called right after the connection is opened by `hub`
      (`schema_migrations`, `nodes`, `users`) and `subscriptions`
      (`subscriptions`, `subscribed_nodes`). A missing table produces one error
      naming the tables and the exact `migrate ... up` command, instead of the
      previous behaviour — a service that started happily and then failed one
      "no such table" at a time. The table check queries `sqlite_master` with a
      bound parameter, so no table name is ever interpolated into SQL.
      Embedding and running the migrations at startup remains an option, but it
      would have to reimplement golang-migrate's `schema_migrations`
      bookkeeping (version + dirty flag) to stay compatible with the CLI.

### E6. tx / DASH (HIGH) — fixed

- [x] An unreachable RTSP source makes `tx` **abort** instead of reporting an
      error, so a typo in the source URL looks like a crash loop rather than a
      configuration mistake. Reproduced deterministically:
      `docker run --rm --network surr_default --entrypoint surr-tx surr-tx run
      --src=rtsp://no-such-host.invalid:8554/cam1 --sink='srt://rx:4200?mode=caller&streamid=publish:<sub-id>'`
      logs `running transmitter`, then `bus watch error="...gst_rtspsrc_retrieve_sdp
      (): Failed to connect. (Generic error): Could not open resource for reading
      and writing."`, then
      `GLib-GIO:ERROR:../../../gio/gthreadedresolver.c:1487:cancelled_cb: assertion
      failed: (g_cancellable_is_cancelled (cancellable))` and `Bail out!`, and
      exits **139**. Under `restart: unless-stopped` that is a restart every few
      seconds, which is what the compose stack did until `TX_SRC` was corrected.
      Note the sink is already publishing to `rx` by the time the source fails,
      so `rx` accepts a publisher that will never carry frames. **Fixed.** The
      abort turned out to be a *shutdown-path* defect rather than a
      failed-source one: the unfixed binary also exits 139 on a clean SIGTERM
      with a reachable source and no bus error at all, so anything that stopped
      the transmitter segfaulted on the way out. The trigger is the state change
      to NULL, not the failed lookup — `g_main_loop_quit` is thread-safe, but
      disposing the pipeline from a thread other than the one running the main
      loop is not. GLib 2.84.4 attaches the resolver's `cancelled_cb` as a
      GSourceFunc on the GLib *worker* context
      (`g_source_attach (data->cancellable_source, g_get_worker_context ())`,
      `gio/gthreadedresolver.c:1545`), so it reaches
      `g_assert (g_cancellable_is_cancelled (cancellable))` at `:1487` from that
      thread with a cancellable that is no longer cancelled. The upstream fix
      (`82c764ce2e42f0d1032627dabcbd742d5f2bd8fa`, GLib MR !3105) is *already*
      in 2.84.4 — it is the commit that introduced the `GWeakRef` guard at
      `:1477` — so this is a different, remaining race and not an ordering
      problem: it reproduces from the loop thread and from the cleanup
      goroutine, for the whole pipeline set and for the downstream pipelines
      alone. The fix therefore stops issuing the teardown state change
      altogether: the pipelines are left in PLAYING, `loop.Quit()` runs from a
      goroutine on ctx cancellation (safe from any thread), and the process
      exits once `loop.Run()` returns. Accepted cost, stated in the code: a
      `--recording` run does not get its trailer written, so an interrupted
      recording needs remuxing before it plays — the better trade against a
      segfault on every shutdown. The second half of the item is also fixed:
      `tx` exited **0** for a source that never connected. `runCommand` now
      returns `context.Cause(ctx)` unless the cause is `context.Canceled`
      (`runFailure`, `tx/cmd/tx/main.go`), `main` returns that as its exit code
      via `os.Exit(run())`, and a failed source is a **1** in the restart loop.
      Verified with a two-probe harness against the container image (clean
      source → `docker stop`; unreachable source → wait for exit): before,
      `clean exit=0 bail=0`, `fail exit=139 bail=1`; after, four runs of
      `clean exit=0 bail=0` / `fail exit=1 bail=0`. Tests:
      `tx/cmd/tx/main_test.go`.
- [x] `tx/internal/streaming/stream.go:61-63` `block()` busy-spins
      (`for s.c.Load() > 1`), and `ref()` increments the same counter, so two
      concurrent DASH requests livelock the server at 100% CPU. **Fixed**: the
      atomic ref-count protocol is gone; `Stream` is now
      `{ d map[string]struct{}; mu sync.RWMutex }` (`stream.go:25-63`) with
      `Names()` (an RLock snapshot, so a caller can render a body while
      walking it), `IsExist`, `Set` and `Delete`.
- [x] `tx/internal/streaming/stream.go:23-57` + `dash.go:25-27`: `d` is
      mutated with no lock; `AvailStream` writes it directly → concurrent map
      read/write panic. **Fixed**: every accessor takes the lock and
      `AvailStream` is deleted.
- [x] `tx/internal/streaming/dash.go:76-90` (and `:56-67`): path traversal —
      `AddStream`/`RemoveStream` join an unvalidated request-body name onto the
      recording dir (`os.MkdirAll`/`os.Create`/`os.Remove`). **Fixed**:
      `validStreamName` (`dash.go:28-46`) rejects empty/`.`/`..`, anything over
      255 bytes, anything that is not its own `filepath.Base` (so no `/` or
      `\`), and control characters; it runs at the top of `AddStream`,
      `RemoveStream` and `OpenManifest`.
- [x] `tx/internal/core/coder/h264enc.go:39` vs `h264enc_va.go:11`: `-tags va`
      does not compile (uint32 bitrate passed to a uint64 parameter).
      **Fixed**: `encElementWithOptions(bitrate uint32)` in the va build;
      `go build -tags va ./...` and `go vet -tags va ./...` are clean.
- [x] `tx/cmd/tx/pipeline.go:46,54,62`: three goroutines assign the shared
      named return `err` → data race and lost errors. **Fixed**: each closure
      assigns a local `watchErr` over a `watched := []*core.Pipeline{...}`
      slice.
- [x] `tx/cmd/tx/pipeline.go:35-39`: the recording pipeline is built even with
      `--recording 0,0,0` (only the specs are nulled) — recording cannot be
      disabled. **Fixed**: `setupPipelines` returns `recording = nil` when
      `cfg.RecordingSpecs.IsZero()`, and the stream is neither registered nor
      its directory created.
- [x] `tx/cmd/tx/main.go:239`: `tx run` aborts if the DASH server is not
      already up, so relay/recording cannot start without a separate
      `tx serve`. **Fixed**: the registration is extracted into
      `registDashStream` (`tx/cmd/tx/main.go:165`) and a failure now only logs
      a warning — recording is still written, it just does not appear in the
      dash index.
- [x] `tx/cmd/tx/pipeline.go:124-136`: the lazy audio pad probe emits EOS on
      every buffer and re-arms on failure → recording can be EOS'd early.
      **Fixed**: the probe is `gst.PadProbeTypeBuffer` only, never sends EOS,
      and returns `gst.PadProbeOK` to retry on link failure or
      `gst.PadProbeRemove` on success.
- [x] `tx/internal/streaming/dash.go:15-27`: `AvailStream` is never called, so
      recorded streams are invisible after a `tx serve` restart. **Fixed**:
      `loadRegisteredStreams()` (`dash.go:77-108`) is called by `NewDash`,
      walks the recording dir and registers every directory holding a
      `.registered` marker; `NewDash` now returns an error.
- [x] `tx/internal/server/dash/dash.go:94,113`: `err = t.Execute(...)` writes
      the enclosing function's `err` from request closures → race. **Fixed**:
      both closures use a local `execErr`.
- [x] `tx/internal/core/muxer/muxer.go` `req()` leaks the request pad on link
      failure (dashsink2's equivalent releases it). **Fixed**: the ghost pad
      and the request pad are released when `srcpad.Link` is not
      `gst.PadLinkOK`.
- [x] `tx/internal/core/coder/h264enc.go:26-45`: only videoscale + a
      width/height capsfilter (no videoconvert/format/framerate/PAR) and no
      `config-interval` on h264parse → non-I420 sources fail to negotiate and
      late-joining SRT nodes may never see SPS/PPS. **Fixed**: a
      `videoconvert` is prepended (`h264enc.go:29`), the capsfilter pins
      `format=I420`, and `h264parse` sets `config-interval: -1`
      (`h264enc.go:57`). `h264enc.Valid()` (`h264enc.go:77`) reports a missing
      factory, and both `setupDownstream`/`setupRecording` turn that into an
      actionable error instead of a nil-pointer crash.
- [x] `tx/internal/sink/srt.go:20-24`: `auto-reconnect:false` + cancel-on-bus
      error kills tx when rx restarts; property-map iteration makes `uri` vs
      `mode` application order nondeterministic. **Fixed**: `auto-reconnect`
      is `true`.
- [x] Dead code: `tx/internal/core/muxer/qtmux.go`,
      `tx/internal/sink/autovideosink.go`; `rtpm4adepay.go` filename typo.
      **Fixed**: both files deleted, the typo file renamed to
      `rtpmp4adepay.go`.

### E7. rx data plane (MEDIUM) — fixed

- [x] `rx/internal/protocols/srt/broadcast.go:54`: subscriber cap counts
      subscription maps and compares with `==` — one subscription admits
      unlimited nodes; the (max+1)-th subscription is rejected. **Fixed**: the
      broadcaster counts *subscriptions* (one per node, `b.count`) instead of
      topics and refuses with `errMaxSubscribers` (`%w`-wrapped, so a caller
      classifies it with `errors.Is`) once `b.count >= b.max`. The cap is still
      `--worker`, which is also the ingress SRT backlog. Test:
      `TestTheSubscriberCapCountsNodesNotTopics`.
- [x] `broadcast.go:60-64`: a reconnecting node's channel is overwritten in
      the map but never closed → the previous `handleCaller` blocks forever on
      `range ch`. **Fixed**: `Subscribe` returns a `*subscription` handle and a
      replacement drains and closes the previous channel (`drain(old.ch)`)
      outside the registry lock, so the old reader unblocks and returns. Test:
      `TestSubscribingANodeAgainClosesThePreviousChannel`.
- [x] `broadcast.go:50-66` + `egress.go:169`: `Subscribe` after `Close(subID)`
      recreates an orphan topic that is never closed or fed. **Fixed**: only
      `Open` creates a topic and only the publisher path calls it; `Subscribe`
      fails with `errNoPublisher` when there is none. Test:
      `TestSubscribeRefusesWithoutAPublisher`.
- [x] `Unsubscribe` closes the channel while holding the registry mutex.
      **Fixed**: the close (and the release of every queued chunk) moved into
      `drain`, called outside the lock. `Unsubscribe` takes the handle and
      removes it only when it is still the registered one, returning early
      otherwise — which also closes the panic window where a replaced handle
      closed its successor's channel. Test:
      `TestUnsubscribingAReplacedHandleLeavesTheReplacement`.
- [x] `ingress.go:66-103` / `egress.go:65-105`: blocking broker RPC inside the
      SRT handshake callback stalls handshakes when the pool is saturated;
      pending maps are keyed by peer address with no TTL and leak. **Fixed**:
      the lookup runs under `context.WithTimeout(ctx, handshakeLookupTimeout)`
      (5s), and both pending maps are the same `pending[V]`
      (`rx/internal/protocols/srt/pending.go`) with a 30s TTL and a 4096-entry
      cap, swept on every put, so an abandoned handshake can no longer leak an
      entry for the lifetime of the process. A full registry is refused with
      `srtgo.RejectionReasonOverload` rather than growing. Tests:
      `pending_test.go`.
- [x] `egress.go:184`: blocking `sock.Write` with no deadline → a stalled
      subscriber wedges the goroutine, pins pooled buffers, and its deferred
      `Unsubscribe` never runs. **Fixed**: a 10s write deadline is armed per
      chunk; a failed write logs `subscriber write` and returns, which runs the
      deferred `Unsubscribe` and releases the buffers. (libsrt sockets are
      created non-blocking, which is what makes the deadline effective — a
      blocking socket ignores it.)
- [x] `ingress.go:150-186`: ctx cancellation does not stop a blocked
      `sock.Read` → `Close(subID)` and `StopSink` are skipped, so nodes keep
      streaming after rx shutdown. **Fixed**: a 1s read deadline is armed per
      iteration; an expired deadline is an idle tick (the buffer is reset and
      returned to the pool, the loop continues), so cancellation is noticed
      within a second. The exit path is the extracted `stopSink`, which is what
      the cancellation used to skip.
- [x] `ingress.go:66-103` never sets an SRT rejection reason (egress does).
      **Fixed**: every ingress rejection path goes through `rejectHandshake`
      (`handshake.go`), which sets the reason and logs it: `BadRequest` for a
      malformed streamid, `Forbidden` for a failed lookup, `Unacceptable` for
      "no subscribed nodes", `Overload` for a full registry. The caller used to
      get a bare `return false`, which libsrt reports as a generic refusal.
- [x] `rx/internal/adapter/rabbitmq/nodes.go`: `StartSink`/`StopSink` always
      return nil (per-node failures only logged) and require exactly one
      sibling. **Fixed.** The swallowed error used to leave the data plane dead
      in a way every component reported as healthy: on a cold start where `tx`
      connects before the node has introduced itself, `rx` accepted the
      publish, `do()` failed with `expected one sibling with
      <node-id>.sink.start command`, `StartSink` logged it and returned nil, and
      `handlePublisher` went on to log `publisher started` and read frames
      nobody was subscribed to. Nothing re-dispatched the command, and because
      `rx` never dropped the publisher, `tx` never reconnected either — every
      container stayed `Up` and the MongoDB document count stayed put. Observed
      with `surr-tx-1` connecting at `21:00:07` and `bustle` registering at
      `21:00:09`. `StartSink`/`StopSink` now try every node and return the first
      failure, which is enough on its own: `handlePublisher` already returned on
      error, and its deferred `sock.Close()` makes `tx` reconnect into the
      now-registered node. Verified by starting the stack with the node absent —
      `rx` logged `start nodes sink` then `publisher stopped`, and the chain
      recovered unaided once `bustle` introduced itself, with MongoDB growing
      again (277 → 283 → 288). Tests:
      `rx/internal/adapter/rabbitmq/nodes_test.go`.
- [x] The recovery above is unbounded. `tx`'s `srtsink` retries a closed socket
      immediately and without backoff (`Socket is broken or closed. Trying to
      reconnect`), so while the node is absent `rx` accepts and drops a publish
      as fast as the two can complete a handshake: **1058 attempts in 31
      seconds** (21:11:57–21:12:28), each one doing a blocking
      `SelectSubscribedNodes` broker RPC inside the SRT listen callback. It ends
      when the node arrives, but a subscription pointing at a node that is not
      running spins at roughly 34 handshakes/second indefinitely. **Fixed** in
      `rx`: `handlePublisher` waits `sinkUnavailableDelay` (2s, ctx-aware)
      before its deferred `Close` lets `tx` reconnect, which caps the loop at
      roughly one handshake per two seconds instead of 34 per second. This
      bounds the load rather than removing it — the retry still belongs to `tx`,
      and a genuine backoff there would be the better fix if the loop ever
      matters again.

### E8. Python node runtime (HIGH) — fixed

- [x] `nodes/lib/bustle/handlers.py:112`: missing `await rep(...)` on the
      invalid-source-address path → the reply is never scheduled and rx's
      caller blocks forever. Type `ReplyFunc` as returning a `Coroutine` so
      Pyright catches this. **Fixed**: the call is awaited and `ReplyFunc` is
      now `Callable[[RouterResponse], Coroutine[Any, Any, None]]`
      (`nodes/lib/core/handlers.py`), which makes a dropped `rep(...)` a Pyright
      error rather than silent.
- [x] `nodes/lib/core/router.py:170,188`: `ack()` before dispatching the handler as
      a fire-and-forget task (reference dropped, no done-callback) → a crash
      between ack and reply loses the request silently, and handler exceptions
      are never retrieved. **Fixed**: `_track_handler_task` keeps the task in
      `self._inflight` and logs the exception of any non-cancelled failure; the
      ack stays before dispatch (the handler owns the reply, and a redelivery
      would duplicate it), but the failure is now reported.
- [x] `nodes/lib/bustle/processing.py:67-70`: frame-worker busy-wait at 100%
      CPU (`get_frame()` is non-blocking) — same item as A3, listed here for
      the standalone fix. **Fixed**: `get_frame(timeout=FRAME_WAIT_SECONDS)`
      with `FRAME_WAIT_SECONDS = 0.5`.
- [x] `nodes/lib/bustle/handlers.py:102,116`: blocking GStreamer state changes
      on the event loop (`set_state(NULL)` on an srtsrc with
      `poll-timeout=10000` can block for seconds) → no introduce, no replies.
      **Fixed**: `_start_sink`/`_stop_sink` run `pipeline.start`/`pipeline.stop`
      through `asyncio.to_thread`, as does the final `streamer.stop()` in
      `nodes/bustle.py`.
- [x] `nodes/lib/bustle/handlers.py:115-116`: `_start_sink` never stops the old
      pipeline → leaked pipeline, two appsink callbacks feeding one queue.
      **Fixed**: `_start_sink` stops the pipeline (not just the worker task)
      before starting the new one.
- [x] `nodes/lib/core/pipeline.py:17,68,77-81`: no synchronization between
      `start()`/`stop()` and `_on_new_sample`; the callback never checks
      `running`. **Fixed**: a `threading.Lock` guards `pipeline`/`running`; the
      callback checks `running` under it; `start()` publishes the new pipeline
      only once fully wired, and `stop()` swaps the reference under the lock
      before calling `set_state(NULL)` outside it.
- [x] `nodes/lib/core/pipeline.py:41-45`: `queue.Full` → `get_nowait()` can raise
      `queue.Empty` on the GStreamer thread. **Fixed**: both the drop-stale
      `get_nowait()` and the `put_nowait()` are individually wrapped, and the
      dance happens outside the lock.
- [x] `nodes/lib/bustle/processing.py:57`: no error handling around
      `insert_one` → a transient pymongo error exits the whole node process;
      `frame_worker` likewise has none (dies silently while `running` stays
      true). **Fixed**: both are wrapped and logged.
- [x] `nodes/lib/core/router.py:50,132-139`: a late reply after a `recv` timeout is
      mis-dispatched as an inbound request (the trackID is popped in `finally`).
      **Fixed**: a cancelled `recv` records the trackID in `_abandoned`
      (TTL 60s, capped at 1024); `_handle_delivery` acks and drops a late reply
      for an abandoned trackID instead of handing it to a command handler.
- [x] `nodes/lib/core/ticker.py`: `interval <= 0` degenerates to a `call_later(0)`
      hot loop. **Fixed**: the interval is floored at `MIN_INTERVAL = 0.001`.
- [x] `nodes/lib/bustle/handlers.py:79-80`: blocking pymongo aggregation on the
      event loop (up to 5 weeks of timeseries). **Fixed**: moved to
      `asyncio.to_thread(cls._aggregate, ...)`.
- [x] `nodes/lib/bustle/processing.py:39`: naive local `datetime.now()` in a
      timeseries `timeField` with seconds granularity → buckets shift when
      TZ != UTC; zero-detection classes are omitted instead of zero-filled.
      **Fixed**: `datetime.now(timezone.utc)` in `processing.py` and in all
      four `_analytics` windows. (Zero-fill was already implicit: every frame
      dict carries every configured class via `counts.get(c, 0)`.)
- [x] `nodes/bustle.py:222-227`: Mongo init failure logs and returns → exit
      code 0 for a dead node; `:385-389` `except KeyboardInterrupt` is dead
      code; `pymongo.MongoClient` is never closed; collection creation is a
      check-then-create race. **Fixed**: `main()` returns an exit code and
      `__main__` does `sys.exit(asyncio.run(main()))` (130 on
      `KeyboardInterrupt`, which is only reachable where
      `add_signal_handler` is unsupported); `init_db` returns the client so it
      can be closed after the broker connection, and creates the collection
      with `try/except CollectionInvalid` instead of a name check.

### E9. Security (HIGH) — fixed

- [x] `hub/cmd/hub/cmdline.go:56`: committed default JWT secret → anyone with
      the repo can forge tokens when `JWT_SECRET` is unset. **Fixed**: the
      default is gone; `resolveJWTSecret` (`hub/cmd/hub/main.go:64`) generates
      32 random bytes per process and logs a warning when no secret is
      configured.
- [x] `hub/internal/usecases/auth/authen.go:94-96`: any HMAC algorithm
      accepted (HS384/HS512) and the issuer is not verified. **Fixed**: the
      keyfunc requires `token.Method == signingMethod` (an exact match, since
      HS384/HS512 are also HMAC) and parsing pins
      `jwt.WithValidMethods(["HS256"])` + `jwt.WithIssuer(TokenIssuer)`
      (`authen.go:106,112-113`).
- [x] `hub/internal/protocols/http/handlers/auth.go:54-72`: no rate limiting
      on signin and the email is logged on failure. **Fixed**: a fixed-window
      limiter (`middlewares.RateLimiter`, `middlewares/ratelimit.go:30-41`)
      allows 10 attempts per minute per client address and answers 429 with
      `Retry-After`; the failure log no longer carries the email. Keyed on the
      address, not the email, so an attacker cannot lock a known account out.
- [x] No ownership check on subscription removal
      (`hub/internal/usecases/subscription.go:57` + subscriptions handler).
      **Fixed**: `DeleteSubscriptions`
      (`subscriptions/internal/db/sqlite/subscription.go`) now deletes with
      `WHERE (id = ? AND user_id = ?)` per row and is all-or-nothing, so a
      subscription that is not the caller's is a no-op; the new
      `protocols.CodeNotFound` carries that back through the broker and the hub
      answers 403 instead of 500.
- [x] `hub/cmd/hub/main.go:209-213`: only `ReadHeaderTimeout` set (no
      Read/Write/IdleTimeout) and no `http.MaxBytesReader` on any handler →
      slowloris / large-body DoS. **Fixed**: `ReadTimeout`/`WriteTimeout`/
      `IdleTimeout` are set and every body is capped at 1 MiB by
      `middlewares.MaxBodyBytes` (`middlewares/bodylimit.go:11`).
- [x] `hub/internal/protocols/http/handlers/auth.go:93,138,167`: `Secure: true`
      cookie with the default plain-HTTP `127.0.0.1:9000` bind breaks local
      dev; logout uses `SameSite=Strict` while signin/refresh use `Lax`.
      **Fixed**: one `refreshCookie` helper (`auth.go:77`) builds all three
      cookies, so `Secure` follows the new `--cookie-secure` flag and
      `SameSite=Lax` is consistent.
- [x] `tx/internal/server/dash/dash.go:177,211`: `POST /stream` and
      `DELETE /stream` are unauthenticated (mitigated only by the loopback
      bind). **Fixed**: both are wrapped in `requireDashToken`
      (`tx/cmd/tx/dash.go:45`), which compares `X-Dash-Token` (or a bearer
      token) in constant time, and with no token configured only a loopback
      caller may mutate.

### E10. Python typing / packaging (MEDIUM) — fixed

**Fixed.** The root `pyproject.toml` now carries the dependency list and the
pyright configuration, `uv.lock` records the resolved transitive graph, `uv
sync` is the one setup command, and `make typecheck` runs the gate — `0 errors,
0 warnings` in `standard` mode over `bustle.py`, `lib/` and `tests/`.
The venv is `./.venv` at the repo root, while all the node code stays in
`nodes/`.

- [x] Pyright is not installed in the node venv and there is no
      `pyrightconfig.json`; default mode reports 7 errors + 1 warning (all
      PyGObject stub attribute issues in `nodes/lib/core/pipeline.py`), strict mode
      111. Either add the config + dependency (TODO A0) or drop the claim.
      **Fixed**: `pyright` and `PyGObject-stubs` are in the `dev` dependency
      group and the config
      lives in the root `pyproject.toml` rather than a `pyrightconfig.json`
      (`include = ["nodes/bustle.py", "nodes/lib/core", "nodes/lib",
      "nodes/tests"]`, `exclude`, `venvPath = "."`, `venv = ".venv"`,
      `extraPaths = ["nodes"]`, `pythonVersion = "3.14"`,
      `typeCheckingMode = "standard"`, `reportMissingModuleSource = "none"`
      for the compiled `gi.repository` bindings). `venvPath`/`venv` matter:
      pyright does not resolve the venv just because it was launched from the
      venv's own `bin/`, so without them every third-party import reports
      `reportMissingImports`. `extraPaths` matters for the same reason at one
      level down: the nodes import `core`/`lib` as top-level names, and the
      config now lives above them. Two traps found
      on the way: (a) the PyPI package `gst-python-stubs` ships an
      overrides-only 3.6 KB `gi-stubs/repository/Gst.pyi` at the same path as
      PyGObject-stubs' real 228 KB file and silently clobbers it, which is what
      produced the original `"FlowReturn" is not a known attribute` errors —
      it must not be installed; (b) the fix was not only annotations:
      `GstVideo.VideoInfo.new_from_caps` returns `Optional[VideoInfo]` in the
      stubs, so `_frame_view` now guards the `None` case instead of reading
      `.stride` off it.
- [x] `nodes/requirements.txt` is a full venv dump and already diverges from
      the installed venv (pins `aio-pika==10.0.1`/`pymongo==4.18.1`; venv has
      10.1.0/4.18.2). Replace with a real dependency list + `pyproject.toml`.
      **Fixed**: the file is deleted. The root `pyproject.toml` lists what the
      code imports (`PyGObject`, `aio-pika`, `pymongo`, `numpy`, `pandas`,
      `libreyolo`) pinned exactly to the versions the runtime was tested
      against; the type-check dependencies sit in the `dev`
      dependency group. `torchvision` is pinned even though nothing imports it
      directly: libreyolo pulls it in, and its PyPI build is compiled against
      a different torch ABI than the CPU wheel, so the pair has to come from the
      same release or importing it dies with `RuntimeError: operator
      torchvision::nms does not exist`. The torch CPU wheel needs
      `[[tool.uv.index]]` + `[tool.uv.sources]` — PEP 621 has no index-URL field,
      so it is the one thing a dependency list alone cannot express. `uv.lock`
      is committed, so the transitive graph is reproducible rather than
      floating. (The list changed again with the detector swap — see D.)
- [x] Lint hygiene (re-checked against the current tree): missing type args on
      `queue.Queue` (`nodes/lib/core/pipeline.py:19`) and `collection.Collection`
      (`nodes/lib/bustle/handlers.py:35,48`,
      `nodes/lib/bustle/processing.py:28`); an untyped `obj` parameter
      (`nodes/lib/core/handlers.py:9`); the unused `Pipeline` Protocol
      (`nodes/lib/bustle/handlers.py:15`, declared and never referenced). Two
      earlier claims were wrong and are dropped: `import threading` *is* used
      (`nodes/lib/core/pipeline.py:2`), and `_coll`/`_node_id` *are* declared as
      class attributes (`nodes/lib/bustle/handlers.py:35-36`).
      **Fixed**: `queue.Queue[numpy.ndarray]`,
      `collection.Collection[dict[str, typing.Any]]` (both files),
      `def json_serializer(obj: typing.Any) -> str`, and the `Pipeline`
      Protocol is deleted — its only occurrence in the tree was its own
      definition.

### E11. Minor (LOW) — fixed

- [x] `internal/db/db.go:61-74`: `Commit`/`Rollback` read `t.DB` without `t.mu`.
- [x] `internal/protocols/handlers.go:30-46`: `GetBytes` base64-decodes any
      valid JSON string with no shape validation, and returns `nil, nil` for
      `"null"` — indistinguishable from an empty result. **Fixed by deletion**:
      it had no caller in Go and no counterpart in Python, and the ambiguity it
      documented cannot be resolved by a helper — a short ASCII message such as
      `"internal"` is also valid base64. What the protocol actually relies on is
      the caller unmarshalling into the type it asked for, which is how the
      hub's analytics proxy reads a Python node's base64 bytes result (a
      `[]byte` target makes encoding/json do the decode). The doc comment now
      states that contract instead of describing the helper. Test:
      `TestABytesResultDecodesThroughABytesTarget`.
- [x] `internal/protocols/rabbitmq/router.go:195-197`: empty `ReplyTo` is
      rejected silently. **Fixed**: the reply closure logs a warning naming the
      command and track id before returning the error. Nothing can be sent back
      — there is no address — so the warning is the only trace the failure
      leaves; without it the requester times out and the log of the side that
      could have answered says nothing about why it did not. Test:
      `TestReplyingWithoutAReplyToIsWarnedAbout`.
- [x] `hub/internal/protocols/http/handlers/nodes.go:122`: `http.Error(w,
      "zero nodes", http.StatusNoContent)` — 204 must have no body.
- [x] `internal/models/user.go:7`: `Passwd` is JSON-serializable (safe only
      because call sites marshal just the ID). **Fixed**: the field is
      `json:"-"`. It is not a theoretical leak —
      `hub/internal/adapter/rabbitmq/subscription.go` marshals a whole
      `models.User` as the `subscriptions.nodes.by.user.get` payload, so the
      argon2id hash was crossing the broker. Request bodies are decoded into a
      dedicated `handlers.credentials` type, so the model no longer has to be
      decodable. Test: `TestAPasswordHashIsNotSerializable`.
- [x] No email/password validation; duplicate email surfaces as 500, not 409.
      Confirmed by hand: the request decodes straight into `models.User`, whose
      password field is tagged `passwd` (`internal/models/user.go:7`), and
      nothing checks it afterwards. A body of
      `{"email":"x@y.z","password":"..."}` — the field named the way almost
      every API names it — is accepted, because Go's decoder ignores unknown
      keys, and the account is created with an **empty password** that then
      authenticates successfully. **Fixed**: `handlers.decodeCredentials`
      (`hub/internal/protocols/http/handlers/credentials.go`) decodes with
      `DisallowUnknownFields`, requires a non-empty `email` and `passwd`, and
      answers 400; `InsertUser` translates SQLite's UNIQUE failure into
      `models.ErrUserExists` and the signup handler answers **409**. The
      translation matches the *extended* constraint code, because
      `SQLITE_CONSTRAINT` also covers NOT NULL, CHECK and foreign key failures.
      Tests: `hub/internal/protocols/http/handlers/users_test.go`,
      `hub/internal/db/sqlite/user_test.go`.
- [x] `rx/cmd/rx/cmdline.go`: `srtHost` splits on `:` → no IPv6 listen addrs.
      **Fixed**: `net.SplitHostPort`, which also rejects `"127.0.0.1"`,
      `":4200"` and `"127.0.0.1:70000"` instead of parsing them into a broken
      host. Tests: `rx/cmd/rx/cmdline_test.go`.
- [x] `tx/cmd/tx/main.go:158` ignored the `url.Parse` error — **fixed**
      (`if err != nil { return err }`). The two shadowing locals are gone too:
      `dashSrv` in `tx/cmd/tx/main.go` and `endpoint` in
      `tx/internal/server/dash/client.go:52` no longer shadow the `dash`
      package and `net/url`.
- [x] `rx/cmd/rx/main.go:41`: the last raw `fmt.Println` in the tree is gone —
      no `fmt.Print*` call survives anywhere (`git grep 'fmt\.Print'` is
      empty). The CLI parse error is now swallowed (`if err != nil { return }`)
      rather than printed; urfave/cli prints its own usage, so nothing is lost,
      but a log line would be better than a silent exit.

## F. Defects found by the live deployment sweep (all fixed)

Section E came from reading the code. This section came from running it: a
`docker compose` stack built from one commit (`1911b36`), driven end-to-end by
`deploy/smoke.sh`. Four defects and one root enabler. Every one of them was
invisible to the review pass, because each needs a *running* system to be
reachable — a node that dies between two broker calls, a duplicate row left in
a database by a previous run, a reply code nothing consumed.

The sweep also confirmed what section E claimed: `tx` now exits 0 on SIGTERM
and republishes after a restart, signup answers 409 for a duplicate email and
400 for an unknown field, and a publisher connecting into an absent node is
retried once every two seconds rather than ~34 times a second.

### F1. rx sink dispatch reported success for a node that never answered (HIGH) — fixed

- [x] `rx/internal/adapter/rabbitmq/nodes.go` `do()` ran the broker round trip
      inside a pool closure that declared its own `err :=`. The `return` inside
      the closure returned from the *closure*, so `do()` returned whatever
      `pools.TaskFunc`'s `Run` returned — `nil` on every path. A failed `Send`
      or `Recv` was logged and then discarded.
- [x] Consequence, reproduced live with `bustle` stopped and `tx` forced to
      reconnect into a node id that was still in the subscription: rx logged
      `sink start dispatched` → `publisher started` with **no error at all**,
      and the publisher only went away three seconds later when the SRT write
      failed (`publisher read error="The socket has been closed"`). The
      `sinkUnavailableDelay` path that section E7 added is reached only when
      `StartSink` returns an error, so for a *registered but dead* node the
      bounded retry never engaged; recovery happened by accident.
- [x] **Fixed** by giving the round trip its own method that returns:
      `do()` now runs `n.roundTrip(rpcCtx, c, payload, sibling)` on the pool
      and receives its outcome over a buffered channel
      (`result := make(chan error, 1)` … `return <-result`), so the caller's
      goroutine waits for the answer instead of for the enqueue. `roundTrip`
      returns every failure and additionally treats a non-zero reply code as
      one, so a node that refuses the command is no longer a success.
      Tests: `rx/internal/adapter/rabbitmq/nodes_test.go` —
      `TestStartSinkReportsASendFailure`, `TestStartSinkReportsARecvFailure`,
      `TestStartSinkReportsANodeThatRefusedTheCommand`.

### F2. The same closure leaked a pool worker per stale dispatch (HIGH) — fixed

- [x] `Recv` had no deadline of its own in that path: `router.Recv`
      (`internal/protocols/rabbitmq/router.go:209`) blocks on
      `ctx.Done()` unless a reply arrives, and the context it was given was the
      process context. A node that is a registered sibling but has died leaves
      every dispatch parked forever.
- [x] Confirmed with a goroutine dump (`docker kill -s QUIT surr-rx-1`): 98
      goroutine headers, one of them
      `rabbitmq.(*router).Recv(...) router.go:209` ←
      `adapter/rabbitmq.nodes.do.func1() nodes.go:120` ←
      `pools.goroutinesPool.Listen.func1() goroutines.go:45`, marked
      `[select, 3 minutes]` — parked since the dispatch. The pool has
      `--worker 28` workers, so a handful of stale dispatches retires the pool
      and every later sink command queues behind them.
- [x] **Fixed**: `var sinkCommandTimeout = 5 * time.Second` bounds the round
      trip (`context.WithTimeout` around it, `defer cancel()`), the same
      discipline the SRT handshake lookup already used. A `var` rather than a
      `const` so a test can shorten it. Test:
      `TestARecvThatNeverAnswersIsBoundedByTheDeadline` — with the timeout
      removed the test hangs and fails after 30s.
- [x] Measured after the fix, in the same live stack: the reconnect rate for a
      *stale-registered* node fell from 1058 cycles in 31s to **4 in 30s** —
      the 2s `sinkUnavailableDelay` plus up to 5s waiting on a node that will
      never answer. The rate therefore now depends on whether the node ever
      registered: never registered fails the lookup at once and cycles every
      2s, stale-registered pays the timeout first. F5 is what removes the
      difference.

### F3. Adding an already-subscribed node answered 500 (MEDIUM) — fixed

- [x] `subscribed_nodes.node_id` is `UNIQUE` — a node belongs to exactly one
      subscription — so the second add fails at the storage layer. That failure
      travelled back as a bare domain error and `POST /subscriptions/add`
      answered **HTTP 500 `unable to add subscription`**.
- [x] The reply-code vocabulary had no way to say "conflict": it stopped at
      `CodeNotFound = -4`. **Fixed** with `protocols.CodeConflict = -5`
      (`internal/protocols/handlers.go`), the
      `models.ErrNodeAlreadySubscribed` sentinel (`internal/models/node.go`),
      an `isUniqueViolation` translation in the subscriptions store
      (`subscriptions/internal/db/sqlite/errors.go` — a deliberate ten-line
      duplicate of the hub's, because the root module must not depend on the
      cgo driver), and a matching branch in the hub's adapter
      (`hub/internal/adapter/rabbitmq/subscription.go`). The endpoint now
      answers **409 `node is already in a subscription`**.
- [x] Tests: `subscriptions/internal/db/sqlite/subscription_test.go`,
      `subscriptions/internal/protocols/rabbitmq/handlers/subscription_test.go`,
      `hub/internal/adapter/rabbitmq/subscription_test.go`,
      `hub/internal/protocols/http/handlers/subscriptions_test.go`.
- [x] Note this was the *cause* of the smoke run's first five failures: a
      previous run's subscription still held the pinned node id, so the add
      failed, `/nodes/availability` answered 204, and every later check
      cascaded. The smoke script now generates a fresh node id per run.

### F4. An empty node list created an empty subscription (LOW) — fixed

- [x] `POST /subscriptions/add` with `{"nodes":[]}` answered **200** with a
      fresh `subscription_id`. A subscription with no nodes is not useful and
      the row is not removable through the API.
- [x] **Fixed**: the handler rejects `len(subsNodes.Nodes) == 0` with 400
      `at least one node is required` and any `node.ID == uuid.Nil` with 400
      `every node needs an id`. Test:
      `TestAddSubscriptionRejectsAnEmptyNodeList`,
      `TestAddSubscriptionRejectsANodeWithoutAnID`.

### F5. The sibling registry never forgets a dead node (MEDIUM) — fixed

- [x] This is the enabler for F1 and F2, and it is the reason they were
      reachable at all: `introduce` adds a sibling and nothing ever removes it,
      so a node that is killed without a graceful `nodes.remove` stays
      resolvable for the life of the hub process. `GetName` then finds exactly
      one candidate, which is why the dispatch proceeds instead of failing with
      `expected one sibling with <id>.sink.start command`.
- [x] F1 and F2 mean a stale entry is now *survivable* — the dispatch fails
      after 5s and the bounded retry takes over — but it is still wrong: every
      attempt costs a full 5s timeout before the node is treated as absent, so
      a stale-registered node cycles four times in 30s where an unregistered
      one cycles fifteen. A registry TTL or a liveness sweep would turn that
      into an immediate answer and make the two cases identical. AGENTS.md has
      described this as unimplemented since the node registration work landed.
- [x] Fixed by expiry, not by a sweep. `SiblingTTL` (`internal/protocols/siblings.go`)
      and `SIBLING_TTL_SECONDS` (`nodes/lib/core/siblings.py`) are both 15s, and
      both registries drop an entry that has not been re-introduced within that
      window. Fifteen because `introduce` is a one-second ticker in every
      service: fifteen consecutive misses is not a slow peer, it is a gone one.
      The check is **lazy** — applied in `Get` and `GetName`, deleting the entry
      as it goes — because that is where the question is asked, and because a
      sweep goroutine would have to be started and stopped in five processes to
      maintain a map with a handful of keys. An entry nobody looks up lingers,
      which is one dead key per dead peer.
- [x] The observable change is the point of the item, and it was measured on
      the live stack: the same smoke check that counted 4 reconnect cycles in
      30s before the expiry (against 15 for a never-registered node) counted
      **13** after it. The two cases have converged, which is what the item
      asked for. The 5s `sinkCommandTimeout` remains, and still matters — it is
      what bounds a dispatch to a node that is *registered and alive but not
      answering*.
- [x] Tests: `TestASiblingThatStoppedIntroducingExpires`,
      `TestAFreshSiblingIsStillResolvable`,
      `TestReIntroducingRefreshesTheDeadline`, `TestExpiryIsPerSibling` (Go,
      entries aged directly rather than by sleeping) and the Python mirror
      against an injected clock, plus `test_the_ttl_mirrors_the_go_side` — a Go
      service and a node disagreeing about when a peer is dead is a difference
      in behaviour that nothing on the wire reveals.

### F6. Dead nodes leave rows in the hub and the analytics registry — fixed

- [x] Found while verifying the analytics rewrite on the live stack, not by a
      test: after a handful of `deploy/smoke.sh` runs the hub's `nodes` table
      held eleven rows for one live node, and the analytics `declarations` table
      held two dead nodes' manifests — still carrying the retired
      `counts_detail`/`counts_summary`/`counts_totals` query names.
- [x] The growth is unbounded and is by design: every smoke run mints a fresh
      node id (because `subscribed_nodes.node_id` is UNIQUE and the API cannot
      move a node between subscriptions), so a persistent `hub-db` volume
      accumulates a row and four declarations per run. Nothing sweeps either
      table, and `nodes.add` carries no timestamp, so a row cannot even be
      dated.
- [x] **Correction to the original write-up.** It claimed
      `GET /nodes/availability` "reads the sibling registry, not the table" and
      that the 204 therefore proved the dead rows were invisible. That was
      wrong: `AvailableNodes` read `SelectAllNodes` — the table — and filtered
      only by subscription, so an unsubscribed dead row *was* offered. The 204
      in that run meant every row was subscribed (smoke.sh subscribes the node
      it mints), which is a different fact. A client that had never subscribed
      would have been offered a node that would never answer, and the failure
      would have surfaced much later as a sink that refused to start.
- [x] Fixed without a migration, by taking the answer from the introduce
      stream instead of from the table. Every peer re-broadcasts its command
      set once a second and a node's commands embed its id
      (`<node-id>.sink.start`), so the sibling registry already knew which
      nodes were up — it just was not asked:
      - `internal/protocols/siblings.go`: NEW `LiveNodeIDs(ids *[]uuid.UUID) int`
        (one entry per node, not per command; the slice is truncated first so a
        pooled buffer can be passed straight in) and `NodeAlive(id uuid.UUID) bool`.
        Both use the existing `SiblingTTL` through the same `fresh()` call
        `GetName` uses: two answers to "is this peer alive" would be a
        difference between listing a node and calling it.
      - `hub/internal/usecases/node.go`: `AvailableNodes` now filters on
        liveness as well as subscription, through a new `LiveNodes` interface.
        An empty registry is a legitimate answer (nothing has introduced itself
        yet) and means an empty list, not "every node is dead".
      - `hub/internal/adapter/rabbitmq/nodes.go`: `GetAnalytics` refuses a node
        that is not advertising itself, returning NEW
        `models.ErrNodeUnavailable`. The manifest outlives the node that declared
        it, so without this the endpoint answers every query with its empty
        shape and the report reads as "this node saw nothing" rather than "this
        node is gone". Checked after the ownership check, so an unsubscribed node
        still gets the same answer as before and no liveness leaks.
      - `hub/internal/protocols/http/handlers/nodes.go`: that error maps to
        **409**, not 500 — the request was well formed and the node is the
        caller's, so the server is not at fault and the client should retry when
        the node is back.
      - `internal/pools/uuid.go`: NEW `UUIDPool`/`UUIDPoolResetAndPut` for the
        per-request id set, following the existing pool pattern.
- [x] Not done: pruning the rows and declarations themselves. They are now
      unreachable through the API (a dead row is never offered and its manifest
      is never answered), so the remaining cost is disk. Deleting them needs
      either a `last_seen` column plus a sweep or a boot-time reconciliation
      against the registry, and both are a migration rather than a read-path
      fix; F5's lazy expiry was the cheaper half of the same idea.
- [x] Tests: `internal/protocols/siblings_test.go` (5 new — id parsed out of
      the commands, one entry per node, expiry through both accessors, the slice
      truncated, the id matched as a prefix and not as a substring),
      `hub/internal/usecases/node_test.go` (NEW, 2 — the three reasons a row is
      not offered, and an empty registry), `hub/internal/protocols/http/handlers/nodes_test.go`
      (NEW, 2 — 409 for a dead node, 500 still for anything else), plus the
      adapter test. All six mutations killed. Two of them initially survived for
      instructive reasons: `Contains` instead of `HasPrefix` is unobservable on a
      well-formed command (killed by a test that advertises an id after a dot,
      pinning the same parse `LiveNodeIDs` does), and the expiry test called
      `LiveNodeIDs` before `NodeAlive` on one registry — and expiry *deletes*,
      so the first call hid the evidence from the second.

## G. Release mechanics

Code quality was never the gap between this repository and one somebody can
run: the tree lints, tests and builds on every push. What was missing was
everything around the artifact. The first full deployment proved it — a stack
came up carrying **four different build ids** (`surr-hub version unknown`,
`surr-rx 0231136-dirty`, `surr-subscriptions unknown`, `surr-tx 1f9ae9d-dirty`)
because each image had been built from whatever tree happened to be checked out,
and nothing asserted the images came from one commit. Every check in
`deploy/smoke.sh` would have passed anyway.

- [x] One version for the repository, injected rather than hand-written:
      `VERSION ?= $(shell git describe --tags --always --dirty)` in the Makefile,
      passed to every image as a build arg and to every binary as
      `-X main.Version`. `--version` and the `version=` field on every log line
      report it, and the Python node resolves the same string independently
      (`nodes/lib/version.py`), so a mixed deployment's two build ids are
      comparable.
- [x] `ProtocolRevision` in the `introduce` payload, mirrored in
      `nodes/lib/core/siblings.py`, warned about rather than refused.
- [x] `deploy/smoke.sh` asserts against the *running* system what
      `docker compose ps` cannot show: the images are the build you think they
      are, the data plane works, `tx` exits 0 on SIGTERM, and an absent node does
      not make rx spin.
- [x] `.github/workflows/ci.yml`: `make lint`, `make test-go` and `make build` in
      `golang:1.27.1-trixie` (both halves of that tag are load-bearing — the
      distro because tx needs GStreamer 1.26 for `C.GstIdStr`, the patch version
      because golangci-lint v2.13.2 cannot read Go 1.27.2 export data), and the
      Python half on the runner where PyGObject builds from source.
- [x] `.github/workflows/release.yml`: a `v*` tag builds and publishes all seven
      images to GHCR, one runner each, stamped with the tag as their version.
      A `verify` job then pulls the published hub image and fails unless
      `--version` reports the tag — the assertion the first deployment was
      missing.
- [x] `v0.1.0` tagged. GHCR packages start private even for a public repository,
      so making them pullable is a one-time setting in the package's own
      settings page; `deploy/README.md` says so.
- [ ] **`go install github.com/booleanism/surr/hub/cmd/hub@latest` does not
      work, and this is a decision rather than an oversight.** The five service
      modules carry `replace github.com/booleanism/surr => ../`, which a
      published module cannot resolve. Dropping them was tried and reverted:
      with them gone, a local edit to `internal/models` became invisible to a
      service build that still exited 0, which is a worse failure than an
      unsupported install path. The supported path is the published image or a
      checkout, and both are documented; publishing the submodules would need
      `hub/vX.Y.Z`-style tags and a release process of its own, for an install
      route nothing in `deploy/` uses.
