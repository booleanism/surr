# TODO

Execution tracker for the designed-but-unbuilt efforts. Design context:
ARCHITECTURE.md "Planned evolution", DESIGN.md §11 (analytics service),
§12 (nodes refactor) and §13 (logging). Principles: whoever writes the data
also writes the query; nodes/core stays node-type-agnostic; the wire stays
Go/Python compatible at every stage.

## A. Nodes refactor (type-agnostic runtime) — DESIGN.md §12

Ordered stages; each lands independently and is wire-compatible.

- [ ] A0. Safety net (before touching anything)
  - [ ] nodes/pyproject.toml: pyright config (strict-ish, exclude .venv),
        proper dependency pins (requirements.txt is a venv dump)
  - [ ] Unit tests for nodes/core (pure python, fakes, no broker):
    - [ ] handlers: RouterResponse to_json/from_json round-trip incl.
          base64-bytes convention (guards the Go wire contract)
    - [ ] siblings: get_name returns all candidates / None
    - [ ] rpc: retry-until-success; try-every-candidate on stale entries
    - [ ] ticker: spacing holds under simulated delay
  - [ ] Golden-file test: one captured to_json() output asserted byte-for-byte
        so accidental wire changes fail CI
- [ ] A1. De-singleton BustleHandlers (lib/bustle/handlers.py)
  - [ ] Class attributes + setup() classmethod → __init__ instance deps
        (node_id, pipeline, model, chosen_classes, async_q, coll)
  - [ ] register(self, handlers_mgr) registers bound handlers
  - [ ] Command names and payloads stay byte-identical
- [ ] A2. Extract core/node.py (the real work)
  - [ ] Node class: node_id/name/family/intro_interval/rpc_timeout, router,
        siblings, handlers, pipeline (duck-typed start/stop),
        registration_payload (opaque bytes), on_start(ctx) hook
  - [ ] Node.run() owns: connect_robust, router.listen, introduce loop,
        nodes.add with retry-until-sibling, graceful shutdown choreography
        (stop pipeline → cancel tasks keeping listen alive → nodes.remove →
        cancel listen)
  - [ ] registration payload builder in core/ (hub's {id, bitrate, width,
        height} contract, node-type-generic)
  - [ ] bustle.py shrinks to: parse args → build streamer/model/queue/
        handlers instance → Node(...).run()
  - [ ] Verify: pyright + tests + manual boot (introduce, nodes.add,
        nodes.remove behave identically before/after)
- [ ] A3. Pipeline/worker hygiene
  - [ ] SRTStreamer: decode chain as parameter (default current
        "tsdemux ! h264parse ! avdec_h264 ! videoconvert"); BGR frames stay
        the contract
  - [ ] Fix frame_worker busy-loop (lib/bustle/processing.py:67-69): add
        get_frame(timeout) via queue.get(block=True, timeout=...)
  - [ ] Thread-safety: guard start()/stop() against the appsink callback
        thread (lock or event)
- [ ] A4. Lifecycle hardening (opt-in only)
  - [ ] Periodic re-registration (--reregister flag, default off; hub upsert
        is idempotent so this is safe)
- [ ] A5. Packaging/docs
  - [ ] pyproject entry point (bustle = bustle:main)
  - [ ] nodes/README: "add a node type" recipe — decode chain + processing
        loop + document shape + query manifest; everything else inherited

Out of scope for A (deliberate): sibling-registry TTL/sweep (cross-language,
Go + Python, its own branch); renames of lib/ layout; async rewrite of the
GStreamer/YOLO path.

## B. Analytics service (writer-owned queries) — DESIGN.md §11

New Go service analytics/ modeled on the subscriptions/ template: broker
router + SQLite registry + read-only Mongo executor. Nodes become write-only;
the service becomes the sole reader. Builds on A2 (the Node runtime hosts the
declare step).

- [ ] B1. analytics service skeleton
  - [ ] analytics/ module: cmdline (broker, family, name, mongo url, dsn),
        main wiring, introduce loop, restart loops — near copy of
        subscriptions/ skeleton
  - [ ] SQLite registry schema: declarations (node_id, collection, queries)
        with idempotent upsert on analytics.declare
- [ ] B2. Commands
  - [ ] analytics.declare: {node_id, collection, queries: [{name,
        params_schema?, pipeline}]} → upsert manifest
  - [ ] analytics.query: {node_id, query_name, params} → substitute params,
        append time-range $match, execute read-only, return JSON
  - [ ] analytics.queries.list: {node_id} → declared query names
- [ ] B3. Executor hardening (the one security surface)
  - [ ] Operator whitelist: allow $match/$group/$project/$sort/$limit/
        $unwind/...; deny $out/$merge/$unionWith
  - [ ] Stage count cap; result-size cap
  - [ ] Output-shape check: declared pipelines must return generic chart
        data (axes/categories, series names, values) — reject ECharts-style
        option structures (xAxis/legend/series.type) at declare time where
        feasible, or at minimum enforce it in review/convention
- [ ] B4. Node side (after A2)
  - [ ] Node runtime: post-registration hook sends analytics.declare with the
        entrypoint-supplied manifest, retry-until-sibling like nodes.add
  - [ ] bustle: convert lib/bustle/analytics.py's two pipelines into a query
        manifest (params: time_cluster window + granularity via placeholders)
        — dropping the ECharts-shaped final $project stages (xAxis/yAxis/
        legend/series.type/showSymbol) in favor of generic chart data:
        categories + named series of values; the presentation layer maps
        these onto whatever chart library it uses
- [ ] B5. Cutover (the one wire-changing step — coordinated commit, Go +
        Python together)
  - [ ] Hub: swap node-analytics adapter from <node-id>.sink.analytics to
        analytics.query (GET /nodes endpoint shape unchanged)
  - [ ] Nodes: delete sink.analytics handler and lib/<type>/analytics.py
  - [ ] Update ARCHITECTURE.md command catalog + data flow step 7, DESIGN.md
        §9 note, this file; move Mongo read credential out of nodes
- [ ] B6. Later (only if needed)
  - [ ] Fleet/wildcard declarations for cross-node rollups
  - [ ] Versioned query names (counts_by_minute@2) if rolling deploys mix
        versions

Sequencing: land A0–A2 first (Node runtime must exist to host the declare
step), then B1–B4 (purely additive, runs side by side with sink.analytics
indefinitely), then B5 in one window. Nothing before B5 breaks anything.
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
- [x] C8. (folds into A2) bustle.py print()s → stdlib logging, matching the
      level semantics of core/ modules (Python side keeps stdlib logging)

## D. Standalone fixes (unrelated to A/B, small)

- [ ] ECharts-shaped analytics output (nodes/lib/bustle/analytics.py): the
      aggregation $project stages build Apache ECharts option objects
      (xAxis/legend/series.type) instead of generic chart data. If you want
      the fix before the analytics service lands (design says fix it there,
      TODO B4), it can be done standalone by restructuring the final stages
      to return {categories, series: [{name, values}]} — consumers currently
      expecting the ECharts shape must adapt in the same change
- [x] tx --recording flag wrote parsed specs into DownstreamSpecs instead of
      RecordingSpecs (tx/cmd/tx/cmdline.go, recording flag Action) — fixed
      and committed (e3ca2db)
- [ ] Replace the `restart = restart || !restart` idiom — it always
      evaluates to true after the first pass, i.e. it is an obfuscated
      `restart = true` (rx/cmd/rx/main.go:185,209,233,257;
      hub/cmd/hub/main.go:160,181,236;
      subscriptions/cmd/subscriptions/main.go:135). Keep the behavior
      (log "restarting <loop> listen" on every re-entry after the first,
      since any re-entry follows an error), just make it readable:
      `restart = true` after the first iteration, or track a `first`
      flag and log when `!first`. No semantic change.
- [ ] POST /subscriptions/remove returns 501 — implement HTTP path or remove
      the route
- [ ] Go test suite (make lint is the only gate today)
- [ ] PostgreSQL support (README TODO)
- [ ] Analytics presentation layer (README TODO)
