# TODO

Execution tracker for the designed-but-unbuilt efforts. Design context:
ARCHITECTURE.md "Planned evolution", DESIGN.md §11 (analytics service),
§12 (nodes refactor) and §13 (logging). Principles: whoever writes the data
also writes the query; nodes/core stays node-type-agnostic; the wire stays
Go/Python compatible at every stage.

Status of the three workstreams: **C (logging) is done**; A and B are open.
Neither A nor B exists on `main`. Prior art for both lives outside it: the
abandoned `feat/analytics-service` branch (surviving only as dangling commits
and `refs/stash`) contains a first `analytics/` module, and
`refs/remotes/gitea/main` commit `66e308b "nodes: pythonic refactor"`
implements roughly section A (`nodes/core/{cli,config,detector,registry,service,storage,stream,worker}.py`,
`nodes/node.py`, `nodes/workers/bustle/*`, deleting `nodes/lib/bustle/*`).
Treat both as prior art to review, not as landed code.

**Section E is different**: it tracks defects found by reading the code during
a full review pass, then re-checked against the working tree. Its checkboxes
are **fix status** (`[x]` = fixed), not "verified" — every item, fixed or not,
was reproduced from the source. **E0–E6, E8 and E9 are now fixed**; E7, E10 and
E11 (all MEDIUM or below) are open. These fixes are **uncommitted** on top of
`dc83aad`; line numbers inside `[x]` items refer to the revision they were
found in, not to the fixed tree.

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
  - [ ] Class attributes + setup() classmethod → `__init__` instance deps
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
- [x] C8. bustle.py print()s → stdlib logging, matching the level semantics of
      core/ modules (Python side keeps stdlib logging). **Correction:** this did
      NOT fold into A2 — it shipped as a standalone module,
      `nodes/core/logging_setup.py` (imported at `bustle.py:16`, called at
      `bustle.py:208`), while A2 is still open. `core/*.py` still use
      module-level loggers rather than the configured root logger.

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
- [ ] Go test suite. **Partly done**: `make test` runs `go test ./...` in every
      module and covers the RPC reply layer
      (`internal/protocols/handlers_test.go`, the `hub` and `subscriptions`
      `handlers` packages) plus `internal/backoff`; the two `handlers` tests
      enumerate the commands their service advertises and fail when one has no
      case. Everything else in the tree — the SRT data plane, the SQLite
      layer, the tx pipelines, the whole `nodes/` runtime — has no test at all.
- [ ] PostgreSQL support (README TODO)
- [ ] Analytics presentation layer (README TODO)

## E. Verified defects (full code review pass)

Every item below was reproduced by reading the code (two of them empirically:
E0 with a scratch SQLite/Go test, E1 with a `json.Marshal` probe). These are
described in DESIGN.md "Known limitations" §A/§B and summarised in
CLAUDE.md "Known landmines". E0 and E1 block the whole media path and the
whole error-reporting path respectively.

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

**Status of the E0–E5 fixes**: E0–E5 are fixed. The only release blocker left
is that none of this is committed. The E1 regression tests
(`internal/protocols/handlers_test.go`, the two handler-package tests,
`internal/backoff/backoff_test.go`) are the only coverage — E2–E5 have none.
The two handler-package tests enumerate the commands each service advertises
and fail when one is untested, so the reply layer cannot regress silently; the
rest of the codebase still has no test.

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
      (`nodes/core/rpc.py`, `retries=None`), with no diagnostic on either side.
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
      in `nodes/core/rpc.py`. The same rewrite landed in
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
      adapters used to log.
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

### E7. rx data plane (MEDIUM)

- [ ] `rx/internal/protocols/srt/broadcast.go:54`: subscriber cap counts
      subscription maps and compares with `==` — one subscription admits
      unlimited nodes; the (max+1)-th subscription is rejected.
- [ ] `broadcast.go:60-64`: a reconnecting node's channel is overwritten in
      the map but never closed → the previous `handleCaller` blocks forever on
      `range ch`.
- [ ] `broadcast.go:50-66` + `egress.go:169`: `Subscribe` after `Close(subID)`
      recreates an orphan topic that is never closed or fed.
- [ ] `Unsubscribe` closes the channel while holding the registry mutex.
- [ ] `ingress.go:66-103` / `egress.go:65-105`: blocking broker RPC inside the
      SRT handshake callback stalls handshakes when the pool is saturated;
      pending maps are keyed by peer address with no TTL and leak.
- [ ] `egress.go:184`: blocking `sock.Write` with no deadline → a stalled
      subscriber wedges the goroutine, pins pooled buffers, and its deferred
      `Unsubscribe` never runs.
- [ ] `ingress.go:150-186`: ctx cancellation does not stop a blocked
      `sock.Read` → `Close(subID)` and `StopSink` are skipped, so nodes keep
      streaming after rx shutdown.
- [ ] `ingress.go:66-103` never sets an SRT rejection reason (egress does).
- [ ] `rx/internal/adapter/rabbitmq/nodes.go`: `StartSink`/`StopSink` always
      return nil (per-node failures only logged) and require exactly one
      sibling.

### E8. Python node runtime (HIGH) — fixed

- [x] `nodes/lib/bustle/handlers.py:112`: missing `await rep(...)` on the
      invalid-source-address path → the reply is never scheduled and rx's
      caller blocks forever. Type `ReplyFunc` as returning a `Coroutine` so
      Pyright catches this. **Fixed**: the call is awaited and `ReplyFunc` is
      now `Callable[[RouterResponse], Coroutine[Any, Any, None]]`
      (`nodes/core/handlers.py`), which makes a dropped `rep(...)` a Pyright
      error rather than silent.
- [x] `nodes/core/router.py:170,188`: `ack()` before dispatching the handler as
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
- [x] `nodes/core/pipeline.py:17,68,77-81`: no synchronization between
      `start()`/`stop()` and `_on_new_sample`; the callback never checks
      `running`. **Fixed**: a `threading.Lock` guards `pipeline`/`running`; the
      callback checks `running` under it; `start()` publishes the new pipeline
      only once fully wired, and `stop()` swaps the reference under the lock
      before calling `set_state(NULL)` outside it.
- [x] `nodes/core/pipeline.py:41-45`: `queue.Full` → `get_nowait()` can raise
      `queue.Empty` on the GStreamer thread. **Fixed**: both the drop-stale
      `get_nowait()` and the `put_nowait()` are individually wrapped, and the
      dance happens outside the lock.
- [x] `nodes/lib/bustle/processing.py:57`: no error handling around
      `insert_one` → a transient pymongo error exits the whole node process;
      `frame_worker` likewise has none (dies silently while `running` stays
      true). **Fixed**: both are wrapped and logged.
- [x] `nodes/core/router.py:50,132-139`: a late reply after a `recv` timeout is
      mis-dispatched as an inbound request (the trackID is popped in `finally`).
      **Fixed**: a cancelled `recv` records the trackID in `_abandoned`
      (TTL 60s, capped at 1024); `_handle_delivery` acks and drops a late reply
      for an abandoned trackID instead of handing it to a command handler.
- [x] `nodes/core/ticker.py`: `interval <= 0` degenerates to a `call_later(0)`
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

### E10. Python typing / packaging (MEDIUM)

- [ ] Pyright is not installed in `nodes/.venv` and there is no
      `pyrightconfig.json`; default mode reports 7 errors + 1 warning (all
      PyGObject stub attribute issues in `nodes/core/pipeline.py`), strict mode
      111. Either add the config + dependency (TODO A0) or drop the claim.
- [ ] `nodes/requirements.txt` is a full venv dump and already diverges from
      the installed venv (pins `aio-pika==10.0.1`/`pymongo==4.18.1`; venv has
      10.1.0/4.18.2). Replace with a real dependency list + `pyproject.toml`.
- [ ] Lint hygiene: unused `import threading` (`nodes/core/pipeline.py:1`),
      missing type args (`Queue`, `Task`, `Collection`), untyped `obj` params
      (`nodes/core/handlers.py:9,12,30`), unused `Pipeline` Protocol
      (`nodes/lib/bustle/handlers.py`), `cls._coll`/`cls._node_id` never
      declared as class attributes.

### E11. Minor (LOW)

- [x] `internal/db/db.go:61-74`: `Commit`/`Rollback` read `t.DB` without `t.mu`.
- [ ] `internal/protocols/handlers.go:30-46`: `GetBytes` base64-decodes any
      valid JSON string with no shape validation, and returns `nil, nil` for
      `"null"` — indistinguishable from an empty result.
- [ ] `internal/protocols/rabbitmq/router.go:195-197`: empty `ReplyTo` is
      rejected silently.
- [x] `hub/internal/protocols/http/handlers/nodes.go:122`: `http.Error(w,
      "zero nodes", http.StatusNoContent)` — 204 must have no body.
- [ ] `internal/models/user.go:7`: `Passwd` is JSON-serializable (safe only
      because call sites marshal just the ID).
- [ ] No email/password validation; duplicate email surfaces as 500, not 409.
- [ ] `rx/cmd/rx/cmdline.go`: `srtHost` splits on `:` → no IPv6 listen addrs.
- [ ] `tx/cmd/tx/main.go:158` ignored the `url.Parse` error — **fixed**
      (`if err != nil { return err }`). Still open: `tx/cmd/tx/main.go:120`
      (`dash := dash.NewDashServer(...)`) still shadows the `dash` package
      imported at `:17`, and `tx/internal/server/dash/client.go:52`
      (`url := c.u.JoinPath(...)`) still shadows `net/url` imported at `:6`.
      Both compile because the shadowed name is only used on the right-hand
      side before it is rebound.
- [x] `rx/cmd/rx/main.go:41`: the last raw `fmt.Println` in the tree is gone —
      no `fmt.Print*` call survives anywhere (`git grep 'fmt\.Print'` is
      empty). The CLI parse error is now swallowed (`if err != nil { return }`)
      rather than printed; urfave/cli prints its own usage, so nothing is lost,
      but a log line would be better than a silent exit.
