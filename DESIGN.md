# Design

Rationale, patterns and trade-offs behind the architecture described in
[ARCHITECTURE.md](ARCHITECTURE.md).

> Sections 1–10 describe decisions that are **implemented on `main`**. Sections
> 11–12 are **designed but not built on `main`**. Section 13 is **implemented**.
> The "Known limitations" section at the end records the defects found in the
> 2026-10 review: E0–E5 are fixed in the working tree but uncommitted, E6–E11 are
> open. TODO.md section E tracks each one with its status.

## Goals

1. **Event-driven microservices** — no service calls another by address; all
   control-plane coordination happens through the RabbitMQ topic exchange.
2. **Polyglot runtime** — Go for the streaming/control services, Python where
   the ML ecosystem lives; both speak one wire protocol.
3. **Efficient media path** — SRT end to end for transport (low latency,
   retransmission), zero-copy fan-out inside rx, pooled buffers everywhere.
4. **Operable as loose processes** — every service is a single binary/script
   with CLI flags; failure of one peer degrades, not crashes, the others.

## Core design decisions

### 1. Sibling discovery instead of a service registry

Instead of Consul/etcd/DNS, every service *broadcasts its capabilities* on
`rpc.broadcast` every second and peers cache
`routing-key → {commands}` in memory. Consequences:

- No extra infrastructure; peer discovery converges within one intro interval
  (~1s default) of a peer starting.
- Calls are resolved by *command*, not by service name: `GetName(command)`
  returns every sibling advertising that command. This is what lets node
  commands be dynamically named (`<node-id>.sink.start`) — the registry keys on
  capability, not identity.
- Cost: the registry is eventually consistent and has **no negative TTL**. A
  crashed peer's entry lingers until overwritten; callers must tolerate stale
  entries (the Python `rpc.call` tries every candidate sibling; the Go
  adapters return an error when no sibling answers). The Go side's
  single-candidate lookups (e.g. hub's analytics adapter requires exactly one
  sibling) are therefore fragile under stale entries.
- The 1s re-introduce also acts as a liveness heartbeat *by side effect*, but
  nothing prunes siblings that stop introducing — stale-sibling sweeping is
  deliberately not implemented yet.

### 2. Correlated request/reply over a topic exchange

RabbitMQ direct-reply semantics are not used; instead each service owns an
exclusive queue bound to `rpc.direct.<name>` and replies are published to the
requester's `ReplyTo` routing key. Match-by-UUID (`CorrelationId` = trackID)
against a pending map makes the exchange effectively a per-service inbox that
multiplexes both inbound requests and replies to outbound requests on one
queue. The same delivery-handler logic discriminates: pending trackID → reply;
unknown trackID → inbound request.

Trade-offs: single queue per service means a slow handler can delay replies
(Go dispatches each delivery through the goroutines pool; Python acks before
running the handler as an `asyncio` task to keep the consumer moving, which
makes Python at-most-once and Go at-least-once on crash). Messages are manually
acked; malformed ones are nacked without requeue so poison messages cannot loop
(Go nacks with requeue only on shutdown).

`RouterResponse.Result` is `json.RawMessage` with a `GetBytes()` decoder that
transparently handles base64-encoded string payloads — this exists because the
Python side base64-encodes binary results inside a JSON string. Any change to
the response shape must be mirrored in both `internal/protocols/handlers.go`
and `nodes/core/handlers.py`. `GetBytes()` has no call sites (the convention
survives because `json.Unmarshal` into `*[]byte` happens to base64-decode
Python's string), and it is the wrong reader for failures: a short ASCII message
such as `internal` is itself valid base64, so it decodes to garbage. Use
`ErrorMessage()` instead — that was the trap behind §A1.

The pending map is the delicate part. `Send` validates the sibling first and
then registers a **buffered** (cap 1) reply channel, so a reply that arrives
after the caller gave up is still handed over and Nacked rather than blocking
the delivery path; `Recv` releases the entry with `defer r.Forget(trackID)`, so
success, timeout and cancellation all clean up. `handleDelivery` discriminates
on that map alone: a known trackID is a reply, anything else is an inbound
request.

### 3. Subscription-scoped streams; node identity in streamids

The system's central notion: a **user creates a subscription binding N nodes**;
`tx` publishes *to a subscription* (`streamid=publish:<sub-id>`); rx expands
that to *the set of subscribed nodes* at connect time. Nodes pull *as
themselves* (`streamid=subscribe:<node-id>`), and rx re-validates that mapping
on every egress connection (exactly one subscription must own the node, else
the connection is rejected with a specific SRT rejection reason).

Design implications:

- Adding/removing a node from a subscription changes fan-out on the next
  publish session, without reconfiguring tx.
- The SRT streamid doubles as an authorization token (validated against the
  subscriptions service at handshake time). It is *not* cryptographic —
  anyone who can reach the socket and knows a valid UUID can connect.
- Authorization of egress happens in the listen callback *before* accept; the
  pending-map-by-peer-address pattern bridges the callback (which cannot
  return data) and the accepted socket. The same callback performs a blocking
  broker RPC, so a slow/absent `subscriptions` sibling stalls SRT handshakes.
- This path was non-functional until the UUID storage-class fix (§A0): the
  membership lookups matched zero rows, so every handshake was rejected.

### 4. Zero-copy fan-out with back-pressure-by-drop

`rx`'s broadcaster publishes each received chunk to every subscriber of the
subscription without copying: a `Chunk` wraps the pooled buffer with an atomic
refcount; the last release returns it to the bytes pool. Subscriber channels
are buffered (256) and sends are non-blocking — a slow node silently drops
chunks (release decrements the refcount so memory is still reclaimed). This
chooses *freshness over completeness*, which is the right call for live
surveillance inference: YOLO counting tolerates dropped frames; a stalled
publisher does not.

Related memory discipline (shared root `internal/pools/`): all hot-path
allocations (byte buffers, node/subscription slices, sibling-name slices) come
from `sync.Pool`s with reset-and-put helpers, and background work runs through
a bounded goroutine "task pool" rather than unbounded `go` statements —
including delivery dispatch, which used to be a bare `go` per message.

Known cracks in this design: the subscriber cap counts subscription *maps*, not
subscribers, and uses `==` (so one subscription admits unlimited nodes); a
reconnecting node's channel is overwritten in the map without being closed (its
reader blocks forever); and `Unsubscribe` closes a channel while holding the
registry lock.

### 5. tx as a standalone GStreamer binary

tx deliberately does *not* join the broker. It is configured entirely by CLI
flags (`--src`, `--sink`, `--name`, `--downstream`, `--recording`) — the
subscription ID simply travels inside the SRT URI. This keeps the media path
independent of RabbitMQ availability and makes tx deployable next to any
camera with zero infra.

Three-pipeline split (producer / downstream / recording) via `intersrc` /
`intersink` elements lets each output have its own encoder settings and
lifecycle: the producer decodes once, downstream re-encodes for SRT at
`--downstream` specs, recording encodes at `--recording` specs (video-only
until audio appears, lazily linked by a pad probe). Hardware codec variants
are selected with Go build tags (`*_va.go` vs `*_default.go`) — though the
`va` variant does not currently compile.

Two caveats the CLI does not express: recording is built **unconditionally**
(`--recording 0,0,0` only nulls the encoder specs), and `tx run` requires a
`tx serve` DASH server to already be reachable because it registers the stream
over HTTP at startup. Default codecs are software (`openh264dec`/`openh264enc`,
`fdkaacdec`/`fdkaacenc`).

### 6. Push-to-node control (`sink.start/stop`), pull-from-node media

When a publisher arrives, rx *pushes* a start command to each subscribed node
(`(node-id).sink.start` with the egress address); the node then *pulls* media
by connecting to the egress socket. Control and data planes cross only at
these handoff moments. On publisher disconnect, rx pushes `sink.stop` and
closes all subscriber channels, so nodes tear down their pipelines.

Node commands embed the node ID rather than using a topic hierarchy because
the node's broker routing key (`rpc.direct.bustle0`) is per-*instance* while
its data-plane identity (the UUID) is stable across restarts and is what
subscriptions reference.

As built, `sink.start` is fire-and-forget with **no reply deadline**: rx's
adapter never reads the reply code, and the node's invalid-source-address
reply is not even awaited (`nodes/lib/bustle/handlers.py:112`), so a bad
address stalls the media goroutine indefinitely.

### 7. Node self-registration with retry-until-hub

A node may boot before the hub; `nodes.add` retries forever (1s interval)
until a sibling serving it appears. The hub's insert is an idempotent upsert
(`ON CONFLICT(id) DO UPDATE`), making re-registration after either side
restarts safe. Registration is **once per run**: if the hub's database is
reset while a node keeps running, the node must be restarted (re-sending
periodically is a known future option, TODO.md A4). Graceful shutdown reverses
the sequence — stop streamer, cancel tasks, `nodes.remove` (with the router
still listening so the reply can arrive), then close the connection.

The retry loop still has no backoff and no deadline, but hub-side rejections are
now diagnosable: error replies are published again (§A1), so the node logs the
reason instead of retrying blind at 1 Hz.

### 8. Analytics shape: store raw, aggregate on read

The node stores one median-count document per drain interval (2s default) in a
MongoDB timeseries collection; chart-ready series are computed by aggregation
pipelines at query time (`<node-id>.sink.analytics`, proxied by the hub's
`GET /nodes/analytics` endpoint). Storing medians per interval (rather than
per-frame counts) bounds write volume; the drain interval and the frame
worker's drop-oldest frame queue (`maxsize=1`) both bias toward recent state.

**Chart-library-agnostic output is the contract — but not yet the reality.**
The intended data contract is generic chart data: axes/categories, series
names, values (e.g. min/avg/max per class, time-bucketed averages), with the
rendering library chosen entirely by the presentation layer. The current
pipelines violate this: their final `$project` stages build Apache ECharts
option objects (`xAxis {type: "category"}`, `yAxis`, `legend`, `series` with
`type: "bar"|"line"`, `showSymbol`) — see `nodes/lib/bustle/analytics.py`.
This couples the aggregation layer to one rendering library and is a known
flaw. It is corrected as part of the analytics service (§11): declared query
manifests must describe generic chart data only, and any
library-specific mapping happens client-side.

Two data-shape caveats in the same path: classes with zero detections in an
interval are omitted rather than zero-filled (so the unwound series has gaps),
and `timestamp` is a naive local `datetime.now()` while the timeseries
granularity is seconds — `$dateTrunc` buckets shift when the host TZ is not
UTC.

**Planned replacement (see §11):** the query side of this decision moves to a
dedicated analytics service. The store-raw/aggregate-on-read principle stays;
what changes is *who owns the query* — the writer declares it, a dedicated
reader executes it.

### 9. Hub auth: JWT access + opaque server-side refresh

- Passwords hashed with Argon2id (memory 64 MiB, t=3, p=2).
- Access token: HS256 JWT, 24h TTL, `user_id` claim; middleware guards
  protected routes.
- Refresh token: 32 random bytes, hex, stored in an **in-memory** token store
  (7-day TTL), rotated on every refresh, delivered as an HttpOnly/Secure/Lax
  cookie scoped to `/auth`.

The in-memory store means refresh tokens do not survive a hub restart and
cannot be validated by multiple hub instances — acceptable for the current
single-instance deployment, but the natural extension point (SQLite/Redis
backed store) if that changes. Expired entries are never swept, so the map
grows for the process lifetime.

Note the CLI ships a default JWT secret (`hub/cmd/hub/cmdline.go:56`); real
deployments must override `--secret`/`JWT_SECRET`. Token validation accepts
any HMAC algorithm (HS384/HS512 as well as HS256) and does not verify the
issuer. Signin has no rate limiting and logs the submitted email on failure.
Subscription removal has no ownership check, so any authenticated user can
remove any subscription.

### 10. Layered service template (hub/subscriptions)

`cmd/<svc>` (CLI + wiring) → `internal/protocols/{http,rabbitmq}` (transport
handlers) → `internal/usecases` (business logic) → `internal/db/sqlite`
(storage), with `internal/adapter/rabbitmq` for the outbound broker calls.
Every layer consumes its dependencies as small consumer-defined interfaces
(e.g. a usecase needs `SelectSubscribedNodes`, not "the subscriptions
service"), which keeps the transport-agnostic core testable and lets the same
usecase be driven by either HTTP or broker handlers.

Cross-cutting conventions:

- **Cleanup registry**: each service holds a `*logging.Cleanups` and calls
  `Register(func() error {...})` where the resource is created; `Run()` drains
  them on context cancellation (SIGINT/SIGTERM) in registration order and
  collects errors. This replaced the older `beforeExit []func() error` slice,
  which raced with shutdown. `Register` is safe at any time, including while the
  drain is running.
- **Restart loops**: every long-running listener (router, pool, ingress,
  egress, HTTP) runs inside `restart.Loop` (`internal/restart/`), which logs and
  re-enters on error with exponential, jittered backoff (`internal/backoff/`)
  instead of crashing the process — and instead of hot-spinning, which is what
  the earlier always-true `restart = restart || !restart` loop did. A broker
  connection that drops is handled separately by `rabbitmq.Supervisor.Watch`:
  it waits on `NotifyClose`, redials, re-declares the queue and installs the new
  channel with `router.ReplaceChannel`. That is why the router never holds a
  channel directly — `Send`, `Introduce`, `Listen` and the reply publishes all
  read it through `channel()`/`queue()`.
- **Error codes on the wire**: handlers reply `Code: 0` on success and negative
  codes (`CodeDecodePayload` -1, `CodeDomain` -2, `CodeMarshalReply` -3) with
  the error text in `Result`, always built by `protocols.ErrorResponse(code,
  err)`/`ResultResponse(payload)`. `Result` is `json.RawMessage`, so the
  constructor JSON-quotes the message — the original `[]byte(err.Error())`
  failed to marshal and silently dropped the reply (§A1).
- **Object pools**: hot-path slices/buffers come from `internal/pools` and are
  reset-and-put back; functions that fill caller-owned slices return counts
  (`(int, error)`) instead of allocating. Bounded pools still truncate at
  capacity, but `scanRows` now reports `ErrBufferFull` naming the row count
  rather than truncating silently, and `NewGoroutinesPool(0)` falls back to
  `DefaultNGoroutinesPool`.
- **UUIDs as BLOBs** in SQLite (16 bytes), parsed to `uuid.UUID` in repos —
  the documented intent, and now honoured on both sides. The write path used to
  bind `uuid.UUID` values (→ TEXT) while the read path bound `ID[:]` (→ BLOB),
  so membership lookups matched nothing (§A0).

### 11. Analytics service: writer-owned queries (planned, not on `main`)

Not yet implemented on `main` — designed, agreed, tracked in TODO.md. A first
attempt exists as dangling commits / a stash from the abandoned
`feat/analytics-service` branch. This section records the decision so the
implementation follows it.

**Problem.** Today each node type owns both the write path (document shape)
and the read path (`<node-id>.sink.analytics` with type-specific aggregation
code). Every node type therefore needs its own broker command handler, its own
query logic, and read access to MongoDB — and a future presentation layer
would need to know which node answers which query. That couples the reader to
the writer's internals in exactly the way the rest of the system avoids.

**Principle.** *Whoever writes the data also writes the query.* Document
shape, write path, and query definitions are one contract that must
co-evolve — so they ship together, as data, from the node. A new Go service
(`analytics/`, modeled on the subscriptions/ template) becomes the sole
MongoDB reader; nodes become write-only. The service's only node-type
knowledge is that a registry exists and entries have names. This is CQRS with
producer-owned read models: the read model travels as a declaration instead
of living where the data lives.

**Mechanism.** After `nodes.add` succeeds (same retry-until-sibling
semantics), the node sends `analytics.declare`:

```
{node_id, collection, queries: [{name, params_schema?, pipeline}]}
```

`pipeline` is Mongo aggregation JSON with parameter placeholders substituted
at query time by the service. Declared pipelines must return **generic chart
data** (axes/categories, series names, values) — never rendering-library
structures like ECharts option objects; library mapping is the presentation
layer's job. The service upserts the manifest into its own
SQLite registry and serves `analytics.query {node_id, query_name, params}` —
substitute, append time-range `$match`, execute read-only, return JSON — plus
`analytics.queries.list {node_id}` for discovery. The hub's analytics
endpoint swaps its adapter from `<node-id>.sink.analytics` to
`analytics.query`; nodes delete `sink.analytics` and `lib/<type>/analytics.py`.

**Why a query registry rather than MongoDB views.** Views were considered and
rejected as the mechanism: view pipelines are static (no parameters — the
`time_cluster` granularity/window dimensions would need one-view-per-granularity
or `$$NOW` tricks), the declaration would still be a Mongo-ism closing the
PostgreSQL TODO door, and deletion/versioning would be implicit. A
pipeline-as-data declaration parameterizes naturally, versions with the writer
by construction (re-declared every boot), and keeps the service an executor
rather than a Mongo-feature consumer. If a engine port ever happens, the
escape hatch is an intermediate query representation — deliberately not built
now.

**What it buys.**

- AI-type-agnostic analytics forever: a new node type declares queries; the
  service and hub are untouched.
- The node-type contract shrinks to decode chain + processing loop +
  document shape + query manifest.
- Credential-level write/read split: nodes hold write-only Mongo credentials,
  the analytics service holds read-only ones. Enforced by auth, not convention.

**Accepted costs.**

- The service executes semi-trusted pipelines: it must enforce an operator
  whitelist (deny `$out`/`$merge`/`$unionWith`), a stage cap, and a
  result-size cap in one executor function. This is the design's one real
  security surface.
- Query names become part of the wire contract between node author and
  presentation layer — hence `analytics.queries.list` is part of the design,
  not decoration.
- Cross-node (fleet) aggregation is a fan-out over per-node collections by
  the service, not a join; acceptable now, extensible later via wildcard
  declarations.
- Rolling deploys can briefly mix query versions across instances of the same
  node type; mitigated by versioned query names if it ever bites.

### 12. Nodes runtime: type-agnostic core (planned, not on `main`)

Not yet implemented on `main` — see TODO.md for staged execution. (A version
of this refactor exists on `refs/remotes/gitea/main` as commit `66e308b
"nodes: pythonic refactor"`, which restructures `nodes/` into
`core/{cli,config,detector,registry,service,storage,stream,worker}.py`,
`node.py`, and `workers/bustle/*`. It is not on `main` and should be treated
as prior art, not as landed code.)

The `nodes/` refactor extracts the type-agnostic lifecycle (boot, introduce
loop, registration, shutdown choreography — ~90% of `bustle.py`'s finally-block)
into `core/node.py` so a new node type is an entrypoint + `lib/<type>/`
package, never a copy of `bustle.py`. Two rules keep it honest: `core/`
contains no AI/model/database specifics (they enter only via hooks and the
pipeline object), and the refactor is wire-compatible at every stage. The
`analytics.declare` call from §11 becomes a post-registration hook on this
runtime. See ARCHITECTURE.md "Planned evolution" for the shape.

### 13. Logging: zerolog, context-first (implemented)

Implemented in `internal/logging/` and wired into all four Go services;
TODO.md section C is complete. Python nodes use stdlib `logging` via
`nodes/core/logging_setup.py`.

**Choice.** zerolog on the Go side: zero-allocation structured logging, one
small dependency, native context support (`zerolog.Ctx`), and it works
identically in the shared modules and in standalone tx. Python nodes keep
stdlib `logging`; level *semantics* match across languages, output formats do
not need to.

**Output & configuration.** `--log-format=console|json` (default console —
this is a dev-first, single-operator repo; flip to JSON when a collector
appears) and `--log-level` (default info) flags on every service, wired like
all other flags in `cmd/<svc>/cmdline.go`. `logging.Setup` returns
`(ctxWithRootLogger, logger)`; unknown levels fall back to info.

**Propagation.** Each `main` builds a root logger annotated with
`service=<sibling name>` and `family=<exchange>`, injects it into the root
context; shared packages and handlers pull it with `zerolog.Ctx(ctx)` (which
degrades to a no-op logger when absent, so packages stay usable in tests).
Components without a context (the rx broadcaster's hot paths) hold a logger
field from construction. The hub's HTTP layer has a status-capturing
middleware (`middlewares.NewRequestLogger`) that injects the request logger
and logs `method`/`path`/`status`/`duration_ms`/`remote`.

**Level policy** — the judgment-call part. Logs must carry the related
values, never bare messages:

| Level | Events (with required fields) |
|---|---|
| debug | 1s introduce broadcasts, sibling-registry updates (sibling, command count), per-RPC send/recv round trips (command, track_id, reply_to, code), HTTP 2xx requests, payload *lengths* |
| info | service start/stop with config summary (broker, family, sibling, listen addrs, pool sizes), restart-loop re-entry, SRT accept (peer, streamid, sub_id/node_id), publisher/subscriber start/stop, `sink.start/stop` dispatched (node_id), auth signin/refresh success (user_id), cleanup results |
| warn | SRT reject (peer, streamid, reason), slow-consumer chunk drops (sampled; sub_id, node_id, dropped), non-zero RPC reply codes (command, code), auth failures (email), malformed-message nacks (reason, track_id), HTTP 4xx |
| error | operation failures, HTTP 5xx, handler/DB errors |

**Canonical fields** — same key everywhere, snake_case: `command`,
`track_id`, `reply_to`, `sibling`, `code`, `sub_id`, `node_id`, `peer`,
`streamid`, `reason`, `dropped`, `method`, `path`, `status`, `duration_ms`,
`remote`, `err` (always via zerolog `.Err()`, never an interpolated string).

**Never log** (security invariant): passwords, refresh tokens, the JWT
secret, credential-bearing URIs (RTSP source URLs can embed user:pass — log
`scheme://host` only, via `logging.RedactURL`), or full RPC/request payloads
(at most their length, at debug). SRT streamids are the deliberate exception:
they are UUID identifiers, not secrets, and belong in accept/reject logs.

**Hot-path rule.** The media path logs no per-chunk events. Slow-subscriber
drops are the only signal worth emitting, and they are sampled (first drop +
counters, or every Nth) so a stalled consumer cannot itself become a log
flood.

**Residual:** one `fmt.Println(err.Error())` remains in `rx/cmd/rx/main.go:41`
for a CLI parse error. Note that `logging.Setup`'s package doc still says it
covers "hub/rx/subscriptions" only, although tx imports it too.

## Known limitations / future work

### A. Correctness defects (found in the 2026-10 review)

The review pass reproduced every item below from the source. **E0–E5 are now
fixed in the working tree (uncommitted, on top of `dc83aad`); E6–E11 are still
open.** TODO.md section E tracks each one with its status.

#### Fixed (E0–E5)

0. **Subscriptions UUID storage-class mismatch — the data plane was dead.**
   `subscriptions/internal/db/sqlite/subscription.go` inserted `uuid.UUID`
   values (google/uuid `Value()` → string → TEXT) while
   `SelectSubscribedNodes`, `SelectSubscriptionByNode` and
   `SelectNodeIDsByUser` bound `ID[:]` → BLOB. SQLite compares storage classes,
   so all three lookups matched **zero rows** (empirically reproduced): rx
   ingress rejected every publisher with "no subscribed nodes", rx egress
   rejected every node with "no unique subscription", and the hub's analytics
   ownership check failed. The same class existed at
   `hub/internal/db/sqlite/node.go`. Fixed by binding `ID[:]` on both sides —
   BLOBs everywhere, matching the migration's `id BLOB PRIMARY KEY`.
1. **RPC error replies were never published.** `RouterResponse.Result` is
   `json.RawMessage` but the handlers assigned `[]byte(err.Error())`, so
   `json.Marshal` failed (`invalid character 's' looking for beginning of
   value`) and the error was discarded (`_ = rep(...)`). Callers timed out
   instead of receiving -1/-2/-3. Fixed with two constructors —
   `protocols.ErrorResponse(code, err)` (which JSON-quotes the message) and
   `protocols.ResultResponse(payload)` — plus named constants
   (`CodeSuccess`, `CodeDecodePayload`, `CodeDomain`, `CodeMarshalReply`) that
   are now documented as part of the wire protocol, and `ErrorMessage()` as
   the read side. Reading a failure with `GetBytes()` was itself a trap: a
   short ASCII message such as `internal` is valid base64, so it decoded to
   garbage.
2. **`call()` double-sent on a cap-1 error channel** — `res <- err` then
   unconditionally `res <- nil`; the second send blocked forever, `done` never
   closed, `call()` blocked until ctx cancellation and leaked a pool worker.
   The same closures also raced on the captured `err`/`resp`. Fixed by
   dropping the `res`/`done` pair: `call()` runs `Recv` in a pool task that
   reports into its own `recvErr chan error` and selects on it against
   `ctx.Done()`, and the sibling loops now try every candidate, accumulate
   failures and return `errors.Join(...)` instead of aborting on the first.
3. **Shared `Transaction` value bled transactions.** `BeginTx` mutated `t.DB`
   on a value created once per process, so overlapping
   `InsertSubscriptions` calls executed on each other's transaction. Fixed by
   serialising `BeginTx` through a single-slot gate that returns an idempotent
   release function and `ErrTxInProgress`.
4. **`hub/internal/protocols/rabbitmq/handlers/node.go` missing `return`** after
   the -3 reply → fell through and replied again with `Code: 0`, two replies
   per request. Fixed.

#### Still open (E6–E11)

5. **`nodes/lib/bustle/handlers.py:112` missing `await rep(...)`** on the
   invalid-source-address path → the reply is never scheduled and rx's caller
   blocks forever. Related: `_handle_inbound_request` acks *before* dispatching
   the handler as a fire-and-forget task (`nodes/core/router.py:170`), so a
   handler crash loses the request silently and its exception is never
   retrieved.
6. **Frame-worker busy-wait** (`nodes/lib/bustle/processing.py:67-70`): a
   non-blocking `get_frame()` in a tight `while` loop burns 100% CPU when no
   frames arrive. Also unsynchronized `start()`/`stop()` against the appsink
   thread, and `_start_sink` restarts the pipeline without stopping the old one.
7. **`tx/internal/streaming/stream.go:61-63`** `block()` is a busy spin
   (`for s.c.Load() > 1`) and `ref()` increments the same counter, so two
   concurrent DASH requests livelock the server at 100% CPU; the underlying
   `d map[string]struct{}` is mutated without a lock (crash on concurrent
   request + manifest fetch). Path traversal in `dash.go:76-90` (`AddStream`
   joins an unvalidated request-body name onto the recording dir).
8. **`-tags va` does not compile**: `tx/internal/core/coder/h264enc.go:39`
   passes `specs.Bitrate` (uint32) to the `va` variant's
   `encElementWithOptions(uint64)`.
9. **ECharts-shaped analytics output**: both pipelines in
   `nodes/lib/bustle/analytics.py` end in a `$project` emitting ECharts option
   objects (`xAxis`/`yAxis`/`legend`/`series.type`/`showSymbol`), violating
   invariant 8. Corrected with the analytics service (§8, §11, TODO.md B4).

### B. Operational / robustness gaps

1. **Tests are still thin.** There is no Python test suite, and the Go tests
   cover only the RPC reply layer (`internal/protocols/handlers_test.go`, the
   `hub` and `subscriptions` `handlers` packages) and `internal/backoff`. For
   everything else `make lint` is the only automated gate.
2. **Restart loops could not recover and spun hot** (fixed, E4): no backoff, a
   one-shot `amqp091.Dial` with no `NotifyClose`, and the always-true
   `restart = restart || !restart` idiom. Routines now run inside
   `restart.Loop` (exponential backoff with deterministic jitter, reset when an
   attempt outlived `backoff.DefaultMax`), and `rabbitmq.Supervisor.Watch`
   redials on `NotifyClose`, re-declares the queue and installs it through
   `router.ReplaceChannel` — which is why the router reads its channel through
   `channel()`/`queue()` accessors instead of holding the original. rx also
   used to re-`Listen` already-bound SRT sockets created once outside the loop;
   it now registers both sockets for cleanup and closes them on cancellation
   (see B4).
3. **Pool/backpressure edges**: `NewGoroutinesPool(0)` underflowed
   (`internal/pools/goroutines.go`) — fixed with a `DefaultNGoroutinesPool`
   fallback, and `subscriptions` now starts its pool at all. Still open: bounded
   pools truncate silently at capacity; the broadcaster's subscriber cap counts
   subscriptions and uses `==`; a reconnecting node's channel is overwritten
   without being closed.
4. **Blocking lifecycle calls ignored cancellation** (fixed, E3):
   `Cleanups.Register` after `Run` started was silently dropped, and
   `SrtSocket.Accept` parks in `srt_epoll_wait` with an infinite timeout so
   SIGTERM could not unblock it. Registration is now safe at any time, and
   `rx/internal/protocols/srt/listen.go` runs the accept loop on its own
   goroutine and closes the socket on `ctx.Done()` — closing is the only thing
   that wakes libsrt, and the socket is spent afterwards. rx registers both the
   ingress and the egress socket; previously neither was.
5. **Broker/RPC lifecycle** (fixed, E2): delivery dispatch was an unbounded
   `go handleDelivery` per message; pending-trackID entries leaked when `Recv`
   was never reached; a late reply blocked forever on the unbuffered channel;
   the hub's sibling loop reused one `res` channel and aborted on the first
   `call` error instead of trying the remaining candidates. Dispatch now goes
   through the pool, the reply channel is buffered (cap 1) so a late reply is
   Nacked without requeueing, `Recv` releases the entry with `defer r.Forget`,
   and the sibling loops try every candidate and `errors.Join` the failures.
   Still open: Go requires exactly one sibling for some commands while Python
   tries all.
6. **Security**: committed default JWT secret; any HMAC alg accepted and no
   issuer check; no signin rate limiting and email logged on failure; no
   ownership check on subscription removal; no request body size limits and
   only `ReadHeaderTimeout` on the HTTP server; DASH stream add/remove
   unauthenticated (mitigated by loopback bind).
7. **Storage hygiene**: schemas are still applied out-of-band with the
   golang-migrate CLI (the services contain no migration code), but `hub` and
   `subscriptions` now call `db.VerifySchema` at startup and exit with one
   error naming the missing tables and the `migrate ... up` command instead of
   failing one "no such table" at a time. `DeleteSubscriptions` now deletes
   `subscribed_nodes` before `subscriptions` in one transaction, an empty list
   is rejected up front rather than panicking, empty node lists are skipped,
   `SelectAllNodesByID` is gone, and expired refresh tokens are swept by
   `sweepLocked` on save. Still open: the migration's `ON DELETE CASCADE` never
   fires because `PRAGMA foreign_keys = ON` is per-connection and not enabled
   at runtime; `pymongo.MongoClient` is never closed and the timeseries
   collection is created with a check-then-create race.
8. **`POST /auth/*` local dev**: the refresh cookie is `Secure: true` while the
   default bind is plain `127.0.0.1:9000`; logout uses `SameSite=Strict` while
   signin/refresh use `Lax`.
9. **Python typing/packaging**: `requirements.txt` is a full venv dump (already
   divergent from the installed venv), there is no `pyproject.toml` and no
   `pyrightconfig.json`, and Pyright is not installed — the documented
   type-check gate does not run (TODO.md A0).
10. **`BustleHandlers` is a class-attribute singleton** (`setup()` classmethod):
    one node instance per process, global mutable state. Resolved by the
    planned nodes refactor — see §12 and TODO.md.
11. **Single RabbitMQ channel per service**: a channel-level error still needs
    the infrastructure to be re-declared. The supervisor now does exactly that
    on a connection drop (`SetupRouter` + `qSetup` + `ReplaceChannel`), but a
    failure that only kills the channel, with the connection alive, is not
    detected.
12. PostgreSQL support and an analytics presentation layer are on the README
    TODO list.

### C. Documentation debt (corrected in this revision)

- The old claim that `POST /subscriptions/remove` returns 501 was stale — it is
  implemented (`hub/internal/protocols/http/handlers/subscriptions.go:75`).
- The old "unstructured logging / bare `fmt.Printf`" limitation and the
  "`internal/logging` (planned)" notes were stale — zerolog is implemented.
- The old `beforeExit []func() error` convention is gone; `Cleanups` replaced it.
- ARCHITECTURE.md's decoder list (`avdec_h264`, `faad`) and node pipeline
  string were inaccurate; the real defaults are `openh264dec`/`fdkaacdec` and
  the pipeline includes `poll-timeout=10000` and the BGR caps/appsink
  properties.
- TODO.md's C8 ("folds into A2") was wrong: the Python logging work shipped as
  a standalone `nodes/core/logging_setup.py` while A2 is still open.

## Invariants to preserve when changing code

1. Go and Python protocol implementations must stay wire-compatible: routing
   keys (`rpc.direct.<name>`, `rpc.broadcast`), introduce shape
   (`{"commands": [...]}` + ReplyTo), RouterResponse JSON shape (incl. the
   base64 string convention), manual acks, nack-without-requeue for malformed
   messages. When the planned analytics commands are added (DESIGN.md §11),
   both sides must change in the same commit.
2. `internal/` root packages are shared by hub/rx/subscriptions/tx — tx must
   stay independent of the broker and of the DB/pool helpers it does not need.
3. Commands follow `<subject>.<verb>`; node commands embed the stable node
   UUID, not the per-instance sibling name.
4. SRT streamid formats are contracts: `publish:<subscription-uuid>` (ingress)
   and `subscribe:<node-uuid>` (egress).
5. Pooled buffers must be returned exactly once on every path — the
   broadcaster's refcount and the `defer ...ResetAndPut` pairs depend on it.
6. `nodes/core/` must stay node-type-agnostic: no AI/model/database specifics
   may land there (they enter via hooks, the pipeline object, or manifests).
   Enforced as part of the nodes refactor (DESIGN.md §12). `logging_setup.py`
   complies; `pipeline.py` currently hard-codes the H264/BGR decode chain
   (TODO.md A3 parameterizes it).
7. Writer-owned queries: any read model for a node type's data must be
   declared by that node type (query manifest), never coded into the
   analytics service (DESIGN.md §11).
8. Aggregation output must be chart-library-agnostic: generic chart data
   (axes, series, values) only — no rendering-library-specific structure
   (e.g. ECharts option objects) may leak into pipelines, manifests, or
   handler responses (DESIGN.md §8, §11). Today's `sink.analytics`
   pipelines violate this; the violation is removed with the analytics
   service cutover, not preserved.
9. Never log secrets: passwords, tokens, the JWT secret, credential-bearing
   URIs (log scheme://host only), or full RPC/request payloads (length at
   debug). SRT streamids are identifiers, not secrets (DESIGN.md §13).
10. **One UUID storage class per column.** UUIDs in SQLite are 16-byte BLOBs;
    bind `uuid[:]` on both write and read paths (DESIGN.md §10, §A0).
