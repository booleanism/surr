# Design

Rationale, patterns and trade-offs behind the architecture described in
[ARCHITECTURE.md](ARCHITECTURE.md).

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
  adapters return an error when no sibling answers).
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
(Go dispatches each delivery on its own goroutine; Python acks before running
the handler as an `asyncio` task to keep the consumer moving). Messages are
manually acked; malformed ones are nacked without requeue so poison messages
cannot loop.

`RouterResponse.Result` is `json.RawMessage` with a `GetBytes()` decoder that
transparently handles base64-encoded string payloads — this exists because the
Python side base64-encodes binary results inside a JSON string. Any change to
the response shape must be mirrored in both `internal/protocols/handlers.go`
and `nodes/core/handlers.py`.

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
  return data) and the accepted socket.

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
a bounded goroutine "task pool" rather than unbounded `go` statements — except
delivery dispatch, which is deliberately unbounded per delivery.

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
are selected with Go build tags (`*_va.go` vs `*_default.go`).

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

### 7. Node self-registration with retry-until-hub

A node may boot before the hub; `nodes.add` retries forever (1s interval)
until a sibling serving it appears. The hub's insert is an idempotent upsert
(`ON CONFLICT(id) DO UPDATE`), making re-registration after either side
restarts safe. Registration is **once per run**: if the hub's database is
reset while a node keeps running, the node must be restarted (re-sending
periodically is a known future option). Graceful shutdown reverses the
sequence — stop streamer, cancel tasks, `nodes.remove` (with the router still
listening so the reply can arrive), then close the connection.

### 8. Analytics shape: store raw, aggregate on read

The node stores one median-count document per drain interval (2s default) in a
MongoDB timeseries collection; chart-ready series are computed by aggregation
pipelines at query time (`<node-id>.sink.analytics`, proxied by the hub's
`GET /nodes` analytics endpoint). Storing medians per interval (rather than
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
backed store) if that changes. Note the CLI ships a default JWT secret; real
deployments must override `--secret`/`JWT_SECRET`.

### 10. Layered service template (hub/subscriptions)

`cmd/<svc>` (CLI + wiring) → `internal/protocols/{http,rabbitmq}` (transport
handlers) → `internal/usecases` (business logic) → `internal/db/sqlite`
(storage), with `internal/adapter/rabbitmq` for the outbound broker calls.
Every layer consumes its dependencies as small consumer-defined interfaces
(e.g. a usecase needs `SelectSubscribedNodes`, not "the subscriptions
service"), which keeps the transport-agnostic core testable and lets the same
usecase be driven by either HTTP or broker handlers.

Cross-cutting conventions:

- **beforeExit lists**: each service accumulates `[]func() error` cleanup
  functions drained on context cancellation (SIGINT/SIGTERM). Resource
  release is registered where the resource is created.
- **Restart loops**: every long-running listener (router, pool, ingress,
  egress, HTTP) runs inside a loop that logs and re-enters on error rather
  than crashing the process.
- **Error codes on the wire**: handlers reply `Code: 0` on success and
  negative codes (-1 decode, -2 domain, -3 encode) with the error text in
  `Result` — the caller decides whether a code is fatal.
- **Object pools**: hot-path slices/buffers come from `internal/pools` and are
  reset-and-put back; functions that fill caller-owned slices return counts
  (`(int, error)`) instead of allocating.
- **UUIDs as BLOBs** in SQLite (16 bytes), parsed to `uuid.UUID` in repos.

### 11. Analytics service: writer-owned queries (planned)

Not yet implemented — designed, agreed, tracked in TODO.md. This section
records the decision so the implementation follows it.

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

### 12. Nodes runtime: type-agnostic core (planned)

Not yet implemented — see TODO.md for staged execution. The `nodes/` refactor
extracts the type-agnostic lifecycle (boot, introduce loop, registration,
shutdown choreography — ~90% of `bustle.py`'s finally-block) into
`core/node.py` so a new node type is an entrypoint + `lib/<type>/` package,
never a copy of `bustle.py`. Two rules keep it honest: `core/` contains no
AI/model/database specifics (they enter only via hooks and the pipeline
object), and the refactor is wire-compatible at every stage. The
`analytics.declare` call from §11 becomes a post-registration hook on this
runtime. See ARCHITECTURE.md "Planned evolution" for the shape.

### 13. Logging: zerolog, context-first (planned)

Not yet implemented — tracked in TODO.md section C.

**Choice.** zerolog on the Go side: zero-allocation structured logging, one
small dependency, native context support (`zerolog.Ctx`), and it works
identically in the shared modules and in standalone tx. Python nodes keep
stdlib `logging` (already used by `core/`); `bustle.py`'s prints migrate to
it during the nodes refactor (§12). Level *semantics* match across languages;
output formats do not need to.

**Output & configuration.** `--log-format=console|json` (default console —
this is a dev-first, single-operator repo; flip to JSON when a collector
appears) and `--log-level` (default info) flags on every service, wired like
all other flags in `cmd/<svc>/cmdline.go`.

**Propagation.** Each `main` builds a root logger annotated with
`service=<sibling name>` and `family=<exchange>`, injects it into the root
context; shared packages and handlers pull it with `zerolog.Ctx(ctx)` (which
degrades to a no-op logger when absent, so packages stay usable in tests).
Components without a context (the rx broadcaster's hot paths) hold a logger
field from construction. The hub's HTTP layer gets a small status-capturing
middleware (or zerolog's `hlog`) so every request carries the same logger.

**Level policy** — the judgment-call part. Logs must carry the related
values, never bare messages:

| Level | Events (with required fields) |
|---|---|
| debug | 1s introduce broadcasts, sibling-registry updates (sibling, command count), per-RPC send/recv round trips (command, track_id, reply_to, code), HTTP 2xx requests, payload *lengths* |
| info | service start/stop with config summary (broker, family, sibling, listen addrs, pool sizes), restart-loop re-entry, SRT accept (peer, streamid, sub_id/node_id), publisher/subscriber start/stop, `sink.start/stop` dispatched (node_id), auth signin/refresh success (user_id), beforeExit cleanup results |
| warn | SRT reject (peer, streamid, reason), slow-consumer chunk drops (sampled; sub_id, node_id, dropped), non-zero RPC reply codes (command, code), auth failures (email), malformed-message nacks (reason, track_id), HTTP 4xx |
| error | operation failures — everything currently `ERRO:` prints — with the error object plus the same context fields, HTTP 5xx, handler/DB errors |

**Canonical fields** — same key everywhere, snake_case: `command`,
`track_id`, `reply_to`, `sibling`, `code`, `sub_id`, `node_id`, `peer`,
`streamid`, `reason`, `dropped`, `method`, `path`, `status`, `duration_ms`,
`remote`, `err` (always via zerolog `.Err()`, never an interpolated string).

**Never log** (security invariant): passwords, refresh tokens, the JWT
secret, credential-bearing URIs (RTSP source URLs can embed user:pass — log
`scheme://host` only), or full RPC/request payloads (at most their length,
at debug). SRT streamids are the deliberate exception: they are
UUID identifiers, not secrets, and belong in accept/reject logs.

**Hot-path rule.** The media path logs no per-chunk events. Slow-subscriber
drops are the only signal worth emitting, and they are sampled (first drop +
counters, or every Nth) so a stalled consumer cannot itself become a log
flood.

## Known limitations / future work

- **No Go or Python tests**; `make lint` is the only automated gate.
- **Unstructured logging**: all Go services use bare `fmt.Printf` with
  `INFO:`/`ERRO:` prefixes — no levels, no fields, no HTTP request logging,
  secrets only accidentally absent. Replaced by zerolog per §13 / TODO.md C.
- **Stale siblings/nodes**: no TTL on the sibling registry; a SIGKILL'd node
  leaves a row in the hub's `nodes` table; a crashed peer's registry entry
  lingers. A sweep/TTL mechanism is the missing piece.
- **Refresh tokens in memory** (see auth above); no horizontal scaling of hub.
- **`POST /subscriptions/remove` returns 501**; subscription removal only
  exists on the broker path.
- **Node reads its own writes**: nodes currently both write and read MongoDB
  (`sink.analytics`). Resolved by the planned analytics service — see §11
  and TODO.md.
- **ECharts-shaped analytics output**: the aggregation pipelines bake Apache
  ECharts option structure (`xAxis`/`legend`/`series.type`) into their final
  `$project` stages (nodes/lib/bustle/analytics.py), coupling the data layer
  to one chart library. Corrected with the analytics service — declared
  queries must return generic chart data (see §8, §11 and TODO.md B4).
- **`BustleHandlers` is a class-attribute singleton** (`setup()` classmethod):
  one node instance per process, global mutable state. Resolved by the
  planned nodes refactor — see §12 and TODO.md.
- **`restart = restart || !restart`** idiom in the listen loops is always true
  after the first pass; harmless (just log wording) but confusing.
- Single RabbitMQ channel per service: a channel-level error (e.g. publish to
  a closed exchange) would require the restart loops to re-declare infra,
  which they currently do not.
- PostgreSQL support and an analytics presentation layer are on the README
  TODO list.

## Invariants to preserve when changing code

1. Go and Python protocol implementations must stay wire-compatible: routing
   keys (`rpc.direct.<name>`, `rpc.broadcast`), introduce shape
   (`{"commands": [...]}` + ReplyTo), RouterResponse JSON shape (incl. the
   base64 string convention), manual acks, nack-without-requeue for malformed
   messages. When the planned analytics commands are added (DESIGN.md §11),
   both sides must change in the same commit.
2. `internal/` root packages are shared by hub/rx/subscriptions only — tx must
   stay independent of them.
3. Commands follow `<subject>.<verb>`; node commands embed the stable node
   UUID, not the per-instance sibling name.
4. SRT streamid formats are contracts: `publish:<subscription-uuid>` (ingress)
   and `subscribe:<node-uuid>` (egress).
5. Pooled buffers must be returned exactly once on every path — the
   broadcaster's refcount and the `defer ...ResetAndPut` pairs depend on it.
6. `nodes/core/` must stay node-type-agnostic: no AI/model/database specifics
   may land there (they enter via hooks, the pipeline object, or manifests).
   Enforced as part of the nodes refactor (DESIGN.md §12).
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
