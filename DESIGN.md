# Design

Rationale, patterns and trade-offs behind the architecture described in
[ARCHITECTURE.md](ARCHITECTURE.md).

> Sections 1–10 describe decisions that are **implemented on `main`**. Sections
> 11–12 are **designed but not built on `main`**. Section 13 is **implemented**.
> The "Known limitations" section at the end records the defects found in the
> 2026-10 review; TODO.md section E tracks each one and its status.

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

`RouterResponse.Result` is `json.RawMessage`, which exists because the Python
side base64-encodes binary results inside a JSON string. Any change to the
response shape must be mirrored in both `internal/protocols/handlers.go` and
`nodes/lib/core/handlers.py`. A caller that wants bytes unmarshals `Result`
into a `[]byte` field and encoding/json does the decode — that is the whole
convention, and it is why there is deliberately no `GetBytes()`-style helper: a
short ASCII failure message such as `internal` is itself valid base64, so a
decoder that "tries base64 and falls back to text" returns garbage for exactly
the replies it is most likely to be called on. Read a failure with
`ErrorMessage()` — conflating the two was the trap behind §A1. (A `GetBytes()`
helper did exist until the E11 pass; it was deleted rather than validated.)

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
surveillance inference: object counting tolerates dropped frames; a stalled
publisher does not.

Related memory discipline (shared root `internal/pools/`): all hot-path
allocations (byte buffers, node/subscription slices, sibling-name slices) come
from `sync.Pool`s with reset-and-put helpers, and background work runs through
a bounded goroutine "task pool" rather than unbounded `go` statements —
including delivery dispatch, which used to be a bare `go` per message.

Known cracks in this design, all closed: the subscriber cap counted
subscription *maps* rather than subscriptions and compared with `==` (so one
subscription admitted unlimited nodes) — it now counts subscriptions and
refuses past the cap with `errMaxSubscribers`; a reconnecting node's channel was
overwritten in the map without being closed, so its reader blocked forever —
`Subscribe` now returns a handle and a replacement drains and closes the old
channel; and `Unsubscribe` closed a channel while holding the registry lock —
the close moved outside the lock, and a handle that is no longer registered is
refused so a replaced subscriber's deferred call cannot close its successor's
channel. The per-iteration `Read`/`Write` deadlines (1s idle, 10s write) are
what make a cancelled ctx or a stalled subscriber visible at all: a bare
`sock.Read` in a loop never returns on cancellation.

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
are selected with Go build tags (`*_va.go` vs `*_default.go`); both now
compile, and `h264enc.Valid()` turns a missing factory (e.g. no `openh264enc`
plugin installed) into a startup error instead of a nil-pointer crash — the AAC
decoders and encoder are checked the same way (`coder.CheckDecoders`,
`aacenc.Valid()`). The encoder inserts a `videoconvert` and pins `format=I420`,
and `h264parse` runs with `config-interval=-1` so a node that subscribes late
still receives SPS/PPS. The software codecs are not hardcoded: `pickElement`
walks a preference list and takes the first factory the runtime has —
`fdkaacdec`/`fdkaacenc` → `faad`/`voaacenc` → `avdec_aac`/`avenc_aac` — because
`fdkaac*` ships only where the non-free fdk-aac was built in, which is why a
developer machine with deb-multimedia has them and a stock Debian image does
not.

Shutdown is deliberately abrupt: the pipelines are **never** brought to NULL.
The process quits the main loop on cancellation and exits, leaving GStreamer to
be torn down by the OS. The reason is a GLib 2.84 race rather than taste — the
NULL transition disposes the RTSP source and cancels the name lookup GIO left
in its threaded resolver, and that cancellation races the resolver's worker
thread (`g_assert (g_cancellable_is_cancelled (cancellable))` in
`gio/gthreadedresolver.c`), so the process dies of SIGSEGV on a clean SIGTERM as
readily as on an unreachable source. It is not an ordering problem: it
reproduces whether the state change is issued from the loop thread or from a
cleanup hook, and whether the whole pipeline set or only the downstream
pipelines are taken down. The accepted cost is that an interrupted `--recording`
is missing its trailer and needs remuxing before it plays. A bus error still
becomes a non-zero exit (`runFailure` reads `context.Cause`), so a source that
never connected is visible to a supervisor instead of looking like success.

Recording is opt-in: `--recording 0,0,0` builds no recording branch at all, so
nothing is written and nothing is registered with the DASH server. `tx run`
registers the stream with `tx serve` over HTTP when that server happens to be
up, but a failure is only logged — a relay or a recording no longer depends on
a separate `tx serve` having been started first.

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
adapter never reads the reply code. The node's invalid-source-address path now
awaits its reply (`nodes/lib/bustle/handlers.py`), so a bad address no longer
stalls the node's event loop on a dropped coroutine — but the media goroutine
still waits for the reply rx never reads.

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
pipelines at query time (declared by the node, executed by the analytics
service, proxied by the hub's `GET /nodes/analytics` endpoint). Storing medians
per interval (rather than
per-frame counts) bounds write volume; the drain interval and the frame
worker's drop-oldest frame queue (`maxsize=1`) both bias toward recent state.

**Chart-library-agnostic output is the contract, and nothing in the stack
breaks it.** The data contract is generic chart data: axes/categories, series
names, values (e.g. min/avg/max per class, time-bucketed averages), with the
rendering library chosen entirely by the presentation layer. The ECharts
option objects the old pipelines built (`xAxis {type: "category"}`, `yAxis`,
`legend`, `series.type`, `showSymbol`) and the `to_echarts` shim that briefly
mapped rows for the legacy handler are both gone with that handler. Keeping the
mapping out of the aggregation is what makes the same pipelines publishable as
the node's query manifest: a pipeline that returns a renderer's option object
could not be validated by the service and could not be reused by a second
renderer.

**Four queries, one document each.** `nodes/lib/bustle/analytics.py` declares
four pipelines, and every one of them ends as a single `$facet` document. The
shape is forced by the service's caps — 2000 documents and 1 MiB — but it is
also the right shape: a query that returned one document per bucket would hit
the cap on a long window and cost a separate pass over the collection for every
chart. One document per query means one scan per query and a report whose size
depends on the number of views, not on the length of the window.

| query | subject | answers |
| --- | --- | --- |
| `activity_by_class` | any | per class: total, average, min, max, stddev, buckets, peak bucket, share of all detections — plus `coverage` |
| `occupancy` | required | `timeline`, `crowd` (band histogram), `moments` (busiest buckets), `trend` (first half vs second), `streaks` (longest busy/quiet run), `volatility` |
| `rhythm` | required | average and peak by hour of day and by day of week, in the node's own offset |
| `class_affinity` | required | how often each other class shares a bucket with the subject |

`coverage` is not a chart; it is the answer to "should I believe the other
three". A node that spent half the window unable to pull frames produces a
perfectly well-formed report about a scene it was not watching, and only the
sample count and the first/last timestamps show it.

**The subject is resolved, not assumed.** Three of the four queries are about a
*subject* class — the one whose presence defines occupancy. `resolve_subject()`
returns `person` only when the node's `--chosen-classes` actually include it,
and the manifest then declares `activity_by_class` alone: a vehicle-counting node
reporting three empty occupancy charts would look exactly like an empty venue,
which is a lie the class-agnostic query does not tell.

**Every average is per bucket, never per document.** The drain interval is not
constant (a restart, a slow batch), so averaging raw documents would let a
densely sampled minute outvote a sparsely sampled hour. Each query groups to
`$dateTrunc(unit_time)` first and averages the bucket averages.

**Bucket labels leave as strings.** The service marshals documents as relaxed
extended JSON, so a BSON date reaches the client as `{"$date": ...}` — an object
no chart can label an axis with. `_iso()` wraps every emitted timestamp in
`$dateToString` with an ISO-8601 UTC format; truncation still groups on the real
date, so ordering and bucketing are unaffected. The day-of-week axis is renamed
from `$dayOfWeek`'s 1..7 to `Sun`..`Sat` for the same reason: a number the client
has to decode is not an insight.

**The charts belong to the node type.** `GET /nodes/analytics` names no query.
It asks the analytics service what the node declared and runs that list, so a
detector with a different story to tell — plates instead of people — declares
its own queries and appears in the report with no change to the hub. The report
is therefore a list of named results rather than a fixed pair of fields:

```
{"window": {"time_cluster": ..., "start_time": ..., "unit_time": ...},
 "queries": [{"name": ..., "description": ..., "data": [...]}, ...]}
```

Each entry carries its rows untouched, or an `error` saying why that one query
could not be answered — one broken chart must not cost the caller the rest. The
request fails only when nothing at all could be answered, because a 200 with an
empty report reads like a node that has never seen anything, which is a
different fact from a service that cannot answer. The window is the endpoint's
to choose (it is a question about how much history to show, not about what the
documents mean) and each query is sent only the parameters its own schema
declares: sending `unit_time` to a query that does not take it is a payload
error at the service, and withholding it from one that does is a missing
parameter.

One consequence worth stating: each query returns one document of named keys
rather than rows, so the consumer reads keys rather than joining axes.
`occupancy` gives six views of one scan; `activity_by_class` gives one entry per
class. A bucket can be missing for one class and present for another (a class
added to `--chosen-classes` mid-run has no earlier buckets), and zero-filling it
would assert a measurement that was never taken. `class_affinity` is the one
place where a zero is a real answer: `buckets` counts the buckets the class was
actually seen in, and `share` divides `together` by the subject's own
occupied-bucket count — its own `$facet` view, because a per-class group cannot
see how many buckets the subject filled. Counting every bucket instead made
`share` a constant `1.0` for every class, a defect no unit test could see and a
live probe could.

One data-shape caveat in the same path: classes with zero detections in an
interval are omitted rather than zero-filled, so the unwound series has gaps.
That is deliberate — a zero means "counted, and there were none", which is a
different statement from "not measured". The timestamps are UTC
(`datetime.now(timezone.utc)`), which the `$dateTrunc` buckets depend on: a
naive local timestamp shifted every bucket by the host's UTC offset.

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

Note the CLI no longer ships a JWT secret: `--secret`/`JWT_SECRET` is empty by
default, and the hub then generates 32 random bytes per process and logs a
warning — tokens stop being valid across a restart, which is fine for a local
test and a trap in any real deployment, so set the secret there. Validation
pins `HS256` and the `iss` claim. Signin is rate limited to 10 attempts per
minute per client address (keyed on the address, not the email, so an attacker
cannot lock a known account out) and no longer logs the submitted email.
Subscription removal is ownership-checked inside the delete statement, and the
`refresh_token` cookie's `Secure` attribute follows `--cookie-secure` so a
plain-HTTP local deployment works without weakening a real one.
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

### 11. Analytics service: writer-owned queries

The service is built, tested, deployed and on the read path (TODO.md B1–B5);
what is left is fleet-wide declarations (B6). This section records the decision
so that what follows keeps its shape. An earlier
first attempt survives as dangling commits and a stash from the abandoned
`feat/analytics-service` branch; it was not the base of this one.

**Problem (as it stood).** Each node type owned both the write path (document
shape) and the read path (`<node-id>.sink.analytics` with type-specific
aggregation code). Every node type therefore needed its own broker command
handler, its own query logic, and read access to MongoDB — and a future
presentation layer would have needed to know which node answered which query.
That coupled the reader to the writer's internals in exactly the way the rest
of the system avoids.

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
layer's job. **Status: landed.** `nodes/lib/bustle/analytics.py` builds the
manifest (`activity_by_class`, plus `occupancy`, `rhythm` and `class_affinity`
when the node counts a subject — §8) and the node publishes
it from `BustleWorker.registered()`, which the runtime runs once `nodes.add`
has succeeded — retried while the service is still booting, bounded at thirty
attempts so a deployment with no analytics service logs a warning a second for
half a minute instead of forever. The parameters are `${start_time}` (ISO-8601)
and `${unit_time}`; `$toDate` wraps the former inside the pipeline because a
string compared against a BSON date orders below every date and would match
nothing.
The service upserts the manifest into its own
SQLite registry and serves `analytics.query {node_id, query_name, params}` —
substitute, append the result cap, execute read-only, return JSON — plus
`analytics.queries.list {node_id}`, which answers with each query's name,
description and parameter schema and never with its pipeline. The hub's
analytics endpoint asks for that list and runs every entry; the nodes'
`sink.analytics` handler is gone, so a node holds a write credential on its own
collection and no read handle at all.

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

  **As built** (`analytics/internal/usecases/query.go`) that surface is an
  *allowlist* of eighteen read-only stages rather than a blocklist, because a
  blocklist loses to nesting: `$facet` can hold a writing stage inside a
  pipeline whose outer shape looks read-only, so the contents of a `$facet` are
  validated recursively against the same list. `$lookup` and `$graphLookup` are
  excluded too — read-only, but they read other nodes' collections. Alongside
  the allowlist: a 24-stage cap, a `$limit` **appended** rather than merged (a
  manifest without one cannot return a whole collection), and a
  2000-document / 1 MiB result cap enforced by the Mongo adapter as it reads the
  cursor, so an over-cap query stops reading instead of building the answer and
  discarding it.
- Parameter substitution happens on the pipeline's **raw bytes**, not on a
  decoded Go map. A map would lose `$sort` operand order, and `{"a":1,"b":-1}`
  sorts differently from `{"b":-1,"a":1}` — the query's meaning would change
  between declare and execute. The pipeline is carried as `json.RawMessage` from
  the registry to the executor and decoded into `[]bson.D` only at the driver
  boundary, which preserves order for the same reason.
- Query names become part of the wire contract between node author and
  presentation layer — hence `analytics.queries.list` is part of the design,
  not decoration.
- **Cross-node (fleet) aggregation is a `$unionWith` fan-out over the members'
  collections, and the membership comes from the caller, not from the query.**
  A fleet query is declared by the node type (`scope: "fleet"`, plus the
  `node_type` it belongs to) and stored once for the type rather than once per
  node. At query time the hub sends the set of node ids the caller owns, the
  service intersects that set with the type's declarations to get the member
  collections, and the executor prepends one `$unionWith` per member beyond the
  first. Three consequences are the point of the design:

  - **The query names no collection.** `$unionWith` is injected *after* the
    pipeline is validated, so `$unionWith` stays out of the manifest allowlist
    and no manifest — however it arrived — can read a collection its author does
    not own. `$lookup`/`$graphLookup` remain refused for the same reason.
  - **The hub still knows no query name.** It cannot decide whether a query is
    fleet-scoped, so it sends the membership with every query and the service
    ignores it for a node-scoped one. The alternative — the hub classifying
    queries — would put a node's vocabulary back into the control plane, which
    is the coupling §11 exists to remove.
  - **No members is an error, not an empty report.** `models.ErrEmptyFleet`
    surfaces as a per-query error, because a fleet report full of empty arrays
    reads as "the deployment saw nothing" — the same lie the hub's liveness
    check refuses for a single node.
- **The fleet arithmetic is two-staged, and the order is load-bearing.** Per
  node per bucket the value is the **average** of that node's documents in the
  bucket; only then are the nodes' values for that bucket **summed**. A bucket
  holds several drain intervals, so summing them first reports a camera that
  sampled twice as often as seeing twice as many people — the first version did
  exactly that, and a live probe (six 10s documents of four people summing to 24
  in one minute) is what caught it, because a structural test cannot see
  arithmetic. Averaging the per-node averages afterwards would be the opposite
  error: a node that reported twice as heavily would count twice as much, so a
  camera restarted mid-window would weigh less than one that was not.
- Rolling deploys can briefly mix query versions across instances of the same
  node type; mitigated by versioned query names if it ever bites.
- **A declaration outlives the node that made it, so liveness is a separate
  question from existence.** The registry is keyed by node id and nothing
  deletes a row, which is deliberate: a node id is minted per install and
  `subscribed_nodes.node_id` is UNIQUE with no endpoint to move a node between
  subscriptions, so every reinstall leaves a row behind. Sweeping them would
  need a timestamp the `nodes.add` payload does not carry.
  The read path therefore asks the sibling registry whether the node is
  advertising itself, and refuses with 409 rather than running the queries: an
  empty window and a dead node both produce empty arrays, and a report full of
  empty arrays reads as "this node saw nothing", which is the one conclusion a
  surveillance system must never present as data. The same reasoning is why
  `/nodes/availability` filters on liveness — offering a dead node is how a
  client ends up subscribed to one.

### 12. Nodes runtime: type-agnostic core

**Status: landed** (`lib/core/node.py`, A2). (A version of this refactor also
exists on `refs/remotes/gitea/main` as commit `66e308b "nodes: pythonic
refactor"`, which restructures `nodes/` into
`core/{cli,config,detector,registry,service,storage,stream,worker}.py`,
`node.py`, and `workers/bustle/*`. It was never merged and its modules now
belong under `nodes/lib/core/` on `main`; treat it as prior art.)

The `nodes/` refactor extracted the type-agnostic lifecycle (boot, introduce
loop, registration, shutdown choreography — ~90% of the old `bustle.py`) into
`lib/core/node.py`, so a new node type is an entrypoint plus a `lib/<type>/`
package rather than a copy of `bustle.py`. Two rules keep it honest: `lib/core/`
contains no AI/model/database specifics (they enter only through the `Worker`
hooks and the pipeline object), and every stage was wire-compatible. The
`analytics.declare` call from §11 becomes the `Worker.registered()` hook on this
runtime. See ARCHITECTURE.md "Node lifecycle" for the ordering.

The reason the shutdown choreography was worth moving rather than copying is
that each of its four steps depends on something the next one still needs
alive: the frame source must stop before the tasks reading it are cancelled
(cancelling a task does not stop the thread it is waiting on), `nodes.remove`
must be sent while the listener is still up or the reply has nowhere to go, and
the listener must be cancelled last or the consumer stays parked on its future
while the connection closes underneath it. Every one of those mistakes produces
a node that hangs on shutdown or leaves a stale row, and none of them raises an
exception — which is why the runtime's tests assert the *order* of the calls
rather than their results.

### 13. Logging: zerolog, context-first (implemented)

Implemented in `internal/logging/` and wired into all five Go services;
TODO.md section C is complete. Python nodes use stdlib `logging` via
`nodes/lib/core/logging_setup.py`.

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

The review pass reproduced every item below from the source. TODO.md section E
is the tracker for these findings and their status.

#### Fixed (E0–E6, E8, E9, E10)

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
   the read side. Reading a failure with the old `GetBytes()` helper was itself
   a trap: a short ASCII message such as `internal` is valid base64, so it
   decoded to garbage. The helper has since been deleted (E11) — a payload is
   read by unmarshalling `Result` into the type the caller asked for.
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
5. **Python node runtime (E8).** `nodes/lib/bustle/handlers.py:112` dropped the
   `await rep(...)` on the invalid-source-address path, so the reply was never
   scheduled and rx's caller blocked forever; `ReplyFunc` is now typed as
   returning a `Coroutine`, which makes the same mistake a Pyright error.
   `_handle_inbound_request` still acks before dispatching (the handler owns the
   reply, and a redelivery would duplicate it), but the task is retained in
   `Router._inflight` with a done-callback that logs its exception instead of
   discarding it. `frame_worker` blocks on `get_frame(timeout=0.5)` instead of
   spinning at 100% CPU; `pipeline.py` guards `start()`/`stop()`/
   `_on_new_sample` with a lock, checks `running` in the callback and tolerates
   `queue.Empty`; `_start_sink` stops the old pipeline before starting the new
   one; the GStreamer state changes and the pymongo aggregation run through
   `asyncio.to_thread`; `insert_one` and detector failures are logged
   instead of killing the task; a `recv` timeout records its trackID so a late
   reply is dropped rather than mis-dispatched as an inbound request; `ticker`
   floors its interval; timestamps are `datetime.now(timezone.utc)`; and
   `bustle.py` exits non-zero when Mongo is unreachable (it used to log and
   return 0, so a dead node looked like a clean run) and closes its client.
6. **tx / DASH (E6).** `stream.go`'s atomic ref-count protocol (`block()`
   busy-spin, unlocked `d map[string]struct{}`) is replaced by a plain
   `sync.RWMutex` with `Names()`/`IsExist`/`Set`/`Delete`; `validStreamName`
   rejects a traversal-shaped stream name before it reaches `filepath.Join`;
   `loadRegisteredStreams()` (called by `NewDash`) replaces the never-called
   `AvailStream`, so recorded streams survive a `tx serve` restart; the three
   `go func()` closures that shared a named `err` use locals; the recording
   pipeline is no longer built when `--recording 0,0,0`; the audio pad probe no
   longer emits EOS per buffer; the muxer releases its request pad on link
   failure; `h264enc` gained a `videoconvert` + `format=I420` and
   `h264parse config-interval=-1`; `srtsink` sets `auto-reconnect:true`; and
   `tx run` no longer aborts when no DASH server is up (registration is best
   effort). `-tags va` now compiles — the `va` variant's
   `encElementWithOptions` takes the same `uint32` as the default one.
7. **Security (E9).** The committed default JWT secret is gone (an ephemeral
   random secret is generated with a warning when none is configured);
   validation pins HS256 *and* the issuer; signin is rate limited per client
   address and no longer logs the email; the refresh cookie's `Secure` attribute
   follows a `--cookie-secure` flag and all three cookies come from one helper;
   the HTTP server sets Read/Write/Idle timeouts and caps bodies at 1 MiB;
   subscription removal is ownership-checked inside the `DELETE` itself (a new
   `protocols.CodeNotFound` carries the miss back through the broker, and the
   hub answers 403); and the DASH control routes require a `X-Dash-Token`
   (loopback-only when no token is configured).

9. **Python typing / packaging (E10, MEDIUM)**: the runtime had no
   `pyproject.toml`/`setup.py` and no type-checker configuration, and
   `requirements.txt` was a `pip freeze` dump that had already drifted from the
   installed venv (it pinned `aio-pika==10.0.1`/`pymongo==4.18.1` against
   10.1.0/4.18.2). The root `pyproject.toml` now declares the direct
   dependencies (pinned exactly, with the torch CPU wheel wired through
   `[tool.uv.sources]` — PEP 621 has no index-URL field, so a plain
   dependency list cannot express the `+cpu` pin that `requirements.txt` got
   from `--extra-index-url`) and configures Pyright
   (`typeCheckingMode = "standard"`, `venvPath`/`venv` pinned to the root
   `.venv`, `extraPaths = ["nodes"]` for the nodes' top-level `core`/`lib`
   imports, `reportMissingModuleSource = "none"` for the compiled
   `gi.repository` bindings). `uv.lock` records the resolved transitive graph
   and `uv sync` is the one setup command; `requirements.txt` is gone, and
   `make typecheck` runs the gate (0 errors). The typing fixes it
   required were real: a `RouterLike` Protocol so `lib/core/rpc.py` no longer
   depends on the concrete `Router`, a `None` guard on
   `GstVideo.VideoInfo.new_from_caps` in `_frame_view`, and type arguments on
   the `queue.Queue`/`pymongo.Collection` attributes. The one landmine: the
   PyPI package `gst-python-stubs` ships an overrides-only `Gst.pyi` at the same
   path as the real PyGObject-stubs file and silently clobbers it, which turns
   every GStreamer enum and constructor into an unknown-attribute error.

#### Fixed in the B5 cutover — do not reintroduce

9. **The cutover (TODO.md B5)** — done. The hub no longer proxies to a node
   command: `GetAnalytics` (`hub/internal/adapter/rabbitmq/nodes.go`) reads the
   node's declaration from the analytics service and runs every query in it, so
   no query name appears in the control plane and a new node type needs no hub
   change. The node's `sink.analytics` handler and `to_echarts` are deleted and
   `BustleHandlers.setup` no longer takes a collection: the node writes with a
   `readWrite` Mongo credential and reads nothing, while the analytics service
   holds `read` only — the split is enforced by credentials rather than by
   convention, which is what makes it worth stating in a review. The window
   (`time_cluster` → start + bucket width) moved into the hub with the adapter,
   because how much history to show is the caller's question, not the writer's.
   Two behaviours are deliberate and easy to "simplify" away: each query is sent
   only the parameters *its own* schema declares, and one failing query is
   reported per entry instead of failing the report (the whole request fails
   only when nothing could be answered).

#### Fixed in the E7/E11 pass — do not reintroduce

10. **rx data plane (E7, MEDIUM)** — fixed. The broadcaster counts subscriptions
    rather than topics and refuses past the cap with `errMaxSubscribers`;
    `Subscribe` returns a handle and a replacement `drain`s the previous
    channel, so the replaced reader unblocks; only `Open` (the publisher path)
    creates a topic, so a subscribe after close fails with `errNoPublisher`
    instead of resurrecting an orphan; `Unsubscribe` takes the handle, removes
    it only if it is still the registered one, and closes outside the lock. The
    handshake callbacks run their broker lookup under a 5s timeout and keep
    pending handshakes in `pending[V]` with a 30s TTL and a 4096-entry cap
    instead of an unbounded map. Writes carry a 10s deadline and reads a 1s
    idle deadline (libsrt sockets are non-blocking, which is what makes a
    deadline effective), so a stalled subscriber or a cancelled ctx is noticed
    instead of wedging the goroutine; every ingress rejection now sets a real
    SRT reason (`BadRequest`/`Forbidden`/`Unacceptable`/`Overload`) rather than
    returning a bare `false`. `StartSink`/`StopSink` return the first per-node
    error after still trying every node, so a publisher whose node is not up is
    dropped rather than served nothing — and the reconnect that follows is
    bounded instead of the measured ~34/second: one handshake per two seconds
    (`sinkUnavailableDelay`) when the node was never registered, and a handful
    more than that while the registry still lists a dead one, because each
    attempt then also spends up to the 5s `sinkCommandTimeout` below waiting for
    an answer that will not come. `SiblingTTL` (F5, §A17) closes that last gap:
    fifteen seconds after the node stopped introducing itself the lookup fails
    at once and both cases cycle identically. The retry itself still belongs to
    `tx`.
11. **Minor items (E11, LOW)** — fixed. `GetBytes` is deleted (a payload is read
    by unmarshalling `Result` into the type the caller asked for; its "try
    base64, fall back to text" guess could not be made correct); an empty
    `ReplyTo` is logged as a warning before the reply is refused; `models.User`
    `Passwd` is `json:"-"`, so the argon2id hash no longer rides the
    `subscriptions.nodes.by.user.get` broker payload; signup and signin decode
    into `handlers.credentials` with `DisallowUnknownFields`, so `password` is a
    400 rather than a silently empty account, and a duplicate email is
    `models.ErrUserExists` → 409 (matched on SQLite's *extended* constraint
    code, because `SQLITE_CONSTRAINT` also covers NOT NULL/CHECK/FK); `rx`
    parses listen addresses with `net.SplitHostPort`, so IPv6 works and
    malformed addresses are rejected; and the two shadowing locals in `tx` are
    renamed.
12. **Detector licensing and coupling (fixed)**: the node ran inference through
    `ultralytics`, which is **AGPL-3.0** while this repository is MIT — a
    copyleft obligation on the whole distribution for anyone who published it —
    and its `Results` object leaked into the counting code (`boxes.cls`,
    `isinstance(boxes.cls, torch.Tensor)`, `model.names` in
    `nodes/lib/bustle/processing.py`), so the licence was not the only thing
    making a swap expensive. `nodes/lib/detector.py` now defines a narrow
    `Detector` Protocol — `names: Mapping[int, str]` and
    `detect(frame) -> list[Detection]`, where `Detection` is a frozen dataclass
    of `class_id`/`confidence`/`xyxy` — and `LibreYOLODetector` implements it
    over **LibreYOLO** (MIT code) with **`LibreDEIMm.pt`** weights (Apache-2.0,
    55.4 mAP50-95 at 19.6 M params, 640 px). DEIM v2 at S/M/L/X was rejected:
    those checkpoints add Meta's DINOv3 licence, which is not OSI-approved and
    forbids military/ITAR uses. Two behaviour changes came with it. First, DEIM
    is a DETR-style top-K decoder with **no NMS**, so overlapping
    low-confidence boxes are counted separately; `--conf` (default `0.25`) is
    now the only filter between raw output and the count. Second, tracking was
    dropped — `model.track()` was called but only `boxes.cls` was ever read, and
    LibreYOLO's `predict` reports no ids. Cost: ~546 ms/frame on CPU at
    1280x720, slower than the `yolo26s.pt` it replaced.

#### Fixed in the deployment sweep (F1–F4) — do not reintroduce

These four were found by *running* the system, not by reading it, and each needs
a live process to be reachable — which is why a review pass missed them and an
end-to-end run did not. TODO.md section F has the evidence.

13. **rx sink dispatch reported success for a node that never answered (F1,
    HIGH)**. `rx/internal/adapter/rabbitmq/nodes.go` ran the broker round trip
    inside a pool closure that declared its own `err :=`, so the `return` in
    that closure returned from the *closure* — `do()` returned whatever
    `pools.TaskFunc`'s `Run` returned, which is `nil` on every path. A failed
    `Send` or `Recv` was logged and discarded, so `StartSink` reported success
    and rx accepted a publisher for a node that had died between the sibling
    lookup and the reply. The publisher then stalled until the SRT write failed,
    so it recovered by accident; the bounded retry that the E7 fix added is
    reached only when `StartSink` *returns* an error, so for a registered but
    dead node it never engaged at all. `do()` now runs a separate `roundTrip`
    method on the pool and receives its outcome over a `make(chan error, 1)`
    (`return <-result`), and `roundTrip` returns every failure — including a
    non-zero reply code, so a node that refuses the command (`invalid source
    address`) is no longer a started sink.
14. **The same closure leaked a pool worker per stale dispatch (F2, HIGH)**.
    `router.Recv` blocks on `ctx.Done()` unless a reply arrives, and the context
    it was given was the process context, so every dispatch to a registered but
    dead node parked forever. A goroutine dump of a live rx showed
    `router.Recv` ← `nodes.do.func1` ← `pools.goroutinesPool.Listen.func1`
    marked `[select, 3 minutes]`, and `rx` has `--worker 28` of them: a handful
    of stale dispatches retires the pool and every later sink command queues
    behind them. `var sinkCommandTimeout = 5 * time.Second` now bounds the round
    trip (a `var`, not a `const`, so a test can shorten it), the same discipline
    the SRT handshake lookup already used. Measured live, this is also what
    spaces the reconnect out for a node the registry still lists: 4 cycles in
    30s, against 15 for a node that was never registered and 1058 in 31s before
    the delay existed. `SiblingTTL` (F5, §A17) then removes the difference
    altogether — after fifteen seconds the stale entry is gone and both cases
    settle at 15.
15. **Adding an already-subscribed node answered 500 (F3, MEDIUM)**.
    `subscribed_nodes.node_id` is `UNIQUE` — a node belongs to exactly one
    subscription — so the second add fails at the storage layer, and that
    failure travelled back as a bare domain error. The reply-code vocabulary
    had no way to say "conflict": it stopped at `CodeNotFound = -4`. It now has
    `protocols.CodeConflict = -5`, a `models.ErrNodeAlreadySubscribed`
    sentinel, an `isUniqueViolation` translation in the subscriptions store (a
    deliberate ten-line duplicate of the hub's, because the root module must not
    depend on the cgo driver), and a matching branch in the hub's adapter — so
    the endpoint answers 409. The reply codes are part of the wire protocol, so
    the Python mirror in `nodes/lib/core/handlers.py` has to be kept in step.
16. **An empty node list created an empty subscription (F4, LOW)**.
    `POST /subscriptions/add` with `{"nodes":[]}` answered 200 with a fresh
    `subscription_id` — a row with no members that the API cannot remove. The
    handler now rejects an empty list and any `node.ID == uuid.Nil` with 400.

#### Fixed in the F5 pass — do not reintroduce

17. **The sibling registry forgot nothing (F5, MEDIUM)**. `introduce` added a
    sibling to every peer's registry and nothing ever removed one, so a node
    killed without a graceful `nodes.remove` stayed resolvable for the life of
    the hub process: `GetName` named it, the dispatch proceeded, and every
    attempt spent a full 5s `sinkCommandTimeout` before the sink was treated as
    absent — four cycles in 30s where an unregistered node managed fifteen.
    Both registries now expire an entry `SiblingTTL` (15s, `SIBLING_TTL_SECONDS`
    in `nodes/lib/core/siblings.py`) after the last introduce that carried it.
    Fifteen is the number because introduce is a one-second ticker: fifteen
    consecutive misses is not a slow peer, it is a gone one. Expiry is **lazy**
    — checked and applied on `Get`/`GetName`, not by a sweep goroutine — because
    the question it answers is only ever asked at dispatch, and because five
    processes each owning a ticker plus a shutdown path is a lot of machinery
    for a map of a handful of keys. The cost is that an entry nobody asks about
    lingers, which is one dead key per dead peer. `nodes/lib/core/rpc.py`
    already tried every candidate, so on the node side the gain is not fewer
    failures but not sending the first attempt to an address that is gone.

### B. Operational / robustness gaps

1. **Tests are still thin.** The Python `nodes/lib/core` runtime now has a suite
   (`nodes/tests/`, run by `make test-nodes` and wired into
   `make test`), including the detector seam (`lib/detector.py`, exercised
   against a faked `libreyolo`), and the Go tests cover the RPC reply layer
   (`internal/protocols/handlers_test.go`, the `hub` and `subscriptions`
   `handlers` packages), `internal/backoff`, the sibling wire shape and the
   revision warning, the tx codec preference lists and the shutdown verdict, the
   hub's SQLite user store and signup validation, the user model's
   serialization, and the rx data plane (broadcaster, pending handshakes, sink
   dispatch, address parsing). The two `handlers` packages enumerate the
   commands they advertise and fail when one has no case, so the reply layer
   cannot regress silently. The Python side is also type checked by
   `make typecheck` (pyright, `standard`, over `bustle.py`, `lib/` and
   `tests/`). What no test covers is the wiring rather than the logic: nothing
   drives a real SRT socket or a real GStreamer pipeline, because that needs a
   source, a codec and a peer. `make lint` and `make test` remain the only
   automated gates.
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
   fallback, and `subscriptions` now starts its pool at all. The broadcaster's
   subscriber cap now counts subscriptions rather than topics, and a
   reconnecting node's channel is drained and closed on replacement (E7).
   Still open: bounded pools truncate silently at capacity.
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
6. **Security** (fixed, E9): the committed default JWT secret is gone, the
   accepted algorithm and issuer are pinned, signin is rate limited and no
   longer logs the email, subscription removal is ownership-checked, request
   bodies are capped at 1 MiB with full server timeouts, and the DASH control
   routes require a token (loopback-only otherwise).
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
   collection is created with a check-then-create race. (The Mongo items are
   fixed under E8 — the client is closed after the broker connection and the
   collection is created with `try/except CollectionInvalid` — but the
   `PRAGMA foreign_keys` item is still open.)
8. **`POST /auth/*` local dev** (fixed, E9): the refresh cookie's `Secure`
   attribute follows the new `--cookie-secure` flag (default true) so a
   plain-HTTP `127.0.0.1:9000` deployment can opt out, and one `refreshCookie`
   helper builds signin, refresh and logout cookies alike with
   `SameSite=Lax`.
9. **Python typing/packaging** (fixed, E10): the root `pyproject.toml` declares
   the dependencies (exact pins, torch CPU index via `[tool.uv.sources]`) and
   configures Pyright (`standard`, venv-pinned, `extraPaths = ["nodes"]`);
   `uv.lock` records the transitive graph and `uv sync` is the setup step, so
   `requirements.txt` is gone; `make typecheck` is the gate.
10. **`BustleHandlers` was a class-attribute singleton** (fixed, A1): the
    `setup()` classmethod wrote the pipeline, detector and node id onto the
    class, so a second instance replaced the first's dependencies without
    raising and the first node then counted frames from the second's pipeline.
    It is now an ordinary object constructed with its dependencies and
    registered by `register(handlers_mgr)`; the only mutable state left is the
    per-sink worker task. The defect survived because the module had no test
    file at all — `nodes/tests/test_lib_bustle_handlers.py` is new with the fix.
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
  a standalone `nodes/lib/core/logging_setup.py` while A2 is still open.

## Invariants to preserve when changing code

1. Go and Python protocol implementations must stay wire-compatible: routing
   keys (`rpc.direct.<name>`, `rpc.broadcast`), introduce shape
   (`{"commands": [...]}` + ReplyTo), RouterResponse JSON shape (incl. the
   base64 string convention), manual acks, nack-without-requeue for malformed
   messages. The analytics commands (DESIGN.md §11) are part of that surface:
   `analytics.declare`, `analytics.queries.list` and `analytics.query` are
   mirrored by the node only in the sense that the manifest it publishes must
   stay decodable, so a change to the declaration shape changes both sides in
   the same commit.
2. `internal/` root packages are shared by hub/rx/subscriptions/tx — tx must
   stay independent of the broker and of the DB/pool helpers it does not need.
3. Commands follow `<subject>.<verb>`; node commands embed the stable node
   UUID, not the per-instance sibling name.
4. SRT streamid formats are contracts: `publish:<subscription-uuid>` (ingress)
   and `subscribe:<node-uuid>` (egress).
5. Pooled buffers must be returned exactly once on every path — the
   broadcaster's refcount and the `defer ...ResetAndPut` pairs depend on it.
6. `nodes/lib/core/` must stay node-type-agnostic: no AI/model/database specifics
   may land there (they enter via hooks, the pipeline object, or manifests).
   Enforced as part of the nodes refactor (DESIGN.md §12). `logging_setup.py`
   complies; `pipeline.py` takes its decode chain as a parameter
   (`DEFAULT_DECODE_CHAIN`, `--decode-chain`), so the codec a node type reads is
   its own choice rather than the runtime's.
7. Writer-owned queries: any read model for a node type's data must be
   declared by that node type (query manifest), never coded into the
   analytics service (DESIGN.md §11).
8. Aggregation output must be chart-library-agnostic: generic chart data
   (axes, series, values) only — no rendering-library-specific structure
   (e.g. ECharts option objects) may leak into pipelines, manifests, or
   handler responses (DESIGN.md §8, §11). Nothing in the stack emits one any
   more; the check that keeps it that way lives with the pipelines, in
   `nodes/tests/test_lib_bustle_analytics.py`, because a service-side heuristic
   would be guessing at a shape it cannot see the renderer for.
9. Never log secrets: passwords, tokens, the JWT secret, credential-bearing
   URIs (log scheme://host only), or full RPC/request payloads (length at
   debug). SRT streamids are identifiers, not secrets (DESIGN.md §13).
10. **One UUID storage class per column.** UUIDs in SQLite are 16-byte BLOBs;
    bind `uuid[:]` on both write and read paths (DESIGN.md §10, §A0).
