# Architecture

Event-driven, microservices-based AI surveillance system. Five independently
deployable services — four Go binaries and a Python worker runtime — cooperate
over a RabbitMQ topic exchange ("control plane") while video frames travel over
SRT ("data plane"). Analytics land in MongoDB; control state lives in SQLite.

```
                    CONTROL PLANE (RabbitMQ topic exchange "surr0")
   ┌────────────┬──────────────┬────────────────┬─────────────────────┐
   │            │              │                │                     │
   │            │              │                │                     │
┌──┴───┐    ┌───┴──┐    ┌──────┴──────┐   ┌────┴──────┐        ┌──────┴─────┐
│  tx  │    │  rx  │    │    hub      │   │subsriptions│        │ nodes      │
│      │    │      │    │ (HTTP+JWT,  │   │           │        │ (bustle:   │
│      │    │      │    │  SQLite)    │   │ (SQLite)  │        │  YOLO)     │
└──┬───┘    └──┬───┘    └─────────────┘   └───────────┘        └──────┬─────┘
   │           │                                                  │
   │  SRT      │  SRT                                             │ MongoDB
   │ caller    │  listener                                        │ (timeseries)
   └──────────>┤<─────────────────────────────────────────────────┘
              │
        camera RTSP feeds tx; rx fans frames out to every subscribed node
```

## Services

| Service  | Language | Module path       | Role |
|----------|----------|-------------------|------|
| tx       | Go       | `tx/`             | Standalone GStreamer transmitter: RTSP ingest, transcode, SRT publish and/or DASH recording |
| rx       | Go       | `rx/`             | SRT ingress/egress relay and fan-out broker between publishers and AI nodes |
| hub      | Go       | `hub/`            | Control plane: HTTP API (JWT auth, users, nodes, subscriptions, analytics proxy) + broker commands |
| subscriptions | Go  | `subscriptions/`  | SQLite-backed subscription registry answering node-membership queries |
| nodes    | Python  | `nodes/`          | AI workers; `bustle.py` is the YOLO object counter |

There is **no go.work**; each Go module is self-contained. All four service
modules (`hub`, `rx`, `subscriptions`, `tx`) reach the shared root packages
via `replace github.com/booleanism/surr => ../`. `tx` never joins the broker —
it remains broker-free at runtime, but does import shared root packages
(`internal/logging`) at build time.

## End-to-end data flow

1. A camera exposes an RTSP feed.
2. `tx run` pulls the RTSP source, depayloads (RTP H264 / AAC), decodes, and:
   - re-encodes + MPEG-TS muxes + pushes over SRT **caller** mode to rx's
     ingress, with `streamid=publish:<subscription-id>`; and/or
   - writes a DASH recording (mp4 segments + `manifest.mpd`) to disk, served by
     `tx serve` or an external HTTP server.
3. `rx` ingress accepts the SRT connection, parses the streamid, asks the
   `subscriptions` service (over the broker) which nodes are subscribed to that
   subscription, then starts relaying frames.
4. For each subscribed node, `rx` sends `<node-id>.sink.start` carrying the SRT
   egress address; the node connects back to rx's egress socket with
   `streamid=subscribe:<node-id>`.
5. `rx` fans every inbound chunk out to all subscribers of that subscription
   (zero-copy, ref-counted buffers).
6. The node's GStreamer pipeline (`srtsrc ! tsdemux ! h264parse ! avdec_h264 !
   videoconvert ! appsink`) pulls BGR frames; YOLO counts chosen classes per
   frame; a periodic "normalizer" drains per-frame counts, takes the median,
   and inserts one document per interval into a MongoDB timeseries collection
   named after the node ID.
7. Analytics are read back through the hub (`GET /nodes/...`) which forwards the
   query over the broker to `<node-id>.sink.analytics`; the node runs MongoDB
   aggregation pipelines that return chart series.
   **Known flaw:** the pipelines currently bake Apache ECharts option
   structure (`xAxis`/`yAxis`/`legend`/`series` with `type: "bar"|"line"`,
   `showSymbol`) into the aggregation `$project` stages — the output is
   ECharts-shaped, not chart-library-agnostic. The fix is part of the planned
   analytics service: generic chart data (axes/categories, series names,
   values), with any rendering library chosen by the presentation layer —
   see "Planned evolution" below and DESIGN.md §8/§11.
   **Planned replacement:** this read path moves to a dedicated analytics
   service — see "Planned evolution" below and DESIGN.md §11.

## Control plane: the sibling discovery protocol

All broker-connected services (Go and Python) implement the same RPC protocol
over a shared topic exchange (the "family", default `surr0`):

1. **Setup** — each service declares the durable topic exchange, an exclusive
   auto-delete queue bound to `rpc.direct.<name>` (its direct routing key) and
   `rpc.broadcast`.
2. **Introduce** — every second (ticker in each `main`), each service
   broadcasts an `introduce` message (Type=`introduce`, ReplyTo=its direct
   routing key) whose body is the JSON `{"commands": [...]}` list of commands
   it serves. Peers store these in an in-memory siblings registry
   (`internal/protocols/siblings.go`, `nodes/core/siblings.py`) keyed by
   routing key.
3. **Request** — a caller resolves which sibling serves a command
   (`GetName(command)`), then `Send`s a request: new UUID `CorrelationId`
   ("trackID"), `ReplyTo` = caller's direct routing key, Type = command,
   ContentType = `application/json`, body = JSON payload. A per-trackID
   channel/future is parked in a pending map.
4. **Reply** — the serving side dispatches the delivery to its handler (the
   pending map misses → inbound request), the handler replies by publishing a
   `RouterResponse{Result json.RawMessage, Code int}` to the caller's
   `ReplyTo` with the same CorrelationId. The caller matches it via the
   pending map and unblocks `Recv`.
5. **Malformed/duplicate messages** are nacked without requeue.

Go implementation: `internal/protocols/rabbitmq/router.go` +
`internal/protocols/handlers.go`. Python mirror: `nodes/core/router.py` +
`nodes/core/handlers.py`. Python serializes `RouterResponse` with
base64-encoded bytes; Go mirrors this in `RouterResponse.GetBytes`.

### Command catalog

| Command | Served by | Purpose |
|---|---|---|
| `nodes.add` / `nodes.remove` | hub | Node self-(de)registration into hub SQLite |
| `subscriptions.add` / `subscriptions.remove` | subscriptions | Create/delete subscriptions |
| `subscriptions.nodes.get` | subscriptions | Nodes subscribed to a subscription ID |
| `subscriptions.by.nodeid.get` | subscriptions | The subscription a node belongs to |
| `subscriptions.nodes.by.user.get` | subscriptions | Node IDs subscribed by a user |
| `subscriptions.nodes.all.get` | subscriptions | All currently-subscribed nodes |
| `<node-id>.sink.start` / `<node-id>.sink.stop` | node | Start/stop the node's SRT pull pipeline |
| `<node-id>.sink.analytics` | node | Aggregated analytics (currently ECharts-shaped — known flaw, see below) — **planned for removal** |
| `introduce` | everyone | Capability advertisement (protocol-level) |

Planned commands (not yet implemented — see "Planned evolution" and DESIGN.md §11):

| Command | Served by | Purpose |
|---|---|---|
| `analytics.declare` | analytics (planned) | Node declares its query manifest (collection + named aggregation pipelines) |
| `analytics.query` | analytics (planned) | Execute a declared query by name with params; replaces `sink.analytics` |
| `analytics.queries.list` | analytics (planned) | Discover which queries a node exposes |

Commands are conventionally named `<subject>.<verb>`; node commands embed the
node ID because the node ID is the node's broker routing identity for data-plane
addressing.

### Node lifecycle

- On boot a node retries `nodes.add` until the hub is discovered (the RPC call
  retries forever until it succeeds — see `nodes/core/rpc.py`).
- The hub upserts idempotently (`ON CONFLICT(id) DO UPDATE`), so
  re-registration after restart is safe.
- On SIGTERM/SIGINT the node stops workers, sends `nodes.remove`, then closes
  the broker connection. A SIGKILL'd node leaves a stale row in the hub; there
  is no TTL/stale sweep yet.

## Data plane: SRT topology

`rx` owns two SRT listener sockets:

- **Ingress** (`--ingress`, default `127.0.0.1:4200`): publishers (tx) connect
  in caller mode with `streamid=publish:<sub-id>`. The listen callback validates
  the streamid, resolves subscribed nodes via `subscriptions.nodes.get`, and
  stashes the result in a pending map keyed by peer address; the accepted
  socket is then pumped in a goroutine, publishing each read chunk to the
  in-process broadcaster. It also fires `<node-id>.sink.start` (with the egress
  address) at every subscribed node, and `sink.stop` when the publisher
  disconnects.
- **Egress** (`--egress`, default `127.0.0.1:4201`): nodes connect in caller
  mode with `streamid=subscribe:<node-id>`. The listen callback validates the
  node against `subscriptions.by.nodeid.get` (exactly one subscription must
  match) and rejects with the appropriate SRT rejection reason otherwise. The
  accepted socket is then fed from the broadcaster.

The **broadcaster** (`rx/internal/protocols/srt/broadcast.go`) is the fan-out
core: `topic[subID][nodeID] -> chan *Chunk`. Chunks wrap a pooled `*[]byte`
with an atomic refcount; the last subscriber to release a chunk returns the
buffer to the bytes pool. Per-subscriber channels are buffered (256) and
non-blocking on publish — a slow node drops chunks rather than back-pressuring
the publisher. Publisher disconnect (`Close(subID)`) tears down all subscribers
of that subscription.

## Storage

- **hub SQLite** (`hub.db`): `users`, `nodes` (see
  `hub/internal/migrations/sqlite/`). UUIDs stored as 16-byte BLOBs.
- **subscriptions SQLite** (`subscriptions.db`): `subscriptions`,
  `subscribed_nodes` join table. No migration runner — schema is created
  externally/manually.
- **MongoDB** (`nodes` DB): one timeseries collection per node (name = node ID
  without dashes; `timeField: timestamp`, `metaField: node`, granularity
  seconds). Documents: `{node: {id, type}, timestamp, counts: {class: n}}`.
  Today nodes both write and read this data. **Planned split:** nodes become
  write-only; a new analytics service becomes the sole reader (see below).

## Service internals

### tx (`tx/`)

`cmd/tx/main.go` + `pipeline.go` wire three GStreamer pipelines:

- **producer**: `rtspsrc` → (dynamic pad-added dispatch by caps) → RTP
  depayloaders (`rtph264depay` / `rtpmp4adepay` / `rtpmp4gdepay`) → decoders
  (`avdec_h264` or VAAPI build-tag variants, `faad`/AAC) → `intersink`s named
  `vid`/`aud`. The intersinks hand buffers to the other pipelines.
- **downstream**: `intersrc(vid)` → `h264enc` → `mpegtsmux` → `srtsink`
  (caller mode, URI from `--sink`).
- **recording**: `intersrc(vid)` → `h264enc` (+ `intersrc(aud)` → `aacenc`,
  lazily linked on first audio buffer via a pad probe) → `dashsink2` writing
  `manifest.mpd` + segments under `<recording-dir>/<stream-name>/`.

`serve` runs a small HTTP server listing/playing recorded DASH streams
(dash.js player template), backed by `internal/streaming` + `internal/server/dash`.
`tx run` also registers its stream with a running DASH server's manager API
(`/streams` style endpoints via `dash.NewStreamManager`).

Build-tag variants exist for hardware-accelerated codecs
(`h264dec_va.go`, `h264enc_va.go` vs `_default.go`). `tx/dist/run.sh` bundles
GStreamer libs and isolates `GST_PLUGIN_PATH` — use it when running the
packaged binary.

### rx (`rx/`)

`cmd/rx/main.go` wires: RabbitMQ router + siblings + goroutines pool, the two
SRT sockets, the broadcaster, and two outbound adapters
(`internal/adapter/rabbitmq/`): `nodes` (sends `<node-id>.sink.start/stop`) and
`subscriptions` (membership queries). Every long-running component
(router/pool/ingress/egress) runs inside a restart-on-error loop.

### hub (`hub/`)

`cmd/hub/main.go` wires both transports:

- **RabbitMQ**: `nodes.add` / `nodes.remove` handlers → `usecases.Node` →
  SQLite repos. Outbound adapters for subscription and node analytics calls.
- **HTTP** (`internal/protocols/http/`): per-domain `ServeMux`s mounted under
  `/auth`, `/users`, `/nodes`, `/subscriptions`:
  - `POST /auth/signin|refresh|logout` — JWT access token (HS256, 24h) +
    opaque refresh token (32 random bytes, 7-day TTL) stored server-side in an
    in-memory token store; refresh cookie is HttpOnly/Secure/Lax on `/auth`.
  - `POST /subscriptions/add`, `GET /subscriptions/sinks` (auth-protected).
  - user and node handlers (node analytics proxied over the broker).

Layering: `cmd/<svc>` → `internal/protocols/{http,rabbitmq}` (transport) →
`internal/usecases` (business logic) → `internal/db/sqlite` (storage), with
`internal/adapter/rabbitmq` bridging usecases to the router when the hub is the
caller. SQLite access goes through a transaction factory (`internal/db.TXFactory`).

### subscriptions (`subscriptions/`)

No HTTP surface; serves the six `subscriptions.*` broker commands from SQLite.
Same layering as hub minus HTTP.

### nodes (`nodes/`, Python)

- `bustle.py` — CLI entrypoint (argparse), wiring, signal handling, graceful
  shutdown (stop streamer → cancel background tasks → `nodes.remove` → stop
  router listen).
- `core/` — protocol runtime mirroring the Go side: `router.py`,
  `siblings.py`, `handlers.py`, `rpc.py` (retrying command-resolving call),
  `pipeline.py` (SRTStreamer appsink pipeline), `ticker.py`.
- `lib/bustle/` — the actual worker: `handlers.py`
  (`<node-id>.sink.start/stop/analytics`), `processing.py` (frame_worker thread
  running `model.track` + async normalizer draining into MongoDB),
  `analytics.py` (MongoDB aggregation pipelines producing chart series —
  currently ECharts-shaped, a known flaw the planned analytics service fixes
  by returning generic chart data instead).
  `sink.analytics` and `analytics.py` are planned for removal once the
  analytics service lands (see below).
- Environment: `nodes/.venv` (PyGObject/GStreamer, ultralytics YOLO, pymongo,
  aio-pika, torch). Type-check with Pyright against this venv.

## Shared root packages (`internal/`, imported by hub/rx/subscriptions)

- `internal/protocols/` — Command/HandlerFunc/Router types, handlers registry,
  sibling model, RouterResponse wire shape.
- `internal/protocols/rabbitmq/` — the router (exchange/queue setup,
  introduce, Send/Recv, delivery dispatch).
- `internal/pools/` — sync.Pool-based object pools (bytes, goroutines/task
  running, sibling-name slices, nodes, subscriptions). Always `Get()` from a
  pool and `Put()` it back rather than allocating.
- `internal/models/` — Node, User, Subscription, SubscribedNodes, Worker,
  SRT models shared across services.
- `internal/db/` — SQLite transaction-factory helper.

- `internal/logging/` (planned) — shared zerolog setup: `--log-level` /
  `--log-format` flags, console (default) or JSON output, root logger with
  `service`/`family` fields injected into ctx. See DESIGN.md §13.

## Build & tooling

- `make lint` — golangci-lint every module (`.golangci.yml` at repo root:
  cyclop max 10, govet enable-all with strict shadowing, gofumpt/gci).
- `make build` — builds every `cmd/` entrypoint into `bin/surr-<name>`;
  `make build-tx` (also build-rx/hub/subscriptions) for one;
  `RELEASE_TYPE=release` strips and trimpaths.
- No Go tests exist yet; lint is the only automated check.
- Python node has no test suite; verify with Pyright and a manual run.

## Planned evolution (not yet implemented)

Three workstreams have been designed but not built. They are recorded here
and in DESIGN.md so implementation follows the agreed shape; TODO.md tracks
execution.

### 1. Analytics service (CQRS with producer-owned read models)

A new Go service `analytics/` becomes the sole reader of MongoDB; nodes become
write-only. The core principle: **whoever writes the data also writes the
query** — the node declares its read models, the service executes them without
understanding them.

```
node (writer)                         analytics service (reader)
──────────────────────────────        ───────────────────────────────
owns document shape                   owns nothing node-specific
owns write path                       registry: (node_id) → query manifest
owns query manifest (as data)         sanitized executor (operator whitelist)
```

Flow:

1. On boot, after `nodes.add`, the node declares its query manifest over the
   broker: `analytics.declare` with
   `{node_id, collection, queries: [{name, params_schema?, pipeline}]}`.
   Idempotent — re-declared every boot, so upgraded aggregation logic ships
   with the upgraded writer.
2. The service upserts the manifest (SQLite registry) and serves
   `analytics.query {node_id, query_name, params}` — substitutes params,
   appends the time-range `$match`, runs the aggregation read-only, returns
   JSON.
3. `analytics.queries.list {node_id}` lets a presentation layer discover
   queryable names without hardcoding them.
4. The hub's node-analytics endpoint swaps its adapter from
   `<node-id>.sink.analytics` to `analytics.query`.
5. Nodes delete `sink.analytics` handlers and `lib/<type>/analytics.py`.

Consequences:

- The analytics service is AI-type-agnostic by construction; adding a node
  type never touches it or the hub.
- Declared queries return **generic chart data** (axes/categories, series
  names, values) — chart-library-agnostic by contract. The current
  ECharts-shaped output (`xAxis`/`legend`/`series.type` baked into
  aggregations) is a known flaw that dies with `sink.analytics`; the
  presentation layer maps generic data onto whatever chart library it uses.
- Mongo credentials split by role: write-only in nodes, read-only in the
  analytics service — enforced by credentials, not convention.
- The node-type contract shrinks to: decode chain + processing loop +
  document shape + query manifest.
- The service must treat declared pipelines as semi-trusted input: operator
  whitelist (no `$out`/`$merge`/`$unionWith`), stage cap, result-size cap.

### 2. Nodes refactor (type-agnostic runtime extraction)

Staged internal refactor of `nodes/` — no architecture change, wire-compatible
throughout (see TODO.md for the ordered stages). Highlights: de-singleton
`BustleHandlers`, extract the generic `Node` runtime into `core/node.py`
(owning boot/registration/declare/shutdown), parameterize the decode chain,
fix the frame-worker busy-loop, add a `nodes/` test suite. The Node runtime
gains the `analytics.declare` post-registration hook as part of this work.

### 3. Structured logging (zerolog)

All Go services replace ad-hoc `fmt.Printf("INFO:/ERRO:")` output with
zerolog: a shared `internal/logging` package (imported by all four service
modules including tx), context-propagated loggers, a documented level and
field policy, and a strict never-log list (credentials, tokens,
credential-bearing URIs, full payloads). `internal/logging` also provides
`Cleanups`, the mutex-guarded beforeExit replacement that removed the
shutdown data races. Python nodes keep stdlib `logging`;
`bustle.py`'s prints migrate during the nodes refactor. See DESIGN.md §13
and TODO.md section C.
