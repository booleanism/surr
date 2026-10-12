# Architecture

Event-driven, microservices-based AI surveillance system. Five independently
deployable processes — four Go binaries and a Python worker runtime — cooperate
over a RabbitMQ topic exchange ("control plane") while video frames travel over
SRT ("data plane"). Analytics land in MongoDB; control state lives in SQLite.

```
                    CONTROL PLANE (RabbitMQ topic exchange "surr0")
   ┌────────────┬──────────────┬────────────────┬─────────────────────┐
   │            │              │                │                     │
┌──┴───┐    ┌───┴──┐    ┌──────┴──────┐   ┌────┴───────┐      ┌──────┴─────┐
│  tx  │    │  rx  │    │    hub      │   │subscriptions│     │   nodes    │
│      │    │      │    │ (HTTP+JWT,  │   │            │     │ (bustle:   │
│      │    │      │    │  SQLite)    │   │ (SQLite)   │     │  DEIM)     │
└──┬───┘    └──┬───┘    └─────────────┘   └────────────┘     └──────┬─────┘
   │           │                                                   │
   │  SRT      │  SRT                                              │ MongoDB
   │ caller    │  listener                                         │ (timeseries)
   └──────────>┤<──────────────────────────────────────────────────┘
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
| nodes    | Python   | `nodes/`          | AI workers; `bustle.py` is the object counter (LibreDEIMm) |

There is **no go.work**; each Go module is self-contained. All four service
modules (`hub`, `rx`, `subscriptions`, `tx`) reach the shared root packages
via `replace github.com/booleanism/surr => ../`. `tx` never joins the broker —
it remains broker-free at runtime, but does import shared root packages
(`internal/logging`) at build time.

## End-to-end data flow

1. A camera exposes an RTSP feed.
2. `tx run` pulls the RTSP source, depayloads (RTP H264 / AAC), decodes, and:
   - re-encodes + MPEG-TS muxes + pushes over SRT **caller** mode to rx's
     ingress, with `streamid=publish:<subscription-id>`; and
   - writes a DASH recording (mp4 segments + `manifest.mpd`) to disk under
     `<--dir>/<--name>/`, served by `tx serve` or an external HTTP server.
     Note the recording pipeline is built **unconditionally**: `--recording
     0,0,0` only nulls the encoder specs, it does not disable recording.
   `tx run` also registers its stream with the DASH server's HTTP manager API
   at startup, so a `tx serve` instance must already be listening on `--port`
   (default 8000) or `run` aborts.
3. `rx` ingress accepts the SRT connection, parses the streamid, asks the
   `subscriptions` service (over the broker) which nodes are subscribed to that
   subscription, then starts relaying frames.
4. For each subscribed node, `rx` sends `<node-id>.sink.start` carrying the SRT
   egress address; the node connects back to rx's egress socket with
   `streamid=subscribe:<node-id>`.
5. `rx` fans every inbound chunk out to all subscribers of that subscription
   (zero-copy, ref-counted buffers).
6. The node's GStreamer pipeline pulls BGR frames; the detector (LibreDEIMm,
   loaded through LibreYOLO) counts chosen classes per frame; a periodic
   "normalizer" drains per-frame counts, takes the median,
   and inserts one document per interval into a MongoDB timeseries collection
   named after the node ID.
7. Analytics are read back through the hub (`GET /nodes/analytics`) which
   forwards the query over the broker to `<node-id>.sink.analytics`; the node
   runs MongoDB aggregation pipelines that return chart series.
   **Known flaw:** the pipelines currently bake Apache ECharts option
   structure (`xAxis`/`yAxis`/`legend`/`series` with `type: "bar"|"line"`,
   `showSymbol`) into the aggregation `$project` stages — the output is
   ECharts-shaped, not chart-library-agnostic. The fix is part of the planned
   analytics service: generic chart data (axes/categories, series names,
   values), with any rendering library chosen by the presentation layer —
   see "Planned evolution" below and DESIGN.md §8/§11.
   **Planned replacement:** this read path moves to a dedicated analytics
   service — see "Planned evolution" below and DESIGN.md §11.

> ⚠️ Steps 3–5 depend on `subscriptions.nodes.get` / `subscriptions.by.nodeid.get`.
> `subscriptions.add` must write UUIDs in the storage class the lookups bind —
> BLOB, via `id[:]`. Binding TEXT against a BLOB lookup matches zero rows
> silently, which made rx reject every publisher. See "Known defects" below.

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
   (`internal/protocols/siblings.go`, `nodes/lib/core/siblings.py`) keyed by
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
5. **Malformed/duplicate messages** are nacked without requeue. (Go nacks with
   `requeue=true` when it is shutting down; Python always nacks with
   `requeue=false` — a deliberate-looking but undocumented divergence.)

Response codes on the wire are `0` success, `-1` payload decode error, `-2`
domain/usecase error, `-3` response marshal error, `-4` not found. They are
built by `protocols.ResultResponse`/`ErrorResponse` (the error text is
JSON-quoted into `Result`), and the reader for a failure is
`RouterResponse.ErrorMessage()`. A payload is read by unmarshalling `Result`
into the type the caller asked for — there is deliberately no `GetBytes()`
helper, because a short ASCII failure message such as `internal` is itself
valid base64, so a "try base64, fall back to text" decoder returns garbage for
exactly the replies it is most likely to be called on.

Go implementation: `internal/protocols/rabbitmq/router.go` +
`internal/protocols/handlers.go`. Python mirror: `nodes/lib/core/router.py` +
`nodes/lib/core/handlers.py`. Python serializes `RouterResponse` with
base64-encoded bytes in a JSON string, so a Go caller that wants those bytes
declares a `[]byte` target and lets encoding/json do the decode.

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
node UUID rather than the node's per-instance sibling name
(`rpc.direct.<name>`, e.g. `bustle0`) because the UUID is the stable
data-plane identity that survives a rename or restart.

### Node lifecycle

- On boot a node retries `nodes.add` until the hub is discovered (the RPC call
  retries forever until it succeeds — see `nodes/lib/core/rpc.py`). There is no
  backoff, so an error reply that never arrives becomes a 1 Hz retry loop.
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
  disconnects. Ingress listen backlog = the broadcaster's subscription cap
  (`--worker`, default 28). A rejected publish carries an SRT rejection reason:
  `bad request` for a malformed streamid, `forbidden` for a failed lookup,
  `unacceptable` when no node is subscribed, `overload` when the registry is
  full.
- **Egress** (`--egress`, default `127.0.0.1:4201`): nodes connect in caller
  mode with `streamid=subscribe:<node-id>`. The listen callback validates the
  node against `subscriptions.by.nodeid.get` (exactly one subscription must
  match) and rejects with the appropriate SRT rejection reason otherwise
  (`bad request` / `forbidden` / `unacceptable`). The accepted socket is then
  fed from the broadcaster. Egress listen backlog is hard-coded to 10.

Both listen callbacks perform a **broker RPC inside the SRT handshake callback**
(before the connection is accepted), under a 5s timeout, so a saturated pool
delays a handshake rather than parking it indefinitely. Pending handshakes live
in `pending[V]` with a 30s TTL and a 4096-entry cap, so an abandoned handshake
cannot leak an entry for the life of the process. SRT accept cannot be
interrupted by context cancellation:
`rx/internal/protocols/srt/listen.go` closes the socket to break the
`srt_epoll_wait` park, which is why the socket is registered with `Cleanups`.

The **broadcaster** (`rx/internal/protocols/srt/broadcast.go`) is the fan-out
core: `topic[subID][nodeID] -> chan *Chunk`. Chunks wrap a pooled `*[]byte`
with an atomic refcount; the last subscriber to release a chunk returns the
buffer to the bytes pool. Per-subscriber channels are buffered (256) and
non-blocking on publish — a slow node drops chunks (logged on the first drop and
every 1000th) rather than back-pressuring the publisher. Publisher disconnect
(`Close(subID)`) tears down all subscribers of that subscription. The
subscriber cap counts *subscriptions* (one per node, `--worker` again) and
refuses past it with `errMaxSubscribers`; `Subscribe` returns a handle, so a
replaced subscriber's channel is drained and closed instead of leaving its
reader blocked forever. Both loops arm a deadline per iteration (1s idle on
reads, 10s on writes), which is what makes a cancelled ctx or a stalled peer
visible.

## Storage

- **hub SQLite** (`hub.db`): `users`, `nodes` (see
  `hub/internal/migrations/sqlite/`). UUIDs stored as 16-byte BLOBs. Schemas are
  applied out-of-band with the golang-migrate CLI — the service never creates
  its own tables, but it does verify them: `db.VerifySchema` runs at startup and
  exits with one error naming any missing table and the `migrate ... up` command.
- **subscriptions SQLite** (`subscriptions.db`): `subscriptions`,
  `subscribed_nodes` join table, likewise applied with golang-migrate and
  likewise verified at startup. Inserts and reads both bind `ID[:]` (BLOB); the
  write path used to bind `uuid.UUID` values (→ TEXT), which made every
  membership lookup match nothing.
- **MongoDB** (`nodes` DB): one timeseries collection per node (name = node ID
  without dashes; `timeField: timestamp`, `metaField: node`, granularity
  seconds). Documents: `{node: {id, type}, timestamp, counts: {class: n}}`.
  Timestamps are written with a naive local `datetime.now()`. Today nodes both
  write and read this data. **Planned split:** nodes become write-only; a new
  analytics service becomes the sole reader (see below).

## Service internals

### tx (`tx/`)

`cmd/tx/main.go` + `pipeline.go` wire three GStreamer pipelines:

- **producer**: `rtspsrc` → (dynamic pad-added dispatch by caps) → RTP
  depayloaders (`rtph264depay` / `rtpmp4adepay` / `rtpmp4gdepay`) → decoders
  (`openh264dec` by default, `vah264dec` under the `va` build tag; AAC through
  `pickElement` over `fdkaacdec`/`faad`/`avdec_aac`, whichever the runtime has)
  → `intersink`s named `vid`/`aud`. The intersinks hand buffers to
  the other pipelines.
- **downstream**: `intersrc(vid)` → `h264enc` (`videoconvert` + videoscale +
  capsfilter pinning `format=I420` → `openh264enc` by default, `vah264enc`
  under `va`; then `h264parse` with `config-interval=-1`) → `mpegtsmux` →
  `srtsink` (caller mode, `auto-reconnect:true`, URI from `--sink`).
- **recording**: only built when `--recording` is not `0,0,0`. `intersrc(vid)` →
  `h264enc` (+ `intersrc(aud)` → `aacenc`, lazily linked on first audio buffer
  via a pad probe that never sends EOS) → `dashsink2` writing `manifest.mpd` +
  segments under `<recording-dir>/<stream-name>/`.

`serve` runs a small HTTP server listing/playing recorded DASH streams
(dash.js player template), backed by `internal/streaming` + `internal/server/dash`.
On startup it scans the recording directory for `.registered` markers and
repopulates the in-memory stream set, so recordings survive a restart. The
mutating routes (`POST`/`DELETE /stream`) are behind `requireDashToken`
(`--dash-token`): with a token configured the `X-Dash-Token` header must match
(constant-time), and with none only a loopback caller may mutate.

`tx run` registers its stream with a running DASH server's manager API
(`dash.NewStreamManager`, `POST /stream`, forwarding `--dash-token`) as a
best-effort step: if no `tx serve` is up it logs a warning and continues.

Build-tag variants exist for hardware-accelerated codecs
(`h264dec_va.go`, `h264enc_va.go` vs `_default.go`); both variants compile and
must keep identical signatures.

### rx (`rx/`)

`cmd/rx/main.go` wires: RabbitMQ router + siblings + goroutines pool, the two
SRT sockets, the broadcaster, and two outbound adapters
(`internal/adapter/rabbitmq/`): `nodes` (sends `<node-id>.sink.start/stop`) and
`subscriptions` (membership queries). Every long-running component
(router/pool/ingress/egress) runs inside `restart.Loop`, and
`rabbitmq.Supervisor.Watch` redials the broker on `NotifyClose` and reinstalls
the channel with `router.ReplaceChannel`. The SRT sockets are created **once**,
outside those loops, and are both registered with `Cleanups`; because libsrt
parks in `srt_epoll_wait` with an infinite timeout, `restart.Loop` cannot
unblock a routine by cancelling ctx alone — `rx/internal/protocols/srt/listen.go`
closes the socket instead, which is what actually wakes it. A cancelled routine
leaves its socket spent, which is fine: the routine is only stopped at shutdown.

### hub (`hub/`)

`cmd/hub/main.go` wires both transports:

- **RabbitMQ**: `nodes.add` / `nodes.remove` handlers → `usecases.Node` →
  SQLite repos. Outbound adapters for subscription and node analytics calls.
- **HTTP** (`internal/protocols/http/`): per-domain `ServeMux`s mounted under
  `/auth`, `/users`, `/nodes`, `/subscriptions`:
  - `POST /auth/signin|refresh|logout` — JWT access token (HS256, 24h, issuer
    `surr`) + opaque refresh token (32 random bytes, 7-day TTL) stored
    server-side in an in-memory token store; signin is rate limited to 10
    attempts/minute per client address; all three cookies come from one helper
    (HttpOnly, `Secure` per `--cookie-secure`, `SameSite=Lax`, `Path=/auth`).
  - `POST /users/signup`, `GET /nodes/availability`,
    `GET /nodes/analytics`, `POST /subscriptions/add`,
    `POST /subscriptions/remove` (auth-protected). Removing a subscription the
    caller does not own answers 403.

Every request body is capped at 1 MiB (`middlewares.MaxBodyBytes`) and the
server sets Read/Write/Idle timeouts on top of `ReadHeaderTimeout`.

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
- `lib/core/` — protocol runtime mirroring the Go side: `router.py`,
  `siblings.py`, `handlers.py`, `rpc.py` (retrying command-resolving call),
  `pipeline.py` (`SRTStreamer`, the appsink pipeline), `ticker.py`,
  `logging_setup.py` (stdlib logging with a `node_id` filter).
- `lib/bustle/` — the actual worker: `handlers.py`
  (`<node-id>.sink.start/stop/analytics`), `processing.py` (frame_worker thread
  running `detector.detect` + async normalizer draining into MongoDB),
  `analytics.py` (MongoDB aggregation pipelines producing chart series —
  currently ECharts-shaped, a known flaw the planned analytics service fixes
  by returning generic chart data instead).
  `sink.analytics` and `analytics.py` are planned for removal once the
  analytics service lands (see below).
- `lib/detector.py` — the detector seam. `Detector` is a `Protocol`
  (`names: Mapping[int, str]`, `detect(frame) -> list[Detection]`) and
  `LibreYOLODetector` implements it over LibreYOLO's `LibreDEIMm.pt`. Nothing
  downstream sees a LibreYOLO or torch type: `Detection` is a frozen dataclass
  of `class_id`/`confidence`/`xyxy`, so `processing.py` counts plain Python
  values. The library import is inside `LibreYOLODetector.__init__`, so
  importing the module never needs the detector installed. `--conf` is
  forwarded to the model and is the only filter — DEIM decodes top-K with no
  NMS, so overlapping low-confidence boxes are counted separately. Tracking is
  deliberately not used (LibreYOLO's `predict` reports no ids, and the counter
  never read them).
- Pipeline string (`nodes/lib/core/pipeline.py`):
  `srtsrc name=src poll-timeout=10000 ! tsdemux ! h264parse ! avdec_h264 !
  videoconvert ! video/x-raw, format=BGR ! appsink name=app_sink
  emit-signals=true max-buffers=1 drop=true`, connected with
  `srt://<addr>?mode=caller&streamid=subscribe:<node-id>`.
- Environment: `.venv` at the repo root (PyGObject/GStreamer, LibreYOLO,
  pymongo, aio-pika, torch), created by `uv sync` from the root
  `pyproject.toml` — which pins the direct dependencies exactly (plus
  `torchvision`, which nothing imports directly but LibreYOLO pulls in, so it is
  pinned to the same release as the CPU torch wheel) and wires the torch CPU wheel
  through `[tool.uv.sources]`. `uv.lock` holds the full transitive graph.
  `[tool.uv] package = false` keeps `uv sync` to dependencies, because there is
  no packaging step: `nodes/` is a directory of scripts, not a package, so the
  imports are `from lib.core import ...` / `from lib.bustle import ...` with `nodes/`
  as the root, and the node runs as `uv run python nodes/bustle.py` from the repo
  root.
- Tests: `nodes/tests/` (stdlib `unittest`, no pytest) covers the `nodes/lib/core`
  runtime and the detector seam (`lib/detector.py`, with a faked `libreyolo`);
  run it with `make test-nodes`, or from the repo root with
  `.venv/bin/python -m unittest discover -s nodes/tests -t nodes` (the `-t nodes`
  is what puts `nodes/` on `sys.path`). Classes that need the GStreamer bindings
  skip themselves where those are absent.

## Shared root packages (`internal/`, imported by hub/rx/subscriptions/tx)

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
- `internal/logging/` — shared zerolog setup: `--log-level` / `--log-format`
  flags, console (default) or JSON output, root logger with `service`/`family`
  fields injected into ctx, `RedactURL`, and the `Cleanups` registry. See
  DESIGN.md §13.

## Known defects

These are recorded in full in DESIGN.md ("Known limitations") and TODO.md
section E. The 2026-10 review pass reproduced every one of them from the
source; TODO.md section E is the tracker for their status.

### Fixed (E0–E11) — do not reintroduce

| Defect | Effect | Fix |
|---|---|---|
| `subscriptions` UUID TEXT-vs-BLOB mismatch | every membership lookup returned 0 rows → rx rejected all publishers and all nodes | every bind is `id[:]` (BLOB) |
| error replies were unmarshalable (`Result json.RawMessage` set from `[]byte`) | `-1/-2/-3` codes never reached callers; they timed out | `protocols.ResultResponse`/`ErrorResponse` + named `Code*` constants; `ErrorMessage()` to read failures |
| `call()` double-send on a cap-1 error channel (3 adapters) | a failed RPC leaked a pool goroutine and blocked until ctx cancel | `call()` selects on an internal `recvErr` channel; sibling loops try every candidate and `errors.Join` |
| router `knownTrackID` never released on success; late replies went unacked | one map entry leaked per successful RPC | buffered reply channel (cap 1) + `Recv`'s `defer r.Forget(trackID)`; a late reply is Nacked without requeue |
| `hub/internal/protocols/rabbitmq/handlers/node.go` missing `return` after the `-3` reply | two replies to one request | `return` added |
| `subscriptions` `InsertSubscriptions` reused one `Transaction` value | overlapping transactions bled into each other | `BeginTx` serialises callers through a single-slot gate and returns an idempotent release |
| `DeleteSubscriptions` left `subscribed_nodes` rows behind | orphaned rows kept feeding membership queries | deletes both tables in one transaction; empty lists rejected up front |
| `scanRows` truncated silently at `cap(*buf)` | >256 rows dropped quietly | returns `ErrBufferFull` naming the row count |
| restart loops re-entered with no delay and nothing reconnected after a broker drop | hot CPU/log spin with the service dead until restart | `internal/backoff` + `internal/restart.Loop` + `rabbitmq.Supervisor` (`NotifyClose` → redial → `router.ReplaceChannel`) |
| `internal/pools/goroutines.go` `for range p.n - 1` | underflow → ~4.29B goroutines with `--goroutines 0` | `DefaultNGoroutinesPool` fallback. `subscriptions` also never started its pool at all |
| blocking `Listen`/`Accept` could not observe ctx | shutdown had to wait for a connection; the accept goroutine outlived its routine | `rx/internal/protocols/srt/listen.go` closes the socket on cancellation |
| an unmigrated database failed one "no such table" at a time | a fresh checkout looked like a code bug | `db.VerifySchema` at startup names the missing tables and the `migrate ... up` command |
| tx/DASH `Stream` used an atomic ref-count (`block()` busy-spin, unlocked map) | two concurrent DASH requests livelocked the server at 100% CPU | plain `sync.RWMutex` with `Names()`/`IsExist`/`Set`/`Delete` |
| unvalidated stream name joined onto the recording dir | path traversal via `POST`/`DELETE /stream` | `validStreamName` rejects separators, `.`/`..`, over-long names and control chars |
| `AvailStream` was never called | recorded streams were invisible after a `tx serve` restart | `loadRegisteredStreams()` runs from `NewDash` and scans for `.registered` markers |
| `-tags va` did not compile (`h264enc_va.go`, uint32 → uint64) | the va variant was unbuildable | same signature in both variants; `go build -tags va ./...` is part of the gate |
| recording pipeline built even with `--recording 0,0,0` | recording could not be disabled | `setupPipelines` returns a nil recording branch when the specs are zero |
| `tx run` aborted without a reachable `tx serve` | a relay/recording could not start on its own | `registDashStream` logs a warning and continues; registration is best effort |
| audio pad probe emitted EOS per buffer; three `go func()` closures shared the named `err` | recording could be EOS'd early; racy lost errors | probe is buffer-only and returns per-link results; each closure uses a local error |
| `h264enc` lacked `videoconvert`/`format=I420`/`config-interval`; `srtsink` had `auto-reconnect:false` | non-I420 sources failed to negotiate, late SRT joiners missed SPS/PPS, an rx restart killed tx | `videoconvert` + pinned I420 + `config-interval=-1`; `auto-reconnect:true` |
| Python node: dropped `await rep(...)`, blocking GStreamer/pymongo calls on the loop, 100% CPU frame loop, unsynchronized pipeline start/stop, unhandled `insert_one`/detector failures, late replies mis-dispatched as requests, naive timestamps | a caller blocked forever; a transient error silently killed counting or persistence | awaited reply + `Coroutine`-typed `ReplyFunc`; `asyncio.to_thread` for blocking calls; blocking `get_frame(timeout=...)`; a lock around the pipeline; logged failures; an abandoned-trackID set; UTC timestamps |
| frame path: `reshape((height, width, 3))` assumed tightly packed rows, and `get_frame()` lost its non-blocking default when it moved to `get(timeout=...)` | any `video/x-raw` width whose row was not 4-byte aligned (e.g. 1279) raised `ValueError` on the GStreamer streaming thread; an argument-less `get_frame()` blocked the caller forever | `_frame_view` strides rows from `GstVideo.VideoInfo.new_from_caps`, falls back to packed rows and returns `None` when the layout does not fit; the redundant full-frame `.copy()` is gone; `get_frame(timeout=None)` is non-blocking again |
| hub security: committed JWT secret, any-HMAC validation, no signin rate limit, email logged, no ownership check on removal, unbounded bodies, only `ReadHeaderTimeout`, `Secure:true` cookies, unauthenticated DASH control routes | token forgery, brute force, cross-user deletion, slowloris, broken local dev, unauthenticated stream deletion | ephemeral secret + warning, pinned HS256 and issuer, 10/min per-address limiter, ownership enforced inside the `DELETE`, 1 MiB `MaxBytesReader` + Read/Write/Idle timeouts, `--cookie-secure`, `requireDashToken` (loopback-only without a token) |
| detector: ultralytics was the inference engine, and it is **AGPL-3.0** while this repository is MIT; its `Results` object also leaked into the counting code (`boxes.cls`, `isinstance(..., torch.Tensor)`, `model.names`) | publishing the node would have imposed AGPL on the whole repository, and the leak made any swap a cross-cutting edit | a narrow `Detector` Protocol in `nodes/lib/detector.py` (`detect(frame) -> list[Detection]`) with `LibreYOLODetector` behind it, on **LibreYOLO** (MIT code) with **`LibreDEIMm.pt`** weights (Apache-2.0, 55.4 mAP50-95, 19.6 M params, 640 px); DEIM v2 S/M/L/X was rejected because those checkpoints add Meta's non-OSI DINOv3 licence. `--conf` replaces ultralytics' NMS as the only filter, and unused tracking was dropped |
| Python typing/packaging: no `pyproject.toml`, no type-checker config, `requirements.txt` a `pip freeze` dump already divergent from the venv; missing type args on `queue.Queue`/`pymongo.Collection`, untyped `json_serializer(obj)`, an unused `Pipeline` Protocol | the documented type-check gate did not run, so the `None`-returning `GstVideo.VideoInfo.new_from_caps` was unguarded and a fresh venv could resolve different dependency versions | root `pyproject.toml` (exact pins, the torch CPU index, `typeCheckingMode = "standard"`, `venvPath`/`venv`/`extraPaths` pinned) plus `uv.lock` for the transitive graph, and `uv sync` as the one setup command; `make typecheck` (0 errors); `_frame_view` guards `None`; `RouterLike` Protocol in `lib/core/rpc.py`; the type args and the dead Protocol are gone. Never install `gst-python-stubs` — it clobbers the real `Gst.pyi` |
| tx aborted on *every* shutdown, not only on an unreachable source (a GLib 2.84 race in `gio/gthreadedresolver.c` against a NULL state change issued from a thread other than the main loop's) | a source typo looked like a crash loop, and `tx` exited 0 even when the source never connected | the pipelines are never brought to NULL — `loop.Quit()` runs on ctx cancellation and the process exits after `loop.Run()`; `runFailure` turns a bus error into a non-zero exit |
| rx data plane: the subscriber cap counted topics, a replaced node's channel was overwritten without closing, `Subscribe` after `Close` recreated an orphan topic, `Unsubscribe` closed under the registry lock | fan-out dropped or double-counted subscribers; a replaced reader blocked forever | the cap counts subscriptions (`errMaxSubscribers`); `Subscribe` returns a handle and a replacement `drain`s the old channel; only `Open` creates a topic (`errNoPublisher`); the close happens outside the lock |
| rx handshake/lifecycle: blocking broker RPC inside the SRT listen callback, unbounded pending maps, no `Read`/`Write` deadline, no ingress rejection reason, `StartSink`/`StopSink` returned nil | a slow peer wedged accept or a writer; ctx cancellation left nodes streaming; a publisher whose node was down was served nothing and never re-dispatched | 5s lookup timeout; `pending[V]` (30s TTL, 4096 cap); 10s write and 1s read deadlines; `rejectHandshake` codes; `firstErr` propagation plus a 2s `sinkUnavailableDelay` before the publisher is dropped |
| minor: `GetBytes` guessed at base64, an empty `ReplyTo` was refused silently, `models.User.Passwd` was JSON-serializable, a duplicate signup email was a 500, `rx` could not bind IPv6, two tx locals shadowed package names | the argon2id hash crossed the broker; a `password` signup created an empty-password account; failures were confusing | `GetBytes` deleted; the refusal is warned about; `Passwd` is `json:"-"`; `credentials` + `DisallowUnknownFields` + `models.ErrUserExists` → 409; `net.SplitHostPort`; renames |

### Open

Every defect in TODO.md section E is fixed. What remains is tracked in TODO.md
sections A (nodes refactor, packaging) and B (analytics service), plus the
ECharts-shaped analytics output that the analytics service is meant to replace
— none of it a correctness defect in the shipped path.

## Build & tooling

- `make lint` — golangci-lint every module (`.golangci.yml` at repo root:
  cyclop max 10, govet enable-all with strict shadowing, gofumpt/gci).
- `make test` — `go test ./...` in every module, plus `make test-<module>` for
  one. `TESTFLAGS` is appended (e.g. `TESTFLAGS=-race`); `make test TAGS=va`
  also compiles the `vah264enc` variant, which `make lint`/`make build` skip.
  It then runs the Python suite in `nodes/tests`; `make test-nodes` runs that
  alone, `PYTHON=...` picks another interpreter and `NODESTESTFLAGS=-v` adds
  per-test output.
- `make typecheck` — pyright over `nodes/`, configured by the root
  `pyproject.toml` (`typeCheckingMode = "standard"`, `venvPath`/`venv` pinned to
  `.venv` so the third-party imports and the `gi.repository` stubs resolve, and
  `extraPaths = ["nodes"]` so the nodes' `from lib.core import ...` imports resolve
  from a config one level above them). It needs `pyright` and `PyGObject-stubs`
  in that venv (both in the `dev` dependency group, so `uv sync` installs them)
  and must not have `gst-python-stubs`, which ships an overrides-only `Gst.pyi`
  at the same path and silently clobbers the real stubs. `PYRIGHT=` and
  `TYPECHECKFLAGS` override the command and its arguments.
- Python environment — `uv sync` from the repo root creates `./.venv` and
  installs the exact pins from `pyproject.toml` plus the `dev` group; `uv.lock`
  records the resolved transitive graph. `[tool.uv] package = false` keeps it to
  dependencies, since `nodes/` is a directory of scripts rather than a package.
- `make build` — builds every `cmd/` entrypoint into `bin/surr-<name>`;
  `make build-tx` (also build-rx/hub/subscriptions) for one;
  `RELEASE_TYPE=release` strips and trimpaths.
- `make` runs lint, test, typecheck and build in that order.
- Go tests exist for the RPC reply layer and the restart backoff only
  (`internal/protocols/handlers_test.go`, the two `handlers` packages'
  tests, `internal/backoff/backoff_test.go`); lint and those tests are the only
  automated checks for everything else. The two `handlers` tests enumerate the
  commands the service advertises (`AvailableCommand`) and fail when one has no
  case, so registering a handler without covering its reply breaks the build.
- The Python node is covered by `nodes/tests/` (over `nodes/lib/core`: the
  wire shapes, the router's delivery/ack/abandoned-trackID paths, the RPC retry
  ladder, the ticker, logging setup and the GStreamer frame path) and type
  checked by `make typecheck`.

## Planned evolution (not yet implemented on `main`)

Three workstreams have been designed but not built on the canonical branch.
They are recorded here and in DESIGN.md so implementation follows the agreed
shape; TODO.md tracks execution.

> **Note on provenance.** Workstreams 1 and 2 do exist outside `main`: an
> abandoned `feat/analytics-service` branch (surviving only as dangling
> commits and a stash) contains a first `analytics/` Go module, and
> `refs/remotes/gitea/main` contains commit `66e308b "nodes: pythonic refactor"`
> which implements roughly workstream 2 (its runtime package
> `nodes/core/{cli,config,detector,registry,service,storage,stream,worker}.py`
> — the same modules workstream 2 now targets under `nodes/lib/core/` — plus
> `nodes/node.py`, `nodes/workers/bustle/*`, deleting `nodes/lib/bustle/*`).
> `main` is the canonical line and everything below is absent from it.

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
`BustleHandlers`, extract the generic `Node` runtime into `lib/core/node.py`
(owning boot/registration/declare/shutdown), parameterize the decode chain,
fix the frame-worker busy-loop, add a `nodes/` test suite. The Node runtime
gains the `analytics.declare` post-registration hook as part of this work.

### 3. Structured logging (zerolog) — ✅ implemented

All Go services now use zerolog via the shared `internal/logging` package
(imported by all four service modules including tx), with context-propagated
loggers, a documented level and field policy, and a strict never-log list
(credentials, tokens, credential-bearing URIs, full payloads). `internal/logging`
also provides `Cleanups`, the mutex-guarded replacement for the old
`beforeExit` slice that removed the shutdown data races. Python nodes keep
stdlib `logging` (`nodes/lib/core/logging_setup.py`). See DESIGN.md §13 and
TODO.md section C.
