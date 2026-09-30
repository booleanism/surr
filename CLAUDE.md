# CLAUDE.md

This file provides guidance to [CC] () when working with code in this repository.

## Overview

Event-driven AI surveillance system built on GStreamer, SRT, YOLO, MongoDB, SQLite3 and RabbitMQ. Five Go modules plus a Python "nodes" runtime, all communicating over a RabbitMQ topic exchange.

Deep-dive docs: [ARCHITECTURE.md](ARCHITECTURE.md) (services, data flow, protocol, storage), [DESIGN.md](DESIGN.md) (rationale, trade-offs, planned analytics service & nodes refactor, invariants), and [TODO.md](TODO.md) (staged execution tracker). Consult them before non-trivial changes.

## Commands

```sh
make lint          # golangci-lint every module (config: .golangci.yml at repo root)
make build         # build every cmd/ entrypoint into bin/surr-<name>
make build-tx      # build a single binary (also: build-rx, build-hub, build-subscriptions)
make               # lint + build
make clean         # remove .cache/ and bin/

# Build a release binary (stripped, trimpath)
make build-tx RELEASE_TYPE=release

# Run a single service (each is a urfave/cli v3 app; see its cmd/<name>/cmdline.go for flags)
cd hub && go run ./cmd/hub run --help

# Python node (from nodes/)
python bustle.py --help
```

There is no go.work file; each Go module is self-contained. All four service modules (hub, rx, subscriptions, tx) reach the shared root packages via `replace github.com/booleanism/surr => ../` in their go.mod. tx never joins the broker at runtime, but does import shared root packages (`internal/logging`) at build time. There are no Go tests in the repo yet; lint is the only automated check (golangci-lint with strict settings: cyclop max 10, govet enable-all with strict shadowing — run `make lint` before committing).

## Architecture

### Services and data flow

```
camera (RTSP) → tx ──SRT──→ rx ──SRT──→ nodes (Python, YOLO) ──→ MongoDB (analytics)
                                ↑____________ RabbitMQ ____________↓
                    hub (HTTP API, auth)   subscriptions (SQLite)
```

- **tx** (`tx/`): standalone transmitter. Pulls an RTSP source, transcodes via GStreamer (go-gst), publishes SRT (`srt://host:port?mode=caller&streamid=publish:<subscription id>`) and/or records. Also has a DASH `serve` command. `tx/dist/run.sh` bundles GStreamer libs and isolates `GST_PLUGIN_PATH` — use it when running the packaged binary.
- **rx** (`rx/`): receiver/relay. Terminates SRT ingress (where tx publishes), exposes SRT egress that nodes pull frames from. Talks to the broker to look up which nodes are subscribed.
- **hub** (`hub/`): control plane. HTTP API (JWT auth w/ argon2, users, nodes, subscriptions handlers) backed by SQLite, plus RabbitMQ handlers. Migrations: `hub/internal/migrations/sqlite/`.
- **subscriptions** (`subscriptions/`): SQLite-backed service answering which nodes belong to a subscription (e.g. `subscriptions.nodes.get` command). No migration runner — its schema (`subscriptions`, `subscribed_nodes` tables) is created externally.
- **nodes** (`nodes/`): Python workers. `bustle.py` is the object counter (YOLO via ultralytics): a GStreamer pipeline (`nodes/core/pipeline.py`) pulls SRT frames, YOLO counts chosen classes, results are stored in MongoDB and served back over the broker. Long-running: SIGTERM/SIGINT trigger a graceful path that stops workers, deregisters the node from the hub (`nodes.remove`), then closes the broker connection. Type-check with Pyright against `nodes/.venv`.

### Sibling discovery protocol (the core pattern)

All services implement the same "sibling" RPC protocol over a shared RabbitMQ topic exchange (Go side: `internal/protocols/rabbitmq/router.go`; Python mirror: `nodes/core/router.py`):

1. On startup, each service declares the topic exchange, an exclusive queue bound to `rpc.direct.<name>` and `rpc.broadcast`.
2. It broadcasts an `introduce` message advertising its available commands. Peers store these in their siblings registry keyed by routing key (`internal/protocols/siblings.go`, `nodes/core/siblings.py`), so `GetName(command)` resolves which sibling can serve a command.
3. Requests are published with a UUID `CorrelationId` (trackID) and `ReplyTo`; responses come back on the caller's direct routing key and are matched via a pending-trackID map. Python serializes `RouterResponse` with base64-encoded bytes; Go mirrors this in `RouterResponse.GetBytes`.

Commands are conventionally named `<subject>.<verb>` — node commands embed the node ID: `<node-id>.sink.start`, `<node-id>.sink.stop`, `<node-id>.sink.analytics` (see `nodes/lib/bustle/handlers.py`, `rx/internal/adapter/rabbitmq/nodes.go`). Full command catalog: ARCHITECTURE.md.

### Node registration

Python nodes self-register in the hub's `nodes` table: `nodes.add` on boot (retried until the hub introduces itself) and `nodes.remove` on graceful shutdown. The hub serves both commands (`hub/internal/protocols/rabbitmq/handlers/node.go`) with an idempotent upsert (`ON CONFLICT(id) DO UPDATE` in `hub/internal/db/sqlite/node.go`) so re-registration on restart is safe. On the node side, `nodes/core/rpc.py` provides `call()` — resolve the sibling serving a command, send, await reply with timeout/retries; it tries every candidate sibling, tolerating stale registry entries. Registration is once-per-run: a hub database reset while a node keeps running requires restarting the node (or switching to periodic re-send). A SIGKILL'd node leaves a stale row — the sibling-registry TTL/stale-node sweep to clean these is not implemented.

### Shared root packages (imported by hub, rx, subscriptions)

- `internal/protocols/` — Command/HandlerFunc/Router types, the handlers registry, sibling model
- `internal/protocols/rabbitmq/` — the router (exchange/queue setup, introduce, Send/Recv, delivery dispatch)
- `internal/pools/` — sync.Pool-based object pools (bytes, goroutines/task running, sibling-name slices, nodes, subscriptions); always `Get()` from a pool and `Put()` it back rather than allocating
- `internal/models/` — Node, User, Subscription, SRT models shared across services
- `internal/db/` — SQLite helpers

### Per-module layout

hub and subscriptions follow the same layering: `cmd/<svc>/` (CLI + main wiring) → `internal/protocols/` (transport handlers: http/rabbitmq) → `internal/usecases/` (business logic) → `internal/db/sqlite/` (storage) with `internal/adapter/` bridging usecases to the router. rx is similar minus the DB. tx is its own world: `internal/src` (GStreamer sources), `internal/sink` (srt/dash/etc sinks), `internal/streaming`, `internal/core` (coder/muxer/pipeline), `internal/server/dash`.

### Conventions

- Go services use a `beforeExit []func() error` cleanup list drained on context cancellation; register resource cleanup there.
- CLI parsing is hand-rolled on top of urfave/cli v3 into a `CommandLine` struct in `cmd/<svc>/cmdline.go`; new flags go there.
- New broker commands: register a handler on the serving side (`handlers.Set`), which automatically advertises it in `introduce`; callers resolve the sibling via the siblings registry before `Send`/`Recv`.
- Logging (planned): zerolog on the Go side via a shared `internal/logging` package — root logger injected into ctx at `main`, packages read `zerolog.Ctx(ctx)`; console writer default, JSON opt-in; canonical snake_case fields (`command`, `track_id`, `sub_id`, `node_id`, `peer`, ...). Never log passwords, tokens, the JWT secret, credential-bearing URIs (log scheme://host), or full RPC/request payloads (length at debug). See DESIGN.md §13 / TODO.md section C. Python side stays on stdlib `logging`.
- Python nodes mirror the Go protocol exactly (routing keys, introduce, RouterResponse shape) — changes must stay in sync across `nodes/core/` and `internal/protocols/`.
