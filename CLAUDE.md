# CLAUDE.md

This file provides guidance to [CC] () when working with code in this repository.

## Overview

Event-driven AI surveillance system built on GStreamer, SRT, YOLO, MongoDB, SQLite3 and RabbitMQ. Four Go service modules plus a Python "nodes" runtime, all communicating over a RabbitMQ topic exchange.

Deep-dive docs: [ARCHITECTURE.md](ARCHITECTURE.md) (services, data flow, protocol, storage), [DESIGN.md](DESIGN.md) (rationale, trade-offs, invariants, known defects), and [TODO.md](TODO.md) (staged execution tracker). Consult them before non-trivial changes.

## Commands

```sh
make lint          # golangci-lint every module (config: .golangci.yml at repo root)
make test          # go test ./... in every module
make build         # build every cmd/ entrypoint into bin/surr-<name>
make build-tx      # build a single binary (also: build-rx, build-hub, build-subscriptions)
make test-hub      # test one module (also: test-rx, test-subscriptions, test-tx, test-root)
make               # lint + test + build
make clean         # remove .cache/ and bin/

# Build a release binary (stripped, trimpath)
make build-tx RELEASE_TYPE=release

# Run a single service (each is a urfave/cli v3 app; see its cmd/<name>/cmdline.go for flags)
cd hub && go run ./cmd/hub run --help

# Python node (from nodes/)
python bustle.py --help
```

`make build-<name>` builds the module directory whose `cmd/<name>/` contains `main.go`, running `go build` from inside that directory (so the module's own `go.mod` applies) and writing `bin/surr-<name>`. Targets today: `build-hub`, `build-rx`, `build-subscriptions`, `build-tx`. Knobs: `RELEASE_TYPE=release` (adds `-s -w -trimpath`), `TAGS` (build tags), `GOFLAGS`, `LDFLAGS`.

`make test` discovers modules the same way and runs `go test ./...` in each, so a failure in any module fails the target. Knobs: `TESTFLAGS` (e.g. `TESTFLAGS=-race` or `TESTFLAGS=-count=1`) and `TAGS` (`make test TAGS=va` also compiles the vah264enc variant of tx).

There is no `go.work` file; each Go module is self-contained. All four service modules (hub, rx, subscriptions, tx) reach the shared root packages via `replace github.com/booleanism/surr => ../` in their `go.mod`. tx never joins the broker at runtime, but does import the shared root `internal/logging` package at build time.

There are **no Python tests**. Go tests exist but only for the RPC reply layer and the restart backoff (`internal/protocols/handlers_test.go`, `hub/…/handlers/node_test.go`, `subscriptions/…/handlers/subscription_test.go`, `internal/backoff/backoff_test.go`); for everything else `make lint` and `make test` are the only automated gates (golangci-lint, cyclop max-complexity 10, govet enable-all with strict shadowing, gosec, errcheck, gocritic, gofumpt + gci formatting). Run `make` before committing. The two handler tests enumerate the commands their service advertises and fail when one has no case, so adding a handler without a test breaks `make test`. Pyright is *not* installed in `nodes/.venv` and there is no `pyrightconfig.json`, so the "type-check with Pyright" instruction below is aspirational — see TODO.md A0.

## Architecture

### Services and data flow

```
camera (RTSP) → tx ──SRT──→ rx ──SRT──→ nodes (Python, YOLO) ──→ MongoDB (analytics)
                                ↑____________ RabbitMQ ____________↓
                    hub (HTTP API, auth)   subscriptions (SQLite)
```

- **tx** (`tx/`): standalone transmitter. Pulls an RTSP source, transcodes via GStreamer (go-gst), publishes SRT (`srt://host:port?mode=caller&streamid=publish:<subscription id>`) and/or records. Also has a DASH `serve` command. `tx run` requires a `tx serve` DASH server to already be listening, because it registers the stream over HTTP at startup and aborts otherwise.
- **rx** (`rx/`): receiver/relay. Terminates SRT ingress (where tx publishes), exposes SRT egress that nodes pull frames from. Talks to the broker to look up which nodes are subscribed.
- **hub** (`hub/`): control plane. HTTP API (JWT auth w/ argon2, users, nodes, subscriptions handlers) backed by SQLite, plus RabbitMQ handlers. Migrations: `hub/internal/migrations/sqlite/`, applied with the golang-migrate CLI (`migrate -path hub/internal/migrations/sqlite -database "sqlite3://$PWD/hub.db" up`) — the services contain no migration code and never create their own tables.
- **subscriptions** (`subscriptions/`): SQLite-backed service answering which nodes belong to a subscription (e.g. `subscriptions.nodes.get` command). Migrations: `subscriptions/internal/migrations/sqlite/`, applied with the golang-migrate CLI (see the hub bullet) — the service never creates its own tables.
- **nodes** (`nodes/`): Python workers. `bustle.py` is the object counter (YOLO via ultralytics): a GStreamer pipeline (`nodes/core/pipeline.py`) pulls SRT frames, YOLO counts chosen classes, results are stored in MongoDB and served back over the broker. Long-running: SIGTERM/SIGINT trigger a graceful path that stops workers, deregisters the node from the hub (`nodes.remove`), then closes the broker connection. Logging is stdlib `logging` configured by `nodes/core/logging_setup.py`.

### Sibling discovery protocol (the core pattern)

All services implement the same "sibling" RPC protocol over a shared RabbitMQ topic exchange (Go side: `internal/protocols/rabbitmq/router.go`; Python mirror: `nodes/core/router.py`):

1. On startup, each service declares the topic exchange, an exclusive auto-delete queue bound to `rpc.direct.<name>` and `rpc.broadcast`.
2. It broadcasts an `introduce` message (every 1s) advertising its available commands. Peers store these in their siblings registry keyed by routing key (`internal/protocols/siblings.go`, `nodes/core/siblings.py`), so `GetName(command)` resolves which sibling can serve a command.
3. Requests are published with a UUID `CorrelationId` (trackID) and `ReplyTo`; responses come back on the caller's direct routing key and are matched via a pending-trackID map. Python serializes `RouterResponse` with base64-encoded bytes in a JSON string; Go mirrors this in `RouterResponse.GetBytes` / `json.Unmarshal` into `*[]byte`.

Commands are conventionally named `<subject>.<verb>` — node commands embed the node ID: `<node-id>.sink.start`, `<node-id>.sink.stop`, `<node-id>.sink.analytics` (see `nodes/lib/bustle/handlers.py`, `rx/internal/adapter/rabbitmq/nodes.go`). Full command catalog: ARCHITECTURE.md.

### Node registration

Python nodes self-register in the hub's `nodes` table: `nodes.add` on boot (retried until the hub introduces itself) and `nodes.remove` on graceful shutdown. The hub serves both commands (`hub/internal/protocols/rabbitmq/handlers/node.go`) with an idempotent upsert (`ON CONFLICT(id) DO UPDATE` in `hub/internal/db/sqlite/node.go`) so re-registration on restart is safe. On the node side, `nodes/core/rpc.py` provides `call()` — resolve the sibling serving a command, send, await reply with timeout/retries; it tries every candidate sibling, tolerating stale registry entries. Registration is once-per-run: a hub database reset while a node keeps running requires restarting the node (or switching to periodic re-send). A SIGKILL'd node leaves a stale row — the sibling-registry TTL/stale-node sweep to clean these is not implemented.

### Shared root packages (imported by hub, rx, subscriptions, tx)

- `internal/protocols/` — Command/HandlerFunc/Router types, the handlers registry, sibling model
- `internal/protocols/rabbitmq/` — the router (exchange/queue setup, introduce, Send/Recv, delivery dispatch)
- `internal/pools/` — sync.Pool-based object pools (bytes, goroutines/task running, sibling-name slices, nodes, subscriptions); always `Get()` from a pool and `Put()` it back rather than allocating
- `internal/models/` — Node, User, Subscription, SRT models shared across services
- `internal/db/` — SQLite helpers (`Transaction`, `TXFactory`, `VerifySchema`)
- `internal/logging/` — zerolog setup (`Setup`, `RedactURL`) and the `Cleanups` registry
- `internal/backoff/` — exponential backoff with deterministic jitter, used by every restart loop
- `internal/restart/` — `restart.Loop(ctx, name, f)`, the one wrapper every long-running routine runs inside

### Per-module layout

hub and subscriptions follow the same layering: `cmd/<svc>/` (CLI + main wiring) → `internal/protocols/` (transport handlers: http/rabbitmq) → `internal/usecases/` (business logic) → `internal/db/sqlite/` (storage) with `internal/adapter/` bridging usecases to the router. rx is similar minus the DB. tx is its own world: `internal/src` (GStreamer sources), `internal/sink` (srt/dash/etc sinks), `internal/streaming`, `internal/core` (coder/muxer/pipeline), `internal/server/dash`.

### Conventions

- **Resource cleanup**: Go services use `*logging.Cleanups` (`internal/logging/cleanups.go`) — `cleanups.Register(func() error {...})` where the resource is created, `Run()` drains them on context cancellation in registration order. Registration is safe at any time, including while the drain is running. The old `beforeExit []func() error` slice is gone.
- **Long-running routines**: wrap every blocking routine in `restart.Loop(ctx, name, f)` (`internal/restart/`). It re-enters `f` after an exponentially growing, jittered delay (`internal/backoff/`) instead of spinning, logs each failure, and returns once ctx is cancelled. A broker connection that drops is handled by `rabbitmq.Supervisor.Watch`, which redials, re-declares the queue and installs it with `router.ReplaceChannel` — the router reads its channel through `channel()`/`queue()` accessors, so publishes and consumes always use the current one.
- **SQLite schema**: the services do not migrate their own databases. `hub` and `subscriptions` call `db.VerifySchema(ctx, sqlDB, migrationsPath, tables...)` right after opening the connection and exit with one actionable error when a table is missing (TODO E5).
- CLI parsing is hand-rolled on top of urfave/cli v3 into a `CommandLine` struct in `cmd/<svc>/cmdline.go`; new flags go there.
- New broker commands: register a handler on the serving side (`handlers.Set`), which automatically advertises it in `introduce`; callers resolve the sibling via the siblings registry before `Send`/`Recv`.
- **Logging**: zerolog on the Go side via the shared `internal/logging` package — root logger injected into ctx at `main`, packages read `zerolog.Ctx(ctx)`; console writer default, JSON opt-in via `--log-format`; canonical snake_case fields (`command`, `track_id`, `sub_id`, `node_id`, `peer`, ...). Never log passwords, tokens, the JWT secret, credential-bearing URIs (log `scheme://host` via `logging.RedactURL`), or full RPC/request payloads (length at debug). Python nodes stay on stdlib `logging` (`nodes/core/logging_setup.py`). See DESIGN.md §13.
- Python nodes mirror the Go protocol exactly (routing keys, introduce, RouterResponse shape) — changes must stay in sync across `nodes/core/` and `internal/protocols/`.

### Known landmines (verified; see TODO.md section E for the full list)

E0–E6, E8 and E9 (the UUID storage class, the RPC error replies, the RPC
plumbing, shutdown/lifecycle, the restart loops, the storage defects, tx/DASH,
the Python node runtime and the security items) are **fixed in the working tree
but uncommitted** on top of `dc83aad` — read TODO.md section E before touching
those areas, and do not reintroduce them. E7, E10 and E11 are still open.

- **UUID storage class must stay BLOB.** `subscriptions.add` used to insert TEXT while the lookups bound BLOB, and SQLite compares by storage class — the membership queries matched zero rows, so rx rejected every publisher. Every bind is now `id[:]`; keep it that way.
- **Replies go through `protocols.ResultResponse`/`ErrorResponse`.** `RouterResponse.Result` is `json.RawMessage`, so a raw `[]byte(err.Error())` cannot marshal and the failure was silently dropped. The `-1/-2/-3` codes are named constants (`CodeDecodePayload`, `CodeDomain`, `CodeMarshalReply`) and are part of the wire protocol — `nodes/core/handlers.py` mirrors them.
- **Read failures with `ErrorMessage()`, not `GetBytes()`.** A short ASCII message such as `internal` is itself valid base64, so `GetBytes()` returns garbage for exactly the replies it is most likely to be called on.
- **Every RPC registers a trackID and must release it.** `router.Recv` does `defer r.Forget(trackID)`; adapters do the same after `Send`. The reply channel is buffered (cap 1) so a late reply is dropped and Nacked rather than blocking the delivery path.
- **Blocking SRT calls are not cancellable by themselves.** `rx/internal/protocols/srt/listen.go` closes the socket on ctx cancellation because that is the only thing that wakes `srt_epoll_wait`; do not call `sock.Listen`/`Accept` directly from a routine.
- **Both tx encoder build tags must compile.** The `va` and default variants of `encElementWithOptions` must keep the same signature (`bitrate uint32`); `make lint` and `make build` do not pass build tags, so check the `va` variant explicitly with `make test TAGS=va` (or `cd tx && go build -tags va ./... && go vet -tags va ./...`).
- **The hub has no default JWT secret.** `--secret`/`JWT_SECRET` is empty by default and the hub then generates a random per-process secret with a warning (tokens do not survive a restart). Set it explicitly in any deployment.
- **The DASH control routes need `--dash-token`.** `POST`/`DELETE /stream` are behind `requireDashToken`; with no token configured only a loopback caller may register or delete a stream, so widening the bind address cannot silently expose them.
- The analytics pipelines in `nodes/lib/bustle/analytics.py` emit ECharts option objects from the DB, violating DESIGN invariant #8.

Only the RPC reply layer and `internal/backoff` have tests (`internal/protocols/handlers_test.go`, the two `handlers` packages' tests, `internal/backoff/backoff_test.go`). Everything else is exercised by hand. The two `handlers` tests fail when a registered command has no case in their table, so adding a handler means adding a case.
