# AGENTS.md

This file provides guidance to AI coding agents working in this repository. It is read before every task, so it is deliberately slow-moving: it holds the commands, the layout, the conventions and the invariants. Status — what is fixed, what is still open — lives in [TODO.md](TODO.md), not here.

## Overview

Event-driven AI surveillance system built on GStreamer, SRT, LibreYOLO, MongoDB, SQLite3 and RabbitMQ. Four Go service modules plus a Python "nodes" runtime, all communicating over a RabbitMQ topic exchange.

Deep-dive docs: [ARCHITECTURE.md](ARCHITECTURE.md) (services, data flow, protocol, storage), [DESIGN.md](DESIGN.md) (rationale, trade-offs, invariants), and [TODO.md](TODO.md) (staged execution tracker and fix status). Consult them before non-trivial changes.

## Commands

```sh
make lint          # golangci-lint every module (config: .golangci.yml at repo root)
make test          # test-go + test-nodes
make test-go       # go test ./... in every module (no python needed; this is what CI runs)
make test-nodes    # python tests for nodes/lib/core (nodes/tests; needs ./.venv)
make typecheck     # pyright over nodes/ (config: ./pyproject.toml; needs ./.venv)
make build         # build every cmd/ entrypoint into bin/surr-<name>
make               # lint + test + typecheck + build
make clean         # remove .cache/ and bin/

# One module / one binary
make build-tx      # also: build-rx, build-hub, build-subscriptions
make test-hub      # also: test-rx, test-subscriptions, test-tx, test-root

# Bring the whole system up (RabbitMQ, MongoDB, migrators, every service).
# deploy/README.md is the runbook, including the RTSP source the repo does not contain.
cd deploy && VERSION=$(git describe --tags --always --dirty) docker compose --profile tx --profile nodes up -d

# Same, but with a synthetic RTSP source, so no camera and no external container is needed.
cd deploy && VERSION=$(git describe --tags --always --dirty) docker compose --profile demo --profile tx --profile nodes up -d

# Assert what `docker compose ps` cannot show: the images are the build you think they are,
# the data plane works, tx exits 0 on SIGTERM, and an absent node does not make rx spin.
cd deploy && ./smoke.sh

# Build a release binary (stripped, trimpath)
make build-tx RELEASE_TYPE=release

# Run a single service (each is a urfave/cli v3 app; see its cmd/<name>/cmdline.go for flags)
cd hub && go run ./cmd/hub run --help

# Python node (from the repo root; uv sync creates .venv/)
uv sync
uv run python nodes/bustle.py --help
```

Knobs: `RELEASE_TYPE=release` (adds `-s -w -trimpath`), `TAGS` (build tags), `GOFLAGS`, `LDFLAGS`, `VERSION`, `TESTFLAGS` (e.g. `-race`, `-count=1`), `NODESTESTFLAGS`, `PYTHON`, `PYRIGHT`, `TYPECHECKFLAGS`.

`VERSION` defaults to `git describe --tags --always --dirty` and is injected into every binary with `-X main.Version`, so `--version` and the `version=` field on every log line report the build. `LDFLAGS` appends to it rather than replacing it. The Python node resolves the same string independently (`nodes/lib/version.py`), which is what makes a mixed deployment's two build ids comparable. Do not reintroduce a hand-written version constant: the point of the injection is that no constant has to be kept in sync by hand.

CI is `.github/workflows/ci.yml`: `make lint`, `make test-go` and `make build` on one runner (cgo + libsrt + GStreamer), `make test-nodes` and `make typecheck` on another (PyGObject built from source). `make test` is the two halves combined.

`make build-<name>` builds the module directory whose `cmd/<name>/` contains `main.go`, running `go build` from inside that directory (so the module's own `go.mod` applies) and writing `bin/surr-<name>`. `make test` discovers modules the same way, so a failure in any module fails the target. `make test-nodes` uses `./.venv/bin/python` when it exists (`PYTHON=...` overrides) and fails with instructions when that interpreter cannot import `aio_pika`/`numpy`.

`make typecheck` runs pyright from the repo root against `./pyproject.toml`. `venvPath`/`venv` in that file are load-bearing — pyright does not pick the venv up merely because it was launched from the venv's own `bin/` — and so is `extraPaths = ["nodes"]`, which is what resolves the nodes' `from lib.core import ...` imports from a config file that lives one level above them. `typeCheckingMode = "standard"` is the level the runtime is actually held to. The gate is in `make` but deliberately not in `lint`/`test`, which stay usable on a Go-only checkout.

The Python side is a uv project: `./pyproject.toml` declares the dependencies (pinned exactly, with the torch CPU index wired up through `[tool.uv.sources]`) and `./uv.lock` pins the full transitive graph. The venv is `./.venv` — at the repo root, not in `nodes/` — while all the node code stays in `nodes/`. `[tool.uv] package = false` is what lets `uv sync` install the dependencies without trying to build the project, since `nodes/` is a directory of scripts rather than an importable package.

There is no `go.work` file; each Go module is self-contained. All four service modules (hub, rx, subscriptions, tx) reach the shared root packages via `replace github.com/booleanism/surr => ../` in their `go.mod`, so a new cross-module import needs no `go.mod` edit while a new third-party dependency does. tx never joins the broker at runtime, but does import the shared root `internal/logging` package at build time.

## Architecture

### Services and data flow

```
camera (RTSP) → tx ──SRT──→ rx ──SRT──→ nodes (Python, DEIM) ──→ MongoDB (analytics)
                                ↑____________ RabbitMQ ____________↓
                    hub (HTTP API, auth)   subscriptions (SQLite)
```

- **tx** (`tx/`): standalone transmitter. Pulls an RTSP source, transcodes via GStreamer (go-gst), publishes SRT (`srt://host:port?mode=caller&streamid=publish:<subscription id>`) and/or records. Also has a DASH `serve` command. `tx run` requires a `tx serve` DASH server to already be listening, because it registers the stream over HTTP at startup and aborts otherwise.
- **rx** (`rx/`): receiver/relay. Terminates SRT ingress (where tx publishes), exposes SRT egress that nodes pull frames from. Talks to the broker to look up which nodes are subscribed.
- **hub** (`hub/`): control plane. HTTP API (JWT auth w/ argon2, users, nodes, subscriptions handlers) backed by SQLite, plus RabbitMQ handlers.
- **subscriptions** (`subscriptions/`): SQLite-backed service answering which nodes belong to a subscription (e.g. the `subscriptions.nodes.get` command).
- **nodes** (`nodes/`): Python workers. `bustle.py` is the object counter (object detection via LibreYOLO): a GStreamer pipeline (`nodes/lib/core/pipeline.py`) pulls SRT frames, the detector counts chosen classes, results are stored in MongoDB and served back over the broker. The detector sits behind a narrow `Detector` Protocol in `nodes/lib/detector.py` (`detect(frame) -> list[Detection]`) whose only implementation loads LibreYOLO's `LibreDEIMm.pt`; keep it that way — nothing outside that file may import `libreyolo` or touch torch types. Long-running: SIGTERM/SIGINT trigger a graceful path that stops workers, deregisters the node from the hub (`nodes.remove`), then closes the broker connection. Logging is stdlib `logging` configured by `nodes/lib/core/logging_setup.py`. `nodes/` is not a package (no `nodes/__init__.py`): the imports are `from lib.core import ...` / `from lib.bustle import ...` with `nodes/` as the root, and Python puts a script's own directory on `sys.path`, so `uv run python nodes/bustle.py` from the repo root resolves them as-is. The dependencies and the pyright configuration live in the root `./pyproject.toml`, and the venv is the root `./.venv`.

### Sibling discovery protocol (the core pattern)

All services implement the same "sibling" RPC protocol over a shared RabbitMQ topic exchange (Go side: `internal/protocols/rabbitmq/router.go`; Python mirror: `nodes/lib/core/router.py`):

1. On startup, each service declares the topic exchange, an exclusive auto-delete queue bound to `rpc.direct.<name>` and `rpc.broadcast`.
2. It broadcasts an `introduce` message (every 1s) advertising its available commands. Peers store these in their siblings registry keyed by routing key (`internal/protocols/siblings.go`, `nodes/lib/core/siblings.py`), so `GetName(command)` resolves which sibling can serve a command.
3. The same message carries `ProtocolRevision` and the sender's build id. A peer on a different revision is logged as a warning and still registered — a mismatch must be diagnosable, not fatal, so it is on the record before it surfaces as an unexplained failure on some later command. `internal/protocols/siblings.go` holds the number and the bump rule; `nodes/lib/core/siblings.py` mirrors both and must be changed in the same commit.
4. Requests are published with a UUID `CorrelationId` (trackID) and `ReplyTo`; responses come back on the caller's direct routing key and are matched via a pending-trackID map. Python serializes `RouterResponse` with base64-encoded bytes in a JSON string; a Go caller that wants those bytes declares a `[]byte` target and lets encoding/json do the decode.

Commands are conventionally named `<subject>.<verb>` — node commands embed the node ID: `<node-id>.sink.start`, `<node-id>.sink.stop`, `<node-id>.sink.analytics` (see `nodes/lib/bustle/handlers.py`, `rx/internal/adapter/rabbitmq/nodes.go`). Full command catalog: ARCHITECTURE.md.

### Node registration

Python nodes self-register in the hub's `nodes` table: `nodes.add` on boot (retried until the hub introduces itself) and `nodes.remove` on graceful shutdown. The hub serves both commands (`hub/internal/protocols/rabbitmq/handlers/node.go`) with an idempotent upsert (`ON CONFLICT(id) DO UPDATE` in `hub/internal/db/sqlite/node.go`) so re-registration on restart is safe. On the node side, `nodes/lib/core/rpc.py` provides `call()` — resolve the sibling serving a command, send, await reply with timeout/retries; it tries every candidate sibling, tolerating stale registry entries. Registration is once-per-run: a hub database reset while a node keeps running requires restarting the node (or switching to periodic re-send). A SIGKILL'd node leaves a stale row — the sibling-registry TTL/stale-node sweep to clean these is not implemented, and it is TODO.md F5: the stale entry keeps `GetName` returning a candidate, so a dispatch to a dead node costs a full 5s `sinkCommandTimeout` before the sink is treated as unavailable instead of failing at once.

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

Top level: `internal/` is the shared module root (protocols, pools, models, db, logging, restart) that the four service modules import through the `replace` directive; `nodes/` is the Python runtime (`nodes/lib/core` is the type-agnostic runtime, `nodes/lib/<type>` the worker); `deploy/` is the compose stack and its Dockerfiles; `.github/workflows/ci.yml` is CI. `deploy/` is the only place that knows how the services are wired to each other in a real deployment — keep it in step with new flags and new broker commands. `deploy/smoke.sh` is the end-to-end check for that wiring: it is the only thing in the repository that asserts against a *running* system, so when you change a flag, a port, a startup order or a shutdown path, it is what catches the break.

### Conventions

- **Resource cleanup**: Go services use `*logging.Cleanups` (`internal/logging/cleanups.go`) — `cleanups.Register(func() error {...})` where the resource is created, `Run()` drains them on context cancellation in registration order. Registration is safe at any time, including while the drain is running.
- **Long-running routines**: wrap every blocking routine in `restart.Loop(ctx, name, f)` (`internal/restart/`). It re-enters `f` after an exponentially growing, jittered delay (`internal/backoff/`) instead of spinning, logs each failure, and returns once ctx is cancelled. A broker connection that drops is handled by `rabbitmq.Supervisor.Watch`, which redials, re-declares the queue and installs it with `router.ReplaceChannel` — the router reads its channel through `channel()`/`queue()` accessors, so publishes and consumes always use the current one.
- **SQLite schema**: the services do not migrate their own databases and never create their own tables. Migrations live in `<svc>/internal/migrations/sqlite/` and are applied with the golang-migrate CLI (`migrate -path hub/internal/migrations/sqlite -database "sqlite3://$PWD/hub.db" up`); `hub` and `subscriptions` call `db.VerifySchema(ctx, sqlDB, migrationsPath, tables...)` right after opening the connection and exit with one actionable error when a table is missing.
- CLI parsing is hand-rolled on top of urfave/cli v3 into a `CommandLine` struct in `cmd/<svc>/cmdline.go`; new flags go there.
- New broker commands: register a handler on the serving side (`handlers.Set`), which automatically advertises it in `introduce`; callers resolve the sibling via the siblings registry before `Send`/`Recv`.
- **Logging**: zerolog on the Go side via the shared `internal/logging` package — root logger injected into ctx at `main`, packages read `zerolog.Ctx(ctx)`; console writer default, JSON opt-in via `--log-format`; canonical snake_case fields (`command`, `track_id`, `sub_id`, `node_id`, `peer`, ...). Never log passwords, tokens, the JWT secret, credential-bearing URIs (log `scheme://host` via `logging.RedactURL`), or full RPC/request payloads (length at debug). Python nodes stay on stdlib `logging` (`nodes/lib/core/logging_setup.py`). See DESIGN.md §13.
- Python nodes mirror the Go protocol exactly (routing keys, introduce, RouterResponse shape) — changes must stay in sync across `nodes/lib/core/` and `internal/protocols/`.

### Invariants (do not break)

The `E`-numbered items in TODO.md section E are the historical record of how each of these was violated. The rules are what matter; none of them depend on the story.

- **UUID storage class must stay BLOB.** SQLite compares by storage class, so an insert binding TEXT against a lookup binding BLOB matches zero rows *silently*. Every bind is `id[:]`; keep it that way.
- **Replies go through `protocols.ResultResponse`/`ErrorResponse`.** `RouterResponse.Result` is `json.RawMessage`, so a raw `[]byte(err.Error())` cannot marshal and the failure is silently dropped. The `-1/-2/-3` codes are named constants (`CodeDecodePayload`, `CodeDomain`, `CodeMarshalReply`) and are part of the wire protocol — `nodes/lib/core/handlers.py` mirrors them.
- **Read a failure with `ErrorMessage()`; read a payload by unmarshalling `Result` into the type you asked for.** There is deliberately no `GetBytes()`-style helper. A short ASCII failure message such as `internal` is itself valid base64, so any decoder that "tries base64 and falls back to text" returns garbage for exactly the replies it is most likely to be called on; the helper was deleted rather than made clever. A `[]byte` target is what makes encoding/json base64-decode a Python node's bytes result.
- **Request bodies decode into `handlers.credentials`, never into `models.User`.** `models.User.Passwd` is `json:"-"`, so a whole-`User` marshal — which `hub/internal/adapter/rabbitmq/subscription.go` does, to send the `subscriptions.nodes.by.user.get` payload — cannot carry the argon2id hash. Signup and signin use `decodeCredentials`, which sets `DisallowUnknownFields` (so `password` is a 400 rather than a silently empty account) and requires both fields; a duplicate email is `models.ErrUserExists` → 409, matched on SQLite's *extended* constraint code because `SQLITE_CONSTRAINT` also covers NOT NULL, CHECK and foreign keys.
- **Every RPC registers a trackID and must release it.** `router.Recv` does `defer r.Forget(trackID)`; adapters do the same after `Send`. The reply channel is buffered (cap 1) so a late reply is dropped and Nacked rather than blocking the delivery path.
- **A pool-dispatched RPC must hand its outcome back over a channel.** `pools.RunTask.Run` only enqueues the task and returns nil — it never reports what the task did. So `Run(ctx, func() { if err := roundTrip(); err != nil { return } })` returns nil on *every* path, and a caller that trusts it reports success for a node that never answered. `rx/internal/adapter/rabbitmq/nodes.go` `do()` and `hub/internal/adapter/rabbitmq/subscription.go` `call()` both send the result over a `make(chan error, 1)` and wait for it; the buffer is what stops a reply arriving after the caller gave up from blocking a pool worker. The RPC carries its own `context.WithTimeout` for the same reason: without one, `router.Recv` parks on `ctx.Done()` until the *process* context ends, retiring a pool worker per stale dispatch (`rx` has `--worker 28` of them).
- **A non-zero reply code is a failure, not a note.** Both adapters turn `resp.Code != protocols.CodeSuccess` into an error, because a node that refuses a command (`invalid source address`) must not look like a started sink. Add a code to `internal/protocols/handlers.go` and map it in the adapter when a new class of failure has to reach HTTP — `CodeConflict` (`-5`) is what makes `POST /subscriptions/add` answer 409 instead of 500 for a node that is already subscribed. The codes are part of the wire protocol: `nodes/lib/core/handlers.py` mirrors them.
- **Blocking SRT calls are not cancellable by themselves.** `rx/internal/protocols/srt/listen.go` closes the socket on ctx cancellation because that is the only thing that wakes `srt_epoll_wait`; do not call `sock.Listen`/`Accept` directly from a routine. The same applies to an established stream: a bare `sock.Read`/`sock.Write` in a loop never returns on cancellation, so both loops arm a deadline per iteration (1s idle for reads, 10s for writes) and treat a `net.Error` timeout as a tick. This works only because `srtgo` sockets are created non-blocking — a blocking socket ignores the deadline.
- **The SRT fan-out is reference-counted and the registry owns the close.** `broadcaster.Publish` hands one pooled buffer to every subscriber with `refs` set to the subscriber count; each `Chunk.release()` returns it to the pool exactly once. `Subscribe` returns a `*subscription` handle and only `Unsubscribe(handle)` may close a channel — it refuses a handle that is no longer registered, which is what stops a replaced subscriber's deferred call from closing its successor's channel. Never close a subscriber channel anywhere else.
- **Both tx encoder build tags must compile.** The `va` and default variants of `encElementWithOptions` must keep the same signature (`bitrate uint32`); `make lint` and `make build` do not pass build tags, so check the `va` variant explicitly with `make test TAGS=va` (or `cd tx && go build -tags va ./... && go vet -tags va ./...`).
- **Never hardcode a GStreamer element name that may not be installed.** `gst.ElementFactoryMake` returns nil for a factory the distribution does not ship, and the first `Add`/`Link`/`GetStaticPad` through it is a SIGSEGV, not an error. Go through `pickElement` (`tx/internal/core/coder/element.go`) with a preference list, check `Valid()`, and report what was missing. The concrete trap: `fdkaacdec`/`fdkaacenc` exist only where the non-free fdk-aac was built in, so a developer machine with deb-multimedia has them and a stock Debian image does not.
- **`tx run` never brings its pipelines to NULL.** The shutdown path deliberately leaves them in PLAYING and exits after `loop.Run()` returns, because the NULL transition disposes the RTSP source and races GIO's threaded resolver on GLib 2.84 (`g_assert (g_cancellable_is_cancelled (cancellable))` in `gio/gthreadedresolver.c`, which segfaults on a clean SIGTERM as much as on an unreachable source). Re-adding `SetState(gst.StateNull)` — from the loop thread or from a cleanup hook — reintroduces the crash. The accepted cost is that an interrupted `--recording` needs remuxing, and `runFailure` is what turns a bus error into a non-zero exit.
- **`ProtocolRevision` is mirrored in two languages.** `internal/protocols/siblings.go` and `nodes/lib/core/siblings.py` hold the same number and the same bump rule; bump both in the same commit. Adding an *optional* field is not a bump — an older peer ignores what it does not know.
- **The hub has no default JWT secret.** `--secret`/`JWT_SECRET` is empty by default and the hub then generates a random per-process secret with a warning (tokens do not survive a restart). Set it explicitly in any deployment.
- **The DASH control routes need `--dash-token`.** `POST`/`DELETE /stream` are behind `requireDashToken`; with no token configured only a loopback caller may register or delete a stream, so widening the bind address cannot silently expose them.
- **Never install `gst-python-stubs` in the node venv.** It ships an overrides-only `Gst.pyi` at the same path as the real `PyGObject-stubs` file and silently clobbers it, which turns every GStreamer enum and constructor into an unknown-attribute error. See `./pyproject.toml`.
- The analytics pipelines in `nodes/lib/bustle/analytics.py` emit ECharts option objects from the DB, violating DESIGN invariant #8.

## Testing

`make lint`, `make test` and `make typecheck` are the gates — run `make` before committing. Go coverage is narrow and deliberate: the RPC reply layer and the restart backoff (`internal/protocols/handlers_test.go`, `internal/backoff/backoff_test.go`), the sibling wire shape and the revision warning (`internal/protocols/siblings_test.go`, `internal/protocols/rabbitmq/router_test.go`), the tx codec preference lists and the shutdown verdict (`tx/internal/core/coder/element_test.go`, `tx/cmd/tx/main_test.go`), the hub's SQLite user store and signup validation (`hub/internal/db/sqlite/user_test.go`, `hub/internal/protocols/http/handlers/users_test.go`), the user model's serialization (`internal/models/user_test.go`), the rx data plane (`rx/internal/protocols/srt/broadcast_test.go`, `rx/internal/protocols/srt/pending_test.go`, `rx/internal/adapter/rabbitmq/nodes_test.go`, `rx/cmd/rx/cmdline_test.go`), the hub's subscription adapter and add-subscription validation (`hub/internal/adapter/rabbitmq/subscription_test.go`, `hub/internal/protocols/http/handlers/subscriptions_test.go`), the subscriptions store's duplicate-node translation (`subscriptions/internal/db/sqlite/subscription_test.go`), plus the two handler packages, whose tests enumerate the commands their service advertises and fail when one has no case, so adding a handler without a test breaks `make test`. Python coverage is `nodes/tests/` (stdlib `unittest`, no pytest) over the `nodes/lib/core` runtime: wire shapes, the router's delivery/ack/abandoned-trackID paths, the RPC retry ladder, the ticker, logging setup, version discovery, and the GStreamer frame path; the GStreamer-dependent classes skip themselves where the bindings are absent. What no test covers is the wiring rather than the logic — no test drives a real SRT socket or a real GStreamer pipeline; those are exercised by the compose stack (`deploy/`).

<!--
Scope: this file is read before every task, so it must stay slow-moving.
Belongs here: commands and their knobs, the service layout, the protocol,
conventions, invariants, and what to run before committing.
Does NOT belong here: status, test counts, commit hashes, fixed/open lists,
narrative history ("X used to be Y"), or anything ARCHITECTURE.md, DESIGN.md
or TODO.md already owns — link to it instead.
If a line changes every commit, it is in the wrong file.
-->
