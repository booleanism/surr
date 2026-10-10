# surr

Event-driven microservices AI surveillance system, built on GStreamer, SRT, YOLO,
MongoDB, SQLite3 and RabbitMQ.

`surr` moves live camera video from an RTSP source to AI workers and stores the
results as time-series analytics. The media path uses SRT for transport and
zero-copy fan-out; the control plane is a set of small services that discover
each other over a RabbitMQ topic exchange — there is no service registry and no
central orchestrator.

> **Project status: the control plane runs, and every defect found in the 2026-10
> review down to HIGH severity is fixed.** [TODO.md](TODO.md) section E is the
> tracker for the review findings and their status — what is fixed, and what is
> still open — so it is the only place that records it. Tests cover the RPC reply
> layer, the restart backoff and the `nodes/core` runtime, and `make typecheck`
> gates the Python side with pyright; the SRT data plane, the SQLite layer and
> the tx pipelines are exercised by hand.

## How it fits together

```
camera (RTSP) ──→ tx ──── SRT ────→ rx ──── SRT ────→ nodes (YOLO)
                                     │                      │
                                     │                      ↓
                                     │                   MongoDB
                    ┌────────────────┴───────────────┐
                    │      RabbitMQ topic exchange    │
                    └───────┬────────────────┬───────┘
                            │                │
                          hub          subscriptions
                    (HTTP API, auth)      (SQLite)
```

| Service | Module | Role |
| --- | --- | --- |
| **tx** | `tx/` | Transmitter. Pulls an RTSP source, transcodes with GStreamer, publishes SRT and/or records DASH segments. |
| **rx** | `rx/` | Receiver/relay. Terminates SRT ingress, looks up which nodes belong to a subscription, and fans the stream out over SRT egress. |
| **hub** | `hub/` | Control plane. HTTP API (JWT auth, users, nodes, subscriptions) backed by SQLite, plus broker handlers. |
| **subscriptions** | `subscriptions/` | Answers which nodes belong to a subscription. SQLite-backed, no HTTP surface. |
| **nodes** | `nodes/` | Python AI workers. Pull SRT frames, run inference, write results to MongoDB. |

Every service broadcasts an `introduce` message advertising the commands it
serves; peers cache these and route commands by name (not by service address).
Because a node's commands are prefixed with its own UUID
(`<node-id>.sink.start`), the same mechanism dispatches work to individual
nodes. Details: [ARCHITECTURE.md](ARCHITECTURE.md).

## Requirements

- **Go 1.27+** — `rx` and `tx` declare `go 1.27.1`.
- **RabbitMQ** — the message bus every service joins at runtime.
- **SQLite3** — via `mattn/go-sqlite3`, so **cgo is required** to build `hub` and
  `subscriptions`.
- **GStreamer 1.0 + development headers** — required to build `tx` (go-gst) and
  to run the Python nodes.
- **MongoDB** — analytics storage for nodes.
- **Python 3** with GObject/GStreamer bindings, `ultralytics` (YOLO), `pymongo`
  and `aio-pika` — for the nodes runtime. `nodes/requirements.txt` pins the
  direct dependencies of a working venv; `nodes/pyproject.toml` carries the same
  packages as abstract lower bounds and configures the type checker.

## Build

```sh
make            # lint + test + typecheck + build everything
make build      # build only (no lint, no test)
make lint       # golangci-lint, every module
make test       # go test ./..., every module, then the nodes/ python suite
make test-nodes # python tests for nodes/core (nodes/tests)
make typecheck  # pyright over nodes/ (nodes/pyproject.toml)
make clean      # remove .cache/ and bin/
```

`make build` builds each service into `bin/surr-<name>` (`bin/surr-hub`,
`bin/surr-rx`, `bin/surr-subscriptions`, `bin/surr-tx`). Individual targets:
`make build-hub`, `make build-rx`, `make build-subscriptions`, `make build-tx`.
Likewise `make test-hub`, `make test-rx`, `make test-subscriptions`,
`make test-tx`, `make test-root` test one module.

Knobs: `RELEASE_TYPE=release` adds `-s -w -trimpath`; `TAGS` passes build tags;
`GOFLAGS` / `LDFLAGS` are appended; `TESTFLAGS` is appended to `go test` (e.g.
`make test TESTFLAGS=-race`). `make test-nodes` runs with `nodes/.venv/bin/python`
when it exists and `PYTHON=<interpreter>` overrides that; `NODESTESTFLAGS=-v`
gives per-test output. `make typecheck` runs pyright from inside `nodes/` against
`nodes/pyproject.toml` (`PYRIGHT=<path>` and `TYPECHECKFLAGS` override the
command and its arguments). It needs `pyright` and `PyGObject-stubs` in
`nodes/.venv`, and must **not** have `gst-python-stubs` installed — that package
writes an overrides-only `Gst.pyi` over the real stubs and makes every
GStreamer enum and constructor an unknown-attribute error.

The Go tests cover the RPC reply layer and the backoff used by the restart loops.
The Python suite in `nodes/tests` covers the `nodes/core` runtime — the wire
shapes, the router's ack/abandoned-trackID paths, the RPC retry ladder, the
ticker, logging setup and the GStreamer frame path — and the classes that need
the GStreamer bindings skip themselves where those are missing. `make typecheck`
adds pyright in `standard` mode over `nodes/bustle.py`, `nodes/core/`,
`nodes/lib/` and `nodes/tests/`. Everything else (the SRT data plane, the SQLite
layer, the tx pipelines) is exercised by hand. The two handler tests enumerate
the commands their service advertises and fail when one has no case, so
registering a handler without a test breaks `make test`.

Note that neither `make lint` nor `make build` passes build tags: check the
`vah264enc` variant of tx explicitly with `make test TAGS=va`.

## Quick start

### 1. Start the infrastructure

RabbitMQ on `127.0.0.1:5672`, MongoDB on `127.0.0.1:27017`, and an RTSP source
(the `tx --src` flag default example assumes one at `rtsp://127.0.0.1:8554/cam1`).

### 2. Run the SQLite migrations

The services contain no migration code — they never create their own tables. The
`.sql` files in `internal/migrations/sqlite/` follow the
[golang-migrate](https://github.com/golang-migrate/migrate) naming convention and
are applied with the `migrate` CLI:

```sh
migrate -path hub/internal/migrations/sqlite \
        -database "sqlite3://$PWD/hub.db" up
migrate -path subscriptions/internal/migrations/sqlite \
        -database "sqlite3://$PWD/subscriptions.db" up
```

This creates the tables and a `schema_migrations` bookkeeping table. Use `down`
to roll back one version. Run this before starting `hub` and `subscriptions`:
both check for their tables at startup (`internal/db/schema.go`) and exit with
one error naming the missing tables and the `migrate ... up` command, rather
than opening an empty database and failing one query at a time.

> **The `migrate` CLI must be built with the `sqlite3` driver.** Stock
> `golang-migrate` release binaries do not include it, so `sqlite3://` fails
> with `unknown driver sqlite3`. Build your own (needs cgo, because the driver
> is `mattn/go-sqlite3`):
>
> ```sh
> go install -tags 'sqlite3' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
> ```

### 3. Start the control plane

```sh
./bin/surr-hub run              # HTTP API on 127.0.0.1:9000
./bin/surr-subscriptions run
```

Useful `hub` flags: `-a/--http` (listen address), `-d/--database`, `-s/--secret`
(JWT signing key — **set this**; with no secret the hub generates a random one
per process and tokens stop working after a restart), `--cookie-secure` (set the
refresh cookie's `Secure` attribute; disable only for plain-HTTP local dev),
`-b/--broker`, `-f/--family`, `-n/--name`. Use `./bin/surr-hub run --help` for
the full list; `subscriptions` takes the same `-d/-b/-f/-n` flags.

### 4. Start the relay

```sh
./bin/surr-rx run               # SRT ingress 127.0.0.1:4200, egress 127.0.0.1:4201
```

`-w/--worker` sets the ingress backlog and the subscriber cap (default 28);
`-i/--ingress` and `-e/--egress` move the SRT endpoints.

### 5. Start the transmitter

`tx run` registers its stream with a DASH server at startup, but a failure is
only logged, so `serve` does not have to be running:

```sh
./bin/surr-tx serve                              # DASH server, default port 8000
./bin/surr-tx run \
  --src rtsp://127.0.0.1:8554/cam1 \
  --sink 'srt://127.0.0.1:4200?mode=caller&streamid=publish:<subscription-id>' \
  --name cam1
```

`--downstream <width>,<height>,<bitrate>` sets the outgoing encode
(default `1280,720,300`). `--recording` takes the same format and defaults to
`0,0,0`, which disables recording entirely. `serve` accepts `--dash-token`;
the stream-registering routes require it when set, and with no token only a
loopback caller may register or delete a stream.

### 6. Start a node

Run from the `nodes/` directory (the runtime imports `core` and `lib` as
top-level packages):

```sh
cd nodes
python bustle.py --node-id <uuid>
```

The node needs YOLO weights — pass `-m/--model <path>`, and note that weight
files are not committed to this repository. `--mongo-url`, `--mongo-db`,
`--broker`, `-f/--family`, `-c/--chosen-classes` and `--drain-interval` are the
other knobs; `python bustle.py --help` lists them all.

The node self-registers with `hub` on boot (`nodes.add`) and deregisters on
graceful shutdown (`nodes.remove`). It retries registration until the hub
appears, so start order does not matter.

## HTTP API

All routes are served by `hub` on `-a/--http` (default `127.0.0.1:9000`).
Protected routes require `Authorization: Bearer <access token>`.

| Method | Path | Auth | Purpose |
| --- | --- | --- | --- |
| `POST` | `/users/signup` | — | Create a user |
| `POST` | `/auth/signin` | — | Exchange credentials for a JWT + refresh cookie |
| `POST` | `/auth/refresh` | cookie | Rotate the refresh token, mint a new access token |
| `POST` | `/auth/logout` | cookie | Revoke the refresh token |
| `GET` | `/nodes/availability` | ✓ | Nodes not yet bound to a subscription |
| `GET` | `/nodes/analytics?node_id=&time_cluster=` | ✓ | Analytics for one node (proxied to the node over the broker) |
| `POST` | `/subscriptions/add` | ✓ | Bind nodes to a new subscription |
| `POST` | `/subscriptions/remove` | ✓ | Remove a subscription |

A typical first run: `POST /users/signup`, `POST /auth/signin` to get a token,
then `POST /subscriptions/add` with the node IDs to obtain a subscription ID —
that ID is what goes into the `tx --sink` streamid.

## Nodes

| Node | Description |
| --- | --- |
| **Bustle** | Object counter. Runs YOLO over the SRT stream, counts chosen classes per frame, takes a per-interval median, and writes one document per interval into a MongoDB time-series collection named after the node. |

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) — services, data flow, protocol, storage, service internals.
- [DESIGN.md](DESIGN.md) — design rationale, trade-offs, known limitations, invariants.
- [TODO.md](TODO.md) — staged work (nodes refactor, analytics service, logging) and verified defects.
- [AGENTS.md](AGENTS.md) — orientation for AI coding agents working in this repo.

## License

MIT — see [LICENSE](LICENSE).

Copyright (c) 2026 Mambaul Hisam. Reach me at `me [at] ulhism [dot] id`.
