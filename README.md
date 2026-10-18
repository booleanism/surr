# surr

Event-driven microservices AI surveillance system, built on GStreamer, SRT,
LibreYOLO, MongoDB, SQLite3 and RabbitMQ.

`surr` moves live camera video from an RTSP source to AI workers and stores the
results as time-series analytics. The media path uses SRT for transport and
zero-copy fan-out; the control plane is a set of small services that discover
each other over a RabbitMQ topic exchange — there is no service registry and no
central orchestrator.

> **Project status: the control plane runs, and every defect found in the 2026-10
> review is fixed.** [TODO.md](TODO.md) section E is the tracker for those
> findings — each defect as it was found, with the fix appended — and sections A
> and B are the work still ahead: the nodes refactor, and the `analytics` service,
> which is built and deployed but not yet on the request path.
> Tests cover the RPC reply layer, the restart backoff, the rx data plane and the
> `nodes/lib/core` runtime, and `make typecheck` gates the Python side with
> pyright; what is exercised by hand is the wiring — a real SRT socket and a real
> GStreamer pipeline — through the compose stack in [deploy/](deploy/README.md).

## How it fits together

```
camera (RTSP) ──→ tx ──── SRT ────→ rx ──── SRT ────→ nodes (DEIM)
                                     │                      │
                                     │                      ↓
                                     │                   MongoDB
                    ┌────────────────┴───────────────┐
                    │      RabbitMQ topic exchange    │
                    └───────┬────────────────┬───────┘
                            │                │
                          hub          subscriptions      analytics
                    (HTTP API, auth)      (SQLite)       (mongo read path)
```

| Service | Module | Role |
| --- | --- | --- |
| **tx** | `tx/` | Transmitter. Pulls an RTSP source, transcodes with GStreamer, publishes SRT and/or records DASH segments. |
| **rx** | `rx/` | Receiver/relay. Terminates SRT ingress, looks up which nodes belong to a subscription, and fans the stream out over SRT egress. |
| **hub** | `hub/` | Control plane. HTTP API (JWT auth, users, nodes, subscriptions) backed by SQLite, plus broker handlers. |
| **subscriptions** | `subscriptions/` | Answers which nodes belong to a subscription. SQLite-backed, no HTTP surface. |
| **analytics** | `analytics/` | Read path over MongoDB. Keeps a registry of the aggregation queries each node declared about its own documents, and runs one by name on request. Not yet on the request path. |
| **nodes** | `nodes/` | Python AI workers. Pull SRT frames, run inference, write results to MongoDB. |

Every service broadcasts an `introduce` message advertising the commands it
serves; peers cache these and route commands by name (not by service address).
Because a node's commands are prefixed with its own UUID
(`<node-id>.sink.start`), the same mechanism dispatches work to individual
nodes. Details: [ARCHITECTURE.md](ARCHITECTURE.md).

## Requirements

- **Go 1.27+** — `rx` and `tx` declare `go 1.27.1`.
- **RabbitMQ** — the message bus every service joins at runtime.
- **SQLite3** — via `mattn/go-sqlite3`, so **cgo is required** to build `hub`,
  `subscriptions` and `analytics`.
- **GStreamer 1.0 + development headers** — required to build `tx` (go-gst) and
  to run the Python nodes.
- **MongoDB** — analytics storage for nodes.
- **Python 3** with GObject/GStreamer bindings, `libreyolo` (object
  detection), `pymongo`
  and `aio-pika` — for the nodes runtime. The dependencies are declared in the
  root `pyproject.toml`, pinned exactly, with `uv.lock` holding the full
  transitive graph. The venv lives at the repo root as `.venv/`.
- **[uv](https://docs.astral.sh/uv/)** — installs and runs the Python side.

## Build

```sh
make            # lint + test + typecheck + build everything
make build      # build only (no lint, no test)
make lint       # golangci-lint, every module
make test       # go test ./..., every module, then the nodes/ python suite
make test-nodes # python tests for nodes/lib/core (nodes/tests)
make typecheck  # pyright over nodes/ (pyproject.toml)
make clean      # remove .cache/ and bin/
```

The Python side needs its venv before `make test-nodes` / `make typecheck` can
run. From the repo root:

```sh
uv sync         # creates .venv/ and installs the node dependencies + dev tools
```

`uv sync` installs the exact pins from `pyproject.toml` and the `dev` group
(`pyright`, `PyGObject-stubs`) into `./.venv`. It does **not** install the
project itself: `nodes/` is a directory of scripts, not an importable package,
so `[tool.uv] package = false` keeps `uv sync` to dependencies only.

```sh
uv run python nodes/bustle.py --help    # run the node from the repo root
```

`make build` builds each service into `bin/surr-<name>` (`bin/surr-hub`,
`bin/surr-rx`, `bin/surr-subscriptions`, `bin/surr-analytics`, `bin/surr-tx`).
Individual targets: `make build-hub`, `make build-rx`, `make build-subscriptions`,
`make build-analytics`, `make build-tx`. Likewise `make test-hub`,
`make test-rx`, `make test-subscriptions`, `make test-analytics`, `make test-tx`,
`make test-root` test one module.

Knobs: `RELEASE_TYPE=release` adds `-s -w -trimpath`; `TAGS` passes build tags;
`GOFLAGS` / `LDFLAGS` are appended; `TESTFLAGS` is appended to `go test` (e.g.
`make test TESTFLAGS=-race`). `make test-nodes` runs with `./.venv/bin/python`
when it exists and `PYTHON=<interpreter>` overrides that; `NODESTESTFLAGS=-v`
gives per-test output. `make typecheck` runs pyright from the repo root against
`pyproject.toml` (`PYRIGHT=<path>` and `TYPECHECKFLAGS` override the command and
its arguments). It needs `pyright` and `PyGObject-stubs` in `./.venv` — both are
in the `dev` dependency group, so `uv sync` installs them — and must **not** have
`gst-python-stubs` installed: that package writes an overrides-only `Gst.pyi`
over the real stubs and makes every GStreamer enum and constructor an
unknown-attribute error.

The Go tests cover the RPC reply layer and the backoff used by the restart loops.
The Python suite in `nodes/tests` covers the `nodes/lib/core` runtime — the wire
shapes, the router's ack/abandoned-trackID paths, the RPC retry ladder, the
ticker, logging setup and the GStreamer frame path — and the classes that need
the GStreamer bindings skip themselves where those are missing. `make typecheck`
adds pyright in `standard` mode over `nodes/bustle.py`, `nodes/lib/` and
`nodes/tests/`. Everything else (the SRT data plane, the SQLite
layer, the tx pipelines) is exercised by hand. The two handler tests enumerate
the commands their service advertises and fail when one has no case, so
registering a handler without a test breaks `make test`.

Note that neither `make lint` nor `make build` passes build tags: check the
`vah264enc` variant of tx explicitly with `make test TAGS=va`.

`make lint`, `make test-go` and `make build` also run in CI on every push and
pull request (`.github/workflows/ci.yml`), alongside the Python tests and
pyright. `make test` is `test-go` plus `test-nodes`; CI splits them because the
Go half needs a cgo toolchain and the Python half needs the GStreamer typelibs,
and neither should wait on the other.

## Quick start

There are two ways to run this: the compose stack in `deploy/`, which brings up
RabbitMQ, MongoDB, both migrators and every service on the right networks, or by
hand, which is what the rest of this section describes. **Start with
[`deploy/README.md`](deploy/README.md)** unless you are working on a single
service — it is the shorter path, and it documents the one component the
repository does not contain (an RTSP source). The stack has a `demo` profile
that supplies a synthetic one, so `docker compose --profile demo --profile tx
--profile nodes up -d` runs the whole system with no camera and no external
containers. `deploy/smoke.sh` then asserts the behaviour that is invisible from
`docker compose ps`.

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

> In the compose stack this is the `migrate-hub` / `migrate-subscriptions`
jobs, which run to completion before the services start. They exist because
that `migrate` CLI cannot be pulled as a stock image at all — see
`deploy/migrate.Dockerfile`.

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

Run from the repo root. `nodes/` is not a package — the runtime imports `lib`
as a top-level name, and Python puts a script's own directory on
`sys.path`, so the path below resolves as-is:

```sh
uv run python nodes/bustle.py --node-id <uuid>
```

The node needs detector weights. The default, `LibreDEIMm.pt`, is resolved
relative to `./weights/` and downloaded from Hugging Face on first use; pass
`-m/--model <path>` to point somewhere else, and note that weight files are not
committed to this repository. `--conf` (default `0.25`) drops detections below a
score and matters more than it looks: DEIM decodes top-K with **no NMS**, so
every box the model reports above the threshold is counted. `--mongo-url`,
`--mongo-db`, `--broker`, `-f/--family`, `-c/--chosen-classes`,
`--decode-chain` and `--drain-interval` are the other knobs; `uv run python
nodes/bustle.py --help` lists them all. `--decode-chain` is the GStreamer
chain between the SRT source and the appsink, defaulting to the
h264-in-MPEG-TS path `tx` sends; change it only for a node type fed a
different codec, since the BGR frame the worker receives is fixed.

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

Two field names surprise people. The password field is `passwd`, not
`password`; the signup and signin bodies are decoded with unknown fields
rejected, so `password` is a **400** naming the expected body rather than an
account with an empty password. `/subscriptions/add` takes
`{"nodes":[{"id":"..."}]}`, not a flat list of ids — a body with no usable
node is a **400**, and a node that is already in another subscription is a
**409**, because a node belongs to exactly one subscription. A second signup for
the same address is a **409** too, not a 500.

## Nodes

| Node | Description |
| --- | --- |
| **Bustle** | Object counter. Runs LibreYOLO's `LibreDEIMm` detector over the SRT stream, counts chosen classes per frame, takes a per-interval median, and writes one document per interval into a MongoDB time-series collection named after the node. |

## Documentation

- [deploy/README.md](deploy/README.md) — run the whole system with Docker Compose, including the RTSP source it needs (`deploy/smoke.sh` verifies a running stack).
- [ARCHITECTURE.md](ARCHITECTURE.md) — services, data flow, protocol, storage, service internals.
- [DESIGN.md](DESIGN.md) — design rationale, trade-offs, known limitations, invariants.
- [TODO.md](TODO.md) — staged work (nodes refactor, analytics service, logging) and verified defects.
- [AGENTS.md](AGENTS.md) — orientation for AI coding agents working in this repo.

## License

MIT — see [LICENSE](LICENSE).

This covers the code in this repository. The node's default detector weights
(`LibreDEIMm.pt`) are published by the LibreYOLO project under Apache-2.0, and
the `libreyolo` package itself is MIT. The detector runs behind the `Detector`
Protocol in `nodes/lib/detector.py`, so the weights and the library are both
replaceable — but note that LibreYOLO's own checkpoint catalogue is mixed:
licences are per checkpoint, and some families (DEIMv2 at S/M/L/X, among
others) carry Meta's DINOv3 licence, which is not OSI-approved. Check the
checkpoint's licence before swapping one in.

Copyright (c) 2026 Mambaul Hisam. Reach me at `me [at] ulhism [dot] id`.
