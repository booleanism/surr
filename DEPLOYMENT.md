# Deploying surr without Docker

Everything in this repository runs as a plain process. There are six of them —
four Go services that must be up before anything can be seen, one Go transmitter,
and one Python worker — and they find each other over RabbitMQ rather than
through a config file, so the order they are started in matters less than it
looks. The only things Docker is used for here are the two pieces of
infrastructure, and even those are optional.

This is a runbook: what to install, in what order to start it, and how to prove
it works. For why the system is shaped this way, see
[ARCHITECTURE.md](ARCHITECTURE.md) and [DESIGN.md](DESIGN.md).

- [1. What to install](#1-what-to-install)
- [2. The one dependency your distro will not have](#2-the-one-dependency-your-distro-will-not-have)
- [3. Infrastructure](#3-infrastructure)
- [4. Build](#4-build)
- [5. Create the databases](#5-create-the-databases)
- [6. Start the services](#6-start-the-services)
- [7. First run: a user, a subscription, a stream](#7-first-run-a-user-a-subscription-a-stream)
- [8. Verify it end to end](#8-verify-it-end-to-end)
- [9. Shut down](#9-shut-down)
- [10. Troubleshooting](#10-troubleshooting)

## 1. What to install

| Tool | Version | Why it is needed |
| --- | --- | --- |
| Go | 1.27.1 or newer | `rx` and `tx` declare `go 1.27.1`; the other modules are lower but the workspace is one build list. |
| GStreamer | 1.26 or newer | `go-gst v0.0.2` references `GstIdStr`, which GStreamer only added in 1.26. On 1.24 `tx` does not compile at all. |
| GStreamer plugins | base, good, bad, ugly, libav | `srtsink`, `srtsrc`, `tsdemux`, `h264parse`, `avdec_h264`, `openh264enc`, the AAC decoders, `videoconvert`, `appsink`. |
| libsrt | 1.5 or newer | `srtsink`/`srtsrc` come from gst-plugins-bad, which links libsrt. |
| Rust | 1.83 or newer | Only to build `intersrc` (section 2). |
| Python | 3.12 or newer | `nodes/`, via `uv`. |
| uv | any recent | Creates `./.venv` from `pyproject.toml` + `uv.lock`. |
| RabbitMQ | 4.x | The broker every process introduces itself on. |
| MongoDB | 8.x | Timeseries collections for the node's documents; the analytics service reads them. |
| (none extra) | — | The migrations are compiled into each service, so no separate `migrate` tool is needed — see section 5. |
| curl, jq | any | Only for `smoke.sh`. |

A PyGObject/GStreamer Python binding is needed too (`python3-gi` on Debian,
`PyGObject` from PyPI elsewhere); `uv sync` builds the Python side but cannot
install the system typelibs.

Debian trixie satisfies all of the above except Rust and the plugin package for
`intersrc`. It ships GStreamer 1.26.2, GLib 2.84 and libsrt 1.5.4.

## 2. The one dependency your distro will not have

`tx` does not push buffers from one pipeline to another directly: the producer
pipeline hands video and audio to the downstream (and recording) pipelines
through `intersrc` / `intersink`, which come from
[gst-plugins-rs](https://gitlab.freedesktop.org/gstreamer/gst-plugins-rs) (crate
`gst-plugin-inter`, library `libgstrsinter.so`).

**Debian does not package gst-plugins-rs at all.** Without it `tx run` fails at
startup, and it fails in the worst possible way:

```
GStreamer-CRITICAL **: gst_bin_add: assertion 'GST_IS_ELEMENT (element)' failed
panic: runtime error: invalid memory address or nil pointer dereference
```

`gst.ElementFactoryMake` returns nil for a factory the installation does not
have, and the first `Add` through that nil pointer is a segfault rather than an
error. So build it before you deploy anything:

```sh
git clone --depth 1 --branch gstreamer-1.26.11 \
    https://gitlab.freedesktop.org/gstreamer/gst-plugins-rs.git
cd gst-plugins-rs
cargo build --release -p gst-plugin-inter
```

Then make the plugin discoverable, either by installing it or by pointing
GStreamer at the build directory:

```sh
sudo install -m 644 target/release/libgstrsinter.so \
    "$(pkg-config --variable=pluginsdir gstreamer-1.0)/"
# or, without installing:
export GST_PLUGIN_PATH="$PWD/target/release"
```

Verify before going further:

```sh
gst-inspect-1.0 intersrc >/dev/null && gst-inspect-1.0 intersink >/dev/null \
    && echo "inter plugins present"
```

**Why that tag and not the newest one.** gst-plugins-rs is versioned in lockstep
with GStreamer: a tag's `gst` dependency has to match the GStreamer the host
links. `gstreamer-1.26.11` declares `gst = { branch = "0.24", version = "0.24" }`
and `rust-version = "1.83"`, which is what a GStreamer 1.26 host needs. `master`
has already moved to `gst` 0.26 (an alpha) and `rust-version = "1.92"`, and its
pkg-config floor is newer than what trixie ships — building `master` against
trixie fails. Match the tag to your GStreamer minor version.

Also check the codec elements, because two of them are not where you expect:

```sh
for e in srtsink srtsrc intersrc intersink tsdemux h264parse avdec_h264 \
         openh264enc faad avdec_aac videoconvert appsink; do
    gst-inspect-1.0 "$e" >/dev/null 2>&1 || echo "MISSING: $e"
done
```

`fdkaacenc` / `fdkaacdec` are **not** required. `tx` picks a decoder at startup
and reports which one it got (`aac decoder selected element=...`), trying
`fdkaacdec`, then `faad`, then `avdec_aac`, and the same for the encoder with
`fdkaacenc`, `voaacenc`, `avenc_aac`. The fdk-aac plugin exists only in
repositories that ship the non-free library, which is why the fallback exists.

`vah264dec` is likewise optional. It appears when `/dev/dri` exists; without a
render node the `va` plugin loads but registers zero features, and the default
(non-`va`) build is what runs.

## 3. Infrastructure

RabbitMQ and MongoDB are the only containers. Both can be replaced by a native
install — nothing in the services knows the difference — but these are the
versions and settings the system is developed against.

```sh
docker run -d --name surr-rabbitmq -p 5672:5672 -p 15672:15672 \
    rabbitmq:4-management

docker run -d --name surr-mongo -p 27017:27017 \
    -e MONGO_INITDB_ROOT_USERNAME=root \
    -e MONGO_INITDB_ROOT_PASSWORD=root \
    -v surr_mongo-data:/data/db \
    mongo:8
```

Wait for both before starting anything:

```sh
until (echo > /dev/tcp/127.0.0.1/5672) 2>/dev/null; do sleep 1; done
until (echo > /dev/tcp/127.0.0.1/27017) 2>/dev/null; do sleep 1; done
```

### 3a. MongoDB users (optional, recommended)

The analytics service only reads, and the node only writes, so they can hold
different credentials. Create them against the database they will authenticate
against:

```sh
mongosh "mongodb://root:root@127.0.0.1:27017/?authSource=admin" <<'JS'
db.getSiblingDB("nodes").createUser({
  user: "surr-writer", pwd: "surr-writer-pw",
  roles: [{ role: "readWrite", db: "nodes" }],
});
db.getSiblingDB("nodes").createUser({
  user: "surr-reader", pwd: "surr-reader-pw",
  roles: [{ role: "read", db: "nodes" }],
});
JS
```

The `authSource` in those URLs is the part that bites. A MongoDB URL with no
database path authenticates against `admin`, so a user created in `nodes` is
rejected by a service that was given
`mongodb://surr-reader:pw@host:27017` — with a SCRAM conversation error that
says nothing about the real problem. Always name the database the user lives in:

```sh
--mongo-url "mongodb://surr-reader:pw@127.0.0.1:27017/?authSource=nodes"
```

The node gets the same treatment via `--mongo-url`.

If you skip this section, use `root` for both — that is what the defaults do.

## 4. Build

```sh
make build          # bin/surr-hub, -subscriptions, -analytics, -rx, -tx
make                # lint + test + typecheck + build
```

`make build` builds each module from inside its own directory and writes
`bin/surr-<name>`. `VERSION` defaults to `git describe --tags --always --dirty`
and is injected with `-X main.Version`, so every binary reports the commit it
came from:

```sh
$ ./bin/surr-hub --version
surr-hub version v0.1.0-12-gabc1234
```

Set `VERSION=...` explicitly when building outside a git checkout, and
`RELEASE_TYPE=release` to add `-s -w -trimpath`.

Python:

```sh
uv sync             # creates ./.venv from pyproject.toml + uv.lock
```

`go install` does not work for these services. Each module carries a
`replace github.com/booleanism/surr => ../` directive so it can resolve the
shared `internal/` package from the working tree, and a `replace` to a relative
path is meaningless to `go install`. Build with `make`.

## 5. Create the databases

Three SQLite files, each owned by one service. They are not created on demand:
the service verifies its schema at startup and exits with one actionable error
if a table is missing.

The migrations are compiled into the binary that needs them, so there is no
separate tool to install and no way for the schema and the service to come from
different checkouts:

```sh
./bin/surr-hub migrate -d "$PWD/hub.db" up
./bin/surr-subscriptions migrate -d "$PWD/subscriptions.db" up
./bin/surr-analytics migrate -d "$PWD/analytics.db" up
```

The mode is the leaf command — `up` (the default), `down`, or `status` — and
`-d/--database` is accepted on either side of it. `up` names each migration it
applied and prints the resulting version; `down` reverts exactly one version.
`migrate` exits non-zero on failure, unlike `run`, so a deploy script stops
instead of starting a service against the wrong schema.

The `.sql` files still use the
[golang-migrate](https://github.com/golang-migrate/migrate) naming convention and
the same `schema_migrations` bookkeeping table, so if you prefer the CLI it can
read and continue a database the subcommand migrated — but it has to be built
with `-tags sqlite3` (the stock release binary answers `unknown driver sqlite3`),
which is exactly the friction the subcommand exists to remove.

Verify:

```sh
sqlite3 hub.db ".tables"            # nodes schema_migrations users
sqlite3 subscriptions.db ".tables"  # schema_migrations subscribed_nodes subscriptions
sqlite3 analytics.db ".tables"      # declarations schema_migrations
```

The services never migrate implicitly — only the explicit `migrate` subcommand
does. A service started against an unmigrated database stops immediately rather
than serving half a schema.

## 6. Start the services

Run each in its own terminal or under a supervisor. The commands below assume
you are in the repository root and that the databases are in the current
directory (the defaults are `./hub.db`, `./subscriptions.db`, `./analytics.db`).

**Order:** `subscriptions` and `analytics` first, then `hub`, then `rx`. Nothing
breaks if they start in another order — every service re-broadcasts its
`introduce` every second and retries until its siblings appear — but starting
the two command servers before the hub that calls them keeps the startup logs
free of retry noise. `tx` is the one process that fails fast, because it
publishes to `rx` immediately and a transmitter with nowhere to publish should
not pretend to work.

```sh
# 1. the subscription store
./bin/surr-subscriptions run

# 2. the analytics registry + query executor
./bin/surr-analytics run \
    --mongo-url "mongodb://root:root@127.0.0.1:27017/?authSource=admin"

# 3. the control plane (HTTP API)
JWT_SECRET="$(openssl rand -hex 32)" ./bin/surr-hub run

# 4. the SRT relay
./bin/surr-rx run
```

All five Go services accept the same logging pair (`--log-level`,
`--log-format console|json`) and the same sibling identity pair (`--family`,
`--name`). Leave the identities alone unless you are running two instances of
the same service.

Defaults worth knowing:

| Service | Flags | Default |
| --- | --- | --- |
| `surr-hub` | `--http` | `127.0.0.1:9000` |
| | `--database` | `./hub.db` |
| | `--secret` / `JWT_SECRET` | **none** — see below |
| | `--cookie-secure` | `true` |
| `surr-subscriptions` | `--database` | `./subscriptions.db` |
| `surr-analytics` | `--database` | `./analytics.db` |
| | `--mongo-url` | `mongodb://127.0.0.1:27017/` |
| | `--mongo-db` | `nodes` |
| `surr-rx` | `--ingress` | `127.0.0.1:4200` (where `tx` publishes) |
| | `--egress` | `127.0.0.1:4201` (where nodes pull) |
| | `--worker` | `28` |
| all Go services | `--broker` | `amqp://guest:guest@127.0.0.1:5672//` |

Two of those defaults will surprise you in a real deployment:

- **The hub has no default JWT secret.** With `--secret` unset it generates a
  random per-process secret and logs a warning; every token dies on restart.
  Always set it.
- **`--cookie-secure` is `true`**, so the refresh-token cookie carries `Secure`
  and a browser on plain HTTP will drop it. Set `--cookie-secure=false` for a
  local deployment without TLS. The access token is unaffected.

Then the node:

```sh
uv run python nodes/bustle.py --node-id "$(uuidgen)"
```

The node needs no address for `rx`: when a subscription it belongs to goes
live, `rx` sends it `<node-id>.sink.start` with the egress address to pull from.
It registers itself with the hub (`nodes.add`) and declares its analytics
queries as soon as the hub and the analytics service are visible.

Finally the transmitter, once you have a subscription id (section 7):

```sh
./bin/surr-tx run \
    --src "rtsp://127.0.0.1:8554/cam1" \
    --sink "srt://127.0.0.1:4200?mode=caller&streamid=publish:${SUBSCRIPTION_ID}" \
    --name cam1
```

The `streamid` is the routing: `rx` reads the subscription id out of it, asks
the `subscriptions` service which nodes belong to that subscription, and starts a
sink on each. A publish with an unknown or empty subscription is refused at the
handshake, which is the correct failure — there is no default destination.

`tx run` also accepts `--recording w,h,bitrate` (off by default, `0,0,0`) and
serves a DASH index over `tx serve --port 8000 --dir <recording dir>`. The DASH
server is optional: `tx run` registers its recording with it if one is listening
and relays and records regardless if not.

## 7. First run: a user, a subscription, a stream

```sh
HUB=http://127.0.0.1:9000

# an account
curl -sS -X POST "$HUB/users/signup" \
    -H 'Content-Type: application/json' \
    -d '{"email":"me@example.com","passwd":"hunter2hunter2"}'

# a token
TOKEN=$(curl -sS -X POST "$HUB/auth/signin" \
    -H 'Content-Type: application/json' \
    -d '{"email":"me@example.com","passwd":"hunter2hunter2"}' | jq -r .access_token)

# which nodes are running and free
curl -sS "$HUB/nodes/availability" -H "Authorization: Bearer $TOKEN"

# bind one to a new subscription
SUB=$(curl -sS -X POST "$HUB/subscriptions/add" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d "{\"nodes\":[{\"id\":\"$NODE_ID\"}]}" | jq -r .subscription_id)

# then start tx with streamid=publish:$SUB
```

`/nodes/availability` lists only nodes that are advertising themselves on the
broker right now *and* are not yet subscribed. A node that was killed without a
graceful shutdown keeps its row in the hub's database, but it stops being
offered — liveness is read from the broker, not from the row — so an empty list
means "nothing is running", not "nothing has ever run".

The analytics report for a node is one request:

```sh
curl -sS "$HUB/nodes/analytics?node_id=$NODE_ID&time_cluster=minutes" \
    -H "Authorization: Bearer $TOKEN" | jq .
```

`time_cluster` is one of `minutes`, `hours`, `dailies` (anything else falls back
to the widest window). The report contains one entry per query the node declared
— the hub knows no query names — and answers **409** for a node that is not
running, because a stored declaration outlives the node and empty results would
read as "this node saw nothing".

## 8. Verify it end to end

```sh
./smoke.sh
```

It starts the infrastructure if it is not already running, applies the
migrations to a throwaway database directory, starts every service and a
synthetic RTSP source, and asserts 48 facts about the whole chain: that the six
binaries agree on a version, that every migration applied, the client-error
status codes, node registration and its five declared queries, subscription,
`tx` connecting to `rx` and a document reaching MongoDB, the analytics report
with its window, bucket width and label types, the 409 for a stopped node and
the 200 once it returns, and `tx` exiting 0 on SIGTERM. It tears everything down
afterwards, including on failure, and prints a pass/fail summary.

Every knob is also a flag, which is what `--help` lists; the environment
variables below are the same settings.

Useful knobs:

| Variable | Default | Meaning |
| --- | --- | --- |
| `SRC` | unset | Use a real RTSP source instead of starting the synthetic one. |
| `START_INFRA` | `1` | Start RabbitMQ/Mongo containers when the ports are closed. |
| `SKIP_BUILD` | `0` | Skip `make build` and use the existing `bin/`. |
| `KEEP` | `0` | Keep the run directory (databases + logs) instead of deleting it. |
| `HUB_PORT` etc. | see the header | Move a port if something else owns it. |

## 9. Shut down

Reverse order, and let the node go first:

```sh
# node: SIGTERM, and wait for it to exit
kill -TERM <node pid>

# tx
kill -TERM <tx pid>

# rx, then the control plane
kill -TERM <rx pid>
kill -TERM <hub pid>
kill -TERM <analytics pid>
kill -TERM <subscriptions pid>
```

Every process has a graceful path on SIGTERM, and the order is worth respecting
for one reason: the node's shutdown deregisters it (`nodes.remove`) while its
broker listener is still up, so the hub's row is removed rather than left behind.
Killing the hub first does not corrupt anything — the row simply stays, and
stops being offered because it stops being advertised — but a clean shutdown
leaves a clean table.

`tx` exits 0 on SIGTERM. It deliberately never brings its GStreamer pipelines
back to `NULL`: the transition disposes the RTSP source and races GIO's threaded
resolver on GLib 2.84, which segfaults on a clean shutdown as readily as on an
unreachable source. The cost is that an interrupted `--recording` needs
remuxing; the benefit is that a supervisor sees exit 0 instead of a crash loop.

## 10. Troubleshooting

| Symptom | Cause | Fix |
| --- | --- | --- |
| `tx` dies with `gst_bin_add: assertion 'GST_IS_ELEMENT (element)' failed` and a nil-pointer panic | `intersrc` is not installed | Section 2. |
| `tx`: `h264 encoder elements unavailable` | no `openh264enc`, and not built with `-tags va` | install `openh264enc`, or build with `make build-tx TAGS=va` and provide `/dev/dri`. |
| `tx`: `unable to reach source` | wrong RTSP URL, or the camera is not publishing | `ffprobe <url>` first. |
| `rx` logs `publisher started` but the node never logs a sink start | the subscription id in the streamid is unknown to `subscriptions` | check `streamid=publish:<uuid>` against the id `POST /subscriptions/add` returned. |
| `/nodes/availability` returns 204 with a node running | the node is not introducing itself: wrong broker URL, or a different `--family` | compare `--broker` and `--family` on the node and the services; every service logs `sibling registry updated`. |
| `/nodes/analytics` returns 409 | the node is not running | it must be up and introducing; this is the intended answer, not a bug. |
| `analytics` restarts with `sasl conversation error: unable to authenticate` | the URL has no database path, so the driver authenticated against `admin` | add `?authSource=nodes`. |
| a service exits immediately with one error naming a table | the database was not migrated | Section 5. |
| `migrate`: `unknown driver sqlite3` | the stock release binary | rebuild with `-tags sqlite3`. |
| every token is rejected after a restart | `--secret` was unset, so the hub generated a fresh one | set `JWT_SECRET`. |
| the refresh cookie never comes back | `--cookie-secure` is `true` on plain HTTP | `--cookie-secure=false`. |
| the detector returns nothing | the frame has nothing to detect | expected with a test pattern; use a real source. |
