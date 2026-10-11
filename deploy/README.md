# Deploying surr

A single compose stack: RabbitMQ as the bus, MongoDB for analytics, the three
Go control/data-plane services, and — behind profiles — a transmitter and a
node.

```
camera (RTSP) → tx ──SRT──→ rx ──SRT──→ bustle (Python, YOLO) ──→ MongoDB
                              ↑____________ RabbitMQ ____________↓
                  hub (HTTP API, auth)   subscriptions (SQLite)
```

| service                 | what it is                                                   | profile |
| ----------------------- | ------------------------------------------------------------ | ------- |
| `rabbitmq`              | the topic exchange every service introduces itself on        | –       |
| `mongo`                 | where nodes write their analytics documents                  | –       |
| `migrate-hub`           | one-shot: creates `nodes`, `users` in the hub database       | –       |
| `migrate-subscriptions` | one-shot: creates `subscriptions`, `subscribed_nodes`        | –       |
| `hub`                   | HTTP API (JWT auth, users, nodes, subscriptions) + SQLite    | –       |
| `subscriptions`         | answers which nodes belong to a subscription                 | –       |
| `rx`                    | terminates SRT ingress, exposes SRT egress for nodes to pull | –       |
| `tx`                    | pulls an RTSP source, transcodes, publishes SRT              | `tx`    |
| `bustle`                | a Python node: pulls frames, counts objects, writes to mongo | `nodes` |

The two profiled services are the ones that need something from outside the
repository — a real RTSP source, and a subscription to publish into — so
`docker compose up -d` deliberately leaves them out and gives you a working
control plane instead of a crash loop.

## Quick start

```sh
cd deploy
cp .env.example .env          # every value has a default; see below for the ones that matter

export VERSION=$(git describe --tags --always --dirty)
docker compose build
docker compose up -d
```

`docker compose ps` should show `rabbitmq` and `mongo` healthy, the two
migrate jobs exited 0, and `hub`, `subscriptions`, `rx` running. The hub is on
<http://localhost:9000>, the RabbitMQ management UI on
<http://localhost:15672> (`surr` / `surr`).

Two variables are worth setting before you go further:

- **`JWT_SECRET`** — the default is a placeholder that is committed to this
  repository. `openssl rand -hex 32`.
- **`VERSION`** — compose cannot run `git describe` itself. Left at `unknown`,
  every service logs `version=unknown`, which is honest but tells you nothing
  about which build is running.

The stack is plain HTTP. The refresh cookie is `Secure` by default, which a
browser will refuse to store over HTTP, so the hub is started with
`--cookie-secure=false`. Put TLS in front (or set `HUB_COOKIE_SECURE=true` and
terminate TLS yourself) before exposing it.

## The RTSP source

This is the one part of the system that is not in the repository: `tx` needs an
RTSP URL to pull from. Two ways to get one.

**A real camera.** Point `TX_SRC` straight at it, or restream it:

```sh
# -c copy avoids re-encoding; drop it if the camera's stream is not H.264.
ffmpeg -rtsp_transport tcp -i rtsp://user:pass@192.168.1.50:554/stream1 \
       -c copy -f rtsp rtsp://127.0.0.1:8554/cam1
```

**A synthetic stream**, which is what you want for a first run — no camera, no
codec surprises:

```sh
# mediamtx listens on :8554 and accepts RTSP ANNOUNCE (push) at any path.
docker run --rm -p 8554:8554 bluenviron/mediamtx:latest
```

then publish into it with either tool:

```sh
# GStreamer. Two things to know about this one: rtspclientsink is NOT in
# gstreamer1.0-plugins-bad on Debian trixie, it is its own package
# (gstreamer1.0-rtsp, which installs libgstrtspclientsink.so), and it takes
# x264enc straight -- inserting rtph264pay fails with "could not link
# rtph264pay0 to rtspclientsink0", because the element does its own RTP
# payloading.
apt-get install -y gstreamer1.0-tools gstreamer1.0-plugins-base \
                   gstreamer1.0-plugins-good gstreamer1.0-plugins-ugly \
                   gstreamer1.0-rtsp

gst-launch-1.0 videotestsrc is-live=true pattern=ball ! \
  video/x-raw,width=1280,height=720,framerate=30/1 ! \
  videoconvert ! x264enc tune=zerolatency speed-preset=ultrafast key-int-max=30 ! \
  rtspclientsink location=rtsp://127.0.0.1:8554/cam1

# ffmpeg, same result, and one binary instead of five packages. Prefer this
# unless you already have the GStreamer plugins installed.
ffmpeg -re -f lavfi -i testsrc=size=1280x720:rate=30 \
       -c:v libx264 -preset ultrafast -tune zerolatency \
       -f rtsp rtsp://127.0.0.1:8554/cam1
```

`tx` runs inside the compose network, so `127.0.0.1` in `TX_SRC` would mean
*tx's own container*. Either add mediamtx to the stack (it needs no
configuration):

```yaml
  mediamtx:
    image: bluenviron/mediamtx:latest
    ports:
      - "8554:8554"
    restart: unless-stopped
```

and use `TX_SRC=rtsp://mediamtx:8554/cam1`, or keep mediamtx on the host and
give the tx service a route to it:

```yaml
  tx:
    extra_hosts:
      - "host.docker.internal:host-gateway"
```

with `TX_SRC=rtsp://host.docker.internal:8554/cam1`.

## Putting a stream through it

With the control plane up and an RTSP source running:

**1. Create a user and get a token.**

```sh
curl -sS -X POST http://localhost:9000/users/signup \
  -H 'Content-Type: application/json' \
  -d '{"email":"me@example.com","passwd":"a-long-password"}'
# {"user_id":"..."}

TOKEN=$(curl -sS -X POST http://localhost:9000/auth/signin \
  -H 'Content-Type: application/json' \
  -d '{"email":"me@example.com","passwd":"a-long-password"}' | jq -r .access_token)
```

**2. Start the node**, so it registers itself with the hub (`nodes.add`) and
waits for work:

```sh
docker compose --profile nodes up -d bustle
```

Its node id is pinned by `BUSTLE_NODE_ID` (default
`50154422-fd3b-4cf0-a761-908fcbe98a1e`). That id is the node's handle
everywhere — the hub's `nodes` table, the subscription's node list, and the
`<node-id>.sink.*` commands — so changing it registers a *different* node
rather than renaming this one.

**3. Subscribe that node to a stream.**

```sh
curl -sS -X POST http://localhost:9000/subscriptions/add \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"nodes":[{"id":"50154422-fd3b-4cf0-a761-908fcbe98a1e"}]}'
# {"subscription_id":"..."}
```

The subscription id is generated here; it is the thing that ties a publisher to
a set of nodes. Put it in `.env` as `TX_SUBSCRIPTION_ID`.

**4. Start the transmitter.**

```sh
docker compose --profile tx up -d tx
```

`tx` publishes to `srt://rx:4200?mode=caller&streamid=publish:<TX_SUBSCRIPTION_ID>`.
That streamid is the entire contract with `rx`: it is how the ingress knows
which subscription the publisher belongs to, and a streamid that matches no row
is refused at the SRT handshake.

**5. Watch it work.** `rx` sees the publisher, asks `subscriptions` which nodes
that subscription contains, and sends `<node-id>.sink.start` with its own
egress address as `source_address`. The node builds
`srt://rx:4201?mode=caller&streamid=subscribe:<node-id>` from that, pulls
frames, runs detection, and writes counts to mongo:

```sh
docker compose logs -f rx bustle
docker compose exec mongo mongosh -u root -p root --authenticationDatabase admin \
  --eval 'db.getSiblingDB("nodes").getCollectionNames()'
```

Tear it down with `docker compose --profile tx --profile nodes down`. Add `-v`
to drop the databases and the downloaded model weights as well.

## How the pieces find each other

Nothing is configured with a peer's address. Every service declares the same
topic exchange and broadcasts an `introduce` message advertising the commands
it can serve; peers keep a registry of that, so a caller resolves
`subscriptions.nodes.get` to whichever sibling offers it. The consequences:

- **Startup order is not critical.** Services retry until their siblings
  introduce themselves. Only the migrations are order-sensitive, which is what
  `depends_on: service_completed_successfully` covers.
- **A node registers once per run.** `nodes.add` is sent on boot and retried
  until the hub answers. Recreating the hub's database while a node keeps
  running leaves that node unregistered until it restarts.
- **Every service must share a family.** `--family` defaults to `surr0`
  everywhere; it is part of the routing, not a label.
- **`rx` tells the node where to pull from.** The `--egress` value is not
  cosmetic: it is handed to the node as `source_address` inside the
  `sink.start` reply. That is why this stack sets `--egress=rx:4201` and not
  `127.0.0.1:4201` — a containerised node must be given a name it can resolve
  to *rx*, or it would try to subscribe to itself.

## Things that will bite you

**The services do not create their own tables.** `hub` and `subscriptions` only
*verify* their schema at startup and exit with `database schema is missing` if
a table is absent; the `migrate-*` jobs are what create it. If you reset a
volume (`docker compose down -v`), the migrator has to run again — it will, on
the next `up`, because `migrate up` is idempotent.

The migrator is built from `migrate.Dockerfile` rather than pulled from
`migrate/migrate`, because that image cannot talk to SQLite at all: golang-
migrate gates its drivers behind build tags and its published build omits
sqlite3, so it fails with `unknown driver sqlite3 (forgotten import?)`. The
image here compiles the same `mattn/go-sqlite3` driver the hub and
subscriptions binaries link, so the schema writer and the readers share one
engine.

**One node belongs to exactly one subscription.** The egress handshake looks up
the node's subscriptions and rejects anything that does not resolve to exactly
one. A node subscribed to two streams will fail to connect rather than pick
one.

**`docker compose restart` does not re-read `.env`.** It restarts the existing
container, whose environment was fixed when it was created. After editing
`.env` — `TX_SUBSCRIPTION_ID` in particular — you need `up -d`, which recreates
the container. Symptom of getting this wrong: `rx` logs
`ingress rejected reason="no subscribed nodes"` for the *old* subscription id
while the new one exists in the database.

**The subscription id belongs to a database, not to the stack.** It is a row in
`subscriptions.db`, so `down -v` takes it with it. After a reset, create a user,
subscribe the node and put the new id in `.env` — a stale `TX_SUBSCRIPTION_ID`
looks exactly like a broken `rx`, because `rx` cannot distinguish a deleted
subscription from one that never existed and rejects the publish.

**Model weights are downloaded on first run.** The node resolves the bare
filename `LibreDEIMm.pt` under `./weights` and fetches it if it is not there,
so the first start needs outbound HTTPS and a few minutes. The `weights` volume
keeps it across rebuilds; `.dockerignore` keeps it out of the build context.

**SRT is UDP.** Both rx sockets are UDP, and the ports published here (4200,
4201) exist for the case where only part of the system is containerised. If
`tx` or a node runs on the host instead, point it at the published port and
remember that the *egress* address the node is told comes from rx's `--egress`
flag, which would then need to name a host the node can reach.

**`libsrt` comes in flavours.** The rx image links the openssl build; the
GStreamer packages pull in the gnutls one. They are co-installable because they
install under different SONAMEs, but if you build your own images, make sure
the runtime package (`libsrt1.5-openssl`) matches the one the builder linked
against.

## Building and stamping the version

Every image takes the build id as a build arg and bakes it in, so
`--version` and the `version=` field on every log line agree — across the Go
services and the Python node alike:

```sh
export VERSION=$(git describe --tags --always --dirty)
docker compose build                     # every image, stamped
docker compose exec hub surr-hub --version
docker compose logs hub | head -1        # version=... on the first line
```

A mismatch between two services is logged as a warning rather than hidden:

```
sibling protocol revision mismatch  peer_protocol_revision=1  protocol_revision=2  peer_version=...
```

The protocol revision is the compatibility contract between peers — the version
string is just the build id, and two different builds of the same revision are
compatible by definition.
