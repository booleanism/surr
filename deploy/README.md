# Deploying surr

A single compose stack: RabbitMQ as the bus, MongoDB for analytics, the four
Go control/data-plane services, and — behind profiles — a transmitter and a
node.

```
camera (RTSP) → tx ──SRT──→ rx ──SRT──→ bustle (Python, YOLO) ──→ MongoDB
                              ↑____________ RabbitMQ ____________↓
      hub (HTTP API, auth)   subscriptions (SQLite)   analytics (read path)
```

| service                 | what it is                                                   | profile |
| ----------------------- | ------------------------------------------------------------ | ------- |
| `rabbitmq`              | the topic exchange every service introduces itself on        | –       |
| `mongo`                 | where nodes write their analytics documents                  | –       |
| `migrate-hub`           | one-shot: creates `nodes`, `users` in the hub database       | –       |
| `migrate-subscriptions` | one-shot: creates `subscriptions`, `subscribed_nodes`        | –       |
| `migrate-analytics`     | one-shot: creates `declarations` in the analytics database   | –       |
| `hub`                   | HTTP API (JWT auth, users, nodes, subscriptions) + SQLite    | –       |
| `subscriptions`         | answers which nodes belong to a subscription                 | –       |
| `analytics`             | read path over mongo: runs the queries nodes declared        | –       |
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
RTSP URL to pull from. Three ways to get one.

**The `demo` profile**, which is the shortest path and needs nothing from
you — mediamtx plus an ffmpeg process publishing a synthetic test pattern:

```sh
cd deploy
docker compose --profile demo --profile tx --profile nodes up -d
```

That is the whole thing running end to end with no camera and no external
containers. It is a separate profile rather than part of `tx` on purpose: a real
deployment points `tx` at a real camera, and quietly starting a fake one
whenever `tx` starts would be the wrong default. `demo` publishes
`rtsp://mediamtx:8554/cam1`, which is already the default `TX_SRC`, so you do
not have to set anything.

**A real camera.** Point `TX_SRC` straight at it, or restream it:

```sh
# -c copy avoids re-encoding; drop it if the camera's stream is not H.264.
ffmpeg -rtsp_transport tcp -i rtsp://user:pass@192.168.1.50:554/stream1 \
       -c copy -f rtsp rtsp://127.0.0.1:8554/cam1
```

**A synthetic stream of your own**, if you would rather run the pieces
outside compose — no camera, no codec surprises:

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
*tx's own container*. The `demo` profile above solves that for you; if you are
running mediamtx yourself instead, either put it on the compose network and use
`TX_SRC=rtsp://<container name>:8554/cam1`, or keep it on the host and give the
tx service a route to it:

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

- **Startup order is not critical, but it is not free either.** Services retry
  their broker calls until their siblings introduce themselves, so the graph
  settles on its own. `rx` telling a node to start is the one dispatch that is
  not retried in-process: it happens once, when `tx` connects, so a node that
  has not introduced itself yet is not told, `rx` drops the publisher, and `tx`
  reconnects and tries again. That recovers on its own within a few seconds of
  the node appearing — start `bustle` first anyway if you want a quiet log,
  because until it does the two retry at roughly one handshake every two
  seconds. `rx` waits that moment before dropping the publisher precisely to
  keep the rate down; without the wait the same loop ran at roughly 30
  handshakes a second.
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

## Verifying a deployment

`smoke.sh` brings the stack up and asserts the things that are not visible from
`docker compose ps` — a stack whose containers are all Up can still be running
the wrong build, serving a dead data plane, or segfaulting on every shutdown:

```sh
cd deploy
./smoke.sh
```

It takes about two minutes, exits non-zero if anything failed, and tears the
stack down afterwards unless you pass `KEEP=1`. It checks, in order:

1. **the images are the build you think they are.** This one exists because the
   first stack tested reported a different version on every image — two said
   `unknown` (nobody exported `VERSION` at build time) and two were built from
   trees several commits apart. "The stack ran" proved nothing about which code
   ran. A partly-rebuilt stack is a mixed deployment, which is exactly what the
   protocol revision is there to diagnose, so it is worth catching before it.
2. **the HTTP API behaves**, including the four failures that used to be silent
   or wrong: a duplicate signup is `409` rather than `500`, a body using
   `password` instead of `passwd` is a `400` rather than an empty-password user,
   an add with no nodes is a `400` rather than an empty subscription, and adding
   a node that is already subscribed is a `409` rather than a `500`.
3. **frames reach the node and land in mongo** — the count is read twice, 30s
   apart, and has to climb. Also that the node **declared its analytics
   queries**: the declaration is published from the post-registration hook, so
   a `declared analytics queries` line means the manifest was built,
   serialised, resolved to the analytics service and accepted. Nothing else on
   the live path exercises `analytics.declare`.
   Then the read path end to end: `GET /nodes/analytics` has to come back with
   the five queries the node declared (`activity_by_class`, `occupancy`,
   `rhythm`, `class_affinity`, `fleet_activity`), each with its own rows, plus
   the window the hub chose. `fleet_activity` is the fleet-scoped one: it is
   stored once for the node *type*, and it only answers because the hub sends
   the caller's own node ids with every query and the service unions their
   collections — a one-node fleet here, so the union is a no-op and the numbers
   should match that node's own report. Those names appear because the node declared them — the hub holds none
   of them, which is what makes a new node type's charts work without a hub
   change. `rhythm` is the one worth watching: it takes no bucket width, so it
   only answers if the hub sends each query the parameters *its* schema declares
   and no others. One more check reads a bucket label and asserts it is a
   string: the service marshals documents as relaxed extended JSON, so a BSON
   date would arrive as `{"$date": ...}` and no chart could label an axis with
   it.
   Two more checks bracket the moment the node is stopped, and they are the
   only place the read path's liveness rule is exercised end to end: the
   declaration outlives the node (nothing deletes a row, because `nodes.add`
   carries no timestamp), so `/nodes/analytics` has to answer **409** while the
   node is gone and **200** again once it is back. The 15s the first one waits
   is the sibling registry's own TTL, not a guess.
4. **`tx` exits 0 on SIGTERM.** The assertion that was never made: an
   unreachable source used to make `tx` die of SIGSEGV, and rebuilding with a
   reachable one showed the abort was in the *shutdown* path, so it fired on a
   clean SIGTERM too. Under `restart: unless-stopped` that is a crash loop for
   what is usually a typo.
5. **an absent node does not make `rx` reconnect in a tight loop** — the node
   is stopped and the reconnect cycles are counted for 30s. Before the
   `sinkUnavailableDelay` this was measured at 1058 in 31 seconds; before the
   sibling expiry it was 4, because the registry still listed the stopped node
   and each attempt spent up to the 5s `sinkCommandTimeout` waiting for an
   answer that was not coming. Measured at 13 in 30s on the first run after
   `SiblingTTL` landed, against 15 for a node that was never registered — the
   two cases have converged, which is the point.

It asserts against the running system rather than mocking it, because every
one of these defects lived in the wiring — which is precisely what unit tests
do not reach.

## Things that will bite you

**The services do not create their own tables.** `hub`, `subscriptions` and
`analytics` only *verify* their schema at startup and exit with `database schema is missing` if
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

**A `tx` that cannot reach its source is a restart loop, not a warning.** `tx`
exits non-zero when the source never connected — that is deliberate, so the
failure is visible to compose — and `restart: unless-stopped` brings it
straight back, so the symptom is `tx` cycling every few seconds. The first
lines of each cycle say why:
`bus watch error="... Failed to connect. (Generic error): Could not open
resource for reading and writing."`. That is a `TX_SRC` problem, not a `tx`
problem, and nothing publishes to `rx` while it cycles.

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

**Ask each image what it is before you debug anything else.** An image whose
`VERSION` was left unset reports `version=unknown`, and one built before your
last change reports the commit it was actually built from — both of which look
identical to a working stack from `docker compose ps`:

```sh
for svc in hub subscriptions rx tx; do
  docker run --rm --entrypoint /usr/local/bin/surr-$svc surr-$svc:latest --version
done
```

`./smoke.sh` does exactly this as its first check, and fails if the four do not
agree with the current `git describe`.

A mismatch between two services is logged as a warning rather than hidden:

```
sibling protocol revision mismatch  peer_protocol_revision=1  protocol_revision=2  peer_version=...
```

The protocol revision is the compatibility contract between peers — the version
string is just the build id, and two different builds of the same revision are
compatible by definition.

### Published images

A `v*` tag publishes all seven images instead of building them locally:

```sh
git tag -a v0.1.0 -m "..." && git push origin v0.1.0
```

`.github/workflows/release.yml` builds each one on its own runner (the tx Rust
stage and the bustle torch wheels do not fit on one disk together) and pushes to
`ghcr.io/<owner>/<repo>/<service>:<tag>`, with the tag as the build id. A
`verify` job then pulls the published hub image and fails unless `--version`
reports that tag, because "the images came from the commit I tagged" is the one
thing the earlier stack could not answer for itself.

The packages are pullable anonymously once published (`docker run --rm
ghcr.io/booleanism/surr/hub:v0.1.0 --version` needs no login). A package created
by a workflow against a *private* repository inherits that visibility instead, so
making the repository private later silently requires a login to pull — check the
package's own settings page rather than assuming the repository's.

To run a published build instead of building it, drop the `build:` blocks from
`docker-compose.yml` (or override the image) — the deployment needs nothing from
the build stage beyond the binary.

### What a rebuild actually costs

**Do not prune the builder cache to make room for a rebuild.** Freeing space by
throwing away the cache and then immediately rebuilding pays for every download
twice — the cache *is* what makes the rebuild cheap. If the disk is too full to
build, stop and ask the operator for space; do not reclaim it yourself. The one
reclamation that is safe without asking is `docker builder prune --filter
until=1h`, which leaves the cache the current build just wrote.

Builds are cached, and two of the caches are expensive to rebuild, so know what
you are throwing away before you prune:

- **apt runs against BuildKit cache mounts** (`/var/cache/apt` and
  `/var/lib/apt/lists`), so the package index and the `.deb` files survive an
  edit to a `RUN` line above them. This is why no Dockerfile has a
  `rm -rf /var/lib/apt/lists/*` any more — the mounts never reach the image, so
  that cleanup would only wipe the cache.
- **The `bustle` build is the slow one.** Its dependency layer downloads the
  torch CPU wheel and builds PyGObject from source, and it has OOM-killed a
  whole-stack build on a 16 GB machine. Build it on its own when memory is
tight: `docker compose --profile nodes build bustle`.
- **`tx` carries a Rust stage** that compiles the `intersrc` plugin against
  GStreamer, with its own cargo registry and target cache. `docker builder prune
  -a` discards both that and the torch layer; `docker builder prune -f` drops
  only the dangling part, which is usually what you want when disk is short.
