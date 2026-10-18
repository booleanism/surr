#!/usr/bin/env bash
#
# smoke.sh -- assert that a surr deployment is wired together, end to end.
#
# Every check here is something `ps` cannot tell you: that the six binaries are
# the build you think they are, that a client error is a client error, that a node
# actually registers and declares its queries, that tx publishing into rx
# reaches a node that pulls frames and writes documents, that the analytics
# report carries the queries the node declared, that a stopped node is refused
# rather than reported as empty, and that tx exits 0 on SIGTERM.
#
# It uses a throwaway database directory, its own ports and a fresh node id, so
# it can run next to a development stack without touching it. Nothing here needs
# Docker except RabbitMQ and MongoDB -- and if those are already listening, not
# even that.
#
#   ./smoke.sh                 # the whole thing
#   ./smoke.sh --keep          # keep the databases and logs for inspection
#   SRC=rtsp://... ./smoke.sh  # use a real camera instead of the test pattern
#
# Every knob below can also be given as a flag, because a shell variable is the
# one thing an operator will not guess: --keep, --skip-build, --no-infra,
# --src URL, --run-dir DIR. `--help` lists them.
#
# Knobs (all optional):
#   SRC             RTSP source. Unset: a synthetic one is served on RTSP_PORT.
#   START_INFRA     1: start RabbitMQ/Mongo containers when the ports are closed.
#   SKIP_BUILD      1: use the existing bin/ instead of running make build.
#   KEEP            1: keep the run directory (databases, logs) instead of removing it.
#   HUB_PORT, RX_INGRESS_PORT, RX_EGRESS_PORT, RTSP_PORT
#   RUN_DIR         where to put the databases and logs (default: a mktemp -d).

set -uo pipefail

usage() {
    sed -n '3,30p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

# Flags are thin aliases for the environment variables the rest of the file
# reads, so there is exactly one place a setting can come from and one place it
# is documented.
while [ $# -gt 0 ]; do
    case "$1" in
        --keep) KEEP=1 ;;
        --no-keep) KEEP=0 ;;
        --skip-build) SKIP_BUILD=1 ;;
        --no-infra) START_INFRA=0 ;;
        --src)
            shift
            [ $# -gt 0 ] || { printf 'smoke.sh: --src needs a URL\n' >&2; exit 2; }
            SRC=$1
            ;;
        --run-dir)
            shift
            [ $# -gt 0 ] || { printf 'smoke.sh: --run-dir needs a path\n' >&2; exit 2; }
            RUN_DIR_FLAG=$1
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        *)
            printf 'smoke.sh: unknown argument %s (try --help)\n' "$1" >&2
            exit 2
            ;;
    esac
    shift
done

REPO_ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd "$REPO_ROOT" || exit 1

HUB_PORT=${HUB_PORT:-19000}
RX_INGRESS_PORT=${RX_INGRESS_PORT:-14200}
RX_EGRESS_PORT=${RX_EGRESS_PORT:-14201}
RTSP_PORT=${RTSP_PORT:-18554}

START_INFRA=${START_INFRA:-1}
SKIP_BUILD=${SKIP_BUILD:-0}
KEEP=${KEEP:-0}

[ -n "${RUN_DIR_FLAG:-}" ] && RUN_DIR=$RUN_DIR_FLAG

HUB_URL="http://127.0.0.1:${HUB_PORT}"
SRC_WAS_SET=0
[ -n "${SRC:-}" ] && SRC_WAS_SET=1
SRC=${SRC:-"rtsp://127.0.0.1:${RTSP_PORT}/cam1"}

MONGO_URI=${MONGO_URI:-"mongodb://root:root@127.0.0.1:27017/?authSource=admin"}
MONGO_DB=${MONGO_DB:-nodes}
BROKER=${BROKER:-"amqp://guest:guest@127.0.0.1:5672//"}

PASSED=0
FAILED=0
FAILURES=()
PIDS=()
STARTED_CONTAINERS=()

# ---------------------------------------------------------------------------
# output
# ---------------------------------------------------------------------------

step() {
    printf '\n\033[1m== %s\033[0m\n' "$*"
}

ok() {
    PASSED=$((PASSED + 1))
    printf '  \033[32mok\033[0m   %s\n' "$*"
}

no() {
    FAILED=$((FAILED + 1))
    FAILURES+=("$*")
    printf '  \033[31mFAIL\033[0m %s\n' "$*"
}

# check <name> <expected> <actual>
check() {
    local name=$1 want=$2 got=$3
    if [ "$want" = "$got" ]; then
        ok "$name"
    else
        no "$name"
        printf '       want: %s\n       got:  %s\n' "$want" "$got"
    fi
}

# contains <name> <needle> <haystack>
contains() {
    local name=$1 needle=$2 hay=$3
    if printf '%s' "$hay" | grep -qF -- "$needle"; then
        ok "$name"
    else
        no "$name"
        printf '       looking for: %s\n       in:          %.200s\n' "$needle" "$hay"
    fi
}

skip() {
    printf '  \033[33mskip\033[0m %s\n' "$*"
}

die() {
    printf '\n\033[31mfatal:\033[0m %s\n' "$*" >&2
    exit 2
}

# ---------------------------------------------------------------------------
# lifecycle
# ---------------------------------------------------------------------------

start_bg() {
    # start_bg <name> <logfile> <command...>
    #
    # setsid, so the whole process group can be signalled: `uv run python` is a
    # wrapper around the interpreter, and killing only the wrapper leaves a node
    # that keeps introducing itself and holding the broker connection.
    local name=$1 log=$2
    shift 2

    setsid "$@" >"$log" 2>&1 &
    local pid=$!
    PIDS+=("$pid")
    printf '%s\n' "$pid" >"$RUN/pids/$name"

    return 0
}

stop_pid() {
    # stop_pid <pid> -- SIGTERM the group, then SIGKILL after 15s.
    local pid=$1

    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || return 0

    for _ in $(seq 1 150); do
        kill -0 "$pid" 2>/dev/null || return 0
        sleep 0.1
    done

    kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null
    return 0
}

cleanup() {
    local code=$?
    trap - EXIT INT TERM

    for pid in "${PIDS[@]:-}"; do
        [ -n "$pid" ] && stop_pid "$pid"
    done

    for c in "${STARTED_CONTAINERS[@]:-}"; do
        [ -n "$c" ] && docker rm -f "$c" >/dev/null 2>&1
    done

    if [ "${KEEP:-0}" = 1 ]; then
        printf '\nrun directory kept: %s\n' "$RUN"
    elif [ -n "${RUN:-}" ] && [ -d "$RUN" ]; then
        rm -rf "$RUN"
    fi

    exit "$code"
}

trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# waiting
# ---------------------------------------------------------------------------

wait_port() {
    # wait_port <host> <port> <seconds>
    local host=$1 port=$2 secs=$3
    for _ in $(seq 1 $((secs * 5))); do
        (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null && { exec 3>&-; return 0; }
        sleep 0.2
    done
    return 1
}

wait_udp() {
    # wait_udp <port> <seconds> -- SRT is UDP, so /dev/tcp cannot see it.
    local port=$1 secs=$2
    for _ in $(seq 1 $((secs * 5))); do
        ss -lunH 2>/dev/null | grep -q ":${port} " && return 0
        sleep 0.2
    done
    return 1
}

wait_log() {
    # wait_log <file> <pattern> <seconds>
    local file=$1 pattern=$2 secs=$3
    for _ in $(seq 1 $((secs * 5))); do
        [ -f "$file" ] && grep -qF -- "$pattern" "$file" && return 0
        sleep 0.2
    done
    return 1
}

# ---------------------------------------------------------------------------
# 0. preflight
# ---------------------------------------------------------------------------

step "preflight"

for tool in curl jq go uv migrate gst-inspect-1.0 ss; do
    command -v "$tool" >/dev/null 2>&1 || die "$tool is not on PATH"
done

PY="$REPO_ROOT/.venv/bin/python"
[ -x "$PY" ] || die ".venv is missing: run 'uv sync' first"
"$PY" -c 'import gi, aio_pika, pymongo' 2>/dev/null ||
    die ".venv cannot import gi/aio_pika/pymongo: run 'uv sync' (gi comes from the system)"

# The one GStreamer element a distro is least likely to have. tx panics rather
# than erroring when it is missing, so check it here where the message is usable.
missing=""
for e in srtsink srtsrc intersrc intersink tsdemux h264parse avdec_h264 \
    openh264enc videoconvert appsink; do
    gst-inspect-1.0 "$e" >/dev/null 2>&1 || missing="$missing $e"
done
[ -z "$missing" ] || die "missing GStreamer elements:$missing (see DEPLOYMENT.md section 2)"

ok "the required GStreamer elements are installed"

if [ "$SKIP_BUILD" = 1 ]; then
    for b in hub subscriptions analytics rx tx; do
        [ -x "bin/surr-$b" ] || die "bin/surr-$b is missing and SKIP_BUILD=1"
    done
    ok "using the existing binaries"
else
    build_log=$(mktemp)
    if make build >"$build_log" 2>&1; then
        ok "make build"
    else
        tail -20 "$build_log"
        rm -f "$build_log"
        die "make build failed"
    fi
    rm -f "$build_log"
fi

# The binaries must be the build this checkout describes: the whole point of the
# version injection is that no constant has to be kept in sync by hand.
WANT_VERSION=$(git describe --tags --always --dirty 2>/dev/null || echo unknown)
for b in hub subscriptions analytics rx tx; do
    got=$("./bin/surr-$b" --version 2>&1 | awk '{print $NF}')
    check "surr-$b reports the checkout's version" "$WANT_VERSION" "$got"
done

RUN=${RUN_DIR:-$(mktemp -d)}
mkdir -p "$RUN/logs" "$RUN/pids"
RUN=$(cd "$RUN" && pwd)

# ---------------------------------------------------------------------------
# 1. infrastructure
# ---------------------------------------------------------------------------

step "infrastructure"

if wait_port 127.0.0.1 5672 1; then
    ok "RabbitMQ is already listening"
else
    [ "$START_INFRA" = 1 ] || die "RabbitMQ is not listening on 5672 and START_INFRA=0"
    docker rm -f surr-smoke-rabbitmq >/dev/null 2>&1
    docker run -d --rm --name surr-smoke-rabbitmq -p 5672:5672 rabbitmq:4-management >/dev/null ||
        die "could not start RabbitMQ"
    STARTED_CONTAINERS+=(surr-smoke-rabbitmq)
    wait_port 127.0.0.1 5672 90 || die "RabbitMQ did not open 5672"
    ok "RabbitMQ started"
fi

if wait_port 127.0.0.1 27017 1; then
    ok "MongoDB is already listening"
else
    [ "$START_INFRA" = 1 ] || die "MongoDB is not listening on 27017 and START_INFRA=0"
    docker rm -f surr-smoke-mongo >/dev/null 2>&1
    docker run -d --rm --name surr-smoke-mongo -p 27017:27017 \
        -e MONGO_INITDB_ROOT_USERNAME=root \
        -e MONGO_INITDB_ROOT_PASSWORD=root \
        mongo:8 >/dev/null || die "could not start MongoDB"
    STARTED_CONTAINERS+=(surr-smoke-mongo)
    wait_port 127.0.0.1 27017 120 || die "MongoDB did not open 27017"
    sleep 3
    ok "MongoDB started"
fi

"$PY" - "$MONGO_URI" <<'PY' || die "MongoDB is not accepting this MONGO_URI"
import sys
from pymongo import MongoClient
MongoClient(sys.argv[1], serverSelectionTimeoutMS=15000).admin.command("ping")
PY
ok "MongoDB accepts the configured credentials"

# ---------------------------------------------------------------------------
# 2. migrations
# ---------------------------------------------------------------------------

step "migrations"

# The stock migrate release binary has no sqlite3 driver, and a binary built with
# -tags sqlite (the pure-Go driver) uses a different URL scheme. Pick whichever
# driver this binary actually has rather than assuming.
migrate_help=$(migrate -help 2>&1)
if printf '%s' "$migrate_help" | grep -q 'sqlite3'; then
    SCHEME=sqlite3
elif printf '%s' "$migrate_help" | grep -q 'sqlite'; then
    SCHEME=sqlite
else
    die "this migrate has no SQLite driver: rebuild it with -tags sqlite3 (DEPLOYMENT.md section 5)"
fi
ok "migrate has the '$SCHEME' driver"

migrate_db() {
    local svc=$1 file="$RUN/$1.db"
    migrate -path "$svc/internal/migrations/sqlite" -database "${SCHEME}://${file}" up \
        >>"$RUN/logs/migrate-$svc.log" 2>&1
}

for svc in hub subscriptions analytics; do
    migrate_db "$svc" || die "migrating $svc failed (see $RUN/logs/migrate-$svc.log)"
done
ok "the three SQLite databases are migrated"

tables() {
    "$PY" -c "
import sqlite3, sys
print(' '.join(sorted(r[0] for r in sqlite3.connect(sys.argv[1]).execute(
    \"select name from sqlite_master where type='table'\"))))
" "$RUN/$1.db"
}

check "hub.db has its tables" "nodes schema_migrations users" "$(tables hub)"
check "subscriptions.db has its tables" "schema_migrations subscribed_nodes subscriptions" "$(tables subscriptions)"
check "analytics.db has its tables" "declarations schema_migrations" "$(tables analytics)"

# ---------------------------------------------------------------------------
# 3. services
# ---------------------------------------------------------------------------

step "services"

start_bg subscriptions "$RUN/logs/subscriptions.log" \
    ./bin/surr-subscriptions run --database "$RUN/subscriptions.db" --broker "$BROKER" \
    --log-format json
wait_log "$RUN/logs/subscriptions.log" "subscriptions starting" 30 ||
    die "subscriptions did not start: $(tail -5 "$RUN/logs/subscriptions.log")"

start_bg analytics "$RUN/logs/analytics.log" \
    ./bin/surr-analytics run --database "$RUN/analytics.db" --broker "$BROKER" \
    --mongo-url "$MONGO_URI" --mongo-db "$MONGO_DB" --log-format json
wait_log "$RUN/logs/analytics.log" "analytics starting" 30 ||
    die "analytics did not start: $(tail -5 "$RUN/logs/analytics.log")"

start_bg hub "$RUN/logs/hub.log" \
    ./bin/surr-hub run --database "$RUN/hub.db" --broker "$BROKER" \
    --http "127.0.0.1:${HUB_PORT}" --secret "smoke-secret" --cookie-secure=false \
    --log-format json
wait_log "$RUN/logs/hub.log" "hub starting" 30 ||
    die "hub did not start: $(tail -5 "$RUN/logs/hub.log")"

start_bg rx "$RUN/logs/rx.log" \
    ./bin/surr-rx run --broker "$BROKER" \
    --ingress "127.0.0.1:${RX_INGRESS_PORT}" --egress "127.0.0.1:${RX_EGRESS_PORT}" \
    --log-format json
wait_log "$RUN/logs/rx.log" "rx starting" 30 ||
    die "rx did not start: $(tail -5 "$RUN/logs/rx.log")"

wait_port 127.0.0.1 "$HUB_PORT" 30 || die "the hub is not serving HTTP on $HUB_PORT"
ok "the hub is serving HTTP"

wait_udp "$RX_INGRESS_PORT" 30 || die "rx is not listening on the ingress port"
wait_udp "$RX_EGRESS_PORT" 30 || die "rx is not listening on the egress port"
ok "rx is listening on both SRT ports"

check "no service logged an error at startup" "" \
    "$(grep -l '"level":"error"' "$RUN"/logs/{hub,subscriptions,analytics,rx}.log 2>/dev/null | tr '\n' ' ')"

# ---------------------------------------------------------------------------
# 4. the HTTP API's own contract
# ---------------------------------------------------------------------------

step "the HTTP API"

EMAIL="smoke-$(date +%s)@example.com"
PASSWD="smoke-password"

code=$(curl -sS -o "$RUN/signup.json" -w '%{http_code}' -X POST "$HUB_URL/users/signup" \
    -H 'Content-Type: application/json' \
    -d "{\"email\":\"$EMAIL\",\"passwd\":\"$PASSWD\"}")
check "signup is accepted" "200" "$code"
USER_ID=$(jq -r '.user_id // empty' <"$RUN/signup.json")
contains "signup returns a user id" "-" "$USER_ID"

code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$HUB_URL/users/signup" \
    -H 'Content-Type: application/json' \
    -d "{\"email\":\"$EMAIL\",\"passwd\":\"$PASSWD\"}")
check "a duplicate email is a conflict, not a server error" "409" "$code"

# `password` instead of `passwd` is the mistake a client actually makes. It has
# to be a 400: silently ignoring it creates an account with no password.
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$HUB_URL/users/signup" \
    -H 'Content-Type: application/json' \
    -d "{\"email\":\"other-$EMAIL\",\"passwd\":\"$PASSWD\",\"password\":\"$PASSWD\"}")
check "an unknown field is refused" "400" "$code"

code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$HUB_URL/users/signup" \
    -H 'Content-Type: application/json' \
    -d "{\"email\":\"third-$EMAIL\"}")
check "a missing password is refused" "400" "$code"

code=$(curl -sS -o "$RUN/signin.json" -w '%{http_code}' -X POST "$HUB_URL/auth/signin" \
    -H 'Content-Type: application/json' \
    -d "{\"email\":\"$EMAIL\",\"passwd\":\"$PASSWD\"}")
check "signin is accepted" "200" "$code"
TOKEN=$(jq -r '.access_token // empty' <"$RUN/signin.json")
contains "signin returns an access token" "." "$TOKEN"

code=$(curl -sS -o /dev/null -w '%{http_code}' "$HUB_URL/nodes/availability")
check "availability requires a token" "401" "$code"

code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$HUB_URL/subscriptions/add" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d '{"nodes":[]}')
check "a subscription with no nodes is refused" "400" "$code"

code=$(curl -sS -o /dev/null -w '%{http_code}' "$HUB_URL/nodes/analytics?node_id=not-a-uuid" \
    -H "Authorization: Bearer $TOKEN")
check "a malformed node id is a client error" "400" "$code"

# ---------------------------------------------------------------------------
# 5. the node
# ---------------------------------------------------------------------------

step "the node"

NODE_ID=$("$PY" -c 'import uuid; print(uuid.uuid4())')
COLLECTION=${NODE_ID//-/}

start_bg bustle "$RUN/logs/bustle.log" \
    "$PY" nodes/bustle.py --node-id "$NODE_ID" --broker "$BROKER" \
    --mongo-url "$MONGO_URI" --mongo-db "$MONGO_DB" --model weights/LibreDEIMm.pt

wait_log "$RUN/logs/bustle.log" "node registered command=nodes.add" 90 ||
    die "the node did not register: $(tail -10 "$RUN/logs/bustle.log")"
ok "the node registered itself with the hub"

# The declaration is the node's proof that it reached the analytics service: it
# is published from the post-registration hook, so a line here means the
# manifest was built, resolved to a sibling and accepted.
wait_log "$RUN/logs/bustle.log" "declared analytics queries" 90 ||
    die "the node did not declare its queries: $(tail -10 "$RUN/logs/bustle.log")"
declared_line=$(grep -F "declared analytics queries" "$RUN/logs/bustle.log" | tail -1)
contains "the node declared five queries" "queries=5" "$declared_line"

# Liveness comes from the introduce stream, so the hub learns about the node on
# its next broadcast rather than at `nodes.add` time: poll instead of racing it.
available=""
for _ in $(seq 1 30); do
    available=$(curl -sS "$HUB_URL/nodes/availability" -H "Authorization: Bearer $TOKEN" |
        jq -r --arg id "$NODE_ID" '[.[] | select(.id == $id) | .id] | first // empty' 2>/dev/null)
    [ -n "$available" ] && break
    sleep 1
done
check "the running node is offered for subscription" "$NODE_ID" "$available"

# ---------------------------------------------------------------------------
# 6. subscription
# ---------------------------------------------------------------------------

step "subscription"

code=$(curl -sS -o "$RUN/sub.json" -w '%{http_code}' -X POST "$HUB_URL/subscriptions/add" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d "{\"nodes\":[{\"id\":\"$NODE_ID\"}]}")
check "the node is subscribed" "200" "$code"
SUB_ID=$(jq -r '.subscription_id // empty' <"$RUN/sub.json")
contains "the subscription has an id" "-" "$SUB_ID"

# A node belongs to exactly one subscription and no endpoint moves one, so the
# second add is a conflict. Answering 500 here would send a client hunting for a
# server bug that does not exist.
code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$HUB_URL/subscriptions/add" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d "{\"nodes\":[{\"id\":\"$NODE_ID\"}]}")
check "subscribing the same node twice is a conflict" "409" "$code"

available=$(curl -sS "$HUB_URL/nodes/availability" -H "Authorization: Bearer $TOKEN" |
    jq -s -r 'if length == 0 then 0 else (.[0] | if type == "array" then length else 0 end) end')
check "a subscribed node is no longer offered" "0" "$available"

# ---------------------------------------------------------------------------
# 7. the data plane
# ---------------------------------------------------------------------------

step "the data plane"

if [ "$SRC_WAS_SET" = 0 ]; then
    cat >"$RUN/rtsp_demo.py" <<'PY'
"""A synthetic RTSP camera: no Docker, no camera, one GStreamer media factory."""
import gi

gi.require_version("Gst", "1.0")
gi.require_version("GstRtspServer", "1.0")
from gi.repository import GLib, Gst, GstRtspServer

import sys

port, mount = sys.argv[1], sys.argv[2]

Gst.init(None)

server = GstRtspServer.RTSPServer()
server.set_service(port)

factory = GstRtspServer.RTSPMediaFactory()
factory.set_launch(
    "( videotestsrc is-live=true pattern=ball "
    "! video/x-raw,width=640,height=480,framerate=15/1 "
    "! openh264enc bitrate=300000 ! rtph264pay name=pay0 pt=96 config-interval=1 )"
)
factory.set_shared(True)

server.get_mount_points().add_factory(mount, factory)
server.attach(None)

print(f"serving rtsp://127.0.0.1:{port}{mount}", flush=True)
GLib.MainLoop().run()
PY

    start_bg rtsp "$RUN/logs/rtsp.log" \
        "$PY" "$RUN/rtsp_demo.py" "$RTSP_PORT" "/cam1"
    wait_log "$RUN/logs/rtsp.log" "serving rtsp://" 30 ||
        die "the synthetic RTSP source did not start: $(tail -5 "$RUN/logs/rtsp.log")"
    ok "a synthetic RTSP source is serving $SRC"
fi

start_bg tx "$RUN/logs/tx.log" \
    ./bin/surr-tx run --src "$SRC" \
    --sink "srt://127.0.0.1:${RX_INGRESS_PORT}?mode=caller&streamid=publish:${SUB_ID}" \
    --name smoke

wait_log "$RUN/logs/rx.log" "publisher started" 90 ||
    die "tx never connected to rx: $(tail -10 "$RUN/logs/tx.log")"
ok "tx connected to the rx ingress"

wait_log "$RUN/logs/rx.log" "subscriber started" 90 ||
    die "the node never connected to rx: $(tail -10 "$RUN/logs/bustle.log")"
ok "the node connected to the rx egress"

# Documents, not log lines, and not connections: a pipeline that connected but
# carried no frames fails every check above and this one. The node's drain loop
# writes one document per interval.
count_docs() {
    "$PY" - "$MONGO_URI" "$MONGO_DB" "$COLLECTION" <<'PY'
import sys
from pymongo import MongoClient
uri, db, coll = sys.argv[1], sys.argv[2], sys.argv[3]
c = MongoClient(uri, serverSelectionTimeoutMS=15000)[db][coll]
print(c.count_documents({}))
PY
}

docs=0
for _ in $(seq 1 60); do
    docs=$(count_docs 2>/dev/null || echo 0)
    [ "$docs" -gt 0 ] 2>/dev/null && break
    sleep 1
done
if [ "$docs" -gt 0 ] 2>/dev/null; then
    ok "the node wrote $docs document(s) to MongoDB"
else
    no "the node wrote documents to MongoDB"
fi

# ---------------------------------------------------------------------------
# 8. the read path
# ---------------------------------------------------------------------------

step "the read path"

report=$(curl -sS "$HUB_URL/nodes/analytics?node_id=${NODE_ID}&time_cluster=minutes" \
    -H "Authorization: Bearer $TOKEN")

check "the report names the window it used" "minutes" "$(printf '%s' "$report" | jq -r '.window.time_cluster')"
check "the report names its bucket width" "second" "$(printf '%s' "$report" | jq -r '.window.unit_time')"

names=$(printf '%s' "$report" | jq -r '[.queries[].name] | sort | join(" ")')
check "the endpoint runs the queries the node declared" \
    "activity_by_class class_affinity fleet_activity occupancy rhythm" "$names"

errors=$(printf '%s' "$report" | jq -r '[.queries[] | select(.error != null) | .name + ": " + .error] | join("; ")')
check "no declared query failed" "" "$errors"

# A BSON date reaches a client as {"$date": ...} -- an object no chart can label
# an axis with. The service emits ISO strings on purpose; this is the assertion
# that keeps a raw date from coming back.
label_type=$(printf '%s' "$report" | jq -r \
    '[.queries[] | select(.name == "occupancy") | .data[0].timeline.buckets[0] | type] | first')
check "a bucket label is a string, not an extended-JSON date" "string" "$label_type"

# ---------------------------------------------------------------------------
# 9. a node that stopped
# ---------------------------------------------------------------------------

step "a node that stopped"

node_pid=$(cat "$RUN/pids/bustle")
stop_pid "$node_pid"

# The row stays in the hub's database -- `nodes.add` carries no timestamp, so it
# cannot be dated -- and the declaration stays in the analytics registry. Both
# have to stop being served: an empty report reads as "this node saw nothing".
#
# The refusal is not instant, and should not be: liveness is the sibling
# registry, whose entries expire SiblingTTL (15s) after the last introduce. So
# poll for it, and report how long the system took to notice.
code=000
waited=0
for _ in $(seq 1 45); do
    code=$(curl -sS -o "$RUN/dead.json" -w '%{http_code}' \
        "$HUB_URL/nodes/analytics?node_id=${NODE_ID}&time_cluster=minutes" \
        -H "Authorization: Bearer $TOKEN")
    [ "$code" = 409 ] && break
    waited=$((waited + 1))
    sleep 1
done
check "analytics for a stopped node is refused (after ${waited}s)" "409" "$code"

available=$(curl -sS "$HUB_URL/nodes/availability" -H "Authorization: Bearer $TOKEN" |
    jq -s -r 'if length == 0 then 0 else (.[0] | if type == "array" then length else 0 end) end')
check "a stopped node is not offered" "0" "$available"

# rx must stay quiet while the node is gone. This is the F5 regression: before the
# sibling TTL existed, rx kept naming the dead peer, dispatched to it on every
# cycle and spent a 5s timeout on each attempt, so the log grew without bound.
#
# The bound is on log LINES, not on one message. A healthy rx has nothing to do
# here -- its publisher is still connected and the subscriber it lost is already
# gone -- so the correct count is near zero, and a spin shows up as hundreds of
# "recv message" / "non-zero reply code" lines. Counting a single message instead
# would pass whether rx cycled, idled or spun.
before=$(wc -l <"$RUN/logs/rx.log")
sleep 20
after=$(wc -l <"$RUN/logs/rx.log")
quiet=$((after - before))
if [ "$quiet" -le 20 ]; then
    ok "rx stays quiet while the node is gone ($quiet log lines in 20s)"
else
    no "rx stays quiet while the node is gone ($quiet log lines in 20s)" \
        "want at most 20; a spinning rx retries the dead peer and logs each attempt"
fi

# ---------------------------------------------------------------------------
# 10. tx shutdown
# ---------------------------------------------------------------------------

step "shutdown"

tx_pid=$(cat "$RUN/pids/tx")
kill -TERM "$tx_pid" 2>/dev/null
tx_status=""
for _ in $(seq 1 300); do
    if ! kill -0 "$tx_pid" 2>/dev/null; then
        wait "$tx_pid" 2>/dev/null
        tx_status=$?
        break
    fi
    sleep 0.1
done

# 0, not 139: the pipelines are deliberately never brought to NULL, because that
# transition disposes the RTSP source and races GIO's resolver on GLib 2.84.
check "tx exits 0 on SIGTERM" "0" "${tx_status:-still running}"

# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------

step "summary"

if [ "$FAILED" -eq 0 ]; then
    printf '\033[32m%d passed, 0 failed\033[0m\n' "$PASSED"
    exit 0
fi

printf '\033[31m%d passed, %d failed\033[0m\n' "$PASSED" "$FAILED"
for f in "${FAILURES[@]}"; do
    printf '  - %s\n' "$f"
done
printf '\nlogs: %s/logs\n' "$RUN"
KEEP=1
exit 1
