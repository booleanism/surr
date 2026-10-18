#!/usr/bin/env bash
#
# Bring the stack up and assert the things that are easy to get wrong.
#
# Every check here exists because it was wrong at least once, and none of them
# is visible from `docker compose ps` -- a stack whose containers are all Up can
# still be running the wrong build, serving a broken data plane, or segfaulting
# on every shutdown. The checks, in the order they run:
#
#   1. the images are the build you think they are (not `unknown`, and not one
#      behind -- a partly-rebuilt stack is a mixed deployment), every migrator
#      exits 0, and the analytics service joins the broker
#   2. the HTTP API answers, and the client mistakes are client errors: a
#      duplicate signup is 409 rather than 500, a body using `password` is 400,
#      an add with no nodes is 400 and an already-subscribed node is 409
#   3. the node registers itself, declares its analytics queries, and frames
#      reach it and land in mongo -- then GET /nodes/analytics returns those
#      declared queries with the window the hub chose
#   4. tx exits 0 on SIGTERM instead of dying of a GLib assertion
#   5. an absent node does not make rx reconnect in a tight loop, and the chain
#      recovers by itself once the node comes back
#
# It is deliberately not a test suite: it asserts against the running system
# rather than mocking it, because the defects it guards against lived in the
# wiring, which is exactly what unit tests do not reach. Expect it to take about
# three minutes.
#
# Usage:
#
#   cd deploy && ./smoke.sh            # bring up, check, tear down
#   KEEP=1 ./smoke.sh                  # leave the stack running afterwards
#
# Exit status is 0 only if every check passed.

set -uo pipefail

cd "$(dirname "$0")" || exit 1

PROFILES=(--profile demo --profile tx --profile nodes)
COMPOSE=(docker compose "${PROFILES[@]}")

HUB_PORT="${HUB_HTTP_PORT:-9000}"
HUB_URL="http://localhost:${HUB_PORT}"

# A node belongs to exactly one subscription -- `subscribed_nodes.node_id` is
# UNIQUE, and the egress resolves a node id to a single subscription -- and the
# API offers no way to look an existing subscription up or to move a node
# between them. So the default node id is regenerated on every run: reusing the
# pinned `BUSTLE_NODE_ID` would make this script pass on a virgin database and
# fail on the second run, which is the opposite of what a check is for. A fresh
# id also exercises the path a new install takes.
NODE_ID="${NODE_ID:-$(cat /proc/sys/kernel/random/uuid)}"

# A fresh user per run, for the same reason: a duplicate email is a legitimate
# 409 now, but it is not what this run is testing.
EMAIL="smoke-$(date +%s)@example.com"
PASSWORD="smoke-password"

passed=0
failed=0

ok() {
    printf '  \033[32mok\033[0m   %s\n' "$1"
    passed=$((passed + 1))
}

no() {
    printf '  \033[31mFAIL\033[0m %s\n' "$1"
    printf '       %s\n' "$2"
    failed=$((failed + 1))
}

# check <description> <expected> <actual>
check() {
    if [ "$2" = "$3" ]; then
        ok "$1"
    else
        no "$1" "expected [$2], got [$3]"
    fi
}

step() {
    printf '\n==> %s\n' "$1"
}

cleanup() {
    if [ "${KEEP:-0}" = "1" ]; then
        printf '\n==> KEEP=1, leaving the stack up\n'
        return
    fi

    printf '\n==> tearing down\n'
    "${COMPOSE[@]}" down >/dev/null 2>&1
}

trap cleanup EXIT

# Count the documents written by ONE node.
#
# The collection is named after the node id with the dashes stripped, so asking
# for a specific node is both more precise and a sharper check than summing the
# database: a stale collection from an earlier run must not be able to make this
# look healthy. mongo's own system.* collections are excluded because the root
# user is not authorized to aggregate over them.
mongo_count() {
    "${COMPOSE[@]}" exec -T mongo mongosh \
        -u "${MONGO_USER:-root}" -p "${MONGO_PASSWORD:-root}" \
        --authenticationDatabase admin --quiet \
        --eval "const d = db.getSiblingDB('nodes'); const c = d.getCollection('${1//-/}'); print(c ? c.countDocuments() : 0)" 2>/dev/null | tr -d '[:space:]'
}

# --------------------------------------------------------------- 1. the images

step "the images are the build you think they are"

# This is the check that motivated the script. Every image in the first stack
# tested reported a DIFFERENT version -- two said `unknown` (VERSION was not
# exported at build time) and two were built from trees several commits apart --
# so "the stack ran" proved nothing about which code ran.
VERSION="${VERSION:-$(git describe --tags --always --dirty 2>/dev/null || echo unknown)}"

if [ "$VERSION" = "unknown" ]; then
    no "VERSION is set" "compose cannot run git describe itself; export VERSION before building"
else
    ok "VERSION is $VERSION"
fi

for svc in hub subscriptions rx tx analytics; do
    got=$(docker run --rm --entrypoint "/usr/local/bin/surr-${svc}" "surr-${svc}:latest" --version 2>/dev/null | awk '{print $NF}')
    check "${svc} image reports the build id" "$VERSION" "$got"
done

# ---------------------------------------------------------------- 2. bring up

step "bringing the stack up"

BUSTLE_NODE_ID="$NODE_ID" "${COMPOSE[@]}" up -d >/dev/null 2>&1

# The hub is the only service with a published port, so it is the readiness
# probe. Any HTTP status means the listener is there; 000 means it is not.
printf '  waiting for the hub'
code=""
for _ in $(seq 1 60); do
    code=$(curl -s -o /dev/null -w '%{http_code}' "${HUB_URL}/" 2>/dev/null)
    if [ -n "$code" ] && [ "$code" != "000" ]; then
        printf '\n'
        ok "hub answers on ${HUB_URL}"
        break
    fi
    printf '.'
    sleep 2
done
if [ -z "$code" ] || [ "$code" = "000" ]; then
    printf '\n'
    no "hub answers on ${HUB_URL}" "timed out after 120s; docker compose logs hub"
fi

# The migrators must succeed: the services only VERIFY their schema and exit if
# a table is missing, so a failed migration looks like a hub that will not start.
for job in migrate-hub migrate-subscriptions migrate-analytics; do
    jcode=$("${COMPOSE[@]}" ps -a --format '{{.ExitCode}}' "$job" 2>/dev/null | head -1)
    check "${job} exited 0" "0" "${jcode:-missing}"
done

# The analytics service is in the default profile but has no published port and
# no HTTP surface, so nothing above would notice it failing to start. It logs
# "routine started" once it is listening on the broker, which is the only proof
# it joined the mesh rather than merely being up.
printf '  waiting for analytics'
astate=""
for _ in $(seq 1 30); do
    # Match the routine name alone: the log line carries colour codes between
    # "routine=" and the value, so the joined string never appears verbatim.
    astate=$(docker logs surr-analytics-1 2>&1 | grep -c 'router-listen')
    if [ "${astate:-0}" -gt 0 ]; then
        printf '\n'
        ok "analytics is listening on the broker"
        break
    fi
    printf '.'
    sleep 2
done
if [ "${astate:-0}" -eq 0 ]; then
    printf '\n'
    no "analytics is listening on the broker" "no 'routine started' in 60s; docker logs surr-analytics-1"
fi

# ------------------------------------------------------------- 3. the HTTP API

step "the HTTP API behaves"

signup=$(curl -sS -o /dev/null -w '%{http_code}' \
    -X POST "${HUB_URL}/users/signup" \
    -H 'Content-Type: application/json' \
    -d "{\"email\":\"${EMAIL}\",\"passwd\":\"${PASSWORD}\"}" 2>/dev/null)
check "signup answers 200" "200" "${signup:-000}"

# A duplicate email is a client mistake, not a server fault: SQLite reports a
# UNIQUE constraint failure and that has to become 409, not 500.
dup=$(curl -sS -o /dev/null -w '%{http_code}' \
    -X POST "${HUB_URL}/users/signup" \
    -H 'Content-Type: application/json' \
    -d "{\"email\":\"${EMAIL}\",\"passwd\":\"${PASSWORD}\"}" 2>/dev/null)
check "a duplicate signup answers 409" "409" "${dup:-000}"

# The field is `passwd`. Getting it wrong used to be accepted silently with an
# empty password, and the mistake only surfaced later as a failed login.
wrongfield=$(curl -sS -o /dev/null -w '%{http_code}' \
    -X POST "${HUB_URL}/users/signup" \
    -H 'Content-Type: application/json' \
    -d "{\"email\":\"other-${EMAIL}\",\"password\":\"${PASSWORD}\"}" 2>/dev/null)
check "an unknown field name answers 400" "400" "${wrongfield:-000}"

token=$(curl -sS -X POST "${HUB_URL}/auth/signin" \
    -H 'Content-Type: application/json' \
    -d "{\"email\":\"${EMAIL}\",\"passwd\":\"${PASSWORD}\"}" 2>/dev/null | jq -r '.access_token // empty')
if [ -n "$token" ]; then
    ok "signin returns a token"
else
    no "signin returns a token" "no access_token in the response"
fi

# An empty node list used to answer 200 with a fresh subscription id: a row with
# no members that the API cannot remove.
emptylist=$(curl -sS -o /dev/null -w '%{http_code}' \
    -X POST "${HUB_URL}/subscriptions/add" \
    -H "Authorization: Bearer ${token}" \
    -H 'Content-Type: application/json' \
    -d '{"nodes":[]}' 2>/dev/null)
check "an add with no nodes answers 400" "400" "${emptylist:-000}"

# ------------------------------------------------------ 4. the node registers

step "the node registers itself with the hub"

# `/nodes/availability` lists the nodes that are running now and NOT yet
# subscribed to anything, which is exactly the question here: has this node
# announced itself, and is it free to be given work? It is also the only
# endpoint that can answer "which node ids exist", since there is no plain node
# listing. "Running now" is read from the sibling registry, not from the hub's
# `nodes` table, so this check passing means the introduce loop is working and
# not merely that a row was written.
printf '  waiting for node %s' "$NODE_ID"
available=""
for _ in $(seq 1 45); do
    available=$(curl -sS "${HUB_URL}/nodes/availability" \
        -H "Authorization: Bearer ${token}" 2>/dev/null | jq -r --arg id "$NODE_ID" \
        'if type == "array" then (map(.id) | index($id) // empty) else empty end' 2>/dev/null)
    if [ -n "$available" ]; then
        printf '\n'
        ok "the node registered and is unsubscribed"
        break
    fi
    printf '.'
    sleep 4
done
if [ -z "$available" ]; then
    printf '\n'
    no "the node registered and is unsubscribed" \
        "not in /nodes/availability after 180s; docker compose logs bustle hub"
fi

# The declaration is the node's own proof that it reached the analytics service:
# it is published from the post-registration hook, so a line here means the
# manifest was built, serialised, resolved to the right sibling and accepted.
# Nothing else on the live path exercises analytics.declare.
printf '  waiting for the node to declare its queries'
declared=""
for _ in $(seq 1 20); do
    declared=$(docker logs surr-bustle-1 2>&1 | grep -c 'declared analytics queries')
    if [ "${declared:-0}" -gt 0 ]; then
        printf '\n'
        ok "the node declared its analytics queries"
        break
    fi
    printf '.'
    sleep 2
done
if [ "${declared:-0}" -eq 0 ]; then
    printf '\n'
    no "the node declared its analytics queries" \
        "no 'declared analytics queries' in 40s; docker logs surr-bustle-1 surr-analytics-1"
fi

# ----------------------------------------------------------- 5. the data plane

step "frames reach the node and land in mongo"

body=$(curl -sS -X POST "${HUB_URL}/subscriptions/add" \
    -H "Authorization: Bearer ${token}" \
    -H 'Content-Type: application/json' \
    -d "{\"nodes\":[{\"id\":\"${NODE_ID}\"}]}" 2>&1)
sub=$(printf '%s' "$body" | jq -r '.subscription_id // empty' 2>/dev/null)
if [ -n "$sub" ]; then
    ok "a subscription was created"
else
    no "a subscription was created" "response was: ${body}"
fi

# A node belongs to exactly one subscription -- subscribed_nodes.node_id is
# UNIQUE -- so the second add is a conflict. It used to surface as a 500, which
# reads like a broken hub rather than a mistake in the request.
if [ -n "$sub" ]; then
    redup=$(curl -sS -o /dev/null -w '%{http_code}' \
        -X POST "${HUB_URL}/subscriptions/add" \
        -H "Authorization: Bearer ${token}" \
        -H 'Content-Type: application/json' \
        -d "{\"nodes\":[{\"id\":\"${NODE_ID}\"}]}" 2>/dev/null)
    check "adding the same node again answers 409" "409" "${redup:-000}"
fi

# tx reads the subscription id from the environment at container-create time, so
# it has to be recreated rather than restarted for a new one to take effect.
if [ -n "$sub" ]; then
    TX_SUBSCRIPTION_ID="$sub" BUSTLE_NODE_ID="$NODE_ID" "${COMPOSE[@]}" up -d --force-recreate tx >/dev/null 2>&1
fi

printf '  waiting for the first counts'
before=""
for _ in $(seq 1 45); do
    before=$(mongo_count "$NODE_ID")
    if [ -n "$before" ] && [ "$before" != "0" ] && [ "$before" != "null" ]; then
        printf '\n'
        ok "mongo holds ${before} documents for this node"
        break
    fi
    printf '.'
    sleep 4
done
if [ -z "$before" ] || [ "$before" = "0" ] || [ "$before" = "null" ]; then
    printf '\n'
    no "the node writes counts to mongo" "still 0 after 180s; docker compose logs rx bustle tx"
fi

printf '  watching the count for 30s'
sleep 30
after=$(mongo_count "$NODE_ID")
printf '\n'
if [ -n "$after" ] && [ "$after" != "null" ] && [ "$after" -gt "${before:-0}" ] 2>/dev/null; then
    ok "the count climbs (${before} -> ${after})"
else
    no "the count climbs" "${before} -> ${after}"
fi

# The read path end to end: the hub asks the analytics service what this node
# declared, runs every query in it and returns them under their own names. This
# is the assertion that no query name is baked into the hub -- the report has to
# contain the five bustle names because the node declared them, not because the
# hub knows them. `fleet_activity` is the fleet-scoped one, stored once for the
# node type rather than once for the node.
report=$(curl -sS "${HUB_URL}/nodes/analytics?node_id=${NODE_ID}&time_cluster=minutes" \
    -H "Authorization: Bearer ${token}" 2>&1)

declared_names=$(printf '%s' "$report" | jq -r '.queries[].name' 2>/dev/null | sort | tr '\n' ' ')
check "the analytics endpoint returns the declared queries" \
    "activity_by_class class_affinity fleet_activity occupancy rhythm " "${declared_names}"

# A fleet-scoped query is the only one that runs across more than one
# collection. With one node of this type the union is a no-op, so it must answer
# with that node's numbers rather than with an empty fleet -- the failure this
# catches is a member set that came back empty, which would read as "the fleet
# saw nothing".
fleet_avg=$(printf '%s' "$report" \
    | jq -r '[.queries[] | select(.name == "fleet_activity") | .data[0].fleet.average] | first' 2>/dev/null)
node_avg=$(printf '%s' "$report" \
    | jq -r '[.queries[] | select(.name == "activity_by_class") | .data[0].classes[] | select(.name == "person") | .average] | first' 2>/dev/null)
check "a fleet query answers with its single member's numbers" "${node_avg}" "${fleet_avg}"

if printf '%s' "$report" | jq -e '.queries[] | select(.name == "rhythm") | .data' >/dev/null 2>&1; then
    ok "the endpoint answers a query that takes no bucket width"
else
    no "the endpoint answers a query that takes no bucket width" "response was: ${report}"
fi

# A bucket label has to arrive as a string. The service marshals documents as
# relaxed extended JSON, so a BSON date would reach a client as {"$date": ...}
# -- an object no chart can label an axis with.
bucket_type=$(printf '%s' "$report" \
    | jq -r '[.queries[] | select(.name == "occupancy") | .data[0].timeline.buckets[0] | type] | first' 2>/dev/null)
check "a bucket label is a string, not an extended-JSON date" "string" "${bucket_type}"

# The window the hub chose, echoed back: minutes means a 5-minute window
# bucketed by the second. A caller labels its chart from this rather than
# recomputing it.
check "the report names the window it used" \
    "minutes second" \
    "$(printf '%s' "$report" | jq -r '.window.time_cluster + " " + .window.unit_time' 2>/dev/null)"

# ------------------------------------------------------- 6. tx shuts down cleanly

step "tx shuts down without a GLib assertion"

# The assertion that was never made. An unreachable source used to make tx die
# of SIGSEGV -- and rebuilding with a reachable source showed the abort was in
# the SHUTDOWN path, so it fired on a clean SIGTERM too. Under
# `restart: unless-stopped` that is a crash loop for what is usually a typo.
docker stop surr-tx-1 >/dev/null 2>&1
exitcode=$(docker inspect -f '{{.State.ExitCode}}' surr-tx-1 2>/dev/null)
check "tx exits 0 on SIGTERM" "0" "${exitcode:-missing}"

TX_SUBSCRIPTION_ID="${sub:-}" BUSTLE_NODE_ID="$NODE_ID" "${COMPOSE[@]}" up -d tx >/dev/null 2>&1
sleep 15
# And it must still be usable: the teardown is deliberately abrupt (the
# pipelines are never brought to NULL), so the thing to rule out is a socket or
# a pipeline left in a state that stops the next run from publishing.
if "${COMPOSE[@]}" logs --since 20s tx 2>/dev/null | grep -q 'running transmitter'; then
    ok "tx republishes after the restart"
else
    no "tx republishes after the restart" "no 'running transmitter' in the last 20s of logs"
fi

# ------------------------------------------------- 7. an absent node does not spin

step "a publisher connecting into an absent node does not spin"

# rx cannot dispatch <node-id>.sink.start to a node that is not registered, and
# it drops the publisher so tx reconnects into a node that has arrived. Without
# the delay before the drop that cycle ran as fast as an SRT handshake completes
# -- measured at 1058 attempts in 31 seconds.
#
# Stopping the node is not enough to see it: once a publisher is accepted, rx
# keeps reading frames and simply drops them if nobody is subscribed, so an
# established stream does not notice the node leaving. The cycle only happens
# on a NEW handshake, which is the cold-start case, so tx has to reconnect.
docker stop surr-bustle-1 >/dev/null 2>&1

# A dead node must not answer a report. Its declaration outlives it -- nothing
# deletes a row, because `nodes.add` carries no timestamp to date one with -- so
# the read path has to ask the sibling registry whether the node is running.
# Without that check every query comes back with its empty shape, which reads as
# "this node saw nothing" rather than "this node is gone".
#
# The wait is the registry's own TTL (15s), not a guess: the entry survives until
# fifteen missed `introduce` broadcasts have passed.
printf '  waiting for the hub to expire the node'
dead_code=""
for _ in $(seq 1 15); do
    dead_code=$(curl -sS -o /dev/null -w '%{http_code}' \
        "${HUB_URL}/nodes/analytics?node_id=${NODE_ID}&time_cluster=minutes" \
        -H "Authorization: Bearer ${token}" 2>/dev/null)
    if [ "${dead_code}" = "409" ]; then
        printf '\n'
        break
    fi
    printf '.'
    sleep 2
done
[ "${dead_code}" = "409" ] || printf '\n'
check "analytics answers 409 for a node that is not running" "409" "${dead_code}"

TX_SUBSCRIPTION_ID="${sub:-}" BUSTLE_NODE_ID="$NODE_ID" "${COMPOSE[@]}" up -d --force-recreate tx >/dev/null 2>&1

printf '  letting tx connect into the absent node'
sleep 10
printf '\n'

printf '  counting reconnect cycles for 30s'
since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
sleep 30
cycles=$("${COMPOSE[@]}" logs --since "$since" rx 2>/dev/null | grep -c 'publisher stopped')
printf '\n'

# ~15 is one cycle every two seconds. The bounds are generous on purpose: the
# point is to catch "as fast as it can", not to pin the exact rate. The window
# straddles the 15s SiblingTTL, so the first few cycles still pay the 5s
# sinkCommandTimeout for a node the registry has not yet forgotten and the rest
# do not: expect 4 at the low end and ~15 once the entry has expired.
if [ "${cycles:-0}" -ge 4 ] && [ "${cycles:-0}" -le 40 ]; then
    ok "rx reconnects at a bounded rate (${cycles} cycles in 30s)"
else
    no "rx reconnects at a bounded rate" \
        "${cycles} cycles in 30s; expected roughly 15, and ~1000 before the fix"
fi

BUSTLE_NODE_ID="$NODE_ID" "${COMPOSE[@]}" up -d bustle >/dev/null 2>&1
printf '  waiting for the node to be picked up again'
recovered=no
since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
for _ in $(seq 1 20); do
    if "${COMPOSE[@]}" logs --since "$since" rx 2>/dev/null | grep -q 'publisher started'; then
        recovered=yes
        printf '\n'
        ok "the chain recovers once the node returns"
        break
    fi
    printf '.'
    sleep 3
done
if [ "$recovered" = "no" ]; then
    printf '\n'
    no "the chain recovers once the node returns" "no 'publisher started' within 60s"
fi

# The 409 has to be about liveness and not about the node having lost its
# declaration: the same request that was refused while the node was down must
# answer once it is introducing itself again.
live_code=$(curl -sS -o /dev/null -w '%{http_code}' \
    "${HUB_URL}/nodes/analytics?node_id=${NODE_ID}&time_cluster=minutes" \
    -H "Authorization: Bearer ${token}" 2>/dev/null)
check "analytics answers again once the node is back" "200" "${live_code}"

# ------------------------------------------------------------------- the result

printf '\n==> %d passed, %d failed\n' "$passed" "$failed"
[ "$failed" -eq 0 ]
