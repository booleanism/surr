# Adding a node type

A node type is **one script plus one directory**: a `Worker` subclass that says
what this type does, and a `lib/<type>/` package that does it. Everything else
— the broker connection, the command registry, self-registration with the hub,
the introduce ticker, the shutdown ordering — belongs to `lib/core/node.py` and
is inherited, not copied.

`bustle` (the object counter) is the only type today. This file is the recipe it
follows, so a second type is a copy of its shape rather than an archaeology
exercise.

## What the runtime already does for you

Read `nodes/lib/core/node.py` before writing anything. `Node` owns:

| | |
|---|---|
| `lib/core/router.py` | the topic exchange, the exclusive auto-delete queue, `Recv`/`Send`, delivery dispatch |
| `lib/core/siblings.py` | the peer registry built from `introduce`, with a TTL so a dead peer is forgotten |
| `lib/core/rpc.py` | `call()` — resolve the sibling that advertises a command, send, await the reply, retry across candidates |
| `lib/core/ticker.py` | an interval generator that does not drift |
| `lib/core/logging_setup.py` | stdlib `logging`, one format, `--log-level` |
| `lib/core/version.py` | the build id, so a mixed deployment's two halves are comparable |
| `lib/core/handlers.py` | `RouterResponse`, the `0`/`-1`/`-2`/`-3` codes (the Go side mirrors them) |
| `lib/core/pipeline.py` | the SRT → `appsink` pipeline, parameterised by a decode chain |

`Node.run()` is the whole lifecycle, in the one order that works:

1. `Worker.stop()` — the frame source stops **before** the tasks reading from it
   are cancelled, because cancelling a task does not stop the thread it is
   waiting on.
2. Background tasks are cancelled and awaited, so nothing writes while the
   database handle is being released.
3. `nodes.remove` is sent while the router is still listening, because the reply
   has to arrive somewhere. Skipped when registration never completed, which
   would otherwise deregister a row that does not exist.
4. Only then is the listener cancelled.

**Do not reimplement this in a node type.** Getting the order wrong produces a
node that hangs on shutdown or leaves a stale row behind, and neither shows up
until the node is restarted.

## The four things a type supplies

### 1. A decode chain

The runtime's pipeline is fixed: `srtsrc` → *your chain* → `video/x-raw,
format=BGR` → `appsink`. The BGR frame the worker receives is fixed too, so the
chain only has to turn whatever the transmitter sends into BGR.

```python
from lib.core import pipeline as core_pipeline

streamer = core_pipeline.SRTStreamer(node_id, args.decode_chain)
```

`DEFAULT_DECODE_CHAIN = "tsdemux ! h264parse ! avdec_h264 ! videoconvert"`
matches tx's default stream spec (H.264 in MPEG-TS). Change it only when your
type is fed a different codec, and keep `--decode-chain` exposed as a flag
rather than editing the constant — the constant is a default, not a policy.

An empty chain raises `ValueError` at construction. That is deliberate: the
alternative is a launch line with a doubled `!` failing inside
`Gst.parse_launch`, naming the element *after* the splice point.

### 2. A processing loop

Two halves, and the split matters:

- **A blocking frame reader**, run with `asyncio.to_thread`, that pulls frames
  from the streamer and pushes a small result per frame onto an `asyncio.Queue`.
  `processing.frame_worker` in `lib/bustle/` is the reference. Use
  `streamer.get_frame(timeout=...)` — never `get_nowait()` in a loop, which is
  a busy-wait; the timeout is what keeps a stop responsive.
- **A normalizer coroutine**, started from `Worker.start()` and returned as a
  task list, that drains the queue on a ticker and writes one document per
  interval. Batching is why the queue exists: one insert per drain interval
  instead of one per frame.

Both must be cancellable. A task that swallows `CancelledError` hangs the
shutdown instead of failing it.

### 3. A document shape

Whatever you write, write it as a **timeseries collection**, and do it once at
startup:

```python
try:
    db.create_collection(
        str(node_id).replace("-", ""),
        timeseries={"timeField": "timestamp", "metaField": "node", "granularity": "seconds"},
    )
except CollectionInvalid:
    pass
```

`create_collection` is not idempotent; a `list_collection_names()` check-then-
create races with a second node starting on the same database. Creating and
tolerating `CollectionInvalid` is atomic.

Two rules the analytics service depends on:

- **`timestamp` is a BSON date, in UTC.** `datetime.now(timezone.utc)`, not a
  naive local timestamp — the aggregations read it back as UTC, so a naive value
  shifts every bucket by the host's offset.
- **`node` is the meta field** and carries `{id, type}`, with `id` as
  `str(node_id)` *with* the dashes. The analytics registry keys the declaration
  on the same uuid string; a dash-stripped id in one place and a dashed one in
  the other matches nothing, silently.

The node owns exactly one collection and needs only a write handle on it.
Reading it back is the analytics service's job (next section).

### 4. A query manifest

A node does not serve analytics. It publishes, at boot, a list of named
aggregation pipelines it is willing to have run against its collection; the
`analytics` service stores them and executes one when the hub asks. This is what
keeps the hub free of any knowledge of a node type.

Publish from `Worker.registered()`, which runs once `nodes.add` has succeeded —
the first moment a reader has a node to attach a report to:

```python
async def registered(self, r, sibs) -> None:
    await bustle_analytics.declare(
        r, sibs, self._node_id, self._coll.name,
        subject=self._subject, utc_offset=self._utc_offset,
    )
```

`declare()` retries (30 × 1 s by default) because the analytics service may not
have introduced itself yet. Giving up only costs the queries until the node
restarts: nothing can ask a running node to declare again.

Each entry is `{name, description, params_schema, pipeline}`. The executor's
rules are the reason the shapes below are what they are — read
`analytics/internal/usecases/query.go` before writing a pipeline:

- **Read-only stages only, from an allowlist.** `$out`, `$merge`, `$unionWith`,
  `$lookup` and `$graphLookup` are absent. The allowlist is an allowlist rather
  than a blocklist because `$facet` can nest a writing stage inside a pipeline
  that looks read-only from outside.
- **≤ 24 stages, `$facet` nesting ≤ 2.**
- **One document out.** The result is capped at 2000 documents and 1 MiB, and
  the executor appends `{"$limit": 2001}` to every pipeline. A query that emits
  a row per bucket will be truncated; emit **one document with named arrays**
  instead, and put the whole thing behind a single `$facet` so the report costs
  one scan.
- **Every facet result must be nested under its own key.** `$replaceWith` +
  `$mergeObjects` over `{"$arrayElemAt": ["$timeline", 0]}` splices the facet's
  inner fields into the report root, so `timeline: {buckets: []}` comes back
  alongside a stray top-level `buckets`. Wrap each one:
  `{"timeline": {"$ifNull": [{"$arrayElemAt": ["$timeline", 0]}, <default>]}}`.
- **A bare `$group` over an empty window emits no document at all**, which reads
  as a failed query. Always give the pipeline a `$facet` tail so the empty case
  still produces the shape.
- **Declare the parameters you use, and only those.** `params_schema` is a map
  of name → type; a placeholder `${name}` in the pipeline is substituted with
  the JSON encoding of the value (a number stays a number). Today the hub
  supplies `start_time` and `unit_time`, and nothing else: a query that declares
  another name is reported as `the endpoint cannot supply <name>` rather than
  failing the whole report.
- **Validate at declare time, not query time.** A manifest is checked when it
  is published, so a bad pipeline is refused while the node is starting rather
  than when someone opens a report.

Validate the pipelines against a real MongoDB before committing — a shape
mistake (`$case` inside a `$switch`, a `$mergeObjects` where `$merge` was meant,
a `$toDate` applied to a string) is invisible to a unit test and obvious to one
aggregate over seeded data.

## The skeleton

```
nodes/<type>.py           argument parsing, storage setup, Worker, main()
nodes/lib/<type>/
    handlers.py           the commands this type serves
    processing.py         frame reader + normalizer
    analytics.py          the query manifest
nodes/tests/              test_lib_<type>_*.py
```

`nodes/<type>.py` is short: `parse_args()`, `init_db()`, build the `Worker`,
build `Node(...)`, `await n.run(stop_event)`, close the Mongo client. If it
grows, something that belongs in `lib/core` is in it.

Handlers are an **object**, not a class with class attributes — see
`lib/bustle/handlers.py`. Class attributes make a second instance silently
replace the first's pipeline and node id, and the first node then serves its own
commands with the second's dependencies. Register bound methods:

```python
class MyHandlers:
    def __init__(self, node_id, streamer, ...):
        ...
    def register(self, hs):
        hs.set(f"{self._node_id}.sink.start", self._start_sink)
        hs.set(f"{self._node_id}.sink.stop", self._stop_sink)
```

Command names are `<node-id>.<subject>.<verb>` and the id is in the name because
the hub addresses one specific node. `sink.start` carries
`{"source_address": "srt://..."}` and must answer a non-zero code when it
refuses: the Go adapters turn `code != 0` into an error, so a node that refuses
a command cannot look like a started sink.

## Flags

Copy the shared ones from `bustle.py` and add only what is specific to the
type:

| flag | what it feeds |
|---|---|
| `--node-id`, `--name`, `--type`, `--family` | `Node`'s identity; the id names the collection and the commands |
| `--broker`, `--mongo-url`, `--mongo-db` | broker and storage |
| `--intro-interval` | how often `introduce` is broadcast (also the registration retry rate) |
| `--decode-chain`, `--log-level` | the two runtime knobs |

`--rpc-timeout` and `--remove-timeout` are `Node` arguments with defaults
(5 s and 2 s) rather than flags: bustle does not pass them, and a type that
needs to should pass them explicitly instead of inventing a flag nobody sets.

`deploy/docker-compose.yml` and `deploy/nodes.Dockerfile` are the other places
that know about a node type: a new flag belongs there too, and `deploy/smoke.sh`
is what catches the wiring.

## Checklist

- [ ] `Worker` subclass with `attach`, `start`, `stop`, `registered` — no broker
      lifecycle in it.
- [ ] Decode chain exposed as `--decode-chain`, defaulted from
      `DEFAULT_DECODE_CHAIN`.
- [ ] Timeseries collection created tolerantly; `timestamp` in UTC; `node.id`
      with dashes.
- [ ] Manifest published from `registered()`, pipelines inside the stage
      allowlist and the size caps, one document out, every facet nested under
      its own key, only `start_time`/`unit_time` declared.
- [ ] Handlers an instance, command names `<node-id>.<subject>.<verb>`,
      non-zero reply code on refusal.
- [ ] Tests under `nodes/tests/` that need neither a broker nor a GStreamer
      registry (`make test-nodes` runs on a runner without the plugins).
- [ ] `make` green: lint, Go tests, Python tests, pyright.
