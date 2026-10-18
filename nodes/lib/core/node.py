"""The node runtime: join the broker mesh, serve commands, leave cleanly.

A node type is a :class:`Worker` -- handlers, background tasks and two hooks.
Everything else is the same for every type and lives here: the router, the
introduce ticker, self-registration with the hub, and the shutdown ordering.

The shutdown ordering is the reason this module exists. Each step depends on
something the next one needs to still be alive:

  1. ``Worker.stop()`` runs first, because the frame source has to stop before
     the tasks reading from it are cancelled -- cancelling a task does not stop
     the thread it is waiting on.
  2. Background tasks are cancelled and awaited next, so nothing writes to the
     database while it is being handed back.
  3. ``nodes.remove`` is sent while the router is still listening, because the
     reply has to arrive somewhere. Skipped when registration never completed,
     which would otherwise deregister a row that does not exist.
  4. Only then is the listener cancelled.

Doing these in another order produces a node that either hangs on shutdown or
leaves a stale row behind, and neither is visible until the node is restarted.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import typing
import uuid

import aio_pika
from aio_pika import abc

from . import handlers as core_handlers
from . import router as core_router
from . import rpc as core_rpc
from . import siblings as core_siblings
from . import ticker as core_ticker

logger = logging.getLogger(__name__)

# Registration is retried rather than failed, because a node may start before
# the hub has introduced itself. One attempt per introduce interval is enough:
# the registry is rebuilt from the same broadcasts.
DEFAULT_INTRO_INTERVAL = 1.0
DEFAULT_RPC_TIMEOUT = 5.0
DEFAULT_REMOVE_TIMEOUT = 2.0

class Worker:
    """What a node type supplies to :class:`Node`.

    Every method has a default, so a type that needs none of them writes none of
    them. The defaults are not placeholders: a node with no commands, no
    background work and nothing to release is a valid node.
    """

    def attach(self, hs: core_handlers.Handlers) -> None:
        """Register this type's command handlers on ``hs``.

        The command names embed the node id (``<node-id>.sink.start``), which is
        why they are registered here rather than declared statically: the id is
        only known at startup.
        """

    def start(self) -> list[asyncio.Task[typing.Any]]:
        """Start this type's background tasks and return them.

        They are cancelled on shutdown and their exceptions reported, so they
        must be cancellable: a task that swallows ``CancelledError`` hangs the
        shutdown instead of failing it.
        """
        return []

    async def registered(self, r: core_rpc.RouterLike, sibs: core_siblings.Siblings) -> None:
        """Run once ``nodes.add`` has succeeded.

        For anything that can only be published after the hub knows this node
        exists -- a declaration, for instance, which a reader would otherwise
        have no node to attach.
        """

    async def stop(self) -> None:
        """Release resources before the broker connection closes.

        Runs before the background tasks are cancelled, so it must not wait on
        them.
        """

class Node:
    """One running node: identity, broker membership and lifecycle.

    ``register_payload`` is the body of ``nodes.add``, built by the caller
    because the fields it carries (stream width, bitrate) are the node type's.
    """

    def __init__(
        self,
        *,
        node_id: uuid.UUID,
        name: str,
        family: str,
        broker: str,
        version: str,
        register_payload: bytes,
        worker: Worker,
        intro_interval: float = DEFAULT_INTRO_INTERVAL,
        rpc_timeout: float = DEFAULT_RPC_TIMEOUT,
        remove_timeout: float = DEFAULT_REMOVE_TIMEOUT,
    ) -> None:
        self.node_id = node_id
        self._name = name
        self._family = family
        self._broker = broker
        self._version = version
        self._register_payload = register_payload
        self._worker = worker
        self._intro_interval = intro_interval
        self._rpc_timeout = rpc_timeout
        self._remove_timeout = remove_timeout

        self._handlers = core_handlers.Handlers()
        self._siblings = core_siblings.Siblings()

    async def run(self, stop_event: asyncio.Event) -> None:
        """Connect, serve until ``stop_event`` is set or a task fails, then leave.

        A failed task is re-raised: a node whose listener died is not running,
        and reporting that as a clean exit is what makes a supervisor leave a
        broken node up.
        """
        connection = await aio_pika.connect_robust(self._broker)

        async with connection:
            channel = await connection.channel()
            await self.serve(channel, stop_event)

    async def serve(self, channel: abc.AbstractChannel, stop_event: asyncio.Event) -> None:
        """The lifecycle, given an open channel.

        Split from :meth:`run` so the ordering below can be driven against a
        fake channel: none of it needs a live broker, and the parts that can go
        wrong on shutdown are exactly the parts a broker would hide.
        """
        router: typing.Optional[core_router.Router] = None
        listen_task: typing.Optional[asyncio.Task[None]] = None
        stop_waiter: typing.Optional[asyncio.Task[bool]] = None
        tasks: list[asyncio.Task[typing.Any]] = []
        registered_event = asyncio.Event()

        try:
            queue, exchange, routing_id = await core_router.setup_router(
                channel, self._name, self._family
            )
            router = core_router.Router(
                channel, queue, exchange, routing_id, self._handlers, self._siblings
            )

            self._worker.attach(self._handlers)

            listen_task = asyncio.create_task(router.listen())
            logger.info("starting router listen")

            intro_task = asyncio.create_task(
                introduce(self._intro_interval, router, self._version)
            )

            background = self._worker.start()

            register_task = asyncio.create_task(
                self._register(router, registered_event)
            )
            register_task.add_done_callback(self._on_register_done)

            stop_waiter = asyncio.create_task(stop_event.wait())

            tasks.extend([listen_task, intro_task, *background, register_task])

            # listen/intro/background never complete on their own, so any
            # completion here means shutdown or a crashed task. register_task is
            # deliberately excluded: it finishes once nodes.add succeeds and the
            # node keeps running afterwards.
            done, _ = await asyncio.wait(
                [listen_task, intro_task, *background, stop_waiter],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if stop_waiter in done:
                logger.info("shutdown signal received, stopping node name=%s", self._name)
            else:
                for task in done:
                    exc = task.exception()
                    if exc is not None:
                        raise exc

        except asyncio.CancelledError:
            logger.info("shutting down node name=%s", self._name)
        finally:
            await self._drain(router, listen_task, stop_waiter, tasks, registered_event)

    async def _drain(
        self,
        router: typing.Optional[core_router.Router],
        listen_task: typing.Optional[asyncio.Task[None]],
        stop_waiter: typing.Optional[asyncio.Task[bool]],
        tasks: list[asyncio.Task[typing.Any]],
        registered_event: asyncio.Event,
    ) -> None:
        """Shut down in the order the module docstring describes."""
        await self._worker.stop()

        # Stop background work first; listen stays alive so the nodes.remove
        # reply can still be received.
        background = [t for t in tasks if t is not listen_task]
        for task in background:
            if not task.done():
                task.cancel()

        if background:
            # return_exceptions=True keeps one failure from hiding the others,
            # but it also discards them: report each one instead of letting a
            # crashed task vanish.
            for result in await asyncio.gather(*background, return_exceptions=True):
                if isinstance(result, asyncio.CancelledError):
                    continue

                if isinstance(result, BaseException):
                    logger.error("background task failed: %s", result, exc_info=result)

        if (
            registered_event.is_set()
            and listen_task is not None
            and not listen_task.done()
            and router is not None
        ):
            await self._deregister(router)

        # Stop the consumer even when registration never completed: otherwise it
        # stays parked on its future while the caller closes the channel
        # underneath it.
        if listen_task is not None and not listen_task.done():
            listen_task.cancel()
            await asyncio.gather(listen_task, return_exceptions=True)

        if stop_waiter is not None and not stop_waiter.done():
            stop_waiter.cancel()

    async def _deregister(self, router: core_router.Router) -> None:
        """Tell the hub this node is leaving, tolerating a hub that will not hear it.

        A stale row is a nuisance; refusing to shut down over one is worse, and
        the row is what the next ``nodes.add`` overwrites.
        """
        try:
            await asyncio.wait_for(
                core_rpc.call(
                    router,
                    self._siblings,
                    "nodes.remove",
                    self._register_payload,
                    timeout=self._remove_timeout,
                    retry_interval=0.0,
                    retries=0,
                ),
                timeout=self._rpc_timeout,
            )
            logger.info("node deregistered command=nodes.remove node_id=%s", self.node_id)
        except Exception as e:
            logger.warning(
                "nodes.remove failed (stale row may remain in hub) node_id=%s: %s",
                self.node_id,
                e,
            )

    async def _register(
        self, router: core_router.Router, registered_event: asyncio.Event
    ) -> None:
        """Register the node once, retrying until the hub is discovered."""
        await core_rpc.call(
            router,
            self._siblings,
            "nodes.add",
            self._register_payload,
            timeout=self._rpc_timeout,
            retry_interval=self._intro_interval,
        )
        logger.info("node registered command=nodes.add node_id=%s", self.node_id)
        registered_event.set()

        try:
            await self._worker.registered(router, self._siblings)
        except Exception as e:
            # The node is registered and serving; a failed post-registration
            # hook is reported rather than allowed to take it down, because what
            # the hook publishes is recoverable and the node's own commands are
            # not.
            logger.error("post-registration hook failed node_id=%s: %s", self.node_id, e)

    def _on_register_done(self, task: asyncio.Task[None]) -> None:
        """Report a registration that gave up.

        register_task is excluded from the wait set above because it completing
        successfully must not stop the node; without this callback its failure
        would only surface (if at all) at shutdown, as a node that quietly
        stopped being reachable.
        """
        if task.cancelled():
            return

        exc = task.exception()
        if exc is not None:
            logger.error("node registration failed node_id=%s: %s", self.node_id, exc)

async def introduce(
    interval: float,
    router: core_router.Router,
    version: str,
) -> None:
    """Broadcast this node's commands, revision and build id once per interval."""
    ticker_gen = core_ticker.ticker(interval)
    async for _ in ticker_gen:
        await router.introduce(version)

def install_signal_handlers(stop_event: asyncio.Event) -> None:
    """Turn SIGINT/SIGTERM into a graceful stop.

    ``NotImplementedError`` is swallowed because Windows has no
    ``add_signal_handler``; there ``main`` catches ``KeyboardInterrupt`` instead.
    """
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

def register_payload(node_id: uuid.UUID, **fields: typing.Any) -> bytes:
    """Build the ``nodes.add`` body for one node.

    The hub takes a list because a batch add was once expected to arrive; the
    fields beside the id are the node type's own (stream width and bitrate).
    """
    return json.dumps([{"id": str(node_id), **fields}]).encode()
