from __future__ import annotations
import asyncio
import logging
import typing
import uuid
from aio_pika import abc
import aio_pika
from . import handlers, siblings

INTRODUCE_COMMAND = "introduce"
BROADCAST_ROUTING_KEY = "rpc.broadcast"

# A reply that arrives after recv() gave up would otherwise be mis-read as an
# inbound request (its trackID is no longer pending), so trackIDs that were
# abandoned are remembered for a while and the reply is dropped instead.
ABANDONED_TTL = 60.0
ABANDONED_MAX = 1024

logger = logging.getLogger(__name__)

def direct_routing_key(consumer_name: str) -> str:
    return f"rpc.direct.{consumer_name}"

async def setup_router(
    channel: abc.AbstractChannel, name: str, exchange_name: str
) -> tuple[abc.AbstractQueue, abc.AbstractExchange, str]:
    exchange = await channel.declare_exchange(
        exchange_name, abc.ExchangeType.TOPIC, durable=True
    )

    queue = await channel.declare_queue("", exclusive=True, auto_delete=True)

    direct_rk = direct_routing_key(name)
    await queue.bind(exchange, routing_key=direct_rk)
    await queue.bind(exchange, routing_key=BROADCAST_ROUTING_KEY)

    return queue, exchange, direct_rk

class Router:
    def __init__(
        self,
        channel: abc.AbstractChannel,
        queue: abc.AbstractQueue,
        exchange: abc.AbstractExchange,
        direct_routing_key: str,
        get_handler: handlers.Handlers,
        siblings: siblings.Siblings,
    ) -> None:
        self._ch = channel
        self._queue = queue
        self._exchange = exchange
        self._direct_routing_key = direct_routing_key
        self._gh = get_handler
        self._siblings = siblings

        self._known_track_id: dict[uuid.UUID, asyncio.Future[handlers.RouterResponse]] = {}
        self._lock = asyncio.Lock()

        # Handler tasks are retained (and their exceptions retrieved) and
        # abandoned trackIDs are remembered so a late reply is dropped.
        self._inflight: set[asyncio.Task[None]] = set()
        self._abandoned: dict[uuid.UUID, float] = {}

    async def introduce(self, version: str) -> None:
        """Broadcast this node's command set, wire revision and build id.

        version is the build id from lib/version.py, advertised for diagnosis
        only.
        """
        commands = self._gh.available_commands()
        body = siblings.Sibling(
            commands=commands,
            protocol_revision=siblings.PROTOCOL_REVISION,
            version=version,
        ).to_json()

        await self._exchange.publish(
            aio_pika.Message(
                body=body,
                type=INTRODUCE_COMMAND,
                reply_to=self._direct_routing_key,
            ),
            routing_key=BROADCAST_ROUTING_KEY,
        )

    async def send(
        self, command: handlers.Command, payload: bytes, sibling_name: siblings.SiblingName
    ) -> uuid.UUID:
        track_id = uuid.uuid4()

        sibling = self._siblings.get(sibling_name)
        if sibling is None:
            raise ValueError(f"no such sibling: {sibling_name}")

        loop = asyncio.get_running_loop()
        async with self._lock:
            self._known_track_id[track_id] = loop.create_future()

        try:
            await self._exchange.publish(
                aio_pika.Message(
                    body=payload,
                    correlation_id=str(track_id),
                    content_type="application/json",
                    reply_to=self._direct_routing_key,
                    type=command,
                ),
                routing_key=str(sibling_name),
            )
        except Exception:
            async with self._lock:
                self._known_track_id.pop(track_id, None)
            raise

        return track_id

    async def recv(
        self, track_id: uuid.UUID, timeout: typing.Optional[float] = None
    ) -> handlers.RouterResponse:
        async with self._lock:
            fut = self._known_track_id.get(track_id)
        if fut is None:
            raise KeyError("unknown trackID")

        try:
            return await asyncio.wait_for(fut, timeout)
        finally:
            async with self._lock:
                self._known_track_id.pop(track_id, None)
                if fut.cancelled():
                    # wait_for cancelled the future, so the reply either timed
                    # out or this coroutine was cancelled. The peer may still
                    # answer.
                    self._abandon(track_id)

    def _abandon(self, track_id: uuid.UUID) -> None:
        """Remember a trackID whose recv() gave up. Caller holds self._lock."""
        now = asyncio.get_running_loop().time()

        if len(self._abandoned) >= ABANDONED_MAX:
            cutoff = now - ABANDONED_TTL
            for tid, at in list(self._abandoned.items()):
                if at < cutoff:
                    del self._abandoned[tid]

            # Still full: drop the oldest half rather than growing without
            # bound when a peer is slow but never answers.
            if len(self._abandoned) >= ABANDONED_MAX:
                oldest = sorted(self._abandoned.items(), key=lambda kv: kv[1])
                for tid, _ in oldest[: len(oldest) // 2]:
                    del self._abandoned[tid]

        self._abandoned[track_id] = now

    async def listen(self) -> None:
        consumer_tag = await self._queue.consume(self._handle_delivery, no_ack=False)
        try:
            await asyncio.Future()
        finally:
            await self._queue.cancel(consumer_tag)

    async def _handle_delivery(self, message: abc.AbstractIncomingMessage) -> None:
        if message.type == INTRODUCE_COMMAND:
            await self._handle_introduce(message)
            return

        try:
            track_id = uuid.UUID(message.correlation_id or "")
        except (TypeError, ValueError) as e:
            logger.warning("parsing trackID: %s", e)
            await message.nack(requeue=False)  # malformed, discard — don't requeue
            return

        async with self._lock:
            fut = self._known_track_id.get(track_id)

        if fut is None:
            async with self._lock:
                abandoned = self._abandoned.pop(track_id, None)

            if abandoned is not None:
                # A reply to a send() that already timed out. Dispatching it as
                # an inbound request would fail in the handler lookup and log a
                # spurious error.
                logger.debug("dropping late reply for abandoned trackID %s", track_id)
                await message.ack()
                return

            # No pending send() for this trackID -> inbound request, dispatch
            # to the handler.
            await self._handle_inbound_request(message, track_id)
            return

        await self._handle_reply(message, fut)

    async def _handle_introduce(self, message: abc.AbstractIncomingMessage) -> None:
        if not message.reply_to:
            logger.warning("introduce from %r missing reply_to", message.body)
            await message.nack(requeue=False)
            return

        try:
            sibling = siblings.Sibling.from_json(message.body)
        except Exception as e:
            logger.warning("unable to decode introduce body: %s", e)
            await message.nack(requeue=False)
            return

        self._siblings.set(message.reply_to, sibling)

        # Register the peer either way. The command sets usually still line up,
        # and refusing a mismatched peer would turn a warning into an outage;
        # the point is that the disagreement is on the record before it
        # surfaces as an unexplained failure on some later command.
        if sibling.protocol_revision != siblings.PROTOCOL_REVISION:
            logger.warning(
                "sibling protocol revision mismatch sibling=%s "
                "peer_protocol_revision=%d protocol_revision=%d peer_version=%s",
                message.reply_to,
                sibling.protocol_revision,
                siblings.PROTOCOL_REVISION,
                sibling.version or "-",
            )

        logger.debug(
            "sibling registry updated sibling=%s count=%d peer_version=%s",
            message.reply_to,
            len(sibling.commands),
            sibling.version or "-",
        )

        await message.ack()

    async def _handle_inbound_request(
        self, message: abc.AbstractIncomingMessage, track_id: uuid.UUID
    ) -> None:
        try:
            if not message.type:
                raise ValueError("expected message type")

            handler = self._gh.get(message.type)
        except Exception:
            logger.error("get handler: %s", message.type)
            await message.nack(requeue=False)
            return

        await message.ack()

        reply_to = message.reply_to
        correlation_id = message.correlation_id

        async def rep(resp: handlers.RouterResponse) -> None:
            if not reply_to:
                raise ValueError("no reply_to on original request, cannot reply")
            body = resp.to_json()
            await self._exchange.publish(
                aio_pika.Message(
                    body=body,
                    correlation_id=correlation_id,
                    content_type="application/json",
                ),
                routing_key=reply_to,
            )

        task = asyncio.create_task(
            handler(track_id, handlers.RouterRequest(payload=message.body), rep)
        )
        self._track_handler_task(task, message.type or "")

    def _track_handler_task(self, task: asyncio.Task[None], command: str) -> None:
        """Retain a handler task and log the exception it ends with.

        The request is acked before the handler runs, so an exception here
        means the caller never gets a reply and will time out. Without a
        reference and a done-callback the traceback would only appear as
        "Task exception was never retrieved" at interpreter shutdown.
        """
        self._inflight.add(task)

        def _done(t: asyncio.Task[None]) -> None:
            self._inflight.discard(t)
            if t.cancelled():
                return

            exc = t.exception()
            if exc is not None:
                logger.error("handler %s failed: %s", command, exc, exc_info=exc)

        task.add_done_callback(_done)

    async def _handle_reply(
        self, message: abc.AbstractIncomingMessage, fut: asyncio.Future[handlers.RouterResponse]
    ) -> None:
        if message.content_type != "application/json":
            logger.warning(
                "unexpected content type for reply: %s", message.content_type
            )
            await message.nack(requeue=False)
            return

        try:
            resp = handlers.RouterResponse.from_json(message.body)
        except Exception as e:
            logger.warning("unmarshal response: %s", e)
            await message.nack(requeue=False)
            return

        if not fut.done():
            fut.set_result(resp)
            await message.ack()
        else:
            await message.ack()
