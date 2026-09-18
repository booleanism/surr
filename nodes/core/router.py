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

    async def introduce(self) -> None:
        commands = self._gh.available_commands()
        body = siblings.Sibling(commands=commands).to_json()

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

        asyncio.create_task(
            handler(track_id, handlers.RouterRequest(payload=message.body), rep)
        )

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
