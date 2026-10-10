"""Fakes shared by the ``nodes/core`` tests.

``core.router`` reaches RabbitMQ only through ``aio_pika``'s abstract base
classes, so it can be driven with small fakes that record what was published
instead of a live broker. Nothing here opens a connection, so the suite runs
without RabbitMQ.

The fakes implement only the members the router actually calls, so they are not
``aio_pika`` subclasses. The conformance is asserted with a ``typing.cast`` in
the ``deliver``/``setup``/``make_router`` helpers below rather than at every
call site.
"""

from __future__ import annotations

import asyncio
import typing
import uuid

from aio_pika import abc

from core import handlers, router

class FakeExchange:
    """Records every ``publish`` so a test can inspect the wire message."""

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.published: list[tuple[typing.Any, str]] = []

    async def publish(self, message: typing.Any, routing_key: str, **kwargs: typing.Any) -> None:
        if self.fail:
            raise RuntimeError("exchange is down")

        self.published.append((message, routing_key))

    @property
    def last(self) -> tuple[typing.Any, str]:
        return self.published[-1]

class FakeQueue:
    """Captures the consumer callback and the routing keys bound to it."""

    consumer_tag = "ctag-1"

    def __init__(self) -> None:
        self.consumer: typing.Any = None
        self.cancelled: list[str] = []
        self.bound: list[str] = []

    async def consume(self, callback: typing.Any, no_ack: bool = False, **kwargs: typing.Any) -> str:
        self.consumer = callback
        return self.consumer_tag

    async def cancel(self, consumer_tag: str, **kwargs: typing.Any) -> None:
        self.cancelled.append(consumer_tag)

    async def bind(self, exchange: typing.Any, routing_key: str = "", **kwargs: typing.Any) -> None:
        self.bound.append(routing_key)

class FakeChannel:
    """The two ``declare_*`` calls ``core.router.setup_router`` makes."""

    def __init__(self) -> None:
        self.exchange_name: typing.Optional[str] = None
        self.exchange_type: typing.Any = None
        self.durable: typing.Optional[bool] = None
        self.queue_name: typing.Optional[str] = None
        self.queue_kwargs: dict[str, typing.Any] = {}
        self.exchange = FakeExchange()
        self.queue = FakeQueue()

    async def declare_exchange(
        self, name: str, type: typing.Any, durable: bool = False, **kwargs: typing.Any
    ) -> FakeExchange:
        self.exchange_name = name
        self.exchange_type = type
        self.durable = durable
        return self.exchange

    async def declare_queue(self, name: str, **kwargs: typing.Any) -> FakeQueue:
        self.queue_name = name
        self.queue_kwargs = kwargs
        return self.queue

class FakeMessage:
    """An inbound delivery: enough of ``abc.AbstractIncomingMessage`` to dispatch."""

    def __init__(
        self,
        body: bytes = b"",
        type: typing.Optional[str] = None,
        correlation_id: typing.Optional[str] = None,
        reply_to: typing.Optional[str] = None,
        content_type: typing.Optional[str] = "application/json",
    ) -> None:
        self.body = body
        self.type = type
        self.correlation_id = correlation_id
        self.reply_to = reply_to
        self.content_type = content_type
        self.acks = 0
        self.nacks: list[bool] = []

    async def ack(self, **kwargs: typing.Any) -> None:
        self.acks += 1

    async def nack(self, requeue: bool = False, **kwargs: typing.Any) -> None:
        self.nacks.append(requeue)

class ScriptedRouter:
    """A ``core.router.Router`` stand-in for ``core.rpc.call``.

    ``respond`` is called with ``(command, payload, sibling_name)`` and either
    returns a ``RouterResponse`` or raises the exception it returns.
    """

    def __init__(self, respond: typing.Callable[[str, bytes, str], typing.Any]) -> None:
        self.respond = respond
        self.sent: list[tuple[str, str]] = []
        self.timeouts: list[typing.Optional[float]] = []
        self._pending: tuple[str, bytes, str] = ("", b"", "")

    async def send(self, command: str, payload: bytes, sibling_name: str) -> uuid.UUID:
        self._pending = (command, payload, sibling_name)
        self.sent.append((command, sibling_name))
        return uuid.uuid4()

    async def recv(self, track_id: uuid.UUID, timeout: typing.Optional[float] = None) -> handlers.RouterResponse:
        self.timeouts.append(timeout)
        command, payload, sibling_name = self._pending
        outcome = self.respond(command, payload, sibling_name)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

def make_router(
    get_handler: typing.Optional[handlers.Handlers] = None,
    sibs: typing.Any = None,
    exchange: typing.Optional[FakeExchange] = None,
    queue: typing.Optional[FakeQueue] = None,
) -> router.Router:
    """Build a ``Router`` wired to fakes (its channel is never used)."""
    from core import siblings

    return router.Router(
        channel=typing.cast(abc.AbstractChannel, None),
        queue=typing.cast(abc.AbstractQueue, queue or FakeQueue()),
        exchange=typing.cast(abc.AbstractExchange, exchange or FakeExchange()),
        direct_routing_key=router.direct_routing_key("bustle0"),
        get_handler=get_handler or handlers.Handlers(),
        siblings=sibs or siblings.Siblings(),
    )

async def deliver(r: router.Router, message: FakeMessage) -> None:
    """Feed a fake delivery to the router.

    The router is typed against ``aio_pika``'s ABCs, which the fakes
    deliberately do not subclass (they implement only the handful of members
    the router calls), so the conformance is asserted here once instead of at
    every call site.
    """
    await r._handle_delivery(typing.cast(abc.AbstractIncomingMessage, message))  # noqa: SLF001 - test helper

async def setup(
    channel: FakeChannel, name: str = "bustle0", exchange_name: str = "surr0"
) -> tuple[FakeQueue, FakeExchange, str]:
    """``router.setup_router`` against a ``FakeChannel``, returning its fakes."""
    _, _, direct_rk = await router.setup_router(
        typing.cast(abc.AbstractChannel, channel), name, exchange_name
    )
    return channel.queue, channel.exchange, direct_rk

async def drain(r: router.Router, timeout: float = 2.0) -> None:
    """Wait for every handler task the router is still tracking."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while r._inflight and loop.time() < deadline:  # noqa: SLF001 - test helper
        await asyncio.gather(*list(r._inflight), return_exceptions=True)
        await asyncio.sleep(0)

async def settle(times: int = 3) -> None:
    """Let scheduled callbacks run."""
    for _ in range(times):
        await asyncio.sleep(0)
