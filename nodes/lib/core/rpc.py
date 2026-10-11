from __future__ import annotations
import asyncio
import logging
import typing
import uuid

from . import handlers, siblings

logger = logging.getLogger(__name__)

class RouterLike(typing.Protocol):
    """The slice of ``router.Router`` that the RPC helper drives.

    Narrower than ``router.Router`` on purpose: the retry and failover logic
    only sends a request and awaits its reply, and typing it against that
    interface lets the tests drive it with a scripted stand-in that never
    touches a broker.
    """

    async def send(
        self, command: handlers.Command, payload: bytes, sibling_name: siblings.SiblingName
    ) -> uuid.UUID: ...

    async def recv(
        self, track_id: uuid.UUID, timeout: typing.Optional[float] = None
    ) -> handlers.RouterResponse: ...

async def call(
    r: RouterLike,
    sibs: siblings.Siblings,
    command: handlers.Command,
    payload: bytes,
    *,
    timeout: float = 5.0,
    retry_interval: float = 1.0,
    retries: typing.Optional[int] = None,
) -> handlers.RouterResponse:
    """Resolve the sibling serving `command`, send the request and await its reply.

    Retries forever while `retries` is None, so it is safe to call at boot
    before the serving sibling (e.g. the hub) has introduced itself.
    """
    attempt = 0
    while True:
        try:
            return await _call_once(r, sibs, command, payload, timeout)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            attempt += 1
            if retries is not None and attempt > retries:
                raise
            logger.warning("%s attempt %d failed: %s", command, attempt, e)
            await asyncio.sleep(retry_interval)


async def _call_once(
    r: RouterLike,
    sibs: siblings.Siblings,
    command: handlers.Command,
    payload: bytes,
    timeout: float,
) -> handlers.RouterResponse:
    names = sibs.get_name(command)
    if not names:
        raise RuntimeError(f"no sibling with {command} command yet")

    # try every sibling serving this command: stale entries can linger in the
    # registry after the serving service restarts under a new name
    last_err: Exception = RuntimeError(f"no sibling with {command} responded")
    for name in names:
        try:
            track_id = await r.send(command, payload, name)
            resp = await r.recv(track_id, timeout)
        except Exception as e:
            last_err = e
            continue

        if resp.code != 0:
            last_err = RuntimeError(f"{command} rejected by {name} (code {resp.code}): {resp.result!r}")
            continue

        return resp

    raise last_err
