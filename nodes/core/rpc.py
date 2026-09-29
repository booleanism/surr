from __future__ import annotations
import asyncio
import logging
import typing

from . import handlers, router, siblings

logger = logging.getLogger(__name__)

async def call(
    r: router.Router,
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
    r: router.Router,
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
