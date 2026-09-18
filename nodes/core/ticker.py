import asyncio

async def ticker(interval: float):
    loop = asyncio.get_running_loop()
    start_time = loop.time()
    tick = 0
    event = asyncio.Event()

    while True:
        yield tick
        tick += 1
        next_tick_time = start_time + (tick * interval)
        delay = max(0.0, next_tick_time - loop.time())

        loop.call_later(delay, event.set)
        await event.wait()
        event.clear()

