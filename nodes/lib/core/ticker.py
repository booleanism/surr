import asyncio

# A non-positive interval would make every delay 0 and turn the ticker into an
# unthrottled call_later(0) hot loop, starving the event loop it runs on.
MIN_INTERVAL = 0.001

async def ticker(interval: float):
    interval = max(float(interval), MIN_INTERVAL)

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
