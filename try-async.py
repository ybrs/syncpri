import asyncio
from aioredis import create_redis
from functools import partial

async def lock_wait(k, redis):
    print('locking - ', k)
    lock = await redis.execute('LOCK', 'foo', 10)
    print('hello', k, lock)
    await asyncio.sleep(2)
    print('world')
    await redis.execute("RELEASE", 'foo')


async def main():
    redis = await create_redis(('localhost', 7878))
    await asyncio.gather(
        partial(lock_wait, "1")(redis),
        partial(lock_wait, "2")(redis),
    )

asyncio.run(main())
