import asyncio
from collections import deque

from aioredis import create_redis, Redis, create_redis_pool, ConnectionsPool
from functools import partial

from aioredis.util import wait_convert


class Lock:
    def __init__(self, conn, name):
        self.conn = conn
        self.name = name
        self.lock_id = None

    # def __aenter__(self):
    #     self.lock()
    #
    # def __aexit__(self, exc_type, exc_val, exc_tb):
    #     pass

    async def lock(self, wait_timeout):
        fut = self.conn.execute(b'LOCK', self.name, wait_timeout)
        lock_id = await wait_convert(fut, lambda x: x[0])
        self.lock_id = lock_id
        return self.lock_id

    async def release(self):
        await self.conn.execute("RELEASE", 'foo', self.lock_id)


class LockManager:
    def __init__(self):
        self.connections = deque()
        self.in_use_connections = deque()
        self.poolsem = asyncio.Semaphore(0)

    async def prepare_connections(self):
        futures = [create_redis(('localhost', 7878)) for i in range(0, 5)]
        conns = await asyncio.gather(*futures)
        for conn in conns:
            self.connections.append(conn)
            self.poolsem.release()
            print("1 release", self.poolsem)

    async def get(self, name):
        print("conn0 enter>", self.poolsem)

        await self.poolsem.acquire()
        conn = self.connections.popleft()
        if len(self.connections) <= 3:
            await self.prepare_connections()
        self.in_use_connections.append(conn)
        print("conn0 - released", self.poolsem)
        return Lock(conn=conn, name=name)


async def lock_wait(k, lockmanager: LockManager):
    l = await lockmanager.get('foo')
    await l.conn.execute('JOIN', k)

    lock = await l.lock(50 + int(k))
    print('hello', k, lock)
    await asyncio.sleep(1)
    # print("done", k)
    await l.release()


async def lock_wait2(k, redis:ConnectionsPool):
    with await redis as conn:
        lock = await conn.blpop("foobl")
        print("hello ", k, lock)
        await conn.lpush("foobl", k)


async def main():
    lm = LockManager()
    await lm.prepare_connections()
    fns  = [partial(lock_wait, i)(lm) for i in range(1, 10)]
    # await asyncio.gather(
    #     *fns
    # )
    for fn in fns:
        await fn

asyncio.run(main())
