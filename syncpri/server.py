from collections import deque, defaultdict
from copy import copy
from typing import *
import uuid

import aioredis
import click
import asyncio
import collections
import itertools
import sys
import time
from typing import Any
import ujson as json
import hiredis
import uvloop
from six import b, wraps
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

expiration = collections.defaultdict(lambda: float("inf"))  # type: Dict[bytes, float]
dictionary = {}  # type: Dict[bytes, Any]

subscribers = defaultdict(set)

DELIMITER = b"\r\n"
OK_RESPONSE = b"+OK\r\n"


def to_b(arg) -> bytes:
    """
    return bytestring of arg

    :param arg:
    :return:
    """
    if isinstance(arg, bytes):
        return arg
    if isinstance(arg, int):
        return b'%d' % arg
    return b(arg)


def redis_encode(*args):
    "Pack a series of arguments into a value Redis command"
    for arg in args:
        if isinstance(arg, list):
            return redis_encode(*arg)

    result = []
    result.append(b"*")
    result.append(to_b(len(args)))
    result.append(DELIMITER)
    for arg in args:
        if isinstance(arg, int):
            result.append(b':%s' % to_b(arg))
        else:
            result.append(b"$")
            result.append(to_b(len(arg)))
            result.append(DELIMITER)
            result.append(to_b(arg))
            result.append(DELIMITER)
    ret = b"".join(result)
    return ret


class Server:
    def __init__(self, loop, node_name, ip, port):
        self.node_name = node_name
        self.ip = ip
        self.port = port
        self.loop = loop
        self.locks = {}

    async def start(self):
        await self.connect_to_pairs()

    async def connect_to_pairs(self):
        pass

    async def heartbeat(self):
        while True:
            await asyncio.sleep(30)

class Error:
    """
    this is to wrap redis error replies
    """
    def __init__(self, msg):
        self.msg = msg

class LockNode:
    def __init__(self, conn, identifier, timeout, loop=None):
        self.conn: RedisProtocol = conn
        self.callback_timer = None
        self.loop = loop or asyncio.get_event_loop()
        self.timeout = None
        self.identifier = identifier

    def send_wait_timeout_error(self):
        # TODO: remove from waiting nodes now !
        if self.conn:
            self.conn.transport.write(b"-ERR lock wait timeout exceeded\r\n")

    def set_timeout(self, timeout:int):
        self.timeout = timeout
        self.callback_timer = self.loop.call_later(timeout, self.send_wait_timeout_error)

    def remove_ttl_callback(self):
        if self.callback_timer:
            self.callback_timer.cancel()

    def __repr__(self):
        return '<LockNode {} {}>'.format(self.identifier, self.timeout)


class Lock:
    def __init__(self, name, owner, ttl, release_on_lost_conn=None):
        self.name = name
        self.owner = owner
        self.waiting_nodes = []
        self.id = str(uuid.uuid4())
        self.ttl = ttl
        self.release_on_lost_conn = release_on_lost_conn

    def add_waiting_node(self, conn, timeout:int):
        """
        if ttl will expire we'll send an error to the conn.
        if he gets the lock we'll just remove it.

        :param conn:
        :param ttl:
        :return:
        """
        ln = LockNode(conn, conn.identifier, timeout)
        ln.set_timeout(int(timeout))
        self.waiting_nodes.append(ln)
        return ln

    def notify_eof(self, conn):
        if not self.release_on_lost_conn:
            return
        if self.owner == conn:
            self.move_next()

    def _move_next(self):
        logger.debug("moving next %s", self.waiting_nodes)
        if not self.waiting_nodes:
            return
        ln: LockNode = self.waiting_nodes.pop(0)
        ln.remove_ttl_callback()
        logger.debug("next node %s", ln)
        # NOTE: we change the lock id here, this id is nothing but
        # to identify the client
        self.id = str(uuid.uuid4())
        self.owner = ln.conn
        logger.debug("send foo %s", self.id)
        ln.conn.transport.write(redis_encode(self.id))
        return ln

    def move_next(self):
        while True:
            try:
                return self._move_next()
            except Exception as e:
                logger.exception('exception on moving to next node')


class Wait:
    def __init__(self, identifier):
        self.identifier = identifier


def redis_encoded(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        ret = fn(*args, **kwargs)
        if isinstance(ret, Error):
            return b'-ERR %s\r\n' % to_b(ret.msg)
        if isinstance(ret, Wait):
            return
        return redis_encode(ret)
    return wrapper


class RedisProtocol(asyncio.Protocol):

    def __init__(self, loop: asyncio.AbstractEventLoop, server: Server):
        self.loop = loop
        self.dictionary = dictionary
        self.response = collections.deque()
        self.parser = hiredis.Reader()
        self.server = server
        self.transport = None  # type: asyncio.transports.Transport
        self.identifier = None
        self.interests = set()

    # def __repr__(self):
    #     return '<RedisProtocol >'.format(self.transport)

    @redis_encoded
    def CMD_lock(self, lock_name, wait_timeout, auto_release_timeout=0, release_on_lost_conn=1):
        """

        :param lock_name: name of the lock
        :param wait_timeout: if can't acquire lock under wait_timeout seconds, we'll return an error
        :param auto_release_timeout: in case someone forgets to release the lock, we'll wait forever,
                if you set auto_release_timeout, we release the lock after some time (#TODO)
        :param release_on_lost_conn: if the process holding the lock crashes, should we release the lock ?
                default: 1
                it will release the lock after 1 sec. after the connection gets lost
        :return:
        """
        logger.debug("lock %s %s %s %s %s", self, lock_name, wait_timeout,
                     auto_release_timeout, release_on_lost_conn)
        lock: Lock = self.server.locks.get(lock_name)
        if not lock:
            lock = Lock(name=lock_name, ttl=wait_timeout, owner=self)
            lock.auto_release_timeout = int(auto_release_timeout)
            lock.release_on_lost_conn = int(release_on_lost_conn)
            self.server.locks[lock_name]: Lock = lock
            self.interests.add(lock)
            return self.server.locks[lock_name].id
        else:
            logger.debug("adding node to lock")
            lock.add_waiting_node(self, wait_timeout)
            self.interests.add(lock)
            return Wait(self.identifier)

    @redis_encoded
    def CMD_release(self, lock_name, identifier):
        logger.debug("releasing %s %s", lock_name, identifier)
        lock: Lock = self.server.locks.get(lock_name)
        if lock and lock.owner == self and identifier.decode() == str(lock.id):
            logger.debug("moving next %s", lock_name)
            if not lock.move_next():
                del self.server.locks[lock_name]
            return lock.id
        else:
            return Error('lock does not exist (or not owner)')

    def identify(self):
        return self.identifier or ':'.join([str(i) for i in self.transport.get_extra_info('peername')])

    @redis_encoded
    def CMD_node_list(self, lock_name):
        # TODO:
        lock: Lock = self.server.locks.get(lock_name)
        if not lock:
            return Error('lock does not exist')
        out = json.dumps({
            'owner': lock.owner.identify(),
            'waiting': [ln.conn.identify() for ln in lock.waiting_nodes]
        })
        return out

    def CMD_join(self, identifier):
        self.identifier = identifier
        return OK_RESPONSE

    @redis_encoded
    def CMD_subscribe(self, chan):
        subscribers[chan].add(self)
        return [
            'subscribe',
            chan,
            1
        ]

    def CMD_unsubscribe(self, chan):
        subscribers[chan].add(self)
        return OK_RESPONSE

    def CMD_publish(self, chan, message):
        print("command")
        for conn in subscribers[chan]:
            # print(redis_encode(chan, message))
            conn.transport.write(redis_encode('message', chan, message))
        return OK_RESPONSE

    def connection_made(self, transport: asyncio.transports.Transport):
        self.transport = transport

    def eof_received(self):
        print("connection end")
        for lock in self.interests:
            lock.notify_eof(self)

        for k, v in subscribers.items():
            if self in v:
                v.remove(self)

    def data_received(self, data: bytes):
        self.parser.feed(data)

        while 1:
            req = self.parser.gets()
            if req is False:
                break
            else:
                fn = getattr(self, 'CMD_%s' % req[0].decode().lower(), None)
                if not fn:
                    self.transport.writelines([to_b('-ERR unknown command {}\r\n'.format('CMD_%s' % req[0].lower()))])
                    self.response.clear()
                    return

                resp = fn(*req[1:])
                if resp:
                    self.response.append(resp)
        if self.response:
            self.transport.writelines(self.response)
            self.response.clear()

    def CMD_command(self):
        # TODO:?
        return OK_RESPONSE

    # def CMD_set(self, *args) -> Any[bytes, Error]:
    #     # Defaults
    #     key = args[0]
    #     value = args[1]
    #     expires_at = None
    #     cond = b""
    #
    #     largs = len(args)
    #     if largs == 3:
    #         # SET key value [NX|XX]
    #         cond = args[2]
    #     elif largs >= 4:
    #         # SET key value [EX seconds | PX milliseconds] [NX|XX]
    #         try:
    #             if args[2] == b"EX":
    #                 duration = int(args[3])
    #             elif args[2] == b"PX":
    #                 duration = int(args[3]) / 1000
    #             else:
    #                 return Error("syntax error")
    #         except ValueError:
    #             return Error("value is not an integer or out of range")
    #
    #         if duration <= 0:
    #             return Error("invalid expire time in set")
    #
    #         expires_at = time.monotonic() + duration
    #
    #         if largs == 5:
    #             cond = args[4]
    #
    #     if cond == b"":
    #         pass
    #     elif cond == b"NX":
    #         if key in self.dictionary:
    #             return b"$-1\r\n"
    #     elif cond == b"XX":
    #         if key not in self.dictionary:
    #             return b"$-1\r\n"
    #     else:
    #         return b"-ERR syntax error\r\n"
    #
    #     if expires_at:
    #         expiration[key] = expires_at
    #
    #     self.dictionary[key] = value
    #     return b"+OK\r\n"
    #
    # def CMD_get(self, key: bytes) -> bytes:
    #     if key not in self.dictionary:
    #         return b"$-1\r\n"
    #
    #     if key in expiration and expiration[key] < time.monotonic():
    #         del self.dictionary[key]
    #         del expiration[key]
    #         return b"$-1\r\n"
    #     else:
    #         value = self.dictionary[key]
    #         return b"$%d\r\n%s\r\n" % (len(value), value)

    def CMD_ping(self, message=b"PONG"):
        return b"+PONG\r\n"

    # def CMD_incr(self, key):
    #     value = self.dictionary.get(key, 0)
    #     if type(value) is str:
    #         try:
    #             value = int(value)
    #         except ValueError:
    #             return b"-value is not an integer or out of range\r\n"
    #     value += 1
    #     self.dictionary[key] = str(value)
    #     return b":%d\r\n" % (value,)

    def CMD_del(self, key):
        # TODO: missing arguments
        try:
            del self.dictionary[key]
            return b":%d\r\n" % (1,)
        except KeyError as e:
            return b":%d\r\n" % (0,)

    def CMD_lpush(self, key, *values):
        deque = self.dictionary.get(key, collections.deque())
        deque.extendleft(values)
        self.dictionary[key] = deque
        return b":%d\r\n" % (len(deque),)

    def CMD_rpush(self, key, *values):
        print("pushing - ", key, values)
        deque = self.dictionary.get(key, collections.deque())
        deque.extend(values)
        self.dictionary[key] = deque
        return b":%d\r\n" % (len(deque),)

    def CMD_lpop(self, key):
        try:
            deque = self.dictionary[key]  # type: collections.deque
        except KeyError:
            return b"$-1\r\n"
        value = deque.popleft()
        return b"$%d\r\n%s\r\n" % (len(value), value)

    def CMD_rpop(self, key):
        try:
            deque = self.dictionary[key]  # type: collections.deque
        except KeyError:
            return b"$-1\r\n"
        value = deque.pop()
        return b"$%d\r\n%s\r\n" % (len(value), value)

    # def CMD_sadd(self, key, *members):
    #     set_ = self.dictionary.get(key, set())
    #     prev_size = len(set_)
    #     for member in members:
    #         set_.add(member)
    #     self.dictionary[key] = set_
    #     return b":%d\r\n" % (len(set_) - prev_size,)

    # def CMD_hset(self, key, field, value):
    #     hash_ = self.dictionary.get(key, {})
    #     ret = int(field in hash_)
    #     hash_[field] = value
    #     self.dictionary[key] = hash_
    #     return b":%d\r\n" % (ret,)

    # def CMD_spop(self, key):  # TODO add `count`
    #     try:
    #         set_ = self.dictionary[key]  # type: set
    #         elem = set_.pop()
    #     except KeyError:
    #         return b"$-1\r\n"
    #     return b"$%d\r\n%s\r\n" % (len(elem), elem)

    @redis_encoded
    def CMD_lrange(self, key, start, stop):
        start = int(start)
        stop = int(stop)
        try:
            deque = self.dictionary[key]  # type: collections.deque
        except KeyError:
            return b"$-1\r\n"
        return list(itertools.islice(deque, start, stop))

    # def CMD_mset(self, *args):
    #     for i in range(0, len(args), 2):
    #         key = args[i]
    #         value = args[i + 1]
    #         self.dictionary[key] = value
    #     return b"+OK\r\n"


@click.command()
@click.option("--bind-ip", '-h', default="127.0.0.1", help="Host")
@click.option("--bind-port", '-p', default=7878, help="Host")
@click.option("--name", '-n', default=None, help="Node name")
def main(bind_ip, bind_port, name) -> int:
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    loop = asyncio.get_event_loop()

    server = Server(node_name=name, ip=bind_ip, port=bind_port, loop=loop)

    # Each client connection will create a new protocol instance
    def server_creator():
        rps = RedisProtocol(loop, server)
        return rps

    coro = loop.create_server(server_creator, bind_ip, int(bind_port))
    listener = loop.run_until_complete(coro)
    loop.run_until_complete(server.start())

    # Serve requests until Ctrl+C is pressed
    logger.info('Serving on {}'.format(listener.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the server
    listener.close()
    loop.run_until_complete(listener.wait_closed())
    loop.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
