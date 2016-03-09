import asyncio
import aioredis
from asyncio import coroutine


class RedisCache:
    _pool = None
    _host = None
    _port = None
    _minsize = None
    _maxsize = None
    _lock = asyncio.Semaphore(1)

    @classmethod
    @coroutine
    def get_pool(cls):
        if not cls._pool:
            with (yield from cls._lock):
                if not cls._pool:
                    cls._pool = yield from aioredis.create_pool((cls._host, cls._port), minsize=cls._minsize,
                                                                maxsize=cls._maxsize)
        return cls._pool

    @classmethod
    @coroutine
    def connect_v2(cls, host, port, minsize=5, maxsize=10, loop=None):
        """
        Setup a connection pool params
        :param host: Redis host
        :param port: Redis port
        :param loop: Event loop
        """
        cls._host = host
        cls._port = port
        cls._minsize = minsize
        cls._maxsize = maxsize

    @classmethod
    @coroutine
    def connect(cls, host, port, minsize=5, maxsize=10, loop=asyncio.get_event_loop()):
        """
        Setup a connection pool
        :param host: Redis host
        :param port: Redis port
        :param loop: Event loop
        """
        cls._pool = yield from aioredis.create_pool((host, port), minsize=minsize, maxsize=maxsize, loop=loop)

    @classmethod
    @coroutine
    def set_key(cls, key, value, namespace=None, expire=0):
        """
        Set a key in a cache.
        :param key: Key name
        :param value: Value
        :param namespace : Namespace to associate the key with
        :param expire: expiration
        :return:
        """
        with (yield from cls.get_pool()) as redis:
            if namespace is not None:
                key = cls._get_key(namespace, key)
            yield from redis.set(key, value, expire=expire)

    @classmethod
    @coroutine
    def get_key(cls, key, namespace=None):
        with (yield from cls.get_pool()) as redis:
            if namespace is not None:
                key = cls._get_key(namespace, key)
            return (yield from redis.get(key, encoding='utf-8'))

    @classmethod
    @coroutine
    def delete(cls, key, namespace=None):
        with (yield from cls.get_pool()) as redis:
            if namespace is not None:
                key = cls._get_key(namespace, key)
            yield from redis.delete(key)

    @classmethod
    @coroutine
    def clear_namespace(cls, namespace):
        with (yield from cls.get_pool()) as redis:
            pattern = namespace + '*'
            keys = yield from redis.keys(pattern)
            if len(keys):
                yield from redis.delete(*keys)

    @classmethod
    @coroutine
    def exit(cls):
        if cls._pool:
            yield from cls._pool.clear()

    @staticmethod
    def _get_key(namespace, key):
        return namespace + ':' + key
