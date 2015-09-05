import asyncio
import aioredis


class RedisCache:
    def __init__(self):
        self._pool = None

    def connect(self, host, port, minsize=5, maxsize=10, loop=asyncio.get_event_loop()):
        """
        Setup a connection pool
        :param host: Redis host
        :param port: Redis port
        :param loop: Event loop
        """
        self._pool = yield from aioredis.create_pool((host, port), minsize=minsize, maxsize=maxsize, loop=loop)

    def set_key(self, key, value, namespace=None, expire=0):
        """
        Set a key in a cache.
        :param key: Key name
        :param value: Value
        :param namespace : Namespace to associate the key with
        :param expire: expiration
        :return:
        """
        with (yield from self._pool) as redis:
            if namespace is not None:
                key = self._get_key(namespace, key)
            yield from redis.set(key, value, expire=expire)

    def get_key(self, key, namespace=None):
        with (yield from self._pool) as redis:
            if namespace is not None:
                key = self._get_key(namespace, key)
            return (yield from redis.get(key, encoding='utf-8'))

    def delete(self, key, namespace=None):
        with (yield from self._pool) as redis:
            if namespace is not None:
                key = self._get_key(namespace, key)
            yield from redis.delete(key)

    def clear_namespace(self, namespace):
        with (yield from self._pool) as redis:
            pattern = namespace + '*'
            keys = yield from redis.keys(pattern)
            if len(keys):
                yield from redis.delete(*keys)

    def exit(self):
        if self._pool is not None:
            self._pool.clear()

    @staticmethod
    def _get_key(namespace, key):
        return namespace + ':' + key
