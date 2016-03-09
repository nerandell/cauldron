import asyncio
import aioredis
from asyncio import coroutine
from functools import wraps
import json
from collections import OrderedDict
import hashlib


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
    def hmget(cls, fields, namespace=''):
        with (yield from cls.get_pool()) as redis:
            return (yield from redis.hmget(namespace, *fields))

    @classmethod
    @coroutine
    def hmset(cls, field, value, namespace=''):
        with (yield from cls.get_pool()) as redis:
            yield from redis.hmset(namespace, field, value)

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
            keys = [namespace]
            _keys = yield from redis.keys(pattern)
            if _keys:
                keys.extend(_keys)
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

    def asyncio_redis_decorator(name_space=''):
        def wrapped(func):
            @wraps(func)
            def redis_check(cls, *args, **kwargs):
                _args = ''
                if args and len(args) > 0:
                    _args = str(args[1:])
                redis_key = json.dumps(OrderedDict({'func': func.__name__, 'args': _args, 'kwargs': kwargs}))
                digest_key = hashlib.md5(redis_key.encode('utf-8')).hexdigest()
                result = yield from cls.hmget([digest_key], name_space)
                if result and len(result) > 0:
                    return json.loads(result[0])
                else:
                    result = yield from func(*args, **kwargs)
                    yield from cls.hmset(digest_key, json.dumps(result), name_space)
                    return result
            return redis_check
        return wrapped



