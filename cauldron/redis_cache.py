import asyncio
import aioredis
from asyncio import coroutine
from functools import wraps
import json
import hashlib


class RedisCache:
    _pool = None
    _host = None
    _port = None
    _minsize = None
    _maxsize = None
    _lock = asyncio.Semaphore(1)
    _utf8 = 'utf-8'

    @classmethod
    @coroutine
    def get_pool(cls):
        if not cls._pool:
            with (yield from cls._lock):
                if not cls._pool:
                    cls._pool = yield from aioredis.create_pool((cls._host, cls._port), minsize=cls._minsize,
                                                                maxsize=cls._maxsize)
        return (yield from cls._pool)

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
    def set_key_if_not_exists(cls, key, value, namespace=None, expire=0):
        """
        Set a redis key and return True if the key does not exists else return False
        :param key: Key name
        :param value: Value
        :param namespace : Namespace to associate the key with
        :param expire: expiration
        :return:
        """
        with (yield from cls.get_pool()) as redis:
            if namespace:
                key = cls._get_key(namespace, key)
            return (yield from redis.set(key, value, expire=expire, exist='SET_IF_NOT_EXIST'))

    @classmethod
    @coroutine
    def get_key(cls, key, namespace=None):
        with (yield from cls.get_pool()) as redis:
            if namespace is not None:
                key = cls._get_key(namespace, key)
            return (yield from redis.get(key, encoding=cls._utf8))

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
    def hdel(cls, key, namespace):
        with (yield from cls.get_pool()) as redis:
            if namespace is not None:
                yield from redis.hdel(namespace, key)

    @classmethod
    def hgetall(cls, namespace):
        with (yield from cls.get_pool()) as redis:
            if namespace is not None:
                return (yield from redis.hgetall(namespace, encoding=cls._utf8))

    @classmethod
    @coroutine
    def clear_namespace(cls, namespace) -> int:
        pattern = namespace + '*'
        return (yield from cls._delete_by_pattern(pattern))

    @classmethod
    @coroutine
    def _delete_by_pattern(cls, pattern: str) -> int:
        if not pattern:
            return 0
        with (yield from cls.get_pool()) as redis:
            _keys = yield from redis.keys(pattern)
            if _keys:
                yield from redis.delete(*_keys)
        return len(_keys)

    @classmethod
    @coroutine
    def delete_by_prefix(cls, prefix, namespace=None):
        prefix_with_namespace = cls._get_key(namespace, prefix) if namespace else prefix
        pattern = '{}*'.format(prefix_with_namespace)
        return (yield from cls._delete_by_pattern(pattern))

    @classmethod
    @coroutine
    def exit(cls):
        if cls._pool:
            yield from cls._pool.clear()

    @staticmethod
    def _get_key(namespace, key):
        return namespace + ':' + key

    @classmethod
    def asyncio_redis_decorator(cls, name_space=''):
        def wrapped(func):
            @wraps(func)
            def redis_check(*args, **kwargs):
                _args = ''
                if args and len(args) > 0:
                    _args = str(args[1:])
                redis_key = json.dumps({'func': func.__name__, 'args': _args, 'kwargs': kwargs}, sort_keys=True)
                digest_key = hashlib.md5(redis_key.encode(cls._utf8)).hexdigest()
                result = yield from RedisCache.hmget([digest_key], name_space)
                if result and len(result) > 0 and result[0]:
                    return json.loads(result[0].decode(cls._utf8))
                else:
                    result = yield from func(*args, **kwargs)
                    yield from RedisCache.hmset(digest_key, json.dumps(result), name_space)
                    return result
            return redis_check
        return wrapped

    @classmethod
    def redis_cache_decorator(cls, name_space='', expire_time=0):
        def wrapped(func):
            @wraps(func)
            def apply_cache(*args, **kwargs):
                _args = ''
                if args and len(args) > 0:
                    _args = str(args[1:])
                redis_key = json.dumps({'func': func.__name__, 'args': _args, 'kwargs': kwargs}, sort_keys=True)
                digest_key = hashlib.md5(redis_key.encode(cls._utf8)).hexdigest()
                result = yield from RedisCache.get_key(digest_key, name_space)
                if result:
                    return json.loads(result)
                result = yield from func(*args, **kwargs)
                yield from RedisCache.set_key(digest_key, json.dumps(result), name_space, expire_time)
                return result
            return apply_cache
        return wrapped

    @classmethod
    @coroutine
    def run_lua(cls, script: str, keys: list, args: list = None, namespace=None):
        args = args or []
        with (yield from cls.get_pool()) as redis:
            if script:
                if namespace:
                    keys = [cls._get_key(namespace, key) for key in keys]
                return (yield from redis.eval(script=script, keys=keys, args=args))
            return None
