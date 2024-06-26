import hashlib
import pickle

import redis

redis_client = redis.Redis()
REDIS_EXPIRE_TIME = 3600


def _some_hash(data):
    return hashlib.md5(data.encode()).hexdigest()


def _cache(name, value):
    return redis_client.set(f'repo:{_some_hash(name)}', pickle.dumps(value), ex=REDIS_EXPIRE_TIME)


def _load_cached(data):
    return pickle.loads(redis_client.get(f'repo:{_some_hash(data)}'))


def _is_cached(data):
    return redis_client.exists(f'repo:{_some_hash(data)}')
