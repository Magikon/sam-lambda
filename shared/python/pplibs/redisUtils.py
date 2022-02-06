import redis
import pickle
import os
from json import dumps, loads
_REDIS_PORT = "6379"
_REDIS_IP = "10.34.8.111"

def load_cache(unique_key):
    client = redis.Redis(host=_REDIS_IP,port=_REDIS_PORT)
    result = client.get(unique_key)
    if result is None:
        return None
    else:
        return pickle.loads(result)

def set_key_pair(client, unique_key, value):
    # Key is set to expire in 24h or 86400sec
    day_in_seconds = 86400
    result = pickle.dumps(value)
    client.set(unique_key, result, ex=day_in_seconds)

def refresh_cache(unique_key, value):
    client = redis.Redis(host=_REDIS_IP,port=_REDIS_PORT)
    set_key_pair(client, unique_key, value)
