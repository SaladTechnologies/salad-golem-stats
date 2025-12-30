#!/usr/bin/env python3
"""
clear_redis_cache.py

Script to clear all keys from the Redis cache.
"""
import redis
import os

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))


def clear_redis_cache():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    r.flushdb()
    print(f"Redis cache on {REDIS_HOST}:{REDIS_PORT} (db {REDIS_DB}) cleared.")


if __name__ == "__main__":
    clear_redis_cache()
