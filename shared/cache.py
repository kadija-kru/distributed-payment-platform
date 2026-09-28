from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict
from typing import Any

from redis.asyncio import Redis

from shared.config import get_settings


class MemoryCache:
    def __init__(self) -> None:
        self._data: dict[str, tuple[float | None, Any]] = {}
        self._counters: defaultdict[str, int] = defaultdict(int)
        self._counter_expiry: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def get_json(self, key: str) -> Any | None:
        async with self._lock:
            record = self._data.get(key)
            if not record:
                return None
            expires_at, value = record
            if expires_at and expires_at < time.time():
                self._data.pop(key, None)
                return None
            return value

    async def set_json(self, key: str, value: Any, ttl_seconds: int) -> None:
        async with self._lock:
            self._data[key] = (time.time() + ttl_seconds, value)

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._data.pop(key, None)

    async def incr(self, key: str, ttl_seconds: int) -> int:
        async with self._lock:
            expiry = self._counter_expiry.get(key)
            now = time.time()
            if not expiry or expiry < now:
                self._counters[key] = 0
            self._counter_expiry[key] = now + ttl_seconds
            self._counters[key] += 1
            return self._counters[key]


class RedisCache:
    def __init__(self, client: Redis):
        self.client = client

    async def get_json(self, key: str) -> Any | None:
        value = await self.client.get(key)
        return None if value is None else json.loads(value)

    async def set_json(self, key: str, value: Any, ttl_seconds: int) -> None:
        await self.client.set(key, json.dumps(value), ex=ttl_seconds)

    async def delete(self, key: str) -> None:
        await self.client.delete(key)

    async def incr(self, key: str, ttl_seconds: int) -> int:
        async with self.client.pipeline(transaction=True) as pipe:
            pipe.incr(key)
            pipe.expire(key, ttl_seconds, nx=True)
            value, _ = await pipe.execute()
        return int(value)


_cache = None


async def get_cache():
    global _cache
    if _cache is not None:
        return _cache
    settings = get_settings()
    if settings.redis_url.startswith("memory://"):
        _cache = MemoryCache()
        return _cache
    client = Redis.from_url(settings.redis_url, decode_responses=True)
    _cache = RedisCache(client)
    return _cache
