from __future__ import annotations

from fastapi import HTTPException, status

from shared.cache import get_cache
from shared.config import get_settings


async def enforce_rate_limit(subject: str) -> None:
    cache = await get_cache()
    settings = get_settings()
    count = await cache.incr(f"rl:{subject}", 60)
    if count > settings.rate_limit_per_minute:
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="rate limit exceeded")
