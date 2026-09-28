"""Per-user active-job rate limiting via Redis (Mongo fallback)."""

from __future__ import annotations

from pymongo.asynchronous.database import AsyncDatabase
from redis.asyncio import Redis

from app.config import Settings
from app.logging_config import get_logger
from app.repositories import documents as repo

logger = get_logger(__name__)

ACTIVE_JOBS_PREFIX = "active_jobs:"

# KEYS[1] = counter key; ARGV[1] = limit; ARGV[2] = TTL seconds
# Returns 1 on success, 0 if at/over limit.
_ACQUIRE_LUA = """
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
local limit = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
if current >= limit then
  return 0
end
redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], ttl)
return 1
"""

# KEYS[1] = counter key; never go below 0
_RELEASE_LUA = """
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if current <= 0 then
  return 0
end
return redis.call('DECR', KEYS[1])
"""


def active_jobs_key(user_id: str) -> str:
    return f"{ACTIVE_JOBS_PREFIX}{user_id}"


async def acquire(
    redis: Redis,
    db: AsyncDatabase,
    settings: Settings,
    user_id: str,
) -> bool:
    """Try to take one active-job slot. True if acquired."""
    key = active_jobs_key(user_id)
    try:
        result = await redis.eval(
            _ACQUIRE_LUA,
            1,
            key,
            settings.MAX_ACTIVE_PER_USER,
            settings.RATE_LIMIT_KEY_TTL_S,
        )
        return int(result) == 1
    except Exception:
        logger.exception(
            "rate_limit_acquire_redis_failed",
            extra={"user_id": user_id},
        )
        active = await repo.count_active(db, user_id)
        return active < settings.MAX_ACTIVE_PER_USER


async def release(redis: Redis, user_id: str) -> None:
    """Release one slot; never decrements below zero."""
    key = active_jobs_key(user_id)
    try:
        await redis.eval(_RELEASE_LUA, 1, key)
    except Exception:
        logger.exception(
            "rate_limit_release_redis_failed",
            extra={"user_id": user_id},
        )


async def rebuild_counters(
    db: AsyncDatabase,
    redis: Redis,
    settings: Settings,
) -> None:
    """Replace every ``active_jobs:*`` counter with the real count from Mongo.

    Existing keys are deleted first so a user with no active documents but a
    leftover counter is not left blocked.
    """
    async for key in redis.scan_iter(match=f"{ACTIVE_JOBS_PREFIX}*", count=100):
        await redis.delete(key)

    async for user_id, count in repo.active_counts_by_user(db):
        await redis.set(active_jobs_key(user_id), count, ex=settings.RATE_LIMIT_KEY_TTL_S)
