"""Redis client helpers."""

from __future__ import annotations

from redis.asyncio import Redis

from app.config import Settings


def create_redis_client(settings: Settings) -> Redis:
    """Build an async Redis client from settings."""
    return Redis.from_url(settings.REDIS_URL, decode_responses=True)
