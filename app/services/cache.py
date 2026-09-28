"""Content-hash result cache in Redis."""

from __future__ import annotations

import json
from typing import Any

from redis.asyncio import Redis

from app.config import Settings
from app.logging_config import get_logger

logger = get_logger(__name__)

CACHE_KEY_PREFIX = "cache:content:"


def cache_key(content_hash: str) -> str:
    """Build the Redis key for a content hash (never includes document_id)."""
    return f"{CACHE_KEY_PREFIX}{content_hash}"


async def get(redis: Redis, content_hash: str) -> dict[str, Any] | None:
    """Return ``{"summary_text": ..., "tags": [...]}`` or ``None`` on miss/error."""
    key = cache_key(content_hash)
    try:
        raw = await redis.get(key)
    except Exception:
        logger.exception("cache_get_failed", extra={"content_hash": content_hash})
        return None

    if raw is None:
        return None

    try:
        payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        logger.exception("cache_get_invalid_json", extra={"content_hash": content_hash})
        return None

    if not isinstance(payload, dict):
        return None
    if "summary_text" not in payload or "tags" not in payload:
        return None
    return {
        "summary_text": payload["summary_text"],
        "tags": list(payload["tags"]),
    }


async def set(
    redis: Redis,
    settings: Settings,
    content_hash: str,
    summary_text: str,
    tags: list[str],
) -> None:
    """Store summary text and tags under the content-hash key with TTL."""
    key = cache_key(content_hash)
    payload = json.dumps({"summary_text": summary_text, "tags": tags})
    try:
        await redis.set(key, payload, ex=settings.CACHE_TTL_S)
    except Exception:
        logger.exception("cache_set_failed", extra={"content_hash": content_hash})
