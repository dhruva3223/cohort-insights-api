"""Content cache service tests."""

from __future__ import annotations

import pytest

from app.config import Settings
from app.models.document import content_hash
from app.services import cache as cache_service
from tests.conftest import FailingRedis


@pytest.mark.asyncio
async def test_set_then_get_round_trip(make_client) -> None:
    async with make_client() as client:
        redis = client.app.state.redis
        settings = client.app.state.settings
        digest = content_hash("identical content")

        await cache_service.set(
            redis,
            settings,
            digest,
            "summary of content",
            ["alpha", "beta"],
        )
        hit = await cache_service.get(redis, digest)
        assert hit == {
            "summary_text": "summary of content",
            "tags": ["alpha", "beta"],
        }


@pytest.mark.asyncio
async def test_key_contains_no_document_id(make_client) -> None:
    async with make_client() as client:
        redis = client.app.state.redis
        settings = client.app.state.settings
        digest = content_hash("key shape check")
        document_id = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"

        await cache_service.set(redis, settings, digest, "s", ["t"])
        key = cache_service.cache_key(digest)
        assert key == f"cache:content:{digest}"
        assert document_id not in key

        stored = await redis.get(key)
        assert stored is not None
        assert document_id not in stored


@pytest.mark.asyncio
async def test_ttl_is_set(make_client) -> None:
    async with make_client(CACHE_TTL_S=120) as client:
        redis = client.app.state.redis
        settings = client.app.state.settings
        digest = content_hash("ttl check")

        await cache_service.set(redis, settings, digest, "s", ["t"])
        ttl = await redis.ttl(cache_service.cache_key(digest))
        assert 0 < ttl <= 120


@pytest.mark.asyncio
async def test_failing_redis_get_is_miss() -> None:
    result = await cache_service.get(FailingRedis(), content_hash("x"))  # type: ignore[arg-type]
    assert result is None


@pytest.mark.asyncio
async def test_failing_redis_set_raises_nothing() -> None:
    await cache_service.set(
        FailingRedis(),  # type: ignore[arg-type]
        Settings(CACHE_TTL_S=60),
        content_hash("y"),
        "s",
        ["t"],
    )
