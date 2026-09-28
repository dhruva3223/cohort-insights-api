"""Rate limiter service tests."""

from __future__ import annotations

import asyncio

import pytest

from app.services import rate_limiter as rl
from tests.conftest import FailingRedis, slot_count


@pytest.mark.asyncio
async def test_three_acquires_succeed_fourth_fails(make_client) -> None:
    async with make_client(MAX_ACTIVE_PER_USER=3) as client:
        redis = client.app.state.redis
        db = client.app.state.db
        settings = client.app.state.settings
        user = "rate-user-a"

        assert await rl.acquire(redis, db, settings, user) is True
        assert await rl.acquire(redis, db, settings, user) is True
        assert await rl.acquire(redis, db, settings, user) is True
        assert await rl.acquire(redis, db, settings, user) is False
        assert await slot_count(redis, user) == 3


@pytest.mark.asyncio
async def test_release_frees_slot_and_never_below_zero(make_client) -> None:
    async with make_client(MAX_ACTIVE_PER_USER=3) as client:
        redis = client.app.state.redis
        db = client.app.state.db
        settings = client.app.state.settings
        user = "rate-user-b"

        assert await rl.acquire(redis, db, settings, user) is True
        await rl.release(redis, user)
        assert await slot_count(redis, user) == 0

        await rl.release(redis, user)
        await rl.release(redis, user)
        assert await slot_count(redis, user) == 0


@pytest.mark.asyncio
async def test_concurrent_acquires_never_exceed_limit(make_client) -> None:
    async with make_client(MAX_ACTIVE_PER_USER=3) as client:
        redis = client.app.state.redis
        db = client.app.state.db
        settings = client.app.state.settings
        user = "rate-user-c"

        results = await asyncio.gather(
            *[rl.acquire(redis, db, settings, user) for _ in range(20)]
        )
        assert sum(1 for ok in results if ok) == 3
        assert await slot_count(redis, user) == 3


@pytest.mark.asyncio
async def test_acquire_falls_back_to_mongo_when_redis_fails(make_client) -> None:
    async with make_client(MAX_ACTIVE_PER_USER=3) as client:
        db = client.app.state.db
        settings = client.app.state.settings
        user = "rate-user-fallback"

        for i in range(3):
            await db["documents"].insert_one(
                {
                    "document_id": f"doc-{i}",
                    "user_id": user,
                    "status": "queued",
                    "title": "t",
                    "content": "c",
                }
            )

        failing = FailingRedis()
        assert await rl.acquire(failing, db, settings, user) is False  # type: ignore[arg-type]

        await db["documents"].delete_many({"user_id": user})
        assert await rl.acquire(failing, db, settings, user) is True  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_release_redis_error_is_swallowed(make_client) -> None:
    await rl.release(FailingRedis(), "anyone")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_rebuild_counters_sets_values_and_clears_leftovers(make_client) -> None:
    async with make_client(MAX_ACTIVE_PER_USER=3, RATE_LIMIT_KEY_TTL_S=600) as client:
        redis = client.app.state.redis
        db = client.app.state.db
        settings = client.app.state.settings

        await redis.set(rl.active_jobs_key("ghost"), 9, ex=600)
        await redis.set(rl.active_jobs_key("active-user"), 99, ex=600)

        await db["documents"].insert_many(
            [
                {
                    "document_id": "d1",
                    "user_id": "active-user",
                    "status": "queued",
                    "title": "t",
                    "content": "c",
                },
                {
                    "document_id": "d2",
                    "user_id": "active-user",
                    "status": "processing",
                    "title": "t",
                    "content": "c",
                },
                {
                    "document_id": "d3",
                    "user_id": "active-user",
                    "status": "completed",
                    "title": "t",
                    "content": "c",
                },
            ]
        )

        await rl.rebuild_counters(db, redis, settings)

        assert await slot_count(redis, "active-user") == 2
        assert await redis.get(rl.active_jobs_key("ghost")) is None
