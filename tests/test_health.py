"""Health endpoint tests."""

from __future__ import annotations

import pytest
from httpx import AsyncClient

from app.dependencies import get_db
from tests.conftest import FailingRedis


@pytest.mark.asyncio
async def test_health_returns_200_when_up(client: AsyncClient) -> None:
    resp = await client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body == {
        "status": "ok",
        "mongodb": "connected",
        "redis": "connected",
    }


@pytest.mark.asyncio
async def test_health_returns_503_when_redis_fails(make_client) -> None:
    async with make_client() as client:
        client.app.state.redis = FailingRedis()
        resp = await client.get("/health")
        assert resp.status_code == 503
        body = resp.json()
        assert body["status"] == "unhealthy"
        assert body["mongodb"] == "connected"
        assert body["redis"] == "unavailable"


class _FailingDb:
    """Stand-in database whose ping always fails."""

    async def command(self, *args, **kwargs):
        raise ConnectionError("mongodb unavailable for test")


@pytest.mark.asyncio
async def test_health_returns_503_when_mongo_fails(make_client) -> None:
    async with make_client() as client:
        client.app.dependency_overrides[get_db] = _FailingDb
        resp = await client.get("/health")
        assert resp.status_code == 503
        body = resp.json()
        assert body["status"] == "unhealthy"
        assert body["mongodb"] == "unavailable"
        assert body["redis"] == "connected"
