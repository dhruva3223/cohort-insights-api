"""Shared test fixtures.

Requires reachable MongoDB and Redis (for example::

    docker compose up -d mongo redis

).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Callable, Iterable
from contextlib import asynccontextmanager
from typing import Any

import pytest
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from app.config import Settings
from app.main import create_app
from app.services import pipeline
from app.services import rate_limiter as rl

# Dedicated DB / Redis logical DB so tests never touch the compose "api" data.
_TEST_DEFAULTS: dict[str, Any] = {
    "MONGO_URI": "mongodb://localhost:27017",
    "MONGO_DB": "cohort_insights_test",
    "REDIS_URL": "redis://localhost:6379/15",
    "PROCESSING_MIN_S": 0.01,
    "PROCESSING_MAX_S": 0.01,
    "ENRICHING_MIN_S": 0.01,
    "ENRICHING_MAX_S": 0.01,
    "PROCESSING_FAILURE_RATE": 0.0,
    "ENRICHING_FAILURE_RATE": 0.0,
    "LOG_LEVEL": "WARNING",
}


@asynccontextmanager
async def _client_cm(**overrides: Any) -> AsyncIterator[AsyncClient]:
    settings = Settings(**{**_TEST_DEFAULTS, **overrides})
    app = create_app(settings)

    async with LifespanManager(app):
        # Keep lifespan clients for cleanup even if a test swaps app.state.redis.
        cleanup_db = app.state.db
        cleanup_redis = app.state.redis
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            client.app = app  # type: ignore[attr-defined]
            try:
                yield client
            finally:
                await pipeline.cancel_all()
                for name in await cleanup_db.list_collection_names():
                    await cleanup_db.drop_collection(name)
                await cleanup_redis.flushdb()


@pytest.fixture
def make_client() -> Callable[..., Any]:
    """Return an async context manager: ``async with make_client(**overrides) as client``."""

    return _client_cm


@pytest.fixture
async def client(make_client: Callable[..., Any]) -> AsyncIterator[AsyncClient]:
    async with make_client() as ac:
        yield ac


async def wait_for_status(
    client: AsyncClient,
    document_id: str,
    user_id: str,
    statuses: str | Iterable[str],
    timeout: float = 30.0,
) -> dict[str, Any]:
    """Poll GET /documents/{id} until status is in ``statuses`` or timeout."""
    wanted = {statuses} if isinstance(statuses, str) else set(statuses)
    deadline = time.monotonic() + timeout
    last: dict[str, Any] | None = None

    while time.monotonic() < deadline:
        resp = await client.get(
            f"/documents/{document_id}",
            headers={"X-User-ID": user_id},
        )
        if resp.status_code == 200:
            body = resp.json()
            last = body
            if body.get("status") in wanted:
                return body
        await asyncio.sleep(0.05)

    raise TimeoutError(
        f"document {document_id} did not reach {wanted!r} within {timeout}s; last={last!r}"
    )


async def slot_count(redis: Any, user_id: str) -> int:
    raw = await redis.get(rl.active_jobs_key(user_id))
    return int(raw or 0)


class FailingRedis:
    """Redis stand-in where every call raises, to exercise the fallback paths."""

    async def _down(self, *args: Any, **kwargs: Any) -> Any:
        raise ConnectionError("redis down")

    ping = get = set = delete = eval = _down

    def scan_iter(self, *args: Any, **kwargs: Any) -> Any:
        raise ConnectionError("redis down")
