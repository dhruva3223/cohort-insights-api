"""Identity dependency tests (temporary router only; not mounted in the app)."""

from __future__ import annotations

import pytest
from fastapi import APIRouter

from app.dependencies import CurrentUserDep, OptionalHeaderUserDep

_test_router = APIRouter()


@_test_router.get("/_test/me")
async def _me(user_id: CurrentUserDep) -> dict[str, str]:
    return {"user_id": user_id}


@_test_router.get("/_test/optional")
async def _optional(user_id: OptionalHeaderUserDep) -> dict[str, str | None]:
    return {"user_id": user_id}


@pytest.mark.asyncio
async def test_missing_header_returns_400(make_client) -> None:
    async with make_client() as client:
        client.app.include_router(_test_router)
        resp = await client.get("/_test/me")
        assert resp.status_code == 400
        assert "detail" in resp.json()


@pytest.mark.asyncio
async def test_bad_format_returns_400(make_client) -> None:
    async with make_client() as client:
        client.app.include_router(_test_router)
        resp = await client.get("/_test/me", headers={"X-User-ID": "bad id!"})
        assert resp.status_code == 400
        assert "detail" in resp.json()


@pytest.mark.asyncio
async def test_valid_header_passes(make_client) -> None:
    async with make_client() as client:
        client.app.include_router(_test_router)
        resp = await client.get("/_test/me", headers={"X-User-ID": "user_1.ok:2-3"})
        assert resp.status_code == 200
        assert resp.json() == {"user_id": "user_1.ok:2-3"}


@pytest.mark.asyncio
async def test_optional_absent_returns_none(make_client) -> None:
    async with make_client() as client:
        client.app.include_router(_test_router)
        resp = await client.get("/_test/optional")
        assert resp.status_code == 200
        assert resp.json() == {"user_id": None}


@pytest.mark.asyncio
async def test_optional_invalid_returns_400(make_client) -> None:
    async with make_client() as client:
        client.app.include_router(_test_router)
        resp = await client.get("/_test/optional", headers={"X-User-ID": "not valid"})
        assert resp.status_code == 400
        assert "detail" in resp.json()


@pytest.mark.asyncio
async def test_health_stays_open_without_header(client) -> None:
    resp = await client.get("/health")
    assert resp.status_code == 200
