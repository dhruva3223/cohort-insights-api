"""POST /documents submit tests."""

from __future__ import annotations

import asyncio

import pytest
from httpx import AsyncClient

from app.database import DOCUMENTS_COLLECTION
from app.models.document import content_hash
from app.services import cache as cache_service
from app.services import pipeline
from tests.conftest import FailingRedis, slot_count, wait_for_status


@pytest.mark.asyncio
async def test_post_201_reaches_completed(client: AsyncClient) -> None:
    resp = await client.post(
        "/documents",
        headers={"X-User-ID": "u-ok"},
        json={"title": "T", "content": "hello world"},
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["status"] == "queued"
    document_id = body["document_id"]

    done = await wait_for_status(client, document_id, "u-ok", "completed")
    assert done["summary"] is not None
    assert done["tags"] is not None
    assert done["is_stale"] is False
    assert await slot_count(client.app.state.redis, "u-ok") == 0  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_post_identity_header_body_rules(make_client) -> None:
    async with make_client() as client:
        header_only = await client.post(
            "/documents",
            headers={"X-User-ID": "hdr"},
            json={"title": "T", "content": "c1"},
        )
        assert header_only.status_code == 201

        body_only = await client.post(
            "/documents",
            json={"title": "T", "content": "c2", "user_id": "body-user"},
        )
        assert body_only.status_code == 201

        mismatch = await client.post(
            "/documents",
            headers={"X-User-ID": "a"},
            json={"title": "T", "content": "c3", "user_id": "b"},
        )
        assert mismatch.status_code == 400

        neither = await client.post(
            "/documents",
            json={"title": "T", "content": "c4"},
        )
        assert neither.status_code == 400


@pytest.mark.asyncio
async def test_crosswalk_same_ref_same_content_200(client: AsyncClient) -> None:
    first = await client.post(
        "/documents",
        headers={"X-User-ID": "cw"},
        json={
            "title": "Original",
            "content": "same body",
            "client_doc_ref": "ref-same",
        },
    )
    assert first.status_code == 201
    doc_id = first.json()["document_id"]

    second = await client.post(
        "/documents",
        headers={"X-User-ID": "cw"},
        json={
            "title": "Changed Title",
            "content": "same body",
            "client_doc_ref": "ref-same",
        },
    )
    assert second.status_code == 200
    assert second.json()["document_id"] == doc_id

    stored = await client.get(f"/documents/{doc_id}", headers={"X-User-ID": "cw"})
    assert stored.json()["title"] == "Original"


@pytest.mark.asyncio
async def test_crosswalk_failed_repeat_200_no_new_job(make_client) -> None:
    async with make_client(PROCESSING_FAILURE_RATE=1.0) as client:
        first = await client.post(
            "/documents",
            headers={"X-User-ID": "fail-user"},
            json={
                "title": "T",
                "content": "will fail",
                "client_doc_ref": "fail-ref",
            },
        )
        assert first.status_code == 201
        doc_id = first.json()["document_id"]
        failed = await wait_for_status(client, doc_id, "fail-user", "failed")
        assert failed["status"] == "failed"

        repeat = await client.post(
            "/documents",
            headers={"X-User-ID": "fail-user"},
            json={
                "title": "T2",
                "content": "will fail",
                "client_doc_ref": "fail-ref",
            },
        )
        assert repeat.status_code == 200
        assert repeat.json() == {"document_id": doc_id, "status": "failed"}
        assert await slot_count(client.app.state.redis, "fail-user") == 0


@pytest.mark.asyncio
async def test_crosswalk_conflicts(client: AsyncClient) -> None:
    first = await client.post(
        "/documents",
        headers={"X-User-ID": "owner"},
        json={
            "title": "T",
            "content": "v1",
            "client_doc_ref": "conflict-ref",
        },
    )
    assert first.status_code == 201

    different_content = await client.post(
        "/documents",
        headers={"X-User-ID": "owner"},
        json={
            "title": "T",
            "content": "v2",
            "client_doc_ref": "conflict-ref",
        },
    )
    assert different_content.status_code == 409

    other_user = await client.post(
        "/documents",
        headers={"X-User-ID": "intruder"},
        json={
            "title": "T",
            "content": "v1",
            "client_doc_ref": "conflict-ref",
        },
    )
    assert other_user.status_code == 409


@pytest.mark.asyncio
async def test_concurrent_duplicate_posts_one_document(client: AsyncClient) -> None:
    user = "race-user"
    payload = {
        "title": "T",
        "content": "race content",
        "client_doc_ref": "race-ref",
    }

    results = await asyncio.gather(
        client.post("/documents", headers={"X-User-ID": user}, json=payload),
        client.post("/documents", headers={"X-User-ID": user}, json=payload),
    )
    codes = sorted(r.status_code for r in results)
    assert codes == [200, 201]
    bodies = [r.json() for r in results]
    ids = {b["document_id"] for b in bodies}
    assert len(ids) == 1

    count = await client.app.state.db[DOCUMENTS_COLLECTION].count_documents(  # type: ignore[attr-defined]
        {"client_doc_ref": "race-ref"}
    )
    assert count == 1

    doc_id = next(iter(ids))
    await wait_for_status(client, doc_id, user, ("completed", "failed"))
    # Slot counter must not be stuck high after one document finishes.
    assert await slot_count(client.app.state.redis, user) == 0  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_two_posts_without_ref_both_succeed(client: AsyncClient) -> None:
    a = await client.post(
        "/documents",
        headers={"X-User-ID": "noref"},
        json={"title": "A", "content": "one"},
    )
    b = await client.post(
        "/documents",
        headers={"X-User-ID": "noref"},
        json={"title": "B", "content": "two"},
    )
    assert a.status_code == 201
    assert b.status_code == 201
    assert a.json()["document_id"] != b.json()["document_id"]


@pytest.mark.asyncio
async def test_fourth_active_returns_429_then_slot_frees(make_client) -> None:
    async with make_client(
        PROCESSING_MIN_S=1.0,
        PROCESSING_MAX_S=1.0,
        ENRICHING_MIN_S=1.0,
        ENRICHING_MAX_S=1.0,
        MAX_ACTIVE_PER_USER=3,
    ) as client:
        user = "limit-user"
        ids = []
        for i in range(3):
            resp = await client.post(
                "/documents",
                headers={"X-User-ID": user},
                json={"title": "T", "content": f"active-{i}"},
            )
            assert resp.status_code == 201
            ids.append(resp.json()["document_id"])

        fourth = await client.post(
            "/documents",
            headers={"X-User-ID": user},
            json={"title": "T", "content": "active-3"},
        )
        assert fourth.status_code == 429
        assert await slot_count(client.app.state.redis, user) == 3

        await wait_for_status(client, ids[0], user, "completed", timeout=10.0)
        assert await slot_count(client.app.state.redis, user) <= 2


@pytest.mark.asyncio
async def test_cache_hit_completed_no_slot(client: AsyncClient) -> None:
    redis = client.app.state.redis  # type: ignore[attr-defined]
    settings = client.app.state.settings  # type: ignore[attr-defined]
    content = "cached content"
    digest = content_hash(content)
    summary = pipeline.mock_summary(content)
    tags = pipeline.mock_tags(summary)
    await cache_service.set(redis, settings, digest, summary, tags)

    user = "cache-user"
    resp = await client.post(
        "/documents",
        headers={"X-User-ID": user},
        json={"title": "T", "content": content, "client_doc_ref": "cache-ref"},
    )
    assert resp.status_code == 201
    assert resp.json()["status"] == "completed"
    assert await slot_count(redis, user) == 0

    got = await client.get(
        f"/documents/{resp.json()['document_id']}",
        headers={"X-User-ID": user},
    )
    assert got.json()["status"] == "completed"
    assert got.json()["is_stale"] is False


@pytest.mark.asyncio
async def test_second_document_same_content_hits_cache(client: AsyncClient) -> None:
    user = "cache2"
    content = "shared content"
    first = await client.post(
        "/documents",
        headers={"X-User-ID": user},
        json={"title": "One", "content": content},
    )
    assert first.status_code == 201
    await wait_for_status(client, first.json()["document_id"], user, "completed")

    second = await client.post(
        "/documents",
        headers={"X-User-ID": user},
        json={"title": "Two", "content": content},
    )
    assert second.status_code == 201
    assert second.json()["status"] == "completed"
    assert first.json()["document_id"] != second.json()["document_id"]
    assert await slot_count(client.app.state.redis, user) == 0  # type: ignore[attr-defined]

    redis = client.app.state.redis  # type: ignore[attr-defined]
    keys = [k async for k in redis.scan_iter(match="cache:content:*")]
    assert keys
    for key in keys:
        key_str = key.decode() if isinstance(key, bytes) else key
        assert "document_id" not in key_str
        assert first.json()["document_id"] not in key_str
        assert second.json()["document_id"] not in key_str


@pytest.mark.asyncio
async def test_redis_down_post_still_201(make_client) -> None:
    async with make_client() as client:
        client.app.state.redis = FailingRedis()  # type: ignore[attr-defined]
        resp = await client.post(
            "/documents",
            headers={"X-User-ID": "redis-down"},
            json={"title": "T", "content": "still works"},
        )
        assert resp.status_code == 201
        assert resp.json()["status"] == "queued"


@pytest.mark.asyncio
async def test_failed_run_via_post_frees_slot(make_client) -> None:
    """Active-job counter returns to zero after a failed pipeline run."""
    async with make_client(PROCESSING_FAILURE_RATE=1.0) as client:
        user = "fail-slot"
        redis = client.app.state.redis  # type: ignore[attr-defined]
        resp = await client.post(
            "/documents",
            headers={"X-User-ID": user},
            json={"title": "T", "content": "will-fail"},
        )
        assert resp.status_code == 201
        doc_id = resp.json()["document_id"]
        failed = await wait_for_status(client, doc_id, user, "failed")
        assert failed["failed_stage"] == "processing"
        assert await slot_count(redis, user) == 0
