"""PATCH /documents/{id} tests."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from httpx import AsyncClient

from app.database import DOCUMENTS_COLLECTION
from app.models.document import content_hash
from app.repositories import documents as repo
from app.services import cache as cache_service
from app.services import pipeline
from tests.conftest import slot_count, wait_for_status

NOT_FOUND_BODY = {"detail": "Document not found"}


async def _submit(
    client: AsyncClient,
    user: str,
    content: str,
    *,
    ref: str | None = None,
    title: str = "T",
) -> str:
    payload: dict[str, Any] = {"title": title, "content": content}
    if ref is not None:
        payload["client_doc_ref"] = ref
    resp = await client.post(
        "/documents",
        headers={"X-User-ID": user},
        json=payload,
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["document_id"]


@pytest.mark.asyncio
async def test_patch_staleness_then_reprocess(client: AsyncClient) -> None:
    user = "stale-user"
    doc_id = await _submit(client, user, "original")
    await wait_for_status(client, doc_id, user, "completed")

    patched = await client.patch(
        f"/documents/{doc_id}",
        headers={"X-User-ID": user},
        json={"content": "updated"},
    )
    assert patched.status_code == 200
    assert patched.json()["version"] == 2
    assert patched.json()["status"] == "queued"

    immediate = await client.get(
        f"/documents/{doc_id}", headers={"X-User-ID": user}
    )
    assert immediate.status_code == 200
    body = immediate.json()
    assert body["is_stale"] is True
    assert body["summary"] is None
    assert body["tags"] is None

    done = await wait_for_status(client, doc_id, user, "completed")
    assert done["is_stale"] is False
    assert done["summary"]["source_version"] == 2
    assert done["tags"]["source_version"] == 2
    assert done["content"] == "updated"


@pytest.mark.asyncio
async def test_patch_mid_run_supersedes_and_counter_correct(make_client) -> None:
    async with make_client(
        PROCESSING_MIN_S=1.5,
        PROCESSING_MAX_S=1.5,
        ENRICHING_MIN_S=1.5,
        ENRICHING_MAX_S=1.5,
    ) as client:
        user = "super-user"
        redis = client.app.state.redis
        doc_id = await _submit(client, user, "v1-content")
        await asyncio.sleep(0.2)
        assert await slot_count(redis, user) == 1

        patched = await client.patch(
            f"/documents/{doc_id}",
            headers={"X-User-ID": user},
            json={"content": "v2-content"},
        )
        assert patched.status_code == 200
        assert patched.json()["version"] == 2
        # Still active (queued) — same slot retained.
        assert await slot_count(redis, user) == 1

        done = await wait_for_status(client, doc_id, user, "completed", timeout=15.0)
        assert done["content"] == "v2-content"
        assert done["summary"]["source_version"] == 2
        assert done["content_hash"] == content_hash("v2-content")
        assert await slot_count(redis, user) == 0


@pytest.mark.asyncio
async def test_racing_patches_with_expected_version(client: AsyncClient) -> None:
    user = "race-patch"
    doc_id = await _submit(client, user, "base")
    await wait_for_status(client, doc_id, user, "completed")

    results = await asyncio.gather(
        client.patch(
            f"/documents/{doc_id}",
            headers={"X-User-ID": user},
            json={"content": "a", "expected_version": 1},
        ),
        client.patch(
            f"/documents/{doc_id}",
            headers={"X-User-ID": user},
            json={"content": "b", "expected_version": 1},
        ),
    )
    codes = sorted(r.status_code for r in results)
    assert codes == [200, 409]
    winner = next(r for r in results if r.status_code == 200)
    assert winner.json()["version"] == 2


@pytest.mark.asyncio
async def test_stale_expected_version_no_slot(client: AsyncClient) -> None:
    user = "stale-ver"
    redis = client.app.state.redis
    doc_id = await _submit(client, user, "c")
    await wait_for_status(client, doc_id, user, "completed")
    before = await slot_count(redis, user)

    resp = await client.patch(
        f"/documents/{doc_id}",
        headers={"X-User-ID": user},
        json={"content": "new", "expected_version": 99},
    )
    assert resp.status_code == 409
    assert "1" in resp.json()["detail"]
    assert await slot_count(redis, user) == before


@pytest.mark.asyncio
async def test_retry_then_success_slot_once(client: AsyncClient, monkeypatch) -> None:
    user = "retry-ok"
    redis = client.app.state.redis
    doc_id = await _submit(client, user, "orig")
    await wait_for_status(client, doc_id, user, "completed")
    before = await slot_count(redis, user)

    calls = {"n": 0}
    original = repo.patch_content

    async def flaky(db, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            return None
        return await original(db, **kwargs)

    monkeypatch.setattr(repo, "patch_content", flaky)

    resp = await client.patch(
        f"/documents/{doc_id}",
        headers={"X-User-ID": user},
        json={"content": "retry-content"},
    )
    assert resp.status_code == 200
    assert calls["n"] == 2
    # Terminal → queued acquired exactly one net slot for the new job.
    assert await slot_count(redis, user) == before + 1
    await wait_for_status(client, doc_id, user, "completed")
    assert await slot_count(redis, user) == before


@pytest.mark.asyncio
async def test_cache_hit_patch_racing_version_bump_not_left_stale(
    client: AsyncClient, monkeypatch
) -> None:
    user = "race-cache"
    cached_id = await _submit(client, user, "cached-target")
    await wait_for_status(client, cached_id, user, "completed")
    doc_id = await _submit(client, user, "start")
    await wait_for_status(client, doc_id, user, "completed")

    calls = {"n": 0}
    original = repo.patch_content

    async def bump_first(db, **kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            await db[DOCUMENTS_COLLECTION].update_one(
                {"document_id": doc_id}, {"$inc": {"version": 1}}
            )
        return await original(db, **kwargs)

    monkeypatch.setattr(repo, "patch_content", bump_first)

    resp = await client.patch(
        f"/documents/{doc_id}",
        headers={"X-User-ID": user},
        json={"content": "cached-target"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "completed"
    assert calls["n"] == 2

    body = (
        await client.get(f"/documents/{doc_id}", headers={"X-User-ID": user})
    ).json()
    assert body["is_stale"] is False
    assert body["summary"]["source_version"] == body["version"]
    assert body["tags"]["source_version"] == body["version"]


@pytest.mark.asyncio
async def test_retry_exhaustion_unchanged_counter(
    client: AsyncClient, monkeypatch
) -> None:
    user = "retry-fail"
    redis = client.app.state.redis
    doc_id = await _submit(client, user, "orig")
    await wait_for_status(client, doc_id, user, "completed")
    before = await slot_count(redis, user)

    async def always_miss(db, **kwargs):
        return None

    monkeypatch.setattr(repo, "patch_content", always_miss)

    resp = await client.patch(
        f"/documents/{doc_id}",
        headers={"X-User-ID": user},
        json={"content": "never"},
    )
    assert resp.status_code == 409
    assert resp.json()["detail"] == "Document was modified concurrently, retry."
    assert await slot_count(redis, user) == before


@pytest.mark.asyncio
async def test_identical_content_completed_is_noop(client: AsyncClient) -> None:
    user = "noop"
    doc_id = await _submit(client, user, "same")
    await wait_for_status(client, doc_id, user, "completed")

    resp = await client.patch(
        f"/documents/{doc_id}",
        headers={"X-User-ID": user},
        json={"content": "same"},
    )
    assert resp.status_code == 200
    assert resp.json() == {
        "document_id": doc_id,
        "status": "completed",
        "version": 1,
    }


@pytest.mark.asyncio
async def test_identical_content_after_enriching_failure_reruns_only_enriching(
    make_client,
) -> None:
    async with make_client(ENRICHING_FAILURE_RATE=1.0) as client:
        user = "fail-retry"
        redis = client.app.state.redis
        doc_id = await _submit(client, user, "boom")
        failed = await wait_for_status(client, doc_id, user, "failed")
        assert failed["failed_stage"] == "enriching"
        raw = await client.app.state.db[DOCUMENTS_COLLECTION].find_one(
            {"document_id": doc_id}
        )
        assert raw is not None
        saved_summary = raw["summary"]
        assert "tags" not in raw or raw.get("tags") is None

        # If processing ran again it would fail, so completing proves it was skipped.
        client.app.state.settings.PROCESSING_FAILURE_RATE = 1.0
        client.app.state.settings.ENRICHING_FAILURE_RATE = 0.0

        resp = await client.patch(
            f"/documents/{doc_id}",
            headers={"X-User-ID": user},
            json={"content": "boom"},
        )
        assert resp.status_code == 200
        assert resp.json() == {"document_id": doc_id, "status": "enriching", "version": 1}
        done = await wait_for_status(client, doc_id, user, "completed")
        assert done["version"] == 1
        assert done["is_stale"] is False
        assert done["summary"] == saved_summary
        assert await slot_count(redis, user) == 0


@pytest.mark.asyncio
async def test_identical_content_after_processing_failure_reruns_both(make_client) -> None:
    async with make_client(PROCESSING_FAILURE_RATE=1.0) as client:
        user = "fail-retry-proc"
        doc_id = await _submit(client, user, "boom-proc")
        failed = await wait_for_status(client, doc_id, user, "failed")
        assert failed["failed_stage"] == "processing"

        client.app.state.settings.PROCESSING_FAILURE_RATE = 0.0
        resp = await client.patch(
            f"/documents/{doc_id}",
            headers={"X-User-ID": user},
            json={"content": "boom-proc"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "queued"
        assert resp.json()["version"] == 2
        done = await wait_for_status(client, doc_id, user, "completed")
        assert done["is_stale"] is False


@pytest.mark.asyncio
async def test_patch_new_content_then_back_to_cache(client: AsyncClient) -> None:
    user = "cache-round"
    old = "old-content"
    new = "new-content"
    doc_id = await _submit(client, user, old)
    await wait_for_status(client, doc_id, user, "completed")
    old_summary = (await client.get(
        f"/documents/{doc_id}", headers={"X-User-ID": user}
    )).json()["summary"]["text"]

    to_new = await client.patch(
        f"/documents/{doc_id}",
        headers={"X-User-ID": user},
        json={"content": new},
    )
    assert to_new.status_code == 200
    mid = await wait_for_status(client, doc_id, user, "completed")
    assert mid["summary"]["text"] != old_summary
    assert mid["content"] == new

    back = await client.patch(
        f"/documents/{doc_id}",
        headers={"X-User-ID": user},
        json={"content": old},
    )
    assert back.status_code == 200
    assert back.json()["status"] == "completed"
    got = await client.get(f"/documents/{doc_id}", headers={"X-User-ID": user})
    assert got.json()["status"] == "completed"
    assert got.json()["summary"]["text"] == old_summary
    assert got.json()["is_stale"] is False


@pytest.mark.asyncio
async def test_patch_active_to_cached_content_releases_slot(make_client) -> None:
    async with make_client(
        PROCESSING_MIN_S=2.0,
        PROCESSING_MAX_S=2.0,
        ENRICHING_MIN_S=2.0,
        ENRICHING_MAX_S=2.0,
    ) as client:
        user = "active-cache"
        redis = client.app.state.redis
        settings = client.app.state.settings

        cached_content = "already-cached"
        summary = pipeline.mock_summary(cached_content)
        tags = pipeline.mock_tags(summary)
        await cache_service.set(
            redis, settings, content_hash(cached_content), summary, tags
        )

        doc_id = await _submit(client, user, "in-flight")
        await asyncio.sleep(0.15)
        assert await slot_count(redis, user) == 1

        resp = await client.patch(
            f"/documents/{doc_id}",
            headers={"X-User-ID": user},
            json={"content": cached_content},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "completed"
        assert await slot_count(redis, user) == 0

        got = await client.get(f"/documents/{doc_id}", headers={"X-User-ID": user})
        assert got.json()["status"] == "completed"
        assert got.json()["summary"]["text"] == summary
        assert got.json()["version"] == 2

        # Give the old pipeline time to finish; it must not overwrite.
        await asyncio.sleep(2.5)
        final = await client.get(f"/documents/{doc_id}", headers={"X-User-ID": user})
        assert final.json()["version"] == 2
        assert final.json()["content"] == cached_content
        assert final.json()["summary"]["text"] == summary
        assert await slot_count(redis, user) == 0


@pytest.mark.asyncio
async def test_patch_terminal_at_limit_429(make_client) -> None:
    async with make_client(
        MAX_ACTIVE_PER_USER=3,
        PROCESSING_MIN_S=2.0,
        PROCESSING_MAX_S=2.0,
        ENRICHING_MIN_S=2.0,
        ENRICHING_MAX_S=2.0,
    ) as client:
        user = "limit-patch"
        redis = client.app.state.redis
        target = await _submit(client, user, "completed-target")
        await wait_for_status(client, target, user, "completed")

        for i in range(3):
            resp = await client.post(
                "/documents",
                headers={"X-User-ID": user},
                json={"title": "T", "content": f"busy-{i}"},
            )
            assert resp.status_code == 201

        assert await slot_count(redis, user) == 3
        blocked = await client.patch(
            f"/documents/{target}",
            headers={"X-User-ID": user},
            json={"content": "needs-slot"},
        )
        assert blocked.status_code == 429


@pytest.mark.asyncio
async def test_patch_other_user_identical_404(client: AsyncClient) -> None:
    user = "owner"
    doc_id = await _submit(client, user, "secret")
    await wait_for_status(client, doc_id, user, "completed")

    other = await client.patch(
        f"/documents/{doc_id}",
        headers={"X-User-ID": "intruder"},
        json={"content": "hack"},
    )
    missing = await client.patch(
        "/documents/no-such-id",
        headers={"X-User-ID": user},
        json={"content": "hack"},
    )
    assert other.status_code == 404
    assert missing.status_code == 404
    assert other.json() == missing.json() == NOT_FOUND_BODY


@pytest.mark.asyncio
async def test_post_original_content_after_patch_409(client: AsyncClient) -> None:
    user = "ref-patch"
    ref = "stable-ref"
    doc_id = await _submit(client, user, "original", ref=ref)
    await wait_for_status(client, doc_id, user, "completed")

    patched = await client.patch(
        f"/documents/{doc_id}",
        headers={"X-User-ID": user},
        json={"content": "changed"},
    )
    assert patched.status_code == 200
    await wait_for_status(client, doc_id, user, "completed")

    again = await client.post(
        "/documents",
        headers={"X-User-ID": user},
        json={"title": "T", "content": "original", "client_doc_ref": ref},
    )
    assert again.status_code == 409
