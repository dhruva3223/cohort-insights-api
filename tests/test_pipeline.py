"""Pipeline unit tests (direct spawn, no HTTP endpoints)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import pytest
from httpx import AsyncClient

from app.database import DOCUMENTS_COLLECTION
from app.models.document import content_hash
from app.services import cache as cache_service
from app.services import pipeline
from app.services import rate_limiter as rl
from tests.conftest import slot_count, wait_for_status


def _pending_stage(now: datetime) -> dict[str, Any]:
    return {"state": "pending", "error": None, "updated_at": now}


async def _wait_status(
    db: Any,
    document_id: str,
    *statuses: str,
    timeout: float = 5.0,
) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        doc = await db[DOCUMENTS_COLLECTION].find_one({"document_id": document_id})
        if doc is not None and doc.get("status") in statuses:
            return doc
        await asyncio.sleep(0.05)
    raise AssertionError(
        f"document {document_id} did not reach {statuses} within {timeout}s"
    )


async def _insert_queued(
    client: AsyncClient,
    *,
    user_id: str,
    content: str,
    version: int = 1,
    document_id: str | None = None,
) -> dict[str, Any]:
    app = client.app  # type: ignore[attr-defined]
    db = app.state.db
    redis = app.state.redis
    settings = app.state.settings

    assert await rl.acquire(redis, db, settings, user_id) is True

    now = datetime.now(timezone.utc)
    doc_id = document_id or f"doc-{user_id}-{version}"
    doc: dict[str, Any] = {
        "document_id": doc_id,
        "user_id": user_id,
        "title": "Pipeline Test",
        "content": content,
        "content_hash": content_hash(content),
        "version": version,
        "status": "queued",
        "stages": {
            "processing": _pending_stage(now),
            "enriching": _pending_stage(now),
        },
        "created_at": now,
        "updated_at": now,
    }
    await db[DOCUMENTS_COLLECTION].insert_one(doc)
    return doc


@pytest.mark.asyncio
async def test_pipeline_success_writes_cache_and_releases_slot(
    make_client,
) -> None:
    async with make_client() as client:
        app = client.app  # type: ignore[attr-defined]
        db = app.state.db
        redis = app.state.redis
        settings = app.state.settings
        user_id = "pipe-ok"
        content = "hello pipeline"

        doc = await _insert_queued(client, user_id=user_id, content=content)
        pipeline.spawn(
            doc["document_id"],
            doc["version"],
            db=db,
            redis=redis,
            settings=settings,
        )

        result = await _wait_status(db, doc["document_id"], "completed")
        assert result["version"] == 1
        assert result["summary"]["text"] == pipeline.mock_summary(content)
        assert result["summary"]["source_version"] == 1
        assert result["summary"]["source_hash"] == content_hash(content)
        assert result["tags"]["items"] == pipeline.mock_tags(result["summary"]["text"])
        assert result["tags"]["source_version"] == 1
        assert result["stages"]["processing"]["state"] == "succeeded"
        assert result["stages"]["enriching"]["state"] == "succeeded"

        cached = await cache_service.get(redis, content_hash(content))
        assert cached is not None
        assert cached["summary_text"] == result["summary"]["text"]
        assert cached["tags"] == result["tags"]["items"]

        assert await slot_count(redis, user_id) == 0


@pytest.mark.asyncio
async def test_pipeline_stage1_failure_releases_slot(make_client) -> None:
    async with make_client(PROCESSING_FAILURE_RATE=1.0) as client:
        app = client.app  # type: ignore[attr-defined]
        db = app.state.db
        redis = app.state.redis
        settings = app.state.settings
        user_id = "pipe-s1-fail"

        doc = await _insert_queued(client, user_id=user_id, content="boom1")
        pipeline.spawn(
            doc["document_id"],
            doc["version"],
            db=db,
            redis=redis,
            settings=settings,
        )

        result = await _wait_status(db, doc["document_id"], "failed")
        assert result["stages"]["processing"]["state"] == "failed"
        assert result["stages"]["enriching"]["state"] == "pending"
        assert "summary" not in result
        assert await slot_count(redis, user_id) == 0


@pytest.mark.asyncio
async def test_pipeline_stage2_failure_keeps_summary_and_releases(
    make_client,
) -> None:
    async with make_client(ENRICHING_FAILURE_RATE=1.0) as client:
        app = client.app  # type: ignore[attr-defined]
        db = app.state.db
        redis = app.state.redis
        settings = app.state.settings
        user_id = "pipe-s2-fail"
        content = "boom2"

        doc = await _insert_queued(client, user_id=user_id, content=content)
        pipeline.spawn(
            doc["document_id"],
            doc["version"],
            db=db,
            redis=redis,
            settings=settings,
        )

        result = await _wait_status(db, doc["document_id"], "failed")
        assert result["stages"]["processing"]["state"] == "succeeded"
        assert result["stages"]["enriching"]["state"] == "failed"
        assert result["summary"]["text"] == pipeline.mock_summary(content)
        assert result["summary"]["source_version"] == 1
        assert "tags" not in result
        assert await slot_count(redis, user_id) == 0


@pytest.mark.asyncio
async def test_superseded_run_writes_nothing_and_releases_nothing(
    make_client,
) -> None:
    async with make_client(
        PROCESSING_MIN_S=1.0,
        PROCESSING_MAX_S=1.0,
        ENRICHING_MIN_S=1.0,
        ENRICHING_MAX_S=1.0,
    ) as client:
        app = client.app  # type: ignore[attr-defined]
        db = app.state.db
        redis = app.state.redis
        settings = app.state.settings
        user_id = "pipe-supersede"
        content = "v1 content"

        doc = await _insert_queued(client, user_id=user_id, content=content)
        task = pipeline.spawn(
            doc["document_id"],
            doc["version"],
            db=db,
            redis=redis,
            settings=settings,
        )

        await asyncio.sleep(0.2)
        await db[DOCUMENTS_COLLECTION].update_one(
            {"document_id": doc["document_id"]},
            {
                "$set": {
                    "version": 2,
                    "content": "v2 content",
                    "content_hash": content_hash("v2 content"),
                }
            },
        )

        await asyncio.wait_for(task, timeout=5.0)

        stored = await db[DOCUMENTS_COLLECTION].find_one(
            {"document_id": doc["document_id"]}
        )
        assert stored is not None
        assert stored["version"] == 2
        assert "summary" not in stored
        assert "tags" not in stored
        # Old run did not release; counter still held for the acquired slot.
        assert await slot_count(redis, user_id) == 1


@pytest.mark.asyncio
async def test_cancel_all_leaves_active_and_does_not_release(
    make_client,
) -> None:
    async with make_client(
        PROCESSING_MIN_S=2.0,
        PROCESSING_MAX_S=2.0,
    ) as client:
        app = client.app  # type: ignore[attr-defined]
        db = app.state.db
        redis = app.state.redis
        settings = app.state.settings
        user_id = "pipe-cancel"
        content = "cancel me"

        doc = await _insert_queued(client, user_id=user_id, content=content)
        pipeline.spawn(
            doc["document_id"],
            doc["version"],
            db=db,
            redis=redis,
            settings=settings,
        )

        await asyncio.sleep(0.2)
        mid = await db[DOCUMENTS_COLLECTION].find_one(
            {"document_id": doc["document_id"]}
        )
        assert mid is not None
        assert mid["status"] in ("queued", "processing")

        await pipeline.cancel_all()

        after = await db[DOCUMENTS_COLLECTION].find_one(
            {"document_id": doc["document_id"]}
        )
        assert after is not None
        assert after["status"] in ("queued", "processing")
        assert after["status"] != "failed"
        assert after["stages"]["processing"]["state"] != "failed"
        assert await slot_count(redis, user_id) == 1


@pytest.mark.asyncio
async def test_enriching_failure_keeps_summary_via_http(make_client) -> None:
    """Enriching failure leaves failed_stage=enriching and keeps the stage-1 summary."""
    async with make_client(ENRICHING_FAILURE_RATE=1.0) as client:
        user = "enrich-fail"
        resp = await client.post(
            "/documents",
            headers={"X-User-ID": user},
            json={"title": "T", "content": "enrich-boom"},
        )
        assert resp.status_code == 201
        doc_id = resp.json()["document_id"]
        failed = await wait_for_status(client, doc_id, user, "failed")
        assert failed["failed_stage"] == "enriching"

        raw = await client.app.state.db[DOCUMENTS_COLLECTION].find_one(  # type: ignore[attr-defined]
            {"document_id": doc_id}
        )
        assert raw is not None
        assert raw.get("summary") is not None
        assert raw["summary"]["source_version"] == 1
        assert "tags" not in raw
