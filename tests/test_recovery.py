"""Startup recovery tests."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

import pytest

from app.config import Settings
from app.database import (
    DOCUMENTS_COLLECTION,
    create_indexes,
    create_mongo_client,
    get_database,
)
from app.models.document import content_hash
from app.redis_client import create_redis_client
from app.services import rate_limiter as rl
from tests.conftest import _TEST_DEFAULTS, slot_count, wait_for_status


async def _wait_slot_count(redis: Any, user_id: str, expected: int, timeout: float = 2.0) -> int:
    """Poll until the active-jobs counter matches ``expected`` (release trails status)."""
    deadline = time.monotonic() + timeout
    last = -1
    while time.monotonic() < deadline:
        last = await slot_count(redis, user_id)
        if last == expected:
            return last
        await asyncio.sleep(0.05)
    return last


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _stuck_doc(
    *,
    document_id: str,
    user_id: str,
    content: str,
    status: str,
    version: int = 1,
) -> dict[str, Any]:
    now = _utcnow()
    digest = content_hash(content)
    return {
        "document_id": document_id,
        "user_id": user_id,
        "title": "Recover Me",
        "content": content,
        "content_hash": digest,
        "version": version,
        "status": status,
        "stages": {
            "processing": {
                "state": "running" if status == "processing" else "succeeded",
                "error": None,
                "updated_at": now,
            },
            "enriching": {
                "state": "running" if status == "enriching" else "pending",
                "error": None,
                "updated_at": now,
            },
        },
        # Leftover derived fields from a prior attempt; recovery must leave them.
        "summary": {
            "text": "stale-summary",
            "source_version": version - 1 if version > 1 else 0,
            "source_hash": digest,
        },
        "created_at": now,
        "updated_at": now,
    }


@pytest.mark.asyncio
async def test_startup_recovery_resumes_stuck_docs(make_client) -> None:
    user = "recover-user"
    settings = Settings(
        **{
            **_TEST_DEFAULTS,
            "PROCESSING_MIN_S": 0.4,
            "PROCESSING_MAX_S": 0.4,
            "ENRICHING_MIN_S": 0.4,
            "ENRICHING_MAX_S": 0.4,
        }
    )

    mongo = create_mongo_client(settings)
    db = get_database(mongo, settings)
    redis = create_redis_client(settings)
    try:
        for name in await db.list_collection_names():
            await db.drop_collection(name)
        await redis.flushdb()
        await create_indexes(db)

        await db[DOCUMENTS_COLLECTION].insert_many(
            [
                _stuck_doc(
                    document_id="stuck-processing",
                    user_id=user,
                    content="proc-content",
                    status="processing",
                ),
                _stuck_doc(
                    document_id="stuck-enriching",
                    user_id=user,
                    content="enrich-content",
                    status="enriching",
                    version=2,
                ),
            ]
        )
        # Leftover / wrong counter that rebuild_counters must replace.
        await redis.set(rl.active_jobs_key(user), 99, ex=600)
        await redis.set(rl.active_jobs_key("ghost"), 7, ex=600)
    finally:
        await redis.aclose()
        await mongo.close()

    async with make_client(
        PROCESSING_MIN_S=0.4,
        PROCESSING_MAX_S=0.4,
        ENRICHING_MIN_S=0.4,
        ENRICHING_MAX_S=0.4,
    ) as client:
        app_redis = client.app.state.redis  # type: ignore[attr-defined]
        app_db = client.app.state.db  # type: ignore[attr-defined]

        # After recovery: both active, counter rebuilt from Mongo (not 99).
        active = await app_db[DOCUMENTS_COLLECTION].count_documents(
            {"user_id": user, "status": {"$in": ["queued", "processing", "enriching"]}}
        )
        assert active == 2
        assert await slot_count(app_redis, user) == 2
        assert await app_redis.get(rl.active_jobs_key("ghost")) is None

        # Summary leftovers must still be present until stage 1 overwrites.
        enriching_raw = await app_db[DOCUMENTS_COLLECTION].find_one(
            {"document_id": "stuck-enriching"}
        )
        assert enriching_raw is not None
        assert enriching_raw.get("summary", {}).get("text") == "stale-summary"

        done_a = await wait_for_status(
            client, "stuck-processing", user, "completed", timeout=10.0
        )
        done_b = await wait_for_status(
            client, "stuck-enriching", user, "completed", timeout=10.0
        )
        assert done_a["is_stale"] is False
        assert done_b["is_stale"] is False
        assert done_b["version"] == 2
        assert done_b["summary"]["source_version"] == 2

        # Release runs after cache write, so it can lag status=completed briefly.
        assert await _wait_slot_count(app_redis, user, 0) == 0


@pytest.mark.asyncio
async def test_startup_recovery_resumes_enriching_with_current_summary(make_client) -> None:
    user = "recover-enrich"
    settings = Settings(**_TEST_DEFAULTS)
    mongo = create_mongo_client(settings)
    db = get_database(mongo, settings)
    try:
        for name in await db.list_collection_names():
            await db.drop_collection(name)
        await create_indexes(db)
        doc = _stuck_doc(
            document_id="resume-enriching",
            user_id=user,
            content="resume-content",
            status="enriching",
            version=3,
        )
        doc["summary"]["source_version"] = 3
        doc["summary"]["text"] = "summary-from-v3"
        await db[DOCUMENTS_COLLECTION].insert_one(doc)
    finally:
        await mongo.close()

    # If processing ran again it would fail, so completing proves it was skipped.
    async with make_client(PROCESSING_FAILURE_RATE=1.0) as client:
        done = await wait_for_status(
            client, "resume-enriching", user, "completed", timeout=10.0
        )
        assert done["version"] == 3
        assert done["summary"]["text"] == "summary-from-v3"
        assert done["is_stale"] is False
