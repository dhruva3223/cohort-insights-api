"""Two-stage background document processing pipeline."""

from __future__ import annotations

import asyncio
import random
from typing import Any

from pymongo.asynchronous.database import AsyncDatabase
from redis.asyncio import Redis

from app.config import Settings
from app.logging_config import get_logger
from app.models.document import DocumentStatus, content_hash
from app.repositories import documents as repo
from app.services import cache as cache_service
from app.services import rate_limiter

logger = get_logger(__name__)

_tasks: set[asyncio.Task[Any]] = set()


def mock_summary(content: str) -> str:
    """Deterministic mock summary from content."""
    return f"Summary of {content_hash(content)[:16]}"


def mock_tags(summary_text: str) -> list[str]:
    """Deterministic mock tags derived from the stage-1 summary text."""
    digest = content_hash(summary_text)
    return [f"kw_{digest[:8]}", f"kw_{digest[8:16]}", f"kw_{digest[16:24]}"]


def spawn(
    document_id: str,
    version: int,
    *,
    db: AsyncDatabase,
    redis: Redis,
    settings: Settings,
    from_enriching: bool = False,
) -> asyncio.Task[Any]:
    """Schedule a pipeline run for ``document_id`` at ``version``.

    ``from_enriching`` skips processing and reuses the summary already stored
    for this version.
    """
    task = asyncio.create_task(
        _run(
            document_id,
            version,
            db=db,
            redis=redis,
            settings=settings,
            from_enriching=from_enriching,
        ),
        name=f"pipeline:{document_id}:v{version}",
    )
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
    return task


async def cancel_all() -> None:
    """Cancel every in-flight pipeline task and wait for them to finish."""
    tasks = list(_tasks)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def recover(db: AsyncDatabase, redis: Redis, settings: Settings) -> int:
    """Reset active documents, rebuild slot counters, and restart their runs."""
    docs = await repo.reset_active_runs(db)
    await rate_limiter.rebuild_counters(db, redis, settings)
    for doc in docs:
        spawn(
            doc["document_id"],
            int(doc["version"]),
            db=db,
            redis=redis,
            settings=settings,
            from_enriching=doc["status"] == DocumentStatus.ENRICHING,
        )
    logger.info("startup_recovery", extra={"recovered": len(docs)})
    return len(docs)


async def _run(
    document_id: str,
    version: int,
    *,
    db: AsyncDatabase,
    redis: Redis,
    settings: Settings,
    from_enriching: bool = False,
) -> None:
    user_id: str | None = None
    stage = DocumentStatus.ENRICHING if from_enriching else DocumentStatus.PROCESSING

    async def fail(error: str) -> None:
        # Release the slot only if this write took effect (exactly once).
        if await repo.mark_failed(db, document_id, version, stage=stage, error=error):
            await rate_limiter.release(redis, user_id)

    try:
        if from_enriching:
            claimed = await repo.claim_enriching(db, document_id, version)
        else:
            claimed = await repo.claim_processing(db, document_id, version)
        if claimed is None:
            return
        user_id = claimed["user_id"]
        digest = claimed["content_hash"]

        if not from_enriching:
            # Stage 1: processing → summary.
            await asyncio.sleep(
                random.uniform(settings.PROCESSING_MIN_S, settings.PROCESSING_MAX_S)
            )
            if random.random() < settings.PROCESSING_FAILURE_RATE:
                await fail("simulated processing failure")
                return
            summary_text = mock_summary(claimed["content"])
            if not await repo.complete_processing(
                db, document_id, version, summary_text=summary_text, digest=digest
            ):
                return

        # Stage 2: enriching → tags derived from the stored summary.
        stage = DocumentStatus.ENRICHING
        summary_text = await repo.find_summary_text(db, document_id, version)
        if summary_text is None:
            return
        tags = mock_tags(summary_text)
        await asyncio.sleep(
            random.uniform(settings.ENRICHING_MIN_S, settings.ENRICHING_MAX_S)
        )
        if random.random() < settings.ENRICHING_FAILURE_RATE:
            await fail("simulated enriching failure")
            return
        if await repo.complete_enriching(db, document_id, version, tags=tags, digest=digest):
            await cache_service.set(redis, settings, digest, summary_text, tags)
            await rate_limiter.release(redis, user_id)

    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception(
            "pipeline_unexpected_error",
            extra={
                "document_id": document_id,
                "user_id": user_id,
                "version": version,
                "stage": stage.value,
            },
        )
        if user_id is not None:
            await fail("unexpected pipeline error")
