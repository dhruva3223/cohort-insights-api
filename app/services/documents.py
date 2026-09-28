"""Document use cases: reads, submit and patch."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import HTTPException, status
from pymongo.asynchronous.database import AsyncDatabase
from pymongo.errors import DuplicateKeyError
from redis.asyncio import Redis

from app.config import Settings
from app.models.document import (
    ACTIVE_STATUSES,
    DOCUMENT_NOT_FOUND,
    DocumentListResponse,
    DocumentResponse,
    DocumentStatus,
    PatchResponse,
    StageState,
    SubmitResponse,
    content_hash,
    document_to_response,
)
from app.repositories import documents as repo
from app.services import cache as cache_service
from app.services import pipeline
from app.services import rate_limiter

_CONFLICT = "Conflict"
_TOO_MANY = "Too many active documents"
_CONCURRENT = "Document was modified concurrently, retry."
_PATCH_ATTEMPTS = 3


# --- reads ---


def _not_found() -> HTTPException:
    # Same response for missing and non-owned documents.
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=DOCUMENT_NOT_FOUND)


async def get_by_id(db: AsyncDatabase, document_id: str, user_id: str) -> DocumentResponse:
    doc = await repo.find_owned(db, document_id, user_id)
    if doc is None:
        raise _not_found()
    return document_to_response(doc)


async def get_by_ref(db: AsyncDatabase, client_doc_ref: str, user_id: str) -> DocumentResponse:
    """Looked up by ref alone (index), then ownership is checked here."""
    doc = await repo.find_by_ref(db, client_doc_ref)
    if doc is None or doc["user_id"] != user_id:
        raise _not_found()
    return document_to_response(doc)


async def list_for_user(
    db: AsyncDatabase,
    user_id: str,
    *,
    page: int,
    page_size: int,
    status: DocumentStatus | None = None,
) -> DocumentListResponse:
    docs, total = await repo.list_owned(
        db, user_id, status=status, skip=(page - 1) * page_size, limit=page_size
    )
    return DocumentListResponse(
        items=[document_to_response(doc) for doc in docs],
        total=total,
        page=page,
        page_size=page_size,
    )


# --- submit ---


def _crosswalk(
    existing: dict[str, Any], *, user_id: str, digest: str
) -> tuple[SubmitResponse, int]:
    """Same owner and same current content → 200 with current state; else 409."""
    if existing["user_id"] != user_id or existing["content_hash"] != digest:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=_CONFLICT)
    return (
        SubmitResponse(
            document_id=existing["document_id"],
            status=DocumentStatus(existing["status"]),
        ),
        status.HTTP_200_OK,
    )


async def submit(
    db: AsyncDatabase,
    redis: Redis,
    settings: Settings,
    *,
    user_id: str,
    title: str,
    content: str,
    client_doc_ref: str | None,
) -> tuple[SubmitResponse, int]:
    """Create a document. Returns ``(body, http_status)``."""
    digest = content_hash(content)

    if client_doc_ref is not None:
        existing = await repo.find_by_ref(db, client_doc_ref)
        if existing is not None:
            return _crosswalk(existing, user_id=user_id, digest=digest)

    cached = await cache_service.get(redis, digest)
    holds_slot = cached is None
    if holds_slot and not await rate_limiter.acquire(redis, db, settings, user_id):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=_TOO_MANY
        )

    document_id = str(uuid.uuid4())
    try:
        await repo.insert_new(
            db,
            document_id=document_id,
            user_id=user_id,
            title=title,
            content=content,
            digest=digest,
            client_doc_ref=client_doc_ref,
            cached=cached,
        )
    except DuplicateKeyError:
        # Lost a race for the same ref: the first insert wins.
        if holds_slot:
            await rate_limiter.release(redis, user_id)
        winner = await repo.find_by_ref(db, client_doc_ref) if client_doc_ref else None
        if winner is None:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=_CONFLICT)
        return _crosswalk(winner, user_id=user_id, digest=digest)
    except Exception:
        if holds_slot:
            await rate_limiter.release(redis, user_id)
        raise

    if cached is not None:
        return (
            SubmitResponse(document_id=document_id, status=DocumentStatus.COMPLETED),
            status.HTTP_201_CREATED,
        )

    pipeline.spawn(document_id, 1, db=db, redis=redis, settings=settings)
    return (
        SubmitResponse(document_id=document_id, status=DocumentStatus.QUEUED),
        status.HTTP_201_CREATED,
    )


# --- patch ---


def _can_resume_enriching(doc: dict[str, Any]) -> bool:
    """Failed at enriching, with a summary still made for the current version."""
    summary = doc.get("summary")
    return (
        doc["status"] == DocumentStatus.FAILED
        and doc["stages"]["enriching"]["state"] == StageState.FAILED
        and isinstance(summary, dict)
        and summary.get("source_version") == doc["version"]
    )


async def patch(
    db: AsyncDatabase,
    redis: Redis,
    settings: Settings,
    *,
    document_id: str,
    user_id: str,
    content: str,
    expected_version: int | None,
) -> PatchResponse:
    """Replace content, bump the version and reprocess (or reuse a cached result).

    Same content on a document that failed at enriching reruns only enriching,
    at the same version. Every update is conditional on the version and status
    just read. If another writer got there first, the whole decision is re-made
    from a fresh read, up to ``_PATCH_ATTEMPTS`` times.
    """
    digest = content_hash(content)

    for _ in range(_PATCH_ATTEMPTS):
        doc = await repo.find_owned(db, document_id, user_id)
        if doc is None:
            raise _not_found()

        version = int(doc["version"])
        if expected_version is not None and version != expected_version:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Version mismatch; current version is {version}",
            )

        current = DocumentStatus(doc["status"])
        if digest == doc["content_hash"] and current != DocumentStatus.FAILED:
            return PatchResponse(document_id=document_id, status=current, version=version)

        cached = await cache_service.get(redis, digest)
        resume = cached is None and digest == doc["content_hash"] and _can_resume_enriching(doc)
        was_active = current in ACTIVE_STATUSES
        # An active document already holds a slot; a terminal one needs a new
        # slot unless a cache hit completes it immediately.
        needs_slot = cached is None and not was_active
        if needs_slot and not await rate_limiter.acquire(redis, db, settings, user_id):
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail=_TOO_MANY
            )

        try:
            if resume:
                updated = await repo.resume_enriching(db, document_id, user_id, version)
            else:
                updated = await repo.patch_content(
                    db,
                    document_id=document_id,
                    user_id=user_id,
                    version=version,
                    was_active=was_active,
                    content=content,
                    digest=digest,
                    cached=cached,
                )
        except Exception:
            if needs_slot:
                await rate_limiter.release(redis, user_id)
            raise

        if updated is None:
            if needs_slot:
                await rate_limiter.release(redis, user_id)
            continue

        run_version = int(updated["version"])
        if cached is None:
            pipeline.spawn(
                document_id,
                run_version,
                db=db,
                redis=redis,
                settings=settings,
                from_enriching=resume,
            )
        elif was_active:
            # Active → completed via cache: the old run's slot is freed here.
            await rate_limiter.release(redis, user_id)

        return PatchResponse(
            document_id=document_id,
            status=DocumentStatus(updated["status"]),
            version=run_version,
        )

    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=_CONCURRENT)
