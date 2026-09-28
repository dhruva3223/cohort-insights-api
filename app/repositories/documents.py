"""MongoDB queries for the ``documents`` collection.

Every write that touches derived data (status, stages, summary, tags) is
conditional on ``version`` and the expected status, so a write made for a
superseded version matches nothing.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

from pymongo import DESCENDING, ReturnDocument
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.asynchronous.database import AsyncDatabase

from app.database import DOCUMENTS_COLLECTION
from app.models.document import (
    ACTIVE_STATUSES,
    TERMINAL_STATUSES,
    DocumentStatus,
    StageState,
)

_ACTIVE_VALUES = [s.value for s in ACTIVE_STATUSES]
_TERMINAL_VALUES = [s.value for s in TERMINAL_STATUSES]
# The stored summary was made for the document's current version.
_SUMMARY_IS_CURRENT = {"$eq": ["$summary.source_version", "$version"]}


def _collection(db: AsyncDatabase) -> AsyncCollection:
    return db[DOCUMENTS_COLLECTION]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _stage(state: StageState, now: datetime) -> dict[str, Any]:
    return {"state": state.value, "error": None, "updated_at": now}


def _stage_fields(
    stage: str, state: StageState, now: datetime, error: str | None = None
) -> dict[str, Any]:
    return {
        f"stages.{stage}.state": state.value,
        f"stages.{stage}.error": error,
        f"stages.{stage}.updated_at": now,
    }


def _derived(key: str, value: Any, version: int, digest: str) -> dict[str, Any]:
    """``summary`` / ``tags`` subdocument stamped with the version it came from."""
    return {key: value, "source_version": version, "source_hash": digest}


def _at(document_id: str, version: int, status: DocumentStatus) -> dict[str, Any]:
    return {"document_id": document_id, "version": version, "status": status.value}


# --- reads ---


async def find_owned(
    db: AsyncDatabase, document_id: str, user_id: str
) -> dict[str, Any] | None:
    return await _collection(db).find_one(
        {"document_id": document_id, "user_id": user_id}
    )


async def find_by_ref(db: AsyncDatabase, client_doc_ref: str) -> dict[str, Any] | None:
    """Filter on ``client_doc_ref`` only so the planner uses ``client_doc_ref_1``."""
    return await _collection(db).find_one({"client_doc_ref": client_doc_ref})


async def list_owned(
    db: AsyncDatabase,
    user_id: str,
    *,
    status: DocumentStatus | None,
    skip: int,
    limit: int,
) -> tuple[list[dict[str, Any]], int]:
    filt: dict[str, Any] = {"user_id": user_id}
    if status is not None:
        filt["status"] = status.value
    collection = _collection(db)
    total = await collection.count_documents(filt)
    cursor = (
        collection.find(filt)
        .sort([("created_at", DESCENDING), ("_id", DESCENDING)])
        .skip(skip)
        .limit(limit)
    )
    return await cursor.to_list(limit), total


async def count_active(db: AsyncDatabase, user_id: str) -> int:
    return await _collection(db).count_documents(
        {"user_id": user_id, "status": {"$in": _ACTIVE_VALUES}}
    )


async def active_counts_by_user(db: AsyncDatabase) -> AsyncIterator[tuple[str, int]]:
    pipeline = [
        {"$match": {"status": {"$in": _ACTIVE_VALUES}}},
        {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
    ]
    async for row in await _collection(db).aggregate(pipeline):
        if row["_id"] and row["count"] > 0:
            yield str(row["_id"]), int(row["count"])


# --- submit ---


async def insert_new(
    db: AsyncDatabase,
    *,
    document_id: str,
    user_id: str,
    title: str,
    content: str,
    digest: str,
    client_doc_ref: str | None,
    cached: dict[str, Any] | None,
) -> None:
    """Insert a version-1 document: ``queued`` on a cache miss, ``completed`` on a hit.

    Raises ``DuplicateKeyError`` when ``client_doc_ref`` is already taken.
    """
    now = _now()
    stage_state = StageState.SUCCEEDED if cached else StageState.PENDING
    doc: dict[str, Any] = {
        "document_id": document_id,
        "user_id": user_id,
        "title": title,
        "content": content,
        "content_hash": digest,
        "version": 1,
        "status": (DocumentStatus.COMPLETED if cached else DocumentStatus.QUEUED).value,
        "stages": {
            "processing": _stage(stage_state, now),
            "enriching": _stage(stage_state, now),
        },
        "created_at": now,
        "updated_at": now,
    }
    if client_doc_ref is not None:
        doc["client_doc_ref"] = client_doc_ref
    if cached:
        doc["summary"] = _derived("text", cached["summary_text"], 1, digest)
        doc["tags"] = _derived("items", list(cached["tags"]), 1, digest)
    await _collection(db).insert_one(doc)


# --- patch ---


async def patch_content(
    db: AsyncDatabase,
    *,
    document_id: str,
    user_id: str,
    version: int,
    was_active: bool,
    content: str,
    digest: str,
    cached: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Replace content and bump ``version`` if the document is unchanged since read.

    Matches only the version and status class (active or terminal) the caller
    observed. On a cache hit the new version is written ``completed`` with the
    cached results; otherwise it is requeued with results removed. Returns the
    updated document, or ``None`` if a concurrent write got there first.
    """
    now = _now()
    new_version = version + 1
    set_fields: dict[str, Any] = {
        "content": content,
        "content_hash": digest,
        "updated_at": now,
    }
    update: dict[str, Any] = {"$inc": {"version": 1}, "$set": set_fields}
    if cached:
        set_fields.update(
            {
                "status": DocumentStatus.COMPLETED.value,
                "stages.processing": _stage(StageState.SUCCEEDED, now),
                "stages.enriching": _stage(StageState.SUCCEEDED, now),
                "summary": _derived("text", cached["summary_text"], new_version, digest),
                "tags": _derived("items", list(cached["tags"]), new_version, digest),
            }
        )
    else:
        set_fields.update(
            {
                "status": DocumentStatus.QUEUED.value,
                "stages.processing": _stage(StageState.PENDING, now),
                "stages.enriching": _stage(StageState.PENDING, now),
            }
        )
        update["$unset"] = {"summary": "", "tags": ""}

    return await _collection(db).find_one_and_update(
        {
            "document_id": document_id,
            "user_id": user_id,
            "version": version,
            "status": {"$in": _ACTIVE_VALUES if was_active else _TERMINAL_VALUES},
        },
        update,
        return_document=ReturnDocument.AFTER,
    )


# --- pipeline transitions ---


async def claim_processing(
    db: AsyncDatabase, document_id: str, version: int
) -> dict[str, Any] | None:
    """``queued`` → ``processing``. ``None`` if superseded or already claimed."""
    now = _now()
    return await _collection(db).find_one_and_update(
        _at(document_id, version, DocumentStatus.QUEUED),
        {
            "$set": {
                "status": DocumentStatus.PROCESSING.value,
                **_stage_fields("processing", StageState.RUNNING, now),
                "updated_at": now,
            }
        },
        return_document=ReturnDocument.AFTER,
    )


async def resume_enriching(
    db: AsyncDatabase, document_id: str, user_id: str, version: int
) -> dict[str, Any] | None:
    """``failed`` at enriching → ``enriching`` (pending), keeping the stored summary.

    Same version, so the summary made for it stays valid. ``None`` if the
    document isn't in that state any more.
    """
    now = _now()
    return await _collection(db).find_one_and_update(
        {
            **_at(document_id, version, DocumentStatus.FAILED),
            "user_id": user_id,
            "stages.enriching.state": StageState.FAILED.value,
            "$expr": _SUMMARY_IS_CURRENT,
        },
        {
            "$set": {
                "status": DocumentStatus.ENRICHING.value,
                "stages.enriching": _stage(StageState.PENDING, now),
                "updated_at": now,
            }
        },
        return_document=ReturnDocument.AFTER,
    )


async def claim_enriching(
    db: AsyncDatabase, document_id: str, version: int
) -> dict[str, Any] | None:
    """Pending enriching → running. ``None`` if superseded or already claimed."""
    now = _now()
    return await _collection(db).find_one_and_update(
        {
            **_at(document_id, version, DocumentStatus.ENRICHING),
            "stages.enriching.state": StageState.PENDING.value,
        },
        {"$set": {**_stage_fields("enriching", StageState.RUNNING, now), "updated_at": now}},
        return_document=ReturnDocument.AFTER,
    )


async def complete_processing(
    db: AsyncDatabase, document_id: str, version: int, *, summary_text: str, digest: str
) -> bool:
    """Store the summary and move ``processing`` → ``enriching`` in one write."""
    now = _now()
    result = await _collection(db).update_one(
        _at(document_id, version, DocumentStatus.PROCESSING),
        {
            "$set": {
                "summary": _derived("text", summary_text, version, digest),
                **_stage_fields("processing", StageState.SUCCEEDED, now),
                "status": DocumentStatus.ENRICHING.value,
                **_stage_fields("enriching", StageState.RUNNING, now),
                "updated_at": now,
            }
        },
    )
    return result.modified_count == 1


async def find_summary_text(
    db: AsyncDatabase, document_id: str, version: int
) -> str | None:
    """Stage-1 summary text, only if it was produced for ``version``."""
    doc = await _collection(db).find_one({"document_id": document_id, "version": version})
    summary = doc.get("summary") if doc else None
    if not isinstance(summary, dict) or summary.get("source_version") != version:
        return None
    return summary["text"]


async def complete_enriching(
    db: AsyncDatabase, document_id: str, version: int, *, tags: list[str], digest: str
) -> bool:
    """Store tags and move ``enriching`` → ``completed``."""
    now = _now()
    result = await _collection(db).update_one(
        _at(document_id, version, DocumentStatus.ENRICHING),
        {
            "$set": {
                "tags": _derived("items", tags, version, digest),
                **_stage_fields("enriching", StageState.SUCCEEDED, now),
                "status": DocumentStatus.COMPLETED.value,
                "updated_at": now,
            }
        },
    )
    return result.modified_count == 1


async def mark_failed(
    db: AsyncDatabase,
    document_id: str,
    version: int,
    *,
    stage: DocumentStatus,
    error: str,
) -> bool:
    """Fail the run at ``stage``; the document must still be in that status.

    Stage names match their active status (``processing``, ``enriching``).
    """
    now = _now()
    result = await _collection(db).update_one(
        _at(document_id, version, stage),
        {
            "$set": {
                "status": DocumentStatus.FAILED.value,
                **_stage_fields(stage.value, StageState.FAILED, now, error),
                "updated_at": now,
            }
        },
    )
    return result.modified_count == 1


# --- startup recovery ---


async def reset_active_runs(db: AsyncDatabase) -> list[dict[str, Any]]:
    """Reset active documents so their runs can restart at the same version.

    A document in ``enriching`` whose summary is for its current version goes
    back to pending enriching; every other active document goes back to
    ``queued``. Returns the reset documents.
    """
    collection = _collection(db)
    now = _now()
    await collection.update_many(
        {"status": DocumentStatus.ENRICHING.value, "$expr": _SUMMARY_IS_CURRENT},
        {"$set": {"stages.enriching": _stage(StageState.PENDING, now), "updated_at": now}},
    )
    await collection.update_many(
        {
            "$or": [
                {"status": {"$in": [DocumentStatus.QUEUED.value, DocumentStatus.PROCESSING.value]}},
                {
                    "status": DocumentStatus.ENRICHING.value,
                    "$expr": {"$not": [_SUMMARY_IS_CURRENT]},
                },
            ]
        },
        {
            "$set": {
                "status": DocumentStatus.QUEUED.value,
                "stages.processing": _stage(StageState.PENDING, now),
                "stages.enriching": _stage(StageState.PENDING, now),
                "updated_at": now,
            }
        },
    )
    return await collection.find({"status": {"$in": _ACTIVE_VALUES}}).to_list(None)
