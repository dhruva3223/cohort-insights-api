"""Pydantic models, enums, content hashing, and the single read-model function."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, PlainSerializer, StringConstraints

ID_REGEX = r"^[A-Za-z0-9._:-]{1,128}$"
ID_PATTERN = re.compile(ID_REGEX)

# Identical message for missing and non-owned documents (ownership must not leak).
DOCUMENT_NOT_FOUND = "Document not found"


class DocumentStatus(StrEnum):
    QUEUED = "queued"
    PROCESSING = "processing"
    ENRICHING = "enriching"
    COMPLETED = "completed"
    FAILED = "failed"


class StageState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


ACTIVE_STATUSES = frozenset(
    {DocumentStatus.QUEUED, DocumentStatus.PROCESSING, DocumentStatus.ENRICHING}
)
TERMINAL_STATUSES = frozenset({DocumentStatus.COMPLETED, DocumentStatus.FAILED})


def content_hash(content: str) -> str:
    """SHA-256 hex digest of already-stripped content."""
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _utc_iso(value: datetime) -> str:
    # MongoDB returns naive datetimes that are already UTC.
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


Title = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
Content = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100_000)
]
Identifier = Annotated[str, StringConstraints(pattern=ID_REGEX)]
UtcDatetime = Annotated[datetime, PlainSerializer(_utc_iso)]


# --- requests ---


class DocumentCreate(BaseModel):
    title: Title
    content: Content
    client_doc_ref: Identifier | None = None
    user_id: Identifier | None = None


class DocumentPatch(BaseModel):
    content: Content
    expected_version: int | None = Field(default=None, ge=1)


class DocumentListQuery(BaseModel):
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)
    status: DocumentStatus | None = None


# --- responses ---


class StageInfo(BaseModel):
    state: StageState
    error: str | None = None
    updated_at: UtcDatetime | None = None


class StagesResponse(BaseModel):
    processing: StageInfo
    enriching: StageInfo


class SummaryResponse(BaseModel):
    text: str
    source_version: int
    source_hash: str


class TagsResponse(BaseModel):
    items: list[str]
    source_version: int
    source_hash: str


class DocumentResponse(BaseModel):
    """Public document shape; never includes Mongo ``_id``."""

    document_id: str
    user_id: str
    title: str
    content: str
    content_hash: str
    client_doc_ref: str | None = None
    version: int
    status: DocumentStatus
    stages: StagesResponse
    summary: SummaryResponse | None = None
    tags: TagsResponse | None = None
    created_at: UtcDatetime
    updated_at: UtcDatetime
    is_stale: bool
    failed_stage: Literal["processing", "enriching"] | None = None


class SubmitResponse(BaseModel):
    document_id: str
    status: DocumentStatus


class PatchResponse(BaseModel):
    document_id: str
    status: DocumentStatus
    version: int


class DocumentListResponse(BaseModel):
    items: list[DocumentResponse]
    total: int
    page: int
    page_size: int


# --- read model ---


def _made_for(derived: Any, version: int) -> bool:
    return isinstance(derived, dict) and derived.get("source_version") == version


def _failed_stage(
    stages: StagesResponse, status: DocumentStatus
) -> Literal["processing", "enriching"] | None:
    if status != DocumentStatus.FAILED:
        return None
    if stages.processing.state == StageState.FAILED:
        return "processing"
    if stages.enriching.state == StageState.FAILED:
        return "enriching"
    return None


def document_to_response(doc: dict[str, Any]) -> DocumentResponse:
    """Single read-model function for every GET path.

    Returns summary and tags only when both were produced for the document's
    current ``version``; otherwise both are null and ``is_stale`` is true.
    """
    version = int(doc["version"])
    status = DocumentStatus(doc["status"])
    stages = StagesResponse.model_validate(doc["stages"])
    current = _made_for(doc.get("summary"), version) and _made_for(doc.get("tags"), version)

    return DocumentResponse(
        document_id=doc["document_id"],
        user_id=doc["user_id"],
        title=doc["title"],
        content=doc["content"],
        content_hash=doc["content_hash"],
        client_doc_ref=doc.get("client_doc_ref"),
        version=version,
        status=status,
        stages=stages,
        summary=SummaryResponse.model_validate(doc["summary"]) if current else None,
        tags=TagsResponse.model_validate(doc["tags"]) if current else None,
        created_at=doc["created_at"],
        updated_at=doc["updated_at"],
        is_stale=not current,
        failed_stage=_failed_stage(stages, status),
    )
