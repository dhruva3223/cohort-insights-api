"""Unit tests for models and the read-model function (no Mongo)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.models.document import (
    DocumentCreate,
    DocumentListQuery,
    DocumentPatch,
    DocumentStatus,
    StageState,
    content_hash,
    document_to_response,
)


def _base_doc(**overrides):
    now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    doc = {
        "_id": "should-never-appear",
        "document_id": "11111111-1111-4111-8111-111111111111",
        "user_id": "user-1",
        "title": "Hello",
        "content": "body text",
        "content_hash": content_hash("body text"),
        "version": 2,
        "status": "completed",
        "stages": {
            "processing": {
                "state": "succeeded",
                "error": None,
                "updated_at": now,
            },
            "enriching": {
                "state": "succeeded",
                "error": None,
                "updated_at": now,
            },
        },
        "summary": {
            "text": "a summary",
            "source_version": 2,
            "source_hash": content_hash("body text"),
        },
        "tags": {
            "items": ["a", "b"],
            "source_version": 2,
            "source_hash": content_hash("body text"),
        },
        "created_at": now,
        "updated_at": now,
    }
    doc.update(overrides)
    return doc


def test_content_hash_is_sha256_hex() -> None:
    digest = content_hash("hello")
    assert len(digest) == 64
    assert digest == content_hash("hello")
    assert digest != content_hash("hello ")


def test_current_result_is_served() -> None:
    resp = document_to_response(_base_doc())
    assert resp.is_stale is False
    assert resp.summary is not None
    assert resp.summary.text == "a summary"
    assert resp.tags is not None
    assert resp.tags.items == ["a", "b"]
    assert resp.failed_stage is None


def test_missing_tags_nulls_both() -> None:
    doc = _base_doc()
    del doc["tags"]
    resp = document_to_response(doc)
    assert resp.is_stale is True
    assert resp.summary is None
    assert resp.tags is None


def test_summary_at_old_version_nulls_both() -> None:
    doc = _base_doc()
    doc["summary"] = {
        "text": "old",
        "source_version": 1,
        "source_hash": content_hash("body text"),
    }
    resp = document_to_response(doc)
    assert resp.is_stale is True
    assert resp.summary is None
    assert resp.tags is None


def test_mixed_versions_null_both() -> None:
    """Mixed source_versions must hide both summary and tags."""
    doc = _base_doc()
    doc["summary"] = {
        "text": "current summary",
        "source_version": 2,
        "source_hash": content_hash("body text"),
    }
    doc["tags"] = {
        "items": ["old"],
        "source_version": 1,
        "source_hash": content_hash("older"),
    }
    resp = document_to_response(doc)
    assert resp.is_stale is True
    assert resp.summary is None
    assert resp.tags is None


def test_failed_stage_processing() -> None:
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    doc = _base_doc(
        status="failed",
        summary=None,
        tags=None,
        stages={
            "processing": {"state": "failed", "error": "boom", "updated_at": now},
            "enriching": {"state": "pending", "error": None, "updated_at": now},
        },
    )
    # remove null placeholders if present
    doc.pop("summary", None)
    doc.pop("tags", None)
    resp = document_to_response(doc)
    assert resp.failed_stage == "processing"
    assert resp.is_stale is True


def test_failed_stage_enriching() -> None:
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    doc = _base_doc(
        status="failed",
        stages={
            "processing": {"state": "succeeded", "error": None, "updated_at": now},
            "enriching": {"state": "failed", "error": "boom", "updated_at": now},
        },
        summary={
            "text": "kept",
            "source_version": 2,
            "source_hash": content_hash("body text"),
        },
    )
    doc.pop("tags", None)
    resp = document_to_response(doc)
    assert resp.failed_stage == "enriching"
    # summary present in store but tags missing → stale / both null
    assert resp.is_stale is True
    assert resp.summary is None
    assert resp.tags is None


def test_failed_stage_null_when_not_failed() -> None:
    resp = document_to_response(_base_doc(status="processing"))
    assert resp.failed_stage is None


def test_id_never_appears_in_output() -> None:
    resp = document_to_response(_base_doc())
    dumped = resp.model_dump()
    assert "_id" not in dumped
    as_json = resp.model_dump(mode="json")
    assert "_id" not in as_json


def test_datetimes_are_utc_iso() -> None:
    resp = document_to_response(_base_doc())
    payload = resp.model_dump(mode="json")
    assert payload["created_at"].endswith("+00:00")
    assert "T" in payload["created_at"]


def test_create_strips_title_and_content() -> None:
    body = DocumentCreate(title="  Hello  ", content="  world  ")
    assert body.title == "Hello"
    assert body.content == "world"


def test_create_rejects_blank_title() -> None:
    with pytest.raises(ValidationError):
        DocumentCreate(title="   ", content="ok")


def test_create_rejects_blank_content() -> None:
    with pytest.raises(ValidationError):
        DocumentCreate(title="ok", content="   ")


def test_create_rejects_bad_ref() -> None:
    with pytest.raises(ValidationError):
        DocumentCreate(title="ok", content="ok", client_doc_ref="bad ref!")


def test_create_rejects_bad_user_id() -> None:
    with pytest.raises(ValidationError):
        DocumentCreate(title="ok", content="ok", user_id="")


def test_create_accepts_valid_ids() -> None:
    body = DocumentCreate(
        title="t",
        content="c",
        user_id="user_1.2:3-4",
        client_doc_ref="ref.ok:1",
    )
    assert body.user_id == "user_1.2:3-4"
    assert body.client_doc_ref == "ref.ok:1"


def test_patch_strips_and_rejects_blank() -> None:
    assert DocumentPatch(content="  x  ").content == "x"
    with pytest.raises(ValidationError):
        DocumentPatch(content="  ")


def test_list_query_defaults_and_bounds() -> None:
    q = DocumentListQuery()
    assert q.page == 1
    assert q.page_size == 20
    assert q.status is None

    with pytest.raises(ValidationError):
        DocumentListQuery(page=0)
    with pytest.raises(ValidationError):
        DocumentListQuery(page_size=0)
    with pytest.raises(ValidationError):
        DocumentListQuery(page_size=101)
    with pytest.raises(ValidationError):
        DocumentListQuery(status="nope")  # type: ignore[arg-type]

    ok = DocumentListQuery(status=DocumentStatus.QUEUED)
    assert ok.status == DocumentStatus.QUEUED


def test_stage_state_enum_values() -> None:
    assert StageState.PENDING == "pending"
    assert DocumentStatus.COMPLETED == "completed"
