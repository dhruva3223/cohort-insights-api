"""Read endpoint tests (insert documents directly)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from httpx import AsyncClient

from app.database import DOCUMENTS_COLLECTION
from app.models.document import content_hash
from tests.conftest import wait_for_status

NOT_FOUND_BODY = {"detail": "Document not found"}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _base_doc(
    *,
    document_id: str,
    user_id: str,
    content: str = "hello",
    title: str = "Title",
    version: int = 1,
    status: str = "completed",
    client_doc_ref: str | None = None,
    created_at: datetime | None = None,
    summary: dict[str, Any] | None = None,
    tags: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = created_at or _utcnow()
    doc: dict[str, Any] = {
        "document_id": document_id,
        "user_id": user_id,
        "title": title,
        "content": content,
        "content_hash": content_hash(content),
        "version": version,
        "status": status,
        "stages": {
            "processing": {
                "state": "succeeded" if status == "completed" else "pending",
                "error": None,
                "updated_at": now,
            },
            "enriching": {
                "state": "succeeded" if status == "completed" else "pending",
                "error": None,
                "updated_at": now,
            },
        },
        "created_at": now,
        "updated_at": now,
    }
    if client_doc_ref is not None:
        doc["client_doc_ref"] = client_doc_ref
    if summary is not None:
        doc["summary"] = summary
    if tags is not None:
        doc["tags"] = tags
    return doc


def _current_results(content: str, version: int = 1) -> tuple[dict, dict]:
    digest = content_hash(content)
    summary = {
        "text": f"Summary of {digest[:16]}",
        "source_version": version,
        "source_hash": digest,
    }
    tags = {
        "items": ["a", "b"],
        "source_version": version,
        "source_hash": digest,
    }
    return summary, tags


@pytest.mark.asyncio
async def test_get_by_id_owner_ok_and_identical_404s(client: AsyncClient) -> None:
    db = client.app.state.db  # type: ignore[attr-defined]
    summary, tags = _current_results("hello")
    await db[DOCUMENTS_COLLECTION].insert_one(
        _base_doc(
            document_id="read-1",
            user_id="owner",
            client_doc_ref="ref-1",
            summary=summary,
            tags=tags,
        )
    )

    ok = await client.get("/documents/read-1", headers={"X-User-ID": "owner"})
    assert ok.status_code == 200
    body = ok.json()
    assert body["document_id"] == "read-1"
    assert body["is_stale"] is False
    assert body["summary"]["text"] == summary["text"]
    assert body["tags"]["items"] == tags["items"]
    assert "_id" not in body

    other = await client.get("/documents/read-1", headers={"X-User-ID": "other"})
    missing = await client.get("/documents/no-such", headers={"X-User-ID": "owner"})
    assert other.status_code == 404
    assert missing.status_code == 404
    assert other.json() == missing.json() == NOT_FOUND_BODY


@pytest.mark.asyncio
async def test_get_by_ref_owner_ok_and_non_owner_404(client: AsyncClient) -> None:
    db = client.app.state.db  # type: ignore[attr-defined]
    summary, tags = _current_results("ref-content")
    await db[DOCUMENTS_COLLECTION].insert_one(
        _base_doc(
            document_id="read-ref",
            user_id="owner",
            content="ref-content",
            client_doc_ref="partner-ref",
            summary=summary,
            tags=tags,
        )
    )

    ok = await client.get(
        "/documents/by-ref/partner-ref", headers={"X-User-ID": "owner"}
    )
    assert ok.status_code == 200
    assert ok.json()["document_id"] == "read-ref"

    other = await client.get(
        "/documents/by-ref/partner-ref", headers={"X-User-ID": "intruder"}
    )
    missing = await client.get(
        "/documents/by-ref/missing-ref", headers={"X-User-ID": "owner"}
    )
    assert other.status_code == 404
    assert missing.status_code == 404
    assert other.json() == missing.json() == NOT_FOUND_BODY


@pytest.mark.asyncio
async def test_list_pagination_ordering_status_and_path_mismatch(
    client: AsyncClient,
) -> None:
    db = client.app.state.db  # type: ignore[attr-defined]
    base = _utcnow()
    docs = []
    for i in range(5):
        summary, tags = _current_results(f"c{i}")
        docs.append(
            _base_doc(
                document_id=f"list-{i}",
                user_id="lister",
                content=f"c{i}",
                status="completed" if i % 2 == 0 else "failed",
                created_at=base + timedelta(seconds=i),
                summary=summary if i % 2 == 0 else None,
                tags=tags if i % 2 == 0 else None,
            )
        )
    # Another user's document must not appear.
    other_summary, other_tags = _current_results("other")
    docs.append(
        _base_doc(
            document_id="list-other",
            user_id="someone-else",
            content="other",
            created_at=base + timedelta(seconds=99),
            summary=other_summary,
            tags=other_tags,
        )
    )
    await db[DOCUMENTS_COLLECTION].insert_many(docs)

    page1 = await client.get(
        "/users/lister/documents",
        params={"page": 1, "page_size": 2},
        headers={"X-User-ID": "lister"},
    )
    assert page1.status_code == 200
    body1 = page1.json()
    assert body1["total"] == 5
    assert body1["page"] == 1
    assert body1["page_size"] == 2
    assert [item["document_id"] for item in body1["items"]] == ["list-4", "list-3"]

    page2 = await client.get(
        "/users/lister/documents",
        params={"page": 2, "page_size": 2},
        headers={"X-User-ID": "lister"},
    )
    assert [item["document_id"] for item in page2.json()["items"]] == [
        "list-2",
        "list-1",
    ]

    filtered = await client.get(
        "/users/lister/documents",
        params={"status": "failed"},
        headers={"X-User-ID": "lister"},
    )
    assert filtered.status_code == 200
    failed_body = filtered.json()
    assert failed_body["total"] == 2
    assert all(item["status"] == "failed" for item in failed_body["items"])

    mismatch = await client.get(
        "/users/someone-else/documents",
        headers={"X-User-ID": "lister"},
    )
    assert mismatch.status_code == 404
    assert mismatch.json() == NOT_FOUND_BODY


@pytest.mark.asyncio
async def test_stale_and_mixed_version_null_on_every_read_path(
    client: AsyncClient,
) -> None:
    db = client.app.state.db  # type: ignore[attr-defined]
    digest = content_hash("stale-doc")
    # Stale: summary/tags at older source_version than document version.
    stale = _base_doc(
        document_id="stale-1",
        user_id="reader",
        content="stale-doc",
        version=2,
        client_doc_ref="stale-ref",
        summary={
            "text": "old",
            "source_version": 1,
            "source_hash": digest,
        },
        tags={
            "items": ["old"],
            "source_version": 1,
            "source_hash": digest,
        },
    )
    # Mixed: summary matches version, tags do not.
    mixed = _base_doc(
        document_id="mixed-1",
        user_id="reader",
        content="mixed-doc",
        version=3,
        client_doc_ref="mixed-ref",
        summary={
            "text": "new-summary",
            "source_version": 3,
            "source_hash": content_hash("mixed-doc"),
        },
        tags={
            "items": ["old-tags"],
            "source_version": 2,
            "source_hash": content_hash("mixed-doc"),
        },
    )
    await db[DOCUMENTS_COLLECTION].insert_many([stale, mixed])

    paths = [
        ("/documents/stale-1", "stale-1"),
        ("/documents/by-ref/stale-ref", "stale-1"),
        ("/documents/mixed-1", "mixed-1"),
        ("/documents/by-ref/mixed-ref", "mixed-1"),
    ]
    for path, expected_id in paths:
        resp = await client.get(path, headers={"X-User-ID": "reader"})
        assert resp.status_code == 200, path
        body = resp.json()
        assert body["document_id"] == expected_id
        assert body["summary"] is None
        assert body["tags"] is None
        assert body["is_stale"] is True

    listed = await client.get(
        "/users/reader/documents",
        headers={"X-User-ID": "reader"},
    )
    assert listed.status_code == 200
    items = {item["document_id"]: item for item in listed.json()["items"]}
    for doc_id in ("stale-1", "mixed-1"):
        assert items[doc_id]["summary"] is None
        assert items[doc_id]["tags"] is None
        assert items[doc_id]["is_stale"] is True


def _collect_stages(node: Any) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    if isinstance(node, dict):
        if "stage" in node:
            found.append(node)
        for value in node.values():
            found.extend(_collect_stages(value))
    elif isinstance(node, list):
        for item in node:
            found.extend(_collect_stages(item))
    return found


@pytest.mark.asyncio
async def test_by_ref_query_uses_client_doc_ref_index(client: AsyncClient) -> None:
    """Prove the by-ref (and POST existence) filter uses ``client_doc_ref_1``.

    Partial filter in use: ``{"client_doc_ref": {"$exists": true}}``.
    """
    db = client.app.state.db  # type: ignore[attr-defined]
    collection = db[DOCUMENTS_COLLECTION]
    summary, tags = _current_results("indexed")
    await collection.insert_one(
        _base_doc(
            document_id="idx-1",
            user_id="owner",
            content="indexed",
            client_doc_ref="idx-ref",
            summary=summary,
            tags=tags,
        )
    )

    filt = {"client_doc_ref": "idx-ref"}
    explanation = await collection.find(filt).explain()
    stages = _collect_stages(explanation)
    assert any(
        stage.get("stage") == "IXSCAN" and stage.get("indexName") == "client_doc_ref_1"
        for stage in stages
    ), explanation
    assert not any(stage.get("stage") == "COLLSCAN" for stage in stages), explanation


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("filt", "index_name"),
    [
        ({"user_id": "lister"}, "user_id_1_created_at_-1__id_-1"),
        ({"user_id": "lister", "status": "completed"}, "user_id_1_status_1_created_at_-1__id_-1"),
    ],
)
async def test_list_query_reads_index_in_order(
    client: AsyncClient, filt: dict[str, Any], index_name: str
) -> None:
    """The list sort comes from the index, with no in-memory SORT stage."""
    collection = client.app.state.db[DOCUMENTS_COLLECTION]  # type: ignore[attr-defined]
    for i in range(3):
        summary, tags = _current_results(f"list-{i}")
        await collection.insert_one(
            _base_doc(
                document_id=f"list-{i}",
                user_id="lister",
                content=f"list-{i}",
                summary=summary,
                tags=tags,
            )
        )

    explanation = (
        await collection.find(filt).sort([("created_at", -1), ("_id", -1)]).limit(20).explain()
    )
    winning = explanation["queryPlanner"]["winningPlan"]
    stages = _collect_stages(winning)
    assert any(
        stage.get("stage") == "IXSCAN" and stage.get("indexName") == index_name
        for stage in stages
    ), winning
    assert not any(stage.get("stage") == "SORT" for stage in stages), winning


@pytest.mark.asyncio
async def test_ownership_404_body_matches_nonexistent_across_verbs(
    client: AsyncClient,
) -> None:
    """GET, PATCH, by-ref and list share the nonexistent-id 404 body for non-owners."""
    owner = "owner-gap"
    other = "intruder-gap"
    created = await client.post(
        "/documents",
        headers={"X-User-ID": owner},
        json={
            "title": "T",
            "content": "owned",
            "client_doc_ref": "owned-ref",
        },
    )
    assert created.status_code == 201
    doc_id = created.json()["document_id"]
    await wait_for_status(client, doc_id, owner, "completed")

    missing_get = await client.get(
        "/documents/does-not-exist", headers={"X-User-ID": owner}
    )
    assert missing_get.status_code == 404
    assert missing_get.json() == NOT_FOUND_BODY

    cases = [
        await client.get(f"/documents/{doc_id}", headers={"X-User-ID": other}),
        await client.patch(
            f"/documents/{doc_id}",
            headers={"X-User-ID": other},
            json={"content": "x"},
        ),
        await client.get(
            "/documents/by-ref/owned-ref", headers={"X-User-ID": other}
        ),
        await client.get(
            f"/users/{other}/documents", headers={"X-User-ID": owner}
        ),
    ]
    for resp in cases:
        assert resp.status_code == 404
        assert resp.json() == missing_get.json() == NOT_FOUND_BODY
