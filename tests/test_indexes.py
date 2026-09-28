"""Index creation tests."""

from __future__ import annotations

import pytest
from pymongo.errors import DuplicateKeyError

from app.database import DOCUMENTS_COLLECTION, create_indexes

REQUIRED_INDEXES = {
    "document_id_1",
    "user_id_1_status_1_created_at_-1__id_-1",
    "client_doc_ref_1",
    "content_hash_1",
    "user_id_1_created_at_-1__id_-1",
}


@pytest.mark.asyncio
async def test_startup_creates_all_five_indexes(client) -> None:
    info = await client.app.state.db[DOCUMENTS_COLLECTION].index_information()
    assert REQUIRED_INDEXES.issubset(info.keys())

    ref_index = info["client_doc_ref_1"]
    assert ref_index.get("unique") is True
    assert ref_index.get("partialFilterExpression") == {
        "client_doc_ref": {"$exists": True}
    }

    assert info["document_id_1"].get("unique") is True


@pytest.mark.asyncio
async def test_documents_without_client_doc_ref_do_not_collide(client) -> None:
    collection = client.app.state.db[DOCUMENTS_COLLECTION]
    await collection.insert_one(
        {"document_id": "doc-a", "user_id": "u1", "title": "a", "content": "x"}
    )
    await collection.insert_one(
        {"document_id": "doc-b", "user_id": "u1", "title": "b", "content": "y"}
    )
    count = await collection.count_documents({"document_id": {"$in": ["doc-a", "doc-b"]}})
    assert count == 2


@pytest.mark.asyncio
async def test_duplicate_client_doc_ref_collides(client) -> None:
    collection = client.app.state.db[DOCUMENTS_COLLECTION]
    await collection.insert_one(
        {
            "document_id": "doc-1",
            "user_id": "u1",
            "client_doc_ref": "ref-same",
            "content": "a",
        }
    )
    with pytest.raises(DuplicateKeyError):
        await collection.insert_one(
            {
                "document_id": "doc-2",
                "user_id": "u2",
                "client_doc_ref": "ref-same",
                "content": "b",
            }
        )


@pytest.mark.asyncio
async def test_create_indexes_is_idempotent(client) -> None:
    db = client.app.state.db
    await create_indexes(db)
    await create_indexes(db)
    info = await db[DOCUMENTS_COLLECTION].index_information()
    assert REQUIRED_INDEXES.issubset(info.keys())
