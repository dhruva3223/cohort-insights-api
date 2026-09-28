"""MongoDB client helpers (PyMongo Async API)."""

from __future__ import annotations

from pymongo import ASCENDING, DESCENDING, AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase

from app.config import Settings

DOCUMENTS_COLLECTION = "documents"


def create_mongo_client(settings: Settings) -> AsyncMongoClient:
    """Build an AsyncMongoClient. Connects lazily on first operation."""
    return AsyncMongoClient(settings.MONGO_URI)


def get_database(client: AsyncMongoClient, settings: Settings) -> AsyncDatabase:
    """Return the configured database handle."""
    return client[settings.MONGO_DB]


async def create_indexes(db: AsyncDatabase) -> None:
    """Create the five documents indexes idempotently."""
    collection = db[DOCUMENTS_COLLECTION]

    await collection.create_index(
        [("document_id", ASCENDING)],
        unique=True,
        name="document_id_1",
    )
    # Both list indexes end with the list sort (created_at, _id), so pages are
    # read in order from the index instead of being sorted in memory.
    await collection.create_index(
        [
            ("user_id", ASCENDING),
            ("status", ASCENDING),
            ("created_at", DESCENDING),
            ("_id", DESCENDING),
        ],
        name="user_id_1_status_1_created_at_-1__id_-1",
    )
    await collection.create_index(
        [("client_doc_ref", ASCENDING)],
        unique=True,
        name="client_doc_ref_1",
        partialFilterExpression={"client_doc_ref": {"$exists": True}},
    )
    await collection.create_index(
        [("content_hash", ASCENDING)],
        name="content_hash_1",
    )
    await collection.create_index(
        [("user_id", ASCENDING), ("created_at", DESCENDING), ("_id", DESCENDING)],
        name="user_id_1_created_at_-1__id_-1",
    )
