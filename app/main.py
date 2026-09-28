"""Application factory and lifespan."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from app.config import Settings
from app.database import create_indexes, create_mongo_client, get_database
from app.logging_config import get_logger, setup_logging
from app.redis_client import create_redis_client
from app.routers import documents, health, users
from app.services import pipeline

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    setup_logging(settings.LOG_LEVEL)

    mongo_client = create_mongo_client(settings)
    redis = create_redis_client(settings)

    app.state.db = get_database(mongo_client, settings)
    app.state.redis = redis

    await create_indexes(app.state.db)

    recovered = await pipeline.recover(app.state.db, redis, settings)
    logger.info(
        "app_started",
        extra={"mongo_db": settings.MONGO_DB, "recovered": recovered},
    )

    try:
        yield
    finally:
        await pipeline.cancel_all()
        await redis.aclose()
        await mongo_client.close()
        logger.info("app_stopped")


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build and return the FastAPI application."""
    app_settings = settings if settings is not None else Settings()
    app = FastAPI(title="Cohort Insights API", lifespan=lifespan)
    app.state.settings = app_settings
    app.include_router(health.router)
    app.include_router(documents.router)
    app.include_router(users.router)
    return app
