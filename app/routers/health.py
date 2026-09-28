"""Health check endpoint (no authentication)."""

from __future__ import annotations

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from app.dependencies import DbDep, RedisDep
from app.logging_config import get_logger

router = APIRouter(tags=["health"])
logger = get_logger(__name__)


@router.get("/health")
async def health(db: DbDep, redis: RedisDep) -> JSONResponse:
    mongo_status = "unavailable"
    redis_status = "unavailable"

    try:
        await db.command("ping")
        mongo_status = "connected"
    except Exception:
        logger.exception("health_mongodb_ping_failed")

    try:
        if await redis.ping():
            redis_status = "connected"
    except Exception:
        logger.exception("health_redis_ping_failed")

    healthy = mongo_status == redis_status == "connected"
    return JSONResponse(
        content={
            "status": "ok" if healthy else "unhealthy",
            "mongodb": mongo_status,
            "redis": redis_status,
        },
        status_code=status.HTTP_200_OK if healthy else status.HTTP_503_SERVICE_UNAVAILABLE,
    )
