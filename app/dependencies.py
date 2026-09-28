"""FastAPI dependency providers for settings, MongoDB, Redis and identity."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request, status
from pymongo.asynchronous.database import AsyncDatabase
from redis.asyncio import Redis

from app.config import Settings
from app.models.document import ID_PATTERN


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def get_db(request: Request) -> AsyncDatabase:
    return request.app.state.db


def get_redis(request: Request) -> Redis:
    return request.app.state.redis


def optional_header_user(
    x_user_id: Annotated[str | None, Header(alias="X-User-ID")] = None,
) -> str | None:
    """Validated ``X-User-ID``, or ``None`` if absent. Used directly only by POST."""
    if not x_user_id:
        return None
    if not ID_PATTERN.fullmatch(x_user_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid X-User-ID header"
        )
    return x_user_id


def current_user(
    user_id: Annotated[str | None, Depends(optional_header_user)],
) -> str:
    """Required ``X-User-ID`` for every endpoint except ``/health`` and POST."""
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="X-User-ID header is required"
        )
    return user_id


SettingsDep = Annotated[Settings, Depends(get_settings)]
DbDep = Annotated[AsyncDatabase, Depends(get_db)]
RedisDep = Annotated[Redis, Depends(get_redis)]
CurrentUserDep = Annotated[str, Depends(current_user)]
OptionalHeaderUserDep = Annotated[str | None, Depends(optional_header_user)]
