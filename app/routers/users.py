"""User-scoped HTTP endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from app.dependencies import CurrentUserDep, DbDep
from app.models.document import (
    DOCUMENT_NOT_FOUND,
    DocumentListQuery,
    DocumentListResponse,
)
from app.services import documents as documents_service

router = APIRouter(tags=["users"])


@router.get("/users/{user_id}/documents", response_model=DocumentListResponse)
async def list_user_documents(
    user_id: str,
    current_user: CurrentUserDep,
    db: DbDep,
    query: Annotated[DocumentListQuery, Depends()],
) -> DocumentListResponse:
    if user_id != current_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=DOCUMENT_NOT_FOUND,
        )
    return await documents_service.list_for_user(
        db,
        user_id,
        page=query.page,
        page_size=query.page_size,
        status=query.status,
    )
