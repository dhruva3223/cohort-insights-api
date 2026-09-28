"""Document HTTP endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Response, status

from app.dependencies import (
    CurrentUserDep,
    DbDep,
    OptionalHeaderUserDep,
    RedisDep,
    SettingsDep,
)
from app.models.document import (
    DocumentCreate,
    DocumentPatch,
    DocumentResponse,
    PatchResponse,
    SubmitResponse,
)
from app.services import documents as documents_service

router = APIRouter(tags=["documents"])


def _resolve_post_identity(header_user: str | None, body_user: str | None) -> str:
    """Header or body ``user_id``; if both are sent they must match."""
    if header_user and body_user and header_user != body_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="X-User-ID and body user_id must match",
        )
    user_id = header_user or body_user
    if user_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="user_id is required via X-User-ID header or body",
        )
    return user_id


@router.post("/documents", response_model=SubmitResponse)
async def create_document(
    body: DocumentCreate,
    header_user: OptionalHeaderUserDep,
    db: DbDep,
    redis: RedisDep,
    settings: SettingsDep,
    response: Response,
) -> SubmitResponse:
    user_id = _resolve_post_identity(header_user, body.user_id)
    result, code = await documents_service.submit(
        db,
        redis,
        settings,
        user_id=user_id,
        title=body.title,
        content=body.content,
        client_doc_ref=body.client_doc_ref,
    )
    response.status_code = code
    return result


@router.patch("/documents/{document_id}", response_model=PatchResponse)
async def patch_document(
    document_id: str,
    body: DocumentPatch,
    user_id: CurrentUserDep,
    db: DbDep,
    redis: RedisDep,
    settings: SettingsDep,
) -> PatchResponse:
    return await documents_service.patch(
        db,
        redis,
        settings,
        document_id=document_id,
        user_id=user_id,
        content=body.content,
        expected_version=body.expected_version,
    )


@router.get("/documents/by-ref/{ref}", response_model=DocumentResponse)
async def get_document_by_ref(
    ref: str,
    user_id: CurrentUserDep,
    db: DbDep,
) -> DocumentResponse:
    return await documents_service.get_by_ref(db, ref, user_id)


@router.get("/documents/{document_id}", response_model=DocumentResponse)
async def get_document(
    document_id: str,
    user_id: CurrentUserDep,
    db: DbDep,
) -> DocumentResponse:
    return await documents_service.get_by_id(db, document_id, user_id)
