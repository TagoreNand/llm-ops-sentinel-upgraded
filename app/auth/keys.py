"""
API Key authentication for multi-tenant isolation.

Usage:
  POST /auth/keys          — create a new API key (returns the raw key once)
  DELETE /auth/keys/{id}   — revoke a key

Every protected endpoint extracts the app_id from the validated key,
so data is automatically scoped per application.
"""
import hashlib
import secrets
from datetime import datetime

import structlog
from fastapi import APIRouter, Depends, HTTPException, Security
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import ApiKey, get_db

logger = structlog.get_logger()
router = APIRouter()

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def _hash_key(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


async def get_current_app(
    raw_key: str | None = Security(API_KEY_HEADER),
    db: AsyncSession = Depends(get_db),
) -> str:
    """
    Dependency — resolves the caller's app_id from their API key.
    Returns "default" when no key is presented (dev / unauthenticated mode).
    """
    if not raw_key:
        return "default"

    key_hash = _hash_key(raw_key)
    result = await db.execute(
        select(ApiKey).where(ApiKey.key_hash == key_hash, ApiKey.is_active == True)
    )
    api_key = result.scalar_one_or_none()

    if not api_key:
        raise HTTPException(status_code=401, detail="Invalid or revoked API key")

    # Update last_used_at without blocking the request
    await db.execute(
        update(ApiKey).where(ApiKey.id == api_key.id).values(last_used_at=datetime.utcnow())
    )
    await db.commit()

    return api_key.app_id


# ── Management endpoints ───────────────────────────────────────────────────────

class CreateKeyRequest(BaseModel):
    app_id: str
    description: str = ""


class CreateKeyResponse(BaseModel):
    id: str
    app_id: str
    key: str   # shown ONCE — not stored in plaintext
    description: str


@router.post("/keys", response_model=CreateKeyResponse, status_code=201)
async def create_api_key(req: CreateKeyRequest, db: AsyncSession = Depends(get_db)):
    """Create a new API key. The raw key is returned once and never stored."""
    raw = f"sk-sentinel-{secrets.token_urlsafe(32)}"
    api_key = ApiKey(
        key_hash=_hash_key(raw),
        app_id=req.app_id,
        description=req.description,
    )
    db.add(api_key)
    await db.commit()
    await db.refresh(api_key)
    logger.info("api_key_created", app_id=req.app_id, key_id=api_key.id)
    return CreateKeyResponse(id=api_key.id, app_id=req.app_id, key=raw, description=req.description)


@router.delete("/keys/{key_id}", status_code=204)
async def revoke_api_key(key_id: str, db: AsyncSession = Depends(get_db)):
    """Revoke an API key by ID."""
    result = await db.execute(select(ApiKey).where(ApiKey.id == key_id))
    key = result.scalar_one_or_none()
    if not key:
        raise HTTPException(404, "Key not found")
    key.is_active = False
    await db.commit()
    logger.info("api_key_revoked", key_id=key_id)
