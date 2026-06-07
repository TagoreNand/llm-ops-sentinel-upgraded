"""
Prompt Version Registry — upgraded with app_id isolation and Redis key scoping.
"""
import json
import random

import redis.asyncio as aioredis
import structlog
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.keys import get_current_app
from app.config import get_settings
from app.database import PromptVersion, get_db

logger = structlog.get_logger()
settings = get_settings()
router = APIRouter()


def get_redis():
    return aioredis.from_url(settings.redis_url.split("?")[0], decode_responses=True)


def _redis_key(app_id: str, name: str, suffix: str) -> str:
    return f"prompt:{app_id}:{name}:{suffix}"


class CreateVersionRequest(BaseModel):
    name: str
    version: str
    template: str
    deploy_as_canary: bool = True
    canary_traffic_pct: int = 10


class VersionResponse(BaseModel):
    id: str
    name: str
    version: str
    template: str
    is_active: bool
    is_canary: bool
    canary_traffic_pct: int
    avg_score: float | None
    call_count: int
    app_id: str


class ResolveResponse(BaseModel):
    name: str
    version: str
    template: str
    served_canary: bool
    app_id: str


@router.post("/versions", response_model=VersionResponse, status_code=201)
async def create_version(
    req: CreateVersionRequest,
    db: AsyncSession = Depends(get_db),
    app_id: str = Depends(get_current_app),
):
    pv = PromptVersion(
        name=req.name,
        version=req.version,
        template=req.template,
        is_active=not req.deploy_as_canary,
        is_canary=req.deploy_as_canary,
        canary_traffic_pct=req.canary_traffic_pct if req.deploy_as_canary else 0,
        app_id=app_id,
    )
    db.add(pv)
    await db.commit()
    await db.refresh(pv)

    try:
        redis = get_redis()
        await redis.set(
            _redis_key(app_id, req.name, "canary"),
            json.dumps({"version": req.version, "template": req.template, "pct": req.canary_traffic_pct}),
            ex=3600,
        )
        await redis.aclose()
    except Exception as e:
        logger.warning("redis_cache_failed", error=str(e))

    logger.info("prompt_version_created", name=req.name, version=req.version, app_id=app_id)
    return _to_response(pv)


@router.get("/versions/{name}", response_model=list[VersionResponse])
async def list_versions(
    name: str,
    db: AsyncSession = Depends(get_db),
    app_id: str = Depends(get_current_app),
):
    result = await db.execute(
        select(PromptVersion)
        .where(PromptVersion.name == name, PromptVersion.app_id == app_id)
        .order_by(PromptVersion.created_at.desc())
    )
    versions = result.scalars().all()
    if not versions:
        raise HTTPException(404, f"No versions found for prompt '{name}' in app '{app_id}'")
    return [_to_response(v) for v in versions]


@router.get("/resolve/{name}", response_model=ResolveResponse)
async def resolve_prompt(
    name: str,
    app_id: str = Depends(get_current_app),
):
    redis = get_redis()
    try:
        canary_raw = await redis.get(_redis_key(app_id, name, "canary"))
        active_raw = await redis.get(_redis_key(app_id, name, "active"))

        if canary_raw:
            canary = json.loads(canary_raw)
            if random.randint(1, 100) <= canary["pct"]:
                return ResolveResponse(name=name, version=canary["version"],
                                       template=canary["template"], served_canary=True, app_id=app_id)

        if active_raw:
            active = json.loads(active_raw)
            return ResolveResponse(name=name, version=active["version"],
                                   template=active["template"], served_canary=False, app_id=app_id)

        raise HTTPException(404, f"No active prompt version for '{name}'")
    finally:
        await redis.aclose()


@router.post("/versions/{name}/{version}/promote")
async def promote_version(
    name: str,
    version: str,
    db: AsyncSession = Depends(get_db),
    app_id: str = Depends(get_current_app),
):
    result = await db.execute(
        select(PromptVersion).where(
            PromptVersion.name == name,
            PromptVersion.version == version,
            PromptVersion.app_id == app_id,
        )
    )
    pv = result.scalar_one_or_none()
    if not pv:
        raise HTTPException(404, "Version not found")

    pv.is_active = True
    pv.is_canary = False
    pv.canary_traffic_pct = 0
    await db.commit()

    try:
        redis = get_redis()
        await redis.set(
            _redis_key(app_id, name, "active"),
            json.dumps({"version": version, "template": pv.template}),
        )
        await redis.delete(_redis_key(app_id, name, "canary"))
        await redis.aclose()
    except Exception as e:
        logger.warning("redis_promote_failed", error=str(e))

    logger.info("prompt_version_promoted", name=name, version=version, app_id=app_id)
    return {"status": "promoted", "name": name, "version": version, "app_id": app_id}


@router.post("/versions/{name}/{version}/rollback")
async def rollback_version(
    name: str,
    version: str,
    db: AsyncSession = Depends(get_db),
    app_id: str = Depends(get_current_app),
):
    result = await db.execute(
        select(PromptVersion).where(
            PromptVersion.name == name,
            PromptVersion.version == version,
            PromptVersion.app_id == app_id,
        )
    )
    pv = result.scalar_one_or_none()
    if not pv:
        raise HTTPException(404, "Version not found")

    try:
        redis = get_redis()
        await redis.delete(_redis_key(app_id, name, "canary"))
        await redis.set(
            _redis_key(app_id, name, "active"),
            json.dumps({"version": version, "template": pv.template}),
        )
        await redis.aclose()
    except Exception as e:
        logger.warning("redis_rollback_failed", error=str(e))

    logger.warning("prompt_version_rollback", name=name, version=version, app_id=app_id)
    return {"status": "rolled_back", "name": name, "active_version": version, "app_id": app_id}


def _to_response(pv: PromptVersion) -> VersionResponse:
    return VersionResponse(
        id=pv.id,
        name=pv.name,
        version=pv.version,
        template=pv.template,
        is_active=pv.is_active,
        is_canary=pv.is_canary,
        canary_traffic_pct=pv.canary_traffic_pct,
        avg_score=pv.avg_score,
        call_count=pv.call_count,
        app_id=pv.app_id,
    )
