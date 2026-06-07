"""
Golden Set Management API

Create and list golden prompt/response pairs used for:
  - Online eval gates (similarity check in evaluate_response task)
  - Offline batch evaluation (run_batch_eval task)

Endpoints:
  POST /golden/examples          — add a golden example
  GET  /golden/examples          — list examples for app
  DELETE /golden/examples/{id}   — remove an example
  POST /golden/batch-eval        — trigger an offline batch evaluation run
"""
import structlog
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.keys import get_current_app
from app.database import GoldenExample, get_db
from workers.tasks import run_batch_eval

logger = structlog.get_logger()
router = APIRouter()


class GoldenExampleCreate(BaseModel):
    prompt_text: str
    expected_response: str
    tags: dict = {}


class GoldenExampleResponse(BaseModel):
    id: str
    created_at: datetime
    prompt_text: str
    expected_response: str
    app_id: str
    tags: dict


@router.post("/examples", response_model=GoldenExampleResponse, status_code=201)
async def add_example(
    req: GoldenExampleCreate,
    db: AsyncSession = Depends(get_db),
    app_id: str = Depends(get_current_app),
):
    ex = GoldenExample(
        prompt_text=req.prompt_text,
        expected_response=req.expected_response,
        app_id=app_id,
        tags=req.tags,
    )
    db.add(ex)
    await db.commit()
    await db.refresh(ex)
    logger.info("golden_example_added", app_id=app_id, example_id=ex.id)
    return _to_response(ex)


@router.get("/examples", response_model=list[GoldenExampleResponse])
async def list_examples(
    db: AsyncSession = Depends(get_db),
    app_id: str = Depends(get_current_app),
):
    result = await db.execute(
        select(GoldenExample).where(GoldenExample.app_id == app_id)
        .order_by(GoldenExample.created_at.desc())
    )
    return [_to_response(ex) for ex in result.scalars().all()]


@router.delete("/examples/{example_id}", status_code=204)
async def delete_example(
    example_id: str,
    db: AsyncSession = Depends(get_db),
    app_id: str = Depends(get_current_app),
):
    result = await db.execute(
        select(GoldenExample).where(GoldenExample.id == example_id, GoldenExample.app_id == app_id)
    )
    ex = result.scalar_one_or_none()
    if not ex:
        raise HTTPException(404, "Example not found")
    await db.delete(ex)
    await db.commit()


@router.post("/batch-eval")
async def trigger_batch_eval(app_id: str = Depends(get_current_app)):
    """Trigger an offline batch evaluation job against all golden examples."""
    run_batch_eval.delay(app_id=app_id)
    return {"status": "queued", "app_id": app_id}


def _to_response(ex: GoldenExample) -> GoldenExampleResponse:
    return GoldenExampleResponse(
        id=ex.id,
        created_at=ex.created_at,
        prompt_text=ex.prompt_text,
        expected_response=ex.expected_response,
        app_id=ex.app_id,
        tags=ex.tags,
    )
