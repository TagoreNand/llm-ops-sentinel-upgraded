"""
Human Review Queue API

Surfaces failed evaluations, drift events, and auto-rollback triggers
for human inspection and disposition.

Endpoints:
  GET  /review/queue           — list pending items (filterable by app_id, reason)
  POST /review/queue/{id}      — submit a disposition (approved / rejected + note)
  GET  /review/stats           — summary counts by status and reason
"""
import structlog
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.keys import get_current_app
from app.database import ReviewQueueItem, get_db

logger = structlog.get_logger()
router = APIRouter()


class ReviewDecision(BaseModel):
    status: str          # "approved" | "rejected"
    reviewer_note: str = ""


class ReviewItemResponse(BaseModel):
    id: str
    created_at: datetime
    reviewed_at: datetime | None
    reason: str
    call_id: str | None
    drift_event_id: str | None
    status: str
    reviewer_note: str | None
    app_id: str
    details: dict


@router.get("/queue", response_model=list[ReviewItemResponse])
async def list_queue(
    status: str = Query("pending", description="Filter by status: pending | approved | rejected | all"),
    reason: str | None = Query(None, description="Filter by reason: low_score | toxic | golden_mismatch | drift"),
    limit: int = Query(50, le=200),
    db: AsyncSession = Depends(get_db),
    app_id: str = Depends(get_current_app),
):
    """List items in the human review queue for the current app."""
    q = select(ReviewQueueItem).where(ReviewQueueItem.app_id == app_id)
    if status != "all":
        q = q.where(ReviewQueueItem.status == status)
    if reason:
        q = q.where(ReviewQueueItem.reason == reason)
    q = q.order_by(ReviewQueueItem.created_at.desc()).limit(limit)

    result = await db.execute(q)
    items = result.scalars().all()
    return [_to_response(i) for i in items]


@router.post("/queue/{item_id}", response_model=ReviewItemResponse)
async def decide(
    item_id: str,
    decision: ReviewDecision,
    db: AsyncSession = Depends(get_db),
    app_id: str = Depends(get_current_app),
):
    """Submit a human review decision."""
    if decision.status not in ("approved", "rejected"):
        raise HTTPException(400, "status must be 'approved' or 'rejected'")

    result = await db.execute(
        select(ReviewQueueItem).where(
            ReviewQueueItem.id == item_id,
            ReviewQueueItem.app_id == app_id,
        )
    )
    item = result.scalar_one_or_none()
    if not item:
        raise HTTPException(404, "Review item not found")

    item.status = decision.status
    item.reviewer_note = decision.reviewer_note
    item.reviewed_at = datetime.utcnow()
    await db.commit()
    await db.refresh(item)

    logger.info("review_decision_recorded", item_id=item_id, status=decision.status, app_id=app_id)
    return _to_response(item)


@router.get("/stats")
async def review_stats(
    db: AsyncSession = Depends(get_db),
    app_id: str = Depends(get_current_app),
):
    """Summary counts for the review queue."""
    result = await db.execute(
        select(ReviewQueueItem.status, ReviewQueueItem.reason, func.count())
        .where(ReviewQueueItem.app_id == app_id)
        .group_by(ReviewQueueItem.status, ReviewQueueItem.reason)
    )
    rows = result.fetchall()
    return [{"status": r[0], "reason": r[1], "count": r[2]} for r in rows]


def _to_response(i: ReviewQueueItem) -> ReviewItemResponse:
    return ReviewItemResponse(
        id=i.id,
        created_at=i.created_at,
        reviewed_at=i.reviewed_at,
        reason=i.reason,
        call_id=i.call_id,
        drift_event_id=i.drift_event_id,
        status=i.status,
        reviewer_note=i.reviewer_note,
        app_id=i.app_id,
        details=i.details,
    )
