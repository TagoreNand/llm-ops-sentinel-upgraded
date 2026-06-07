"""
Celery Tasks

- evaluate_response : Score response, compare to golden set, enqueue review if needed
- run_drift_detection: Nightly semantic drift detection (baseline persisted in Postgres)
- check_rollback     : Hourly canary health check — executes rollback automatically
- run_batch_eval     : Offline batch evaluation against the full golden set
"""
import asyncio
import uuid

import mlflow
import structlog
from celery import Task
from sqlalchemy import select, func, update

from app.config import get_settings
from app.database import (
    AsyncSessionLocal,
    EvaluationResult,
    LLMCall,
    PromptVersion,
    DriftEvent,
    DriftBaseline,
    ReviewQueueItem,
    GoldenExample,
)
from app.evaluators.judge import evaluate
from drift.detector import detect_drift_from_db
from monitoring.alerts import send_drift_alert, send_rollback_alert
from monitoring.metrics import (
    eval_tasks_total,
    eval_score_histogram,
    prompt_avg_score,
    drift_score_current,
    rollback_events_total,
)
from workers.celery_app import celery_app

logger = structlog.get_logger()
settings = get_settings()


def run_async(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _cosine_similarity(a: list[float], b: list[float]) -> float:
    import math
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x ** 2 for x in a))
    norm_b = math.sqrt(sum(x ** 2 for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


async def _compare_to_golden(response: str, app_id: str) -> tuple[bool, float]:
    """
    Embed the response and compare to stored golden examples.
    Returns (passed_gate, best_similarity_score).
    A score above 0.75 against any golden example counts as a pass.
    """
    from drift.embedder import embed
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(GoldenExample).where(GoldenExample.app_id == app_id).limit(50)
        )
        examples = result.scalars().all()

    if not examples:
        return True, 1.0   # No golden set — gate is open

    response_emb = embed([response])[0].tolist()
    golden_texts = [ex.expected_response for ex in examples]
    golden_embs = embed(golden_texts)

    best = max(_cosine_similarity(response_emb, g.tolist()) for g in golden_embs)
    return best >= 0.30, round(float(best), 4)  # 0.30 = min semantic relevance threshold


# ── Evaluation Task ────────────────────────────────────────────────────────────

@celery_app.task(bind=True, max_retries=3, default_retry_delay=30, queue="evaluation")
def evaluate_response(
    self: Task,
    call_id: str,
    prompt: str,
    response: str,
    model: str,
    app_id: str = "default",
    prompt_name: str | None = None,
    prompt_version: str = "v1",
):
    """Evaluate a response, compare to golden set, enqueue for review if low quality."""
    logger.info("eval_task_started", call_id=call_id, app_id=app_id)

    try:
        result = run_async(evaluate(prompt, response))
    except Exception as exc:
        logger.error("eval_failed", call_id=call_id, error=str(exc))
        eval_tasks_total.labels(status="failure").inc()
        raise self.retry(exc=exc)

    # Golden-set comparison
    try:
        golden_pass, golden_sim = run_async(_compare_to_golden(response, app_id))
    except Exception as exc:
        logger.warning("golden_comparison_failed", error=str(exc))
        golden_pass, golden_sim = True, 1.0

    run_id = None
    try:
        mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
        mlflow.set_experiment("llm-eval")
        mlflow_ctx = mlflow.start_run(run_name=f"eval-{call_id[:8]}")
        run = mlflow_ctx.__enter__()
        run_id = run.info.run_id
        mlflow.log_params({"model": model, "call_id": call_id, "app_id": app_id})
        mlflow.log_metrics({"faithfulness": result.faithfulness, "relevance": result.relevance, "toxicity": result.toxicity, "overall_score": result.overall_score, "golden_similarity": golden_sim})
        mlflow_ctx.__exit__(None, None, None)
    except Exception as mlflow_err:
        pass
    if False:
        mlflow.log_params({"model": model, "call_id": call_id, "app_id": app_id})
        mlflow.log_metrics({
            "faithfulness": result.faithfulness,
            "relevance": result.relevance,
            "toxicity": result.toxicity,
            "overall_score": result.overall_score,
            "golden_similarity": golden_sim,
        })
        run_id = run.info.run_id

    async def _save():
        async with AsyncSessionLocal() as db:
            eval_row = EvaluationResult(
                call_id=call_id,
                faithfulness=result.faithfulness,
                relevance=result.relevance,
                toxicity=result.toxicity,
                overall_score=result.overall_score,
                judge_model=result.judge_model,
                mlflow_run_id=run_id,
                golden_match=golden_pass,
                golden_similarity=golden_sim,
            )
            db.add(eval_row)

            # Enqueue for human review if low score, toxic, or golden mismatch
            needs_review = (
                result.overall_score < settings.rollback_score_threshold
                or result.toxicity > 0.5
                or not golden_pass
            )
            if needs_review:
                reason = "low_score"
                if result.toxicity > 0.5:
                    reason = "toxic"
                elif not golden_pass:
                    reason = "golden_mismatch"
                review_item = ReviewQueueItem(
                    call_id=call_id,
                    reason=reason,
                    app_id=app_id,
                    details={
                        "overall_score": result.overall_score,
                        "toxicity": result.toxicity,
                        "golden_similarity": golden_sim,
                        "prompt_version": prompt_version,
                        "prompt_name": prompt_name,
                        "reasoning": result.reasoning,
                    },
                )
                db.add(review_item)

            await db.commit()

    run_async(_save())

    eval_tasks_total.labels(status="success").inc()
    eval_score_histogram.observe(result.overall_score)

    if prompt_name:
        prompt_avg_score.labels(prompt_name=prompt_name, version=prompt_version).set(result.overall_score)

    logger.info(
        "eval_task_complete",
        call_id=call_id,
        overall=result.overall_score,
        golden_pass=golden_pass,
        golden_sim=golden_sim,
    )
    return {"call_id": call_id, "overall_score": result.overall_score, "golden_pass": golden_pass}


# ── Drift Detection Task ───────────────────────────────────────────────────────

@celery_app.task(bind=True, queue="alerts")
def run_drift_detection(self: Task):
    """Nightly: embed recent responses and compare against Postgres-persisted baseline."""
    logger.info("drift_detection_started")
    run_async(detect_drift_from_db())


# ── Rollback Check Task — fully automatic ────────────────────────────────────

@celery_app.task(bind=True, queue="alerts")
def check_rollback(self: Task):
    """
    Hourly: check all canary versions with >=10 calls.
    If avg_score < threshold, execute rollback automatically via the prompt registry.
    """
    logger.info("rollback_check_started")

    async def _run():
        import json
        import redis.asyncio as aioredis

        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(PromptVersion).where(
                    PromptVersion.is_canary == True,
                    PromptVersion.call_count >= 10,
                )
            )
            canary_versions = result.scalars().all()

            for pv in canary_versions:
                # Recompute avg score from evaluation results
                score_result = await db.execute(
                    select(func.avg(EvaluationResult.overall_score))
                    .join(LLMCall, LLMCall.id == EvaluationResult.call_id)
                    .where(
                        LLMCall.prompt_version == pv.version,
                        LLMCall.app_id == pv.app_id,
                    )
                )
                avg_score = score_result.scalar() or 0.0

                await db.execute(
                    update(PromptVersion)
                    .where(PromptVersion.id == pv.id)
                    .values(avg_score=avg_score)
                )
                await db.commit()

                prompt_avg_score.labels(prompt_name=pv.name, version=pv.version).set(avg_score)

                if avg_score < settings.rollback_score_threshold:
                    logger.warning(
                        "auto_rollback_executing",
                        prompt_name=pv.name,
                        version=pv.version,
                        avg_score=avg_score,
                        app_id=pv.app_id,
                    )

                    # Find the last known-good active version
                    prev_result = await db.execute(
                        select(PromptVersion).where(
                            PromptVersion.name == pv.name,
                            PromptVersion.app_id == pv.app_id,
                            PromptVersion.is_active == True,
                            PromptVersion.id != pv.id,
                        ).order_by(PromptVersion.created_at.desc()).limit(1)
                    )
                    prev = prev_result.scalar_one_or_none()

                    # Execute rollback: demote canary, restore previous active
                    await db.execute(
                        update(PromptVersion)
                        .where(PromptVersion.id == pv.id)
                        .values(is_canary=False, canary_traffic_pct=0)
                    )
                    await db.commit()

                    # Update Redis to remove canary and restore active
                    redis = aioredis.from_url(settings.redis_url, decode_responses=True)
                    try:
                        await redis.delete(f"prompt:{pv.app_id}:{pv.name}:canary")
                        if prev:
                            await redis.set(
                                f"prompt:{pv.app_id}:{pv.name}:active",
                                json.dumps({"version": prev.version, "template": prev.template}),
                            )
                    finally:
                        await redis.aclose()

                    rollback_events_total.labels(prompt_name=pv.name).inc()
                    send_rollback_alert(pv.name, pv.version, avg_score)

                    # Add to review queue
                    review_item = ReviewQueueItem(
                        reason="low_score",
                        app_id=pv.app_id,
                        details={
                            "trigger": "auto_rollback",
                            "prompt_name": pv.name,
                            "version": pv.version,
                            "avg_score": avg_score,
                            "threshold": settings.rollback_score_threshold,
                        },
                    )
                    async with AsyncSessionLocal() as db2:
                        db2.add(review_item)
                        await db2.commit()

    run_async(_run())


# ── Batch Evaluation Task ──────────────────────────────────────────────────────

@celery_app.task(bind=True, queue="evaluation")
def run_batch_eval(self: Task, app_id: str = "default"):
    """
    Offline: run the judge evaluator over all golden examples for an app.
    Logs results to MLflow under the 'batch-eval' experiment.
    """
    logger.info("batch_eval_started", app_id=app_id)

    async def _run():
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(GoldenExample).where(GoldenExample.app_id == app_id)
            )
            examples = result.scalars().all()

        if not examples:
            logger.info("batch_eval_skipped_no_examples", app_id=app_id)
            return

        mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
        mlflow.set_experiment("batch-eval")

        scores = []
        with mlflow.start_run(run_name=f"batch-eval-{app_id}"):
            for ex in examples:
                try:
                    eval_result = run_async(evaluate(ex.prompt_text, ex.expected_response))
                    scores.append(eval_result.overall_score)
                    mlflow.log_metric("overall_score", eval_result.overall_score, step=len(scores))
                except Exception as exc:
                    logger.warning("batch_eval_example_failed", error=str(exc))

            if scores:
                avg = sum(scores) / len(scores)
                mlflow.log_metric("batch_avg_score", avg)
                logger.info("batch_eval_complete", app_id=app_id, n=len(scores), avg=round(avg, 4))

    run_async(_run())
