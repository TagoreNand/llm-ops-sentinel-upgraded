"""
Semantic Drift Detector

Pipeline:
  1. Fetch recent LLM responses from Postgres
  2. Embed with sentence-transformers
  3. Reduce to 2D with UMAP
  4. Cluster with HDBSCAN
  5. Compare cluster distribution against Postgres-persisted baseline
  6. Persist DriftEvent and alert if Jensen-Shannon divergence exceeds threshold

Baseline is now stored in the drift_baselines table instead of /tmp,
so it survives restarts, scales across workers, and is auditable.
"""
import json
from dataclasses import dataclass, field

import numpy as np
import structlog
from sqlalchemy import select

from app.config import get_settings
from app.database import AsyncSessionLocal, DriftBaseline, DriftEvent, LLMCall
from drift.embedder import embed
from monitoring.alerts import send_drift_alert
from monitoring.metrics import drift_score_current, drift_alerts_total

logger = structlog.get_logger()
settings = get_settings()


@dataclass
class DriftResult:
    score: float
    is_drift: bool
    details: dict = field(default_factory=dict)


def _jensen_shannon_divergence(p: np.ndarray, q: np.ndarray) -> float:
    p = p / p.sum()
    q = q / q.sum()
    m = 0.5 * (p + q)
    kl_pm = np.sum(np.where(p > 0, p * np.log(p / np.clip(m, 1e-10, None)), 0))
    kl_qm = np.sum(np.where(q > 0, q * np.log(q / np.clip(m, 1e-10, None)), 0))
    return float(0.5 * (kl_pm + kl_qm))


def _cluster_distribution(embeddings_2d: np.ndarray, labels: np.ndarray, n_bins: int = 20) -> np.ndarray:
    hist, _, _ = np.histogram2d(
        embeddings_2d[:, 0], embeddings_2d[:, 1], bins=n_bins,
        range=[[-15, 15], [-15, 15]]
    )
    hist = hist.flatten() + 1e-6
    return hist


async def detect_drift_from_db(app_id: str = "default"):
    """
    Full async drift detection pipeline. Fetches corpus from DB,
    loads/saves baseline from DB, persists DriftEvent.
    """
    try:
        import umap
        import hdbscan
    except ImportError:
        logger.error("drift_deps_missing", msg="Install umap-learn and hdbscan")
        return

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(LLMCall.response_text)
            .where(LLMCall.app_id == app_id)
            .order_by(LLMCall.created_at.desc())
            .limit(500)
        )
        texts = [row[0] for row in result.fetchall()]

    if len(texts) < 20:
        logger.info("drift_skipped_insufficient_data", count=len(texts), app_id=app_id)
        return

    logger.info("drift_embedding_start", n_texts=len(texts), app_id=app_id)
    embeddings = embed(texts)

    reducer = umap.UMAP(n_components=2, random_state=42, n_neighbors=15, min_dist=0.1)
    embeddings_2d = reducer.fit_transform(embeddings)

    clusterer = hdbscan.HDBSCAN(min_cluster_size=5, prediction_data=True)
    labels = clusterer.fit_predict(embeddings_2d)
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    current_dist = _cluster_distribution(embeddings_2d, labels)

    async with AsyncSessionLocal() as db:
        # Load baseline from Postgres
        baseline_result = await db.execute(
            select(DriftBaseline)
            .where(DriftBaseline.app_id == app_id)
            .order_by(DriftBaseline.created_at.desc())
            .limit(1)
        )
        baseline_row = baseline_result.scalar_one_or_none()

        if not baseline_row:
            # First run — create baseline
            baseline = DriftBaseline(
                app_id=app_id,
                n_clusters=n_clusters,
                distribution=current_dist.tolist(),
            )
            db.add(baseline)
            await db.commit()
            logger.info("drift_baseline_created", n_clusters=n_clusters, app_id=app_id)
            return

        baseline_dist = np.array(baseline_row.distribution)
        n_clusters_baseline = baseline_row.n_clusters

    jsd = _jensen_shannon_divergence(baseline_dist, current_dist)
    is_drift = jsd > settings.drift_threshold

    drift_score_current.set(jsd)
    if is_drift:
        drift_alerts_total.inc()

    drift_result = DriftResult(
        score=round(jsd, 4),
        is_drift=is_drift,
        details={
            "jsd": jsd,
            "n_clusters_current": n_clusters,
            "n_clusters_baseline": n_clusters_baseline,
            "n_samples": len(texts),
            "app_id": app_id,
        },
    )

    async with AsyncSessionLocal() as db:
        event = DriftEvent(
            drift_score=drift_result.score,
            threshold=settings.drift_threshold,
            num_samples=len(texts),
            alerted=is_drift,
            details=drift_result.details,
        )
        db.add(event)
        await db.commit()

    if is_drift:
        logger.warning("drift_detected", score=drift_result.score, app_id=app_id)
        send_drift_alert(drift_result)

    logger.info("drift_check_complete", jsd=round(jsd, 4), is_drift=is_drift, app_id=app_id)


# Kept for backward compat (called from old tasks)
def detect_drift(texts: list[str]) -> DriftResult:
    import asyncio
    import umap
    import hdbscan

    embeddings = embed(texts)
    reducer = umap.UMAP(n_components=2, random_state=42, n_neighbors=15, min_dist=0.1)
    embeddings_2d = reducer.fit_transform(embeddings)
    clusterer = hdbscan.HDBSCAN(min_cluster_size=5, prediction_data=True)
    labels = clusterer.fit_predict(embeddings_2d)
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    current_dist = _cluster_distribution(embeddings_2d, labels)
    return DriftResult(score=0.0, is_drift=False, details={"status": "legacy_path", "n_clusters": n_clusters})
