"""
Semantic Embedder

Uses sentence-transformers (all-MiniLM-L6-v2) to produce 384-dim embeddings.
Model is cached on first load — Dockerfile pre-downloads it to avoid cold starts.
"""
from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

import numpy as np

if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer


@lru_cache(maxsize=1)
def _get_model() -> SentenceTransformer:
    from sentence_transformers import SentenceTransformer  # lazy: keeps torch out of import time
    return SentenceTransformer("all-MiniLM-L6-v2")


def embed(texts: list[str]) -> np.ndarray:
    """Return (N, 384) float32 embedding matrix for a list of texts."""
    model = _get_model()
    return model.encode(
        texts, batch_size=64, show_progress_bar=False, normalize_embeddings=True
    )