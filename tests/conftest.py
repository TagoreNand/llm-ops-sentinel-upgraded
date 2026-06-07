import hashlib

import pytest
import pytest_asyncio
import numpy as np
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from unittest.mock import AsyncMock, patch

from app.main import app
from app.database import Base, get_db

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


@pytest.fixture(autouse=True)
def _fake_embeddings(monkeypatch):
    """Keep sentence-transformers/torch out of tests; deterministic stand-in for embed()."""
    def fake_embed(texts: list[str]) -> np.ndarray:
        if not texts:
            return np.empty((0, 384), dtype="float32")
        out = []
        for t in texts:
            seed = int.from_bytes(hashlib.sha256(t.encode()).digest()[:4], "little")
            v = np.random.default_rng(seed).standard_normal(384).astype("float32")
            v /= np.linalg.norm(v) + 1e-12  # match normalize_embeddings=True
            out.append(v)
        return np.vstack(out).astype("float32")

    monkeypatch.setattr("drift.embedder.embed", fake_embed)
    monkeypatch.setattr("drift.detector.embed", fake_embed, raising=False)


@pytest.fixture(scope="session")
def event_loop_policy():
    import asyncio
    return asyncio.DefaultEventLoopPolicy()


@pytest_asyncio.fixture
async def db_session():
    engine = create_async_engine(TEST_DB_URL)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture
async def client(db_session):
    async def override_db():
        yield db_session
    app.dependency_overrides[get_db] = override_db
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()