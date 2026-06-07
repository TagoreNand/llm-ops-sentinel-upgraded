import structlog
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from app.config import get_settings
from app.database import init_db
from app.api.proxy import router as proxy_router
from app.api.prompts import router as prompts_router
from app.api.review import router as review_router
from app.api.golden import router as golden_router
from app.auth.keys import router as auth_router
from monitoring.metrics import setup_metrics

logger = structlog.get_logger()
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting LLM Ops Sentinel", env=settings.app_env)
    await init_db()
    setup_metrics()
    yield
    logger.info("Shutting down")


app = FastAPI(
    title="LLM Ops Sentinel",
    description=(
        "Production LLM observability, evaluation, drift detection, "
        "prompt versioning, multi-tenant auth, and human review queue."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# OpenTelemetry tracing (no-op if deps not installed)
from app.tracing import setup_tracing
setup_tracing(app)

# Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)

# Routers
app.include_router(proxy_router,   prefix="/v1",     tags=["proxy"])
app.include_router(prompts_router, prefix="/prompts", tags=["prompts"])
app.include_router(review_router,  prefix="/review",  tags=["review"])
app.include_router(golden_router,  prefix="/golden",  tags=["golden-set"])
app.include_router(auth_router,    prefix="/auth",    tags=["auth"])


@app.get("/health")
async def health():
    return {"status": "ok", "env": settings.app_env, "version": "2.0.0"}
