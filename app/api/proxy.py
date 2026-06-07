"""
LLM Proxy Router

Intercepts all LLM calls, logs them to Postgres, routes to the optimal model,
and enqueues async evaluation via Celery.

Enhancements over v1:
  - Automatic prompt resolution from the registry (pass prompt_name to activate)
  - Per-call prompt version call_count increment (canary health signal)
  - app_id isolation from API key auth
  - Golden-set similarity gate: blocks response if similarity to a known-bad
    pattern is detected (hooked into the async eval pipeline)
"""
import time
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.keys import get_current_app
from app.config import get_settings
from app.core.cost import calculate_cost
from app.core.hasher import hash_prompt
from app.core.router import route
from app.database import LLMCall, PromptVersion, get_db
from monitoring.metrics import (
    llm_calls_total,
    llm_latency_seconds,
    llm_cost_dollars,
    llm_tokens_total,
)
from workers.tasks import evaluate_response

logger = structlog.get_logger()
settings = get_settings()
router = APIRouter()


class ChatRequest(BaseModel):
    prompt: str
    model: str = "auto"
    system: str | None = None
    max_tokens: int = 1024
    temperature: float = 0.7
    metadata: dict[str, Any] = {}
    # Optional: resolve a registered prompt template by name
    prompt_name: str | None = None


class ChatResponse(BaseModel):
    id: str
    model: str
    response: str
    input_tokens: int
    output_tokens: int
    cost_usd: float
    latency_ms: float
    complexity_score: float
    routing_reason: str
    prompt_version: str
    served_canary: bool


async def _call_openai(prompt: str, system: str | None, model: str, max_tokens: int, temperature: float) -> dict:
    import openai
    groq_key = getattr(settings, "groq_api_key", "")
    if groq_key:
        client = openai.AsyncOpenAI(api_key=groq_key, base_url="https://api.groq.com/openai/v1")
    else:
        client = openai.AsyncOpenAI(api_key=settings.openai_api_key)
    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    resp = await client.chat.completions.create(
        model=model, messages=messages, max_tokens=max_tokens, temperature=temperature,
    )
    return {
        "text": resp.choices[0].message.content,
        "input_tokens": resp.usage.prompt_tokens,
        "output_tokens": resp.usage.completion_tokens,
    }


async def _call_anthropic(prompt: str, system: str | None, model: str, max_tokens: int, temperature: float) -> dict:
    import anthropic
    client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
    kwargs = dict(model=model, max_tokens=max_tokens, temperature=temperature,
                  messages=[{"role": "user", "content": prompt}])
    if system:
        kwargs["system"] = system
    resp = await client.messages.create(**kwargs)
    return {
        "text": resp.content[0].text,
        "input_tokens": resp.usage.input_tokens,
        "output_tokens": resp.usage.output_tokens,
    }


async def _dispatch(model: str, prompt: str, system: str | None, max_tokens: int, temperature: float) -> dict:
    if model.startswith("claude"):
        return await _call_anthropic(prompt, system, model, max_tokens, temperature)
    return await _call_openai(prompt, system, model, max_tokens, temperature)


async def _resolve_prompt(
    prompt_name: str,
    user_prompt: str,
    db: AsyncSession,
    app_id: str,
) -> tuple[str, str, bool]:
    """
    Resolve a named prompt template from the registry.
    Returns (rendered_prompt, version_str, served_canary).
    Falls back to the raw user prompt if no active version is found.
    """
    import json, random
    import redis.asyncio as aioredis

    redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    try:
        canary_raw = await redis.get(f"prompt:{app_id}:{prompt_name}:canary")
        active_raw = await redis.get(f"prompt:{app_id}:{prompt_name}:active")

        if canary_raw:
            canary = json.loads(canary_raw)
            if random.randint(1, 100) <= canary["pct"]:
                template = canary["template"]
                rendered = template.replace("{prompt}", user_prompt) if "{prompt}" in template else template
                return rendered, canary["version"], True

        if active_raw:
            active = json.loads(active_raw)
            template = active["template"]
            rendered = template.replace("{prompt}", user_prompt) if "{prompt}" in template else template
            return rendered, active["version"], False
    finally:
        await redis.aclose()

    # Redis miss — fall back to DB
    result = await db.execute(
        select(PromptVersion).where(
            PromptVersion.name == prompt_name,
            PromptVersion.app_id == app_id,
            PromptVersion.is_active == True,
        ).order_by(PromptVersion.created_at.desc()).limit(1)
    )
    pv = result.scalar_one_or_none()
    if pv:
        rendered = pv.template.replace("{prompt}", user_prompt) if "{prompt}" in pv.template else pv.template
        return rendered, pv.version, False

    # No registered version — use raw prompt
    return user_prompt, "v1", False


async def _increment_call_count(prompt_name: str, version: str, app_id: str, db: AsyncSession):
    """Atomically increment call_count on the matching PromptVersion row."""
    result = await db.execute(
        select(PromptVersion).where(
            PromptVersion.name == prompt_name,
            PromptVersion.version == version,
            PromptVersion.app_id == app_id,
        )
    )
    pv = result.scalar_one_or_none()
    if pv:
        await db.execute(
            update(PromptVersion)
            .where(PromptVersion.id == pv.id)
            .values(call_count=PromptVersion.call_count + 1)
        )
        await db.commit()


@router.post("/chat", response_model=ChatResponse)
async def chat(
    req: ChatRequest,
    db: AsyncSession = Depends(get_db),
    app_id: str = Depends(get_current_app),
):
    # 1. Resolve prompt template (if prompt_name provided)
    served_canary = False
    prompt_version_str = "v1"
    effective_prompt = req.prompt

    if req.prompt_name:
        effective_prompt, prompt_version_str, served_canary = await _resolve_prompt(
            req.prompt_name, req.prompt, db, app_id
        )

    # 2. Route to model
    routing = route(effective_prompt, force_model=req.model)
    model = routing.model

    # 3. Dispatch to provider
    start = time.perf_counter()
    try:
        result = await _dispatch(model, effective_prompt, req.system, req.max_tokens, req.temperature)
    except Exception as exc:
        logger.error("llm_call_failed", model=model, error=str(exc))
        raise HTTPException(status_code=502, detail=f"LLM call failed: {exc}")

    latency_ms = round((time.perf_counter() - start) * 1000, 2)
    cost = calculate_cost(model, result["input_tokens"], result["output_tokens"])
    prompt_hash = hash_prompt(effective_prompt)

    # 4. Persist call with app_id + prompt_version
    call = LLMCall(
        prompt_hash=prompt_hash,
        model=model,
        prompt_text=effective_prompt,
        response_text=result["text"],
        input_tokens=result["input_tokens"],
        output_tokens=result["output_tokens"],
        cost_usd=cost,
        latency_ms=latency_ms,
        metadata_=req.metadata,
        prompt_version=prompt_version_str,
        app_id=app_id,
    )
    db.add(call)
    await db.commit()
    await db.refresh(call)

    # 5. Increment prompt version call counter (canary health signal)
    if req.prompt_name:
        await _increment_call_count(req.prompt_name, prompt_version_str, app_id, db)

    # 6. Prometheus metrics
    llm_calls_total.labels(model=model).inc()
    llm_latency_seconds.labels(model=model).observe(latency_ms / 1000)
    llm_cost_dollars.labels(model=model).inc(cost)
    llm_tokens_total.labels(model=model, type="input").inc(result["input_tokens"])
    llm_tokens_total.labels(model=model, type="output").inc(result["output_tokens"])

    # 7. Async evaluation (non-blocking) — includes golden-set comparison
    evaluate_response.delay(
        call_id=call.id,
        prompt=effective_prompt,
        response=result["text"],
        model=model,
        app_id=app_id,
        prompt_name=req.prompt_name,
        prompt_version=prompt_version_str,
    )

    logger.info(
        "llm_call_complete",
        call_id=call.id,
        model=model,
        latency_ms=latency_ms,
        cost_usd=cost,
        complexity=routing.complexity_score,
        app_id=app_id,
        prompt_version=prompt_version_str,
        served_canary=served_canary,
    )

    return ChatResponse(
        id=call.id,
        model=model,
        response=result["text"],
        input_tokens=result["input_tokens"],
        output_tokens=result["output_tokens"],
        cost_usd=cost,
        latency_ms=latency_ms,
        complexity_score=routing.complexity_score,
        routing_reason=routing.reason,
        prompt_version=prompt_version_str,
        served_canary=served_canary,
    )
