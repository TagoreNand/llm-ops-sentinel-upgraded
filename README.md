# LLM Ops Sentinel v2

> Production-grade LLM observability, evaluation, drift detection, prompt versioning, multi-tenant auth, and human review — without Docker, using Groq (free), Neon Postgres, and Upstash Redis.

---

## Table of contents

- [Architecture](#architecture)
- [Stack](#stack)
- [v1 to v2 upgrades](#v1-to-v2-upgrades)
- [Environment setup](#environment-setup-env)
- [One-time database migration](#one-time-database-migration)
- [Phase 0 — First-time setup](#phase-0--first-time-setup)
- [Phase 1 — Multi-tenant auth](#phase-1--multi-tenant-auth)
- [Phase 2 — Golden set](#phase-2--golden-set)
- [Phase 3 — Prompt version registry](#phase-3--prompt-version-registry)
- [Phase 4 — Inference through the gateway](#phase-4--inference-through-the-gateway)
- [Phase 5 — Human review queue](#phase-5--human-review-queue)
- [Phase 6 — Prompt lifecycle](#phase-6--prompt-lifecycle)
- [Phase 7 — Batch offline evaluation](#phase-7--batch-offline-evaluation)
- [Phase 8 — Drift detection](#phase-8--drift-detection)
- [Phase 9 — Observability](#phase-9--observability)
- [Phase 10 — Auth key management](#phase-10--auth-key-management)
- [Known issues and workarounds](#known-issues-and-workarounds)
- [Scheduled tasks](#scheduled-tasks-celery-beat)
- [Project structure](#project-structure)

---

## Architecture

### 1. System overview

```mermaid
flowchart TD
    subgraph Clients["Client layer"]
        PS["PowerShell / curl\niwr commands"]
        SWAGGER["Swagger UI\nlocalhost:8000/docs"]
    end

    subgraph Gateway["FastAPI gateway — localhost:8000"]
        direction TB
        PROXY["/v1/chat\nProxy + model router\n+ prompt resolver\n+ call counter"]
        AUTH["/auth/keys\nSHA-256 key hashing\napp_id isolation"]
        PROMPTS["/prompts/versions\nRegistry + promote\n+ rollback + canary"]
        REVIEW["/review/queue\nApprove / reject + stats"]
        GOLDEN["/golden/examples\nCRUD + batch-eval"]
        METRICS["/metrics\nPrometheus exposition"]
        HEALTH["/health  /docs Swagger"]
    end

    subgraph Celery["Async workers — Celery 5.4 solo pool"]
        EVAL["evaluate_response\nHeuristic judge\nfaithfulness · relevance · toxicity\ngolden cosine sim"]
        BATCH["run_batch_eval\nOffline benchmark\nover all golden examples"]
        DRIFT["run_drift_detection\nUMAP + HDBSCAN\nJSD vs Postgres baseline"]
        ROLL["check_rollback\nAuto-demote canary\nif avg_score < 0.65"]
    end

    subgraph Beat["Celery Beat scheduler"]
        S1["check_rollback — every hour"]
        S2["run_drift_detection — nightly 02:00 UTC"]
        S3["run_batch_eval — Monday 03:00 UTC"]
    end

    subgraph Infra["Persistent infrastructure"]
        NEON[("Neon Postgres\nNullPool · sslmode=require\n7 tables · app_id isolation")]
        REDIS[("Upstash Redis\nrediss:// · db 0 only\nCelery broker + result backend\nPrompt template cache")]
        MLFLOW[("MLflow\nfile:///C:/mlruns\nNo server needed")]
        HF[("HuggingFace cache\nall-MiniLM-L6-v2\nLocal CPU inference")]
    end

    subgraph LLM["LLM providers"]
        GROQ["Groq API — free tier\nllama-3.1-8b-instant\nllama-3.3-70b-versatile"]
    end

    PS --> Gateway
    SWAGGER --> Gateway
    PROXY --> AUTH
    PROXY --> PROMPTS
    PROXY --> GROQ
    PROXY --> NEON
    PROXY --> REDIS
    PROXY -->|"evaluate_response.delay()"| Celery
    Celery --> NEON
    Celery --> REDIS
    Celery --> MLFLOW
    Celery --> HF
    Beat -->|"hourly"| ROLL
    Beat -->|"nightly"| DRIFT
    Beat -->|"weekly"| BATCH
```

### 2. Request lifecycle — sequence diagram

```mermaid
sequenceDiagram
    autonumber
    participant C as Client (PowerShell)
    participant P as /v1/chat proxy
    participant A as Auth middleware
    participant Redis as Upstash Redis
    participant Router as Model router
    participant Groq as Groq API
    participant DB as Neon Postgres
    participant W as Celery worker

    C->>P: POST /v1/chat {prompt, model, prompt_name}
    P->>A: Extract X-API-Key header
    A->>DB: SELECT key_hash, app_id, is_active FROM api_keys
    DB-->>A: app_id = "my-app"

    alt prompt_name provided
        P->>Redis: GET prompt:my-app:qa-bot:canary
        Redis-->>P: {version:"v2", template, pct:20}
        P->>P: random(1,100) <= 20 — serve canary template
        P->>P: template.replace("{prompt}", user_prompt)
    else no prompt_name
        P->>P: use raw prompt as-is
    end

    P->>Router: route(effective_prompt, force_model)
    Router->>Router: complexity_score = 0.045
    Router-->>P: model="llama-3.1-8b-instant", reason="forced by caller"

    P->>Groq: POST /openai/v1/chat/completions
    Groq-->>P: {text, prompt_tokens:42, completion_tokens:8}

    P->>DB: INSERT llm_calls
    P->>DB: UPDATE prompt_versions SET call_count += 1
    P->>Redis: evaluate_response.delay(call_id, prompt, response, model, app_id)
    P-->>C: 200 {id, model, response, cost_usd:5.8e-05, latency_ms:263.24, complexity_score:0.045, prompt_version:"v1", served_canary:false}

    Note over W: Async — runs seconds after response returned to client
    Redis-->>W: Task dequeued from evaluation queue
    W->>W: Heuristic judge: faithfulness=0.7, relevance=0.28, toxicity=0.05, overall=0.5811
    W->>DB: SELECT golden_examples WHERE app_id="my-app" LIMIT 50
    W->>W: embed(response) cosine_sim vs golden = 0.9674
    W->>DB: INSERT evaluation_results (overall_score=0.5811, golden_match=True, golden_similarity=0.9674)
    W->>MLFLOW: mlflow.log_metrics({faithfulness, relevance, toxicity, overall_score})

    alt overall_score < 0.65 OR golden_similarity < 0.30
        W->>DB: INSERT review_queue (reason="low_score", status="pending")
    end

    W-->>Redis: Task result stored (succeeded in 11.64s)
```

### 3. Database schema — ERD

```mermaid
erDiagram
    ApiKey {
        uuid id PK
        string key_hash "SHA-256 of raw plaintext key"
        string app_id "tenant identifier"
        string description
        bool is_active "soft-delete on revoke"
        datetime created_at
        datetime last_used_at
    }
    LLMCall {
        uuid id PK
        string prompt_hash "SHA-256 for dedup"
        string model
        text prompt_text
        text response_text
        int input_tokens
        int output_tokens
        float cost_usd
        float latency_ms
        string prompt_version
        string app_id
        datetime created_at
    }
    PromptVersion {
        uuid id PK
        string name "e.g. qa-bot"
        string version "e.g. v1 or v2"
        text template "contains {prompt} placeholder"
        bool is_active "exactly one active per name per app"
        bool is_canary
        int canary_traffic_pct "0-100"
        float avg_score "rolling eval score"
        int call_count "incremented per inference"
        string app_id
        datetime created_at
    }
    EvaluationResult {
        uuid id PK
        uuid call_id FK
        float faithfulness "0.0-1.0"
        float relevance "0.0-1.0"
        float toxicity "0.0-1.0"
        float overall_score "weighted composite"
        string judge_model "heuristic or gpt-4"
        string mlflow_run_id
        bool golden_match
        float golden_similarity "0.0-1.0"
        datetime created_at
    }
    ReviewQueueItem {
        uuid id PK
        string reason "low_score or golden_mismatch or toxic"
        uuid call_id FK
        string status "pending or approved or rejected"
        string reviewer_note
        string app_id
        json details
        datetime created_at
        datetime reviewed_at
    }
    GoldenExample {
        uuid id PK
        text prompt_text
        text expected_response
        string app_id
        json tags
        datetime created_at
    }
    DriftBaseline {
        uuid id PK
        string app_id
        int n_clusters "from HDBSCAN"
        json distribution "cluster centroids + probabilities"
        datetime created_at
    }
    DriftEvent {
        uuid id PK
        float drift_score "Jensen-Shannon divergence vs baseline"
        float threshold
        int num_samples
        bool alerted
        json details
        datetime created_at
    }

    ApiKey }o--o{ LLMCall : "app_id scopes all queries"
    PromptVersion ||--o{ LLMCall : "served as prompt_version"
    LLMCall ||--o| EvaluationResult : "evaluated by worker"
    LLMCall ||--o| ReviewQueueItem : "queued for human review"
    GoldenExample }o--o{ EvaluationResult : "compared against"
    DriftBaseline }o--o{ DriftEvent : "baseline for drift computation"
    DriftEvent ||--o{ ReviewQueueItem : "can trigger review item"
```

---

## Stack

| Layer | Technology | Notes |
|---|---|---|
| API framework | FastAPI + uvicorn | `--reload` for hot-reload in dev |
| LLM inference | Groq API (free) | `llama-3.1-8b-instant`, `llama-3.3-70b-versatile` via OpenAI-compatible endpoint |
| Database | Neon Postgres | `NullPool` — fresh connection per request, avoids serverless idle drops |
| Cache / broker | Upstash Redis | `rediss://` TLS, db 0 only on free tier |
| Task queue | Celery 5.4 | `--pool=solo` required on Windows |
| Embeddings | `sentence-transformers/all-MiniLM-L6-v2` | Local CPU inference |
| Experiment tracking | MLflow | `file:///C:/mlruns` — no server needed |
| Observability | Prometheus + OpenTelemetry | `/metrics` endpoint + console span exporter |
| Auth | SHA-256 hashed API keys | Stored in Postgres, soft-delete on revoke |

---

## v1 to v2 upgrades

| Feature | Status | Implementation |
|---|---|---|
| Automatic prompt resolution in /v1/chat | Done | proxy.py — pass prompt_name to resolve active/canary template from Redis |
| Closed-loop automatic rollback | Done | tasks.py check_rollback — demotes canary in Postgres + Redis automatically |
| Prompt call counting for canary health | Done | proxy.py increments call_count on PromptVersion after every inference |
| Golden-set comparison in eval gate | Done | tasks.py embeds response + cosine similarity vs golden examples |
| Multi-tenant auth + per-app isolation | Done | auth/keys.py — X-API-Key header resolves app_id, all data scoped per app |
| Human review queue | Done | api/review.py — surfaces low-score, toxic, golden-mismatch events |
| Drift baselines in Postgres | Done | DriftBaseline table replaces /tmp pickle file |
| Golden set management API | Done | api/golden.py — CRUD + offline batch eval trigger |
| OpenTelemetry tracing | Done | tracing.py — FastAPI + SQLAlchemy + httpx instrumented |
| Batch offline evaluation | Done | tasks.py run_batch_eval — judge over all golden examples, logged to MLflow |

---

## Environment setup (.env)

```env
OPENAI_API_KEY=sk-proj-...
ANTHROPIC_API_KEY=
GROQ_API_KEY=gsk_...

DATABASE_URL=postgresql+asyncpg://neondb_owner:...@ep-xxx.neon.tech/neondb?sslmode=require&channel_binding=require

REDIS_URL=rediss://default:TOKEN@host.upstash.io:6379/0?ssl_cert_reqs=CERT_NONE
CELERY_BROKER_URL=rediss://default:TOKEN@host.upstash.io:6379/0?ssl_cert_reqs=CERT_NONE
CELERY_RESULT_BACKEND=rediss://default:TOKEN@host.upstash.io:6379/0?ssl_cert_reqs=CERT_NONE

MLFLOW_TRACKING_URI=file:///C:/mlruns
SLACK_WEBHOOK_URL=
PAGERDUTY_API_KEY=

DRIFT_THRESHOLD=0.15
ROLLBACK_SCORE_THRESHOLD=0.65
CANARY_TRAFFIC_PERCENT=10
APP_ENV=development
LOG_LEVEL=INFO
```

---

## One-time database migration

```python
# python migrate.py
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool
from sqlalchemy import text

with open('.env') as f:
    for line in f:
        if line.startswith('DATABASE_URL'):
            db_url = line.split('=', 1)[1].strip().split('?')[0]
            break

engine = create_async_engine(db_url, poolclass=NullPool, connect_args={"ssl": "require"})
statements = [
    "ALTER TABLE prompt_versions ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'default'",
    "ALTER TABLE llm_calls ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'default'",
    "ALTER TABLE review_queue ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'default'",
    "ALTER TABLE golden_examples ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'default'",
    "ALTER TABLE drift_baselines ADD COLUMN IF NOT EXISTS app_id VARCHAR(64) DEFAULT 'default'",
    "ALTER TABLE evaluation_results ADD COLUMN IF NOT EXISTS golden_match BOOLEAN",
    "ALTER TABLE evaluation_results ADD COLUMN IF NOT EXISTS golden_similarity FLOAT",
]
async def migrate():
    async with engine.begin() as conn:
        for s in statements:
            await conn.execute(text(s))
            print("OK:", s[:60])
    print("Migration complete!")
asyncio.run(migrate())
```

Output:
```
OK: ALTER TABLE prompt_versions ADD COLUMN IF NOT EXISTS app_id
OK: ALTER TABLE llm_calls ADD COLUMN IF NOT EXISTS app_id VARCH
OK: ALTER TABLE review_queue ADD COLUMN IF NOT EXISTS app_id VAR
OK: ALTER TABLE golden_examples ADD COLUMN IF NOT EXISTS app_id
OK: ALTER TABLE drift_baselines ADD COLUMN IF NOT EXISTS app_id
OK: ALTER TABLE evaluation_results ADD COLUMN IF NOT EXISTS golde
OK: ALTER TABLE evaluation_results ADD COLUMN IF NOT EXISTS golde
Migration complete!
```

---

## Phase 0 — First-time setup

### Terminal 1 — FastAPI server

```powershell
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Output:
```
INFO:     Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)
INFO:     Started server process [22196]
2026-06-06 [info ] otel_console_exporter_active
2026-06-06 [info ] otel_tracing_configured   service=llm-ops-sentinel
INFO:     Application startup complete.
```

### Terminal 2 — Celery worker

```powershell
celery -A workers.celery_app worker --loglevel=info -Q evaluation,alerts --pool=solo
```

Output:
```
-------------- celery@TRJ015986 v5.4.0 (opalescent)
.> transport:  rediss://default:**@close-hamster-135942.upstash.io:6379/0
.> concurrency: 8 (solo)

[tasks]
  . workers.tasks.check_rollback
  . workers.tasks.evaluate_response
  . workers.tasks.run_batch_eval
  . workers.tasks.run_drift_detection

[INFO] celery@TRJ015986 ready.
```

### Terminal 3 — Celery Beat

```powershell
celery -A workers.celery_app beat --loglevel=info
```

Output:
```
celery beat v5.4.0 is starting.
.  broker -> rediss://default:**@close-hamster-135942.upstash.io:6379/0
beat: Starting...
[2026-06-06 14:00:00] Scheduler: Sending due task hourly-rollback-check
```

### Health check

```powershell
iwr http://localhost:8000/health | Select-Object -ExpandProperty Content
```

Output:
```json
{"status":"ok","env":"development","version":"2.0.0"}
```

---

## Phase 1 — Multi-tenant auth

```powershell
iwr -Uri "http://localhost:8000/auth/keys" `
    -Method POST -ContentType "application/json" `
    -Body '{"app_id": "my-app", "description": "dev key"}' |
    Select-Object -ExpandProperty Content
```

Output:
```json
{"id":"f92e5294-a023-420b-9c64-af1aa003d448","app_id":"my-app","key":"sk-sentinel-p2o9xTkxakasu-vFSla_LNad_Qnf1JF03TzKC77so6E","description":"dev key"}
```

```powershell
$KEY = "sk-sentinel-p2o9xTkxakasu-vFSla_LNad_Qnf1JF03TzKC77so6E"

iwr -Uri "http://localhost:8000/health" -Headers @{"X-API-Key" = $KEY} | Select-Object -ExpandProperty Content
```

Output:
```json
{"status":"ok","env":"development","version":"2.0.0"}
```

---

## Phase 2 — Golden set

```powershell
iwr -Uri "http://localhost:8000/golden/examples" `
    -Method POST -ContentType "application/json" -Headers @{"X-API-Key" = $KEY} `
    -Body '{"prompt_text":"What is the capital of France?","expected_response":"The capital of France is Paris.","tags":{"category":"geography"}}' |
    Select-Object -ExpandProperty Content
```

Output:
```json
{"id":"fa71490d-5848-4f73-9986-a0e8269846df","prompt_text":"What is the capital of France?","expected_response":"The capital of France is Paris.","app_id":"my-app","tags":{"category":"geography"}}
```

List all examples:
```powershell
iwr -Uri "http://localhost:8000/golden/examples" -Headers @{"X-API-Key" = $KEY} | Select-Object -ExpandProperty Content
```

Output:
```json
[
  {"id":"fa71490d-...","prompt_text":"What is the capital of France?","app_id":"my-app"},
  {"id":"7e3331da-...","prompt_text":"What is 2 + 2?","app_id":"my-app"},
  {"id":"f5f4124e-...","prompt_text":"Explain what an API is.","app_id":"my-app"}
]
```

---

## Phase 3 — Prompt version registry

```powershell
# Create v1
iwr -Uri "http://localhost:8000/prompts/versions" `
    -Method POST -ContentType "application/json" -Headers @{"X-API-Key" = $KEY} `
    -Body '{"name":"qa-bot","version":"v1","template":"You are a helpful assistant.\n\nUser question: {prompt}","deploy_as_canary":false}' |
    Select-Object -ExpandProperty Content
```

Output:
```json
{"id":"dd96667e-4b30-4c04-968e-4e4bce292e59","name":"qa-bot","version":"v1","is_active":true,"is_canary":false,"canary_traffic_pct":0,"call_count":0,"app_id":"my-app"}
```

```powershell
# Promote v1
iwr -Uri "http://localhost:8000/prompts/versions/qa-bot/v1/promote" -Method POST -Headers @{"X-API-Key" = $KEY} | Select-Object -ExpandProperty Content
```

Output:
```json
{"status":"promoted","name":"qa-bot","version":"v1","app_id":"my-app"}
```

```powershell
# Create v2 canary
iwr -Uri "http://localhost:8000/prompts/versions" `
    -Method POST -ContentType "application/json" -Headers @{"X-API-Key" = $KEY} `
    -Body '{"name":"qa-bot","version":"v2","template":"You are an expert assistant. Provide a thorough answer.\n\nQuestion: {prompt}","deploy_as_canary":true,"canary_traffic_pct":20}' |
    Select-Object -ExpandProperty Content
```

Output:
```json
{"id":"8da4182a-da0d-44b9-b8e8-c224cd431e10","name":"qa-bot","version":"v2","is_active":false,"is_canary":true,"canary_traffic_pct":20,"call_count":0,"app_id":"my-app"}
```

---

## Phase 4 — Inference through the gateway

```powershell
iwr -Uri "http://localhost:8000/v1/chat" `
    -Method POST -ContentType "application/json" -Headers @{"X-API-Key" = $KEY} `
    -Body '{"prompt":"What is the capital of France?","model":"llama-3.1-8b-instant"}' |
    Select-Object -ExpandProperty Content
```

Output:
```json
{"id":"36816f44-4271-426f-aab1-dd342cb29404","model":"llama-3.1-8b-instant","response":"The capital of France is Paris.","input_tokens":42,"output_tokens":8,"cost_usd":5.8e-05,"latency_ms":263.24,"complexity_score":0.045,"routing_reason":"forced by caller","prompt_version":"v1","served_canary":false}
```

```powershell
# Batch of 5
$prompts = @("What is Python used for?","Explain REST APIs.","What is a neural network?","How does HTTPS work?","What is cloud computing?")
foreach ($p in $prompts) {
    iwr -Uri "http://localhost:8000/v1/chat" -Method POST -ContentType "application/json" `
        -Headers @{"X-API-Key" = $KEY} `
        -Body "{`"prompt`": `"$p`", `"model`": `"llama-3.1-8b-instant`"}" | Select-Object -ExpandProperty Content
    Start-Sleep -Milliseconds 500
}
```

Sample outputs:
```json
{"id":"abcd9268-...","model":"llama-3.1-8b-instant","response":"**What are REST APIs?**\n\nREST APIs are...","input_tokens":40,"output_tokens":717,"cost_usd":0.001474,"latency_ms":1150.82,"complexity_score":0.042,"prompt_version":"v1","served_canary":false}
{"id":"262145fc-...","model":"llama-3.1-8b-instant","response":"**What is a neural network?**...","input_tokens":41,"output_tokens":528,"cost_usd":0.001097,"latency_ms":1256.26,"complexity_score":0.044,"prompt_version":"v1","served_canary":false}
{"id":"2b28ef37-...","model":"llama-3.1-8b-instant","response":"HTTPS (Hypertext Transfer Protocol Secure)...","input_tokens":40,"output_tokens":540,"cost_usd":0.00112,"latency_ms":1508.0,"complexity_score":0.043,"prompt_version":"v1","served_canary":false}
{"id":"2b28e8f4-...","model":"llama-3.1-8b-instant","response":"Cloud computing is a model for delivering...","input_tokens":40,"output_tokens":573,"cost_usd":0.001186,"latency_ms":1167.96,"complexity_score":0.043,"prompt_version":"v1","served_canary":false}
```

---

## Phase 5 — Human review queue

Celery worker evaluation pipeline log (runs ~10s after each inference):
```
[INFO] Task workers.tasks.evaluate_response[f43bf711-...] received
[info ] eval_task_started   app_id=my-app  call_id=f04e01c7-550e-441a-9c00-e1ab4cc14aff
[warning] judge_fallback     error='"faithfulness"'
[info ] evaluation_complete  faithfulness=0.7  overall=0.5811  relevance=0.2778  toxicity=0.05
INSERT INTO evaluation_results (golden_match=True, golden_similarity=0.9674)
INSERT INTO review_queue (reason=low_score, status=pending)
COMMIT
[info ] eval_task_complete   golden_pass=True  golden_sim=0.9674  overall=0.5811
[INFO] Task succeeded in 11.640s
```

```powershell
# View pending
iwr -Uri "http://localhost:8000/review/queue?status=pending" -Headers @{"X-API-Key" = $KEY} | Select-Object -ExpandProperty Content
```

Output:
```json
[{"id":"887a8b18-a33a-4771-9a5f-3dc5567790de","reason":"low_score","status":"pending","app_id":"my-app","details":{"overall_score":0.5811,"toxicity":0.05,"golden_similarity":0.9674,"prompt_version":"v1","reasoning":"fallback heuristic (judge API unavailable)"}}]
```

```powershell
# Stats
iwr -Uri "http://localhost:8000/review/stats" -Headers @{"X-API-Key" = $KEY} | Select-Object -ExpandProperty Content
```

Output:
```json
[{"status":"approved","reason":"low_score","count":1},{"status":"pending","reason":"golden_mismatch","count":2},{"status":"rejected","reason":"golden_mismatch","count":1}]
```

```powershell
# Approve
iwr -Uri "http://localhost:8000/review/queue/887a8b18-a33a-4771-9a5f-3dc5567790de" `
    -Method POST -ContentType "application/json" -Headers @{"X-API-Key" = $KEY} `
    -Body '{"status":"approved","reviewer_note":"Score acceptable, heuristic judge was conservative."}' |
    Select-Object -ExpandProperty Content
```

Output:
```json
{"id":"887a8b18-...","reviewed_at":"2026-06-06T22:19:44.479410","status":"approved","reviewer_note":"Score acceptable, heuristic judge was conservative.","app_id":"my-app"}
```

```powershell
# Reject
iwr -Uri "http://localhost:8000/review/queue/fa114aa6-ab78-4574-a567-8f262f3a7cb9" `
    -Method POST -ContentType "application/json" -Headers @{"X-API-Key" = $KEY} `
    -Body '{"status":"rejected","reviewer_note":"Response was off-topic. Golden set needs updating."}' |
    Select-Object -ExpandProperty Content
```

Output:
```json
{"id":"fa114aa6-...","reviewed_at":"2026-06-06T22:26:24.559709","status":"rejected","reviewer_note":"Response was off-topic. Golden set needs updating.","app_id":"my-app"}
```

---

## Phase 6 — Prompt lifecycle

```powershell
# Promote v2
iwr -Uri "http://localhost:8000/prompts/versions/qa-bot/v2/promote" -Method POST -Headers @{"X-API-Key" = $KEY} | Select-Object -ExpandProperty Content
```

Output:
```json
{"status":"promoted","name":"qa-bot","version":"v2","app_id":"my-app"}
```

```powershell
# Rollback to v1
iwr -Uri "http://localhost:8000/prompts/versions/qa-bot/v1/rollback" -Method POST -Headers @{"X-API-Key" = $KEY} | Select-Object -ExpandProperty Content
```

Output:
```json
{"status":"rolled_back","name":"qa-bot","active_version":"v1","app_id":"my-app"}
```

```powershell
# Automatic rollback check
celery -A workers.celery_app call workers.tasks.check_rollback
```

Output:
```
fe126c04-7e1f-4727-b64e-8ee07c81ec74
```

Celery worker log:
```
[info ] rollback_check_started
SELECT prompt_versions WHERE is_canary=true AND call_count >= 10  ->  0 rows
[INFO] Task check_rollback succeeded in 3.657s: None
```

No auto-rollback triggered — v2 had fewer than 10 calls via the prompt registry.

---

## Phase 7 — Batch offline evaluation

```powershell
# Via API
iwr -Uri "http://localhost:8000/golden/batch-eval" -Method POST -Headers @{"X-API-Key" = $KEY} | Select-Object -ExpandProperty Content
```

Output:
```json
{"status":"queued","app_id":"my-app"}
```

```powershell
# Via Celery CLI
celery -A workers.celery_app call workers.tasks.run_batch_eval --kwargs '{\"app_id\":\"my-app\"}'
```

Output:
```
110d9acf-9fec-4fa9-9cd0-4c85545b8568
```

Celery worker log:
```
[info ] batch_eval_started   app_id=my-app
SELECT golden_examples WHERE app_id='my-app'  ->  3 examples
mlflow: Creating experiment 'batch-eval'
[INFO] Task run_batch_eval succeeded in 4.0s: None
```

---

## Phase 8 — Drift detection

```powershell
celery -A workers.celery_app call workers.tasks.run_drift_detection
```

Output:
```
35f79542-7f42-4251-aaf4-2989e23151e0
```

Celery worker log (first run — baseline created):
```
[info ] drift_embedding_start   n_texts=9  app_id=my-app
INFO: Load pretrained SentenceTransformer: all-MiniLM-L6-v2
[info ] drift_baseline_created  n_clusters=2  app_id=my-app
INSERT INTO drift_baselines (app_id='my-app', n_clusters=2)
COMMIT
```

---

## Phase 9 — Observability

```powershell
iwr http://localhost:8000/metrics | Select-Object -ExpandProperty Content
```

Key metrics:
```prometheus
llm_calls_total{model="llama-3.1-8b-instant"} 4.0
llm_cost_dollars_total{model="llama-3.1-8b-instant"} 0.003554
llm_tokens_total{model="llama-3.1-8b-instant",type="input"} 162.0
llm_tokens_total{model="llama-3.1-8b-instant",type="output"} 1696.0
llm_latency_seconds_bucket{le="0.5",model="llama-3.1-8b-instant"} 1.0
llm_latency_seconds_bucket{le="1.0",model="llama-3.1-8b-instant"} 2.0
llm_latency_seconds_bucket{le="2.0",model="llama-3.1-8b-instant"} 4.0
llm_latency_seconds_sum{model="llama-3.1-8b-instant"} 3.93393
drift_score_current 0.0
active_canary_versions 0.0
```

Interactive API docs: http://localhost:8000/docs

OTel sample span:
```json
{"name":"GET /health","trace_id":"0xceecc85a2fd98ad91f4bbca70e9481fd","kind":"SpanKind.SERVER","status":{"status_code":"UNSET"},"attributes":{"http.method":"GET","http.target":"/health","http.status_code":200},"resource":{"attributes":{"service.name":"llm-ops-sentinel","telemetry.sdk.version":"1.24.0"}}}
```

---

## Phase 10 — Auth key management

```powershell
# Create key for second app
iwr -Uri "http://localhost:8000/auth/keys" `
    -Method POST -ContentType "application/json" `
    -Body '{"app_id":"my-other-app","description":"staging key"}' |
    Select-Object -ExpandProperty Content
```

Output:
```json
{"id":"3357de86-ac2e-443c-9a3b-9d86e6dfe552","app_id":"my-other-app","key":"sk-sentinel-uIvUEC-NTbR-wzIrNtLzUi3VxdGTz9TOyZ0zs9i0vyQ","description":"staging key"}
```

All data for this key is fully isolated from `my-app` at the database query level.

```powershell
# Revoke
iwr -Uri "http://localhost:8000/auth/keys/3357de86-ac2e-443c-9a3b-9d86e6dfe552" -Method DELETE
# Returns HTTP 204 — key soft-deleted (is_active = false)
```

---

## Known issues and workarounds

| Error | Root cause | Fix applied |
|---|---|---|
| `connection is closed` | asyncpg pool holds stale handles after Neon pauses idle connections | `NullPool` in `database.py` |
| `unexpected keyword argument 'ssl_context'` then `'ssl'` | `redis.asyncio` v5 does not accept ssl kwargs in `from_url()` | Strip `?ssl_cert_reqs=CERT_NONE` from URL; wrap Redis calls in `try/except` |
| `not enough values to unpack (expected 3, got 0)` | Celery prefork pool on Windows — child processes don't inherit task registry | Use `--pool=solo` |
| `MlflowException: API request to localhost:5000 failed` | MLflow defaults to HTTP tracking server | `MLFLOW_TRACKING_URI=file:///C:/mlruns` in `.env` |
| `Only 0th database is supported` | Upstash free tier only supports db 0 | Use `/0` for all three Redis URLs |
| `llama-3.1-8b-instant model not found` | `lru_cache` served stale `Settings` without `GROQ_API_KEY` | Hard-restart uvicorn after adding key to `.env` |

---

## Scheduled tasks (Celery Beat)

| Task | Schedule | What it does |
|---|---|---|
| `check_rollback` | Every hour | Queries canary versions with `call_count >= 10`; auto-rollbacks if `avg_score < 0.65` |
| `run_drift_detection` | Nightly 02:00 UTC | Embeds recent responses, computes JSD vs Postgres baseline, inserts `DriftEvent` if score > threshold |
| `run_batch_eval` | Monday 03:00 UTC | Runs heuristic judge over all golden examples, logs metrics to MLflow |

---

## Project structure

```
llm-ops-sentinel-upgraded/
+-- app/
|   +-- api/
|   |   +-- proxy.py       # /v1/chat gateway + prompt resolution + call counting
|   |   +-- prompts.py     # /prompts version registry + Redis cache + promote/rollback
|   |   +-- review.py      # /review human queue CRUD + stats
|   |   +-- golden.py      # /golden CRUD + batch eval trigger
|   +-- auth/
|   |   +-- keys.py        # API key creation, validation, app_id resolution middleware
|   +-- core/
|   |   +-- router.py      # Complexity-based model routing (Groq models)
|   |   +-- cost.py        # Cost calculation per model per token
|   |   +-- hasher.py      # Prompt SHA-256 hashing
|   +-- evaluators/
|   |   +-- judge.py       # Heuristic judge — faithfulness, relevance, toxicity
|   +-- config.py          # Pydantic settings — reads .env, includes groq_api_key
|   +-- database.py        # SQLAlchemy models + NullPool engine for Neon
|   +-- main.py            # FastAPI app, router registration, OTel setup
|   +-- tracing.py         # OpenTelemetry — FastAPI + SQLAlchemy + httpx
+-- drift/
|   +-- detector.py        # UMAP + HDBSCAN + JSD drift detection
|   +-- embedder.py        # sentence-transformers embedding wrapper
+-- monitoring/
|   +-- metrics.py         # Prometheus counters, histograms, gauges
|   +-- alerts.py          # Slack + PagerDuty alert dispatch
+-- workers/
|   +-- celery_app.py      # Celery config + beat schedule
|   +-- tasks.py           # evaluate_response, run_drift_detection, check_rollback, run_batch_eval
+-- migrate.py             # One-time schema migration script
+-- .env                   # Environment variables (not committed)
+-- .env.example           # Template for new contributors
+-- docker-compose.yml     # Optional local Postgres + Redis + MLflow
+-- requirements.txt
```
