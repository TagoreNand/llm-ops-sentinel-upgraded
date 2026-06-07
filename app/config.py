from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # LLM API Keys
    openai_api_key: str = ""
    anthropic_api_key: str = ""
    groq_api_key: str = ""

    # Database
    database_url: str = "postgresql+asyncpg://sentinel:sentinel@localhost:5432/sentinel_db"

    # Redis / Celery
    redis_url: str = "redis://localhost:6379/0"
    celery_broker_url: str = "redis://localhost:6379/1"
    celery_result_backend: str = "redis://localhost:6379/2"

    # MLflow
    mlflow_tracking_uri: str = "http://localhost:5001"

    # Alerting
    slack_webhook_url: str = ""
    pagerduty_api_key: str = ""

    # Thresholds
    drift_threshold: float = 0.15
    rollback_score_threshold: float = 0.65
    canary_traffic_percent: int = 10

    # App
    app_env: str = "development"
    log_level: str = "INFO"

    # Model routing cost map (USD per 1k tokens) — Groq pricing
    model_costs: dict = {
        "llama-3.1-8b-instant": {"input": 0.00005, "output": 0.00008},
        "llama-3.3-70b-versatile": {"input": 0.00059, "output": 0.00079},
    }

    # Complexity routing thresholds
    simple_query_max_tokens: int = 50
    medium_query_max_tokens: int = 200


@lru_cache
def get_settings() -> Settings:
    return Settings()