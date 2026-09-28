from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="APP_", extra="ignore")

    environment: str = "local"
    service_name: str = "gateway"
    database_url: str = "postgresql+asyncpg://payment:payment@postgres:5432/payment_platform"
    redis_url: str = "redis://redis:6379/0"
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic: str = "payment-events"
    kafka_dlq_topic: str = "payment-events-dlq"
    jwt_secret: str = "change-me"
    jwt_algorithm: str = "HS256"
    jwt_issuer: str = "distributed-payment-platform"
    jwt_audience: str = "payment-platform-api"
    rate_limit_per_minute: int = 60
    fraud_reject_above: int = 10000
    max_event_retries: int = 3
    cache_ttl_seconds: int = 30
    publisher_poll_seconds: float = 0.5


@lru_cache
def get_settings() -> Settings:
    return Settings()
