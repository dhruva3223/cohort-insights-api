"""Application settings loaded from environment variables."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All configuration is env-driven with safe local defaults."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    MONGO_URI: str = "mongodb://localhost:27017"
    MONGO_DB: str = "cohort_insights"
    REDIS_URL: str = "redis://localhost:6379/0"

    MAX_ACTIVE_PER_USER: int = 3
    RATE_LIMIT_KEY_TTL_S: int = 3600
    CACHE_TTL_S: int = 86400

    PROCESSING_MIN_S: float = 10.0
    PROCESSING_MAX_S: float = 20.0
    ENRICHING_MIN_S: float = 5.0
    ENRICHING_MAX_S: float = 15.0
    PROCESSING_FAILURE_RATE: float = 0.1
    ENRICHING_FAILURE_RATE: float = 0.1

    LOG_LEVEL: str = "INFO"
