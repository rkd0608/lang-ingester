"""
Settings module for the LS Run Handler.

This module provides configuration settings for the application,
with values loaded from environment variables and sensible defaults.
"""
import os

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings."""

    # App settings
    APP_TITLE: str = "LS Run Handler"
    APP_DESCRIPTION: str = "A simple FastAPI server with run endpoints"
    APP_VERSION: str = "0.1.0"

    # Database settings
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"
    DB_NAME: str = "postgres"

    # S3/MinIO settings
    S3_BUCKET_NAME: str = "runs"
    S3_ENDPOINT_URL: str = "http://localhost:9002"
    S3_ACCESS_KEY: str = "minioadmin1"
    S3_SECRET_KEY: str = "minioadmin1"
    S3_REGION: str = "us-east-1"
    S3_MULTIPART_PART_SIZE_BYTES: int = 8 * 1024 * 1024

    # Redis settings
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_CACHE_ENABLED: bool = True
    REDIS_CACHE_TTL_SECONDS: int = 300
    REDIS_CACHE_KEY_PREFIX: str = "run"
    REDIS_CACHE_MAX_PAYLOAD_BYTES: int = 262144

    # Configure pydantic to read from .env file
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        extra="ignore",
    )


# Create a global settings instance
settings = (
    Settings(_env_file=".env.test")
    if os.getenv("RUN_HANDLER_ENV") == "test"
    else Settings()
)
