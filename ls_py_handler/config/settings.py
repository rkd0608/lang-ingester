"""
Settings module for the LS Run Handler.

This module provides configuration settings for the application,
with values loaded from environment variables and sensible defaults.
"""
import os
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""
    
    # App settings
    APP_TITLE: str = os.getenv("APP_TITLE", "LS Run Handler")
    APP_DESCRIPTION: str = os.getenv("APP_DESCRIPTION", "A simple FastAPI server with run endpoints")
    APP_VERSION: str = os.getenv("APP_VERSION", "0.1.0")
    
    # Database settings
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "postgres")
    DB_NAME: str = os.getenv("DB_NAME", "postgres")
    
    # S3/MinIO settings
    S3_BUCKET_NAME: str = os.getenv("S3_BUCKET_NAME", "runs")
    S3_ENDPOINT_URL: str = os.getenv("S3_ENDPOINT_URL", "http://localhost:9002")
    S3_ACCESS_KEY: str = os.getenv("S3_ACCESS_KEY", "minioadmin1")
    S3_SECRET_KEY: str = os.getenv("S3_SECRET_KEY", "minioadmin1")
    S3_REGION: str = os.getenv("S3_REGION", "us-east-1")
    
    class Config:
        """Pydantic config."""
        env_file = ".env"
        env_file_encoding = "utf-8"


# Create a global settings instance
settings = Settings()
