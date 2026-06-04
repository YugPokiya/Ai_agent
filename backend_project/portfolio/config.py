"""Runtime configuration for local, Docker, Kubernetes, and AWS deployments."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    """Environment-driven application settings."""

    app_name: str = os.getenv("APP_NAME", "python-backend-portfolio")
    environment: str = os.getenv("APP_ENV", "local")
    database_url: str = os.getenv("DATABASE_URL", "postgresql://portfolio:portfolio@localhost:5432/portfolio")
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")


settings = Settings()
