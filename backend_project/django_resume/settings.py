"""Django settings for the portfolio service adapter."""

from __future__ import annotations

import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "development-only-secret-key")
DEBUG = os.getenv("DJANGO_DEBUG", "true").lower() == "true"
ALLOWED_HOSTS = os.getenv("DJANGO_ALLOWED_HOSTS", "*").split(",")
ROOT_URLCONF = "backend_project.django_resume.urls"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
INSTALLED_APPS = ["django.contrib.contenttypes", "django.contrib.auth"]
MIDDLEWARE = ["django.middleware.common.CommonMiddleware"]
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.getenv("POSTGRES_DB", "portfolio"),
        "USER": os.getenv("POSTGRES_USER", "portfolio"),
        "PASSWORD": os.getenv("POSTGRES_PASSWORD", "portfolio"),
        "HOST": os.getenv("POSTGRES_HOST", "localhost"),
        "PORT": os.getenv("POSTGRES_PORT", "5432"),
    }
}
