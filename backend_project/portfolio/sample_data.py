"""Sample profile data for a Python backend developer project."""

from __future__ import annotations

from backend_project.portfolio.models import DeveloperProfile, Project, Skill

DEFAULT_PROFILE = DeveloperProfile(
    name="Your Name",
    title="Python Backend Developer",
    location="Remote / Your City",
    summary=(
        "Python backend developer focused on production APIs, data-backed services, "
        "cloud deployments, and containerized platforms using FastAPI, Flask, Django, "
        "PostgreSQL, Redis, AWS, Docker, and Kubernetes."
    ),
    skills=(
        Skill("Python", "language", 4),
        Skill("FastAPI", "backend_framework", 3),
        Skill("Flask", "backend_framework", 3),
        Skill("Django", "backend_framework", 3),
        Skill("PostgreSQL", "database", 3),
        Skill("Redis", "cache_queue", 2),
        Skill("AWS", "cloud", 2),
        Skill("Docker", "container", 3),
        Skill("Kubernetes", "orchestration", 2),
    ),
    projects=(
        Project(
            slug="fastapi-commerce-api",
            name="FastAPI Commerce API",
            summary="A REST API for catalog, cart, order, authentication, and admin workflows.",
            technologies=("Python", "FastAPI", "PostgreSQL", "Redis", "Docker", "AWS"),
            highlights=(
                "JWT authentication and role-based permissions",
                "PostgreSQL schema for users, products, orders, and audit logs",
                "Redis caching for product listings and rate limits",
            ),
        ),
        Project(
            slug="django-saas-admin",
            name="Django SaaS Admin Platform",
            summary="A multi-tenant admin backend with reporting, background jobs, and APIs.",
            technologies=("Python", "Django", "Django REST Framework", "PostgreSQL", "Celery", "Redis"),
            highlights=(
                "Tenant-aware access controls",
                "Celery workers for scheduled reports and notifications",
                "Optimized ORM queries with pagination and indexes",
            ),
        ),
        Project(
            slug="flask-notification-service",
            name="Flask Notification Service",
            summary="A microservice for email, SMS, and webhook notification delivery.",
            technologies=("Python", "Flask", "PostgreSQL", "Redis", "Docker", "Kubernetes"),
            highlights=(
                "Idempotent delivery API with retry tracking",
                "Redis-backed queue coordination and rate limiting",
                "Kubernetes deployment with health probes and config maps",
            ),
        ),
    ),
)
