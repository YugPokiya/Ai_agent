"""FastAPI application exposing the Python backend developer portfolio."""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query

from backend_project.portfolio.config import settings
from backend_project.portfolio.repository import InMemoryPortfolioRepository

repository = InMemoryPortfolioRepository()
app = FastAPI(title="Python Backend Developer Portfolio API", version="1.0.0")


@app.get("/health")
def health() -> dict[str, str]:
    """Return service health and deployment environment details."""

    return {"status": "ok", "app": settings.app_name, "environment": settings.environment}


@app.get("/profile")
def profile() -> dict:
    """Return the complete backend developer profile."""

    return repository.get_profile().to_dict()


@app.get("/skills")
def skills(category: str | None = Query(default=None)) -> list[dict]:
    """Return skills, optionally filtered by category."""

    return [skill.__dict__ for skill in repository.list_skills(category=category)]


@app.get("/projects")
def projects(technology: str | None = Query(default=None)) -> list[dict]:
    """Return projects, optionally filtered by technology."""

    return [project.__dict__ for project in repository.list_projects(technology=technology)]


@app.get("/projects/{slug}")
def project_detail(slug: str) -> dict:
    """Return one project by slug."""

    project = repository.get_project(slug)
    if project is None:
        raise HTTPException(status_code=404, detail="Project not found")
    return project.__dict__
