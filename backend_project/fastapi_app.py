"""FastAPI application exposing the Python backend developer portfolio."""

from __future__ import annotations

from binascii import Error as Base64DecodeError

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from backend_project.portfolio.config import settings
from backend_project.portfolio.repository import InMemoryPortfolioRepository
from backend_project.resume_ocr.classifier import (
    CATEGORIES,
    classify_resume_bytes,
    classify_resume_text,
    decode_base64_file,
)


class ResumeClassificationRequest(BaseModel):
    """JSON payload for resume classification.

    Provide either raw ``text`` or a base64 encoded file plus ``filename``. The
    base64 path supports text, PDF, and image resumes.
    """

    text: str | None = Field(default=None, description="Raw resume text to classify.")
    filename: str | None = Field(default=None, description="Original resume filename when file_base64 is used.")
    file_base64: str | None = Field(default=None, description="Base64 encoded resume file content.")


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


@app.get("/resume/categories")
def resume_categories() -> list[dict[str, object]]:
    """Return supported resume classification categories and their keyword signals."""

    return [
        {"key": category.key, "label": category.label, "keywords": category.keywords}
        for category in CATEGORIES
    ]


@app.post("/resume/classify")
def classify_resume(payload: ResumeClassificationRequest) -> dict:
    """Classify a resume from raw text or from a base64 encoded resume file."""

    if payload.text and payload.text.strip():
        return classify_resume_text(payload.text)

    if payload.file_base64 and payload.filename:
        try:
            content = decode_base64_file(payload.file_base64)
        except Base64DecodeError as exc:
            raise HTTPException(status_code=400, detail="file_base64 must be valid base64") from exc

        try:
            return classify_resume_bytes(payload.filename, content)
        except (RuntimeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    raise HTTPException(status_code=400, detail="Provide either non-empty text or filename with file_base64")
