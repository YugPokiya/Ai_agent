"""Flask application for a lightweight admin-style portfolio API."""

from __future__ import annotations

from flask import Flask, jsonify, request

from backend_project.portfolio.config import settings
from backend_project.portfolio.repository import InMemoryPortfolioRepository

repository = InMemoryPortfolioRepository()
app = Flask(__name__)


@app.get("/health")
def health():
    """Return service health."""

    return jsonify({"status": "ok", "app": settings.app_name, "environment": settings.environment})


@app.get("/profile")
def profile():
    """Return the complete profile."""

    return jsonify(repository.get_profile().to_dict())


@app.get("/projects")
def projects():
    """Return projects filtered by an optional technology query parameter."""

    technology = request.args.get("technology")
    return jsonify([project.__dict__ for project in repository.list_projects(technology=technology)])
