"""Repository layer for the developer portfolio service."""

from __future__ import annotations

from backend_project.portfolio.models import DeveloperProfile, Project, Skill
from backend_project.portfolio.sample_data import DEFAULT_PROFILE


class InMemoryPortfolioRepository:
    """Simple repository used locally and in tests.

    Production deployments can replace this with a PostgreSQL-backed repository using
    the schema in ``postgres/001_create_portfolio_schema.sql``.
    """

    def __init__(self, profile: DeveloperProfile = DEFAULT_PROFILE) -> None:
        self._profile = profile

    def get_profile(self) -> DeveloperProfile:
        """Return the complete developer profile."""

        return self._profile

    def list_skills(self, category: str | None = None) -> tuple[Skill, ...]:
        """Return all skills or only skills in a category."""

        skills = self._profile.skills
        if category is None:
            return skills
        return tuple(skill for skill in skills if skill.category == category)

    def list_projects(self, technology: str | None = None) -> tuple[Project, ...]:
        """Return all projects or projects that use a technology."""

        projects = self._profile.projects
        if technology is None:
            return projects
        needle = technology.casefold()
        return tuple(project for project in projects if any(item.casefold() == needle for item in project.technologies))

    def get_project(self, slug: str) -> Project | None:
        """Find one project by slug."""

        for project in self._profile.projects:
            if project.slug == slug:
                return project
        return None
