"""Typed portfolio models used by the FastAPI, Flask, and Django adapters."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class Skill:
    """A backend skill grouped by category."""

    name: str
    category: str
    years: float


@dataclass(frozen=True)
class Project:
    """A portfolio project showing production backend experience."""

    slug: str
    name: str
    summary: str
    technologies: tuple[str, ...]
    highlights: tuple[str, ...]


@dataclass(frozen=True)
class DeveloperProfile:
    """Complete Python backend developer profile exposed by the service."""

    name: str
    title: str
    location: str
    summary: str
    skills: tuple[Skill, ...] = field(default_factory=tuple)
    projects: tuple[Project, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dictionary representation."""

        return asdict(self)
