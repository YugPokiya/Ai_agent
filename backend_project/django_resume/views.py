"""Django JSON views backed by the shared portfolio repository."""

from __future__ import annotations

from django.http import JsonResponse

from backend_project.portfolio.repository import InMemoryPortfolioRepository

repository = InMemoryPortfolioRepository()


def health(_request):
    """Return Django adapter health."""

    return JsonResponse({"status": "ok", "framework": "django"})


def profile(_request):
    """Return the complete developer profile."""

    return JsonResponse(repository.get_profile().to_dict())
