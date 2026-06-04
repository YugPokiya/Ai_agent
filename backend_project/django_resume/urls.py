"""URL routes for the Django portfolio adapter."""

from __future__ import annotations

from django.urls import path

from backend_project.django_resume import views

urlpatterns = [
    path("health", views.health),
    path("profile", views.profile),
]
