#!/usr/bin/env python
"""Django management entry point for the portfolio adapter."""

from __future__ import annotations

import os
import sys


def main() -> None:
    """Run Django administrative tasks."""

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend_project.django_resume.settings")
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
