import unittest

from backend_project.portfolio.repository import InMemoryPortfolioRepository


class BackendProjectTests(unittest.TestCase):
    def test_profile_contains_requested_backend_stack(self):
        repository = InMemoryPortfolioRepository()
        skill_names = {skill.name for skill in repository.list_skills()}

        self.assertIn("FastAPI", skill_names)
        self.assertIn("Flask", skill_names)
        self.assertIn("Django", skill_names)
        self.assertIn("PostgreSQL", skill_names)
        self.assertIn("Redis", skill_names)
        self.assertIn("AWS", skill_names)
        self.assertIn("Docker", skill_names)
        self.assertIn("Kubernetes", skill_names)

    def test_projects_can_be_filtered_by_technology(self):
        repository = InMemoryPortfolioRepository()
        projects = repository.list_projects(technology="FastAPI")

        self.assertEqual(len(projects), 1)
        self.assertEqual(projects[0].slug, "fastapi-commerce-api")
