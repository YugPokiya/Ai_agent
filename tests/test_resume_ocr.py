import base64
import unittest

from fastapi import HTTPException

from backend_project.fastapi_app import ResumeClassificationRequest, classify_resume
from backend_project.resume_ocr.classifier import classify_resume_bytes, classify_resume_text, extract_text_from_file


class ResumeOcrClassifierTests(unittest.TestCase):
    def test_classifies_software_engineering_resume_text(self):
        result = classify_resume_text(
            "Senior Software Engineer with Python, FastAPI, Django, microservices, "
            "Docker, Kubernetes, APIs, and unit testing experience."
        )

        self.assertEqual(result["top_category"], "software_engineering")
        self.assertGreater(result["confidence"], 0)
        self.assertIn("python", result["scores"][0]["matched_keywords"])

    def test_classifies_base64_text_resume_through_api_handler(self):
        resume_text = "Data Scientist skilled in machine learning, pandas, numpy, NLP, PyTorch, and feature engineering."
        payload = ResumeClassificationRequest(
            filename="resume.txt",
            file_base64=base64.b64encode(resume_text.encode("utf-8")).decode("ascii"),
        )

        body = classify_resume(payload)

        self.assertEqual(body["top_category"], "data_science_ml")
        self.assertEqual(body["source"]["filename"], "resume.txt")

    def test_extracts_plain_text_file_and_rejects_unsupported_types(self):
        text = extract_text_from_file("resume.txt", b"AWS Docker Kubernetes Terraform DevOps")
        self.assertIn("Terraform", text)

        with self.assertRaises(ValueError):
            classify_resume_bytes("resume.exe", b"not a resume")

    def test_api_handler_returns_400_when_payload_missing_resume_content(self):
        with self.assertRaises(HTTPException) as exc:
            classify_resume(ResumeClassificationRequest())

        self.assertEqual(exc.exception.status_code, 400)
        self.assertIn("Provide either", exc.exception.detail)


if __name__ == "__main__":
    unittest.main()
