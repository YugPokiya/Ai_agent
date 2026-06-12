"""Resume text extraction and content-based classification.

The module keeps OCR/PDF dependencies optional so the API can still classify raw
resume text in lightweight development and test environments. Install the
packages listed in ``requirements.txt`` and the Tesseract system binary to parse
PDF and image resumes.
"""

from __future__ import annotations

import base64
import io
import importlib
import importlib.util
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ResumeCategory:
    """A target resume family and the keywords that identify it."""

    key: str
    label: str
    keywords: tuple[str, ...]


CATEGORIES: tuple[ResumeCategory, ...] = (
    ResumeCategory(
        key="software_engineering",
        label="Software Engineering",
        keywords=(
            "python",
            "java",
            "javascript",
            "typescript",
            "fastapi",
            "django",
            "flask",
            "react",
            "node",
            "api",
            "microservices",
            "backend",
            "frontend",
            "full stack",
            "distributed systems",
            "software engineer",
            "git",
            "unit testing",
        ),
    ),
    ResumeCategory(
        key="data_science_ml",
        label="Data Science / Machine Learning",
        keywords=(
            "machine learning",
            "deep learning",
            "tensorflow",
            "pytorch",
            "scikit-learn",
            "pandas",
            "numpy",
            "nlp",
            "computer vision",
            "model training",
            "feature engineering",
            "statistics",
            "data scientist",
            "llm",
            "rag",
            "mlops",
        ),
    ),
    ResumeCategory(
        key="cloud_devops",
        label="Cloud / DevOps",
        keywords=(
            "aws",
            "azure",
            "gcp",
            "docker",
            "kubernetes",
            "terraform",
            "jenkins",
            "ci/cd",
            "github actions",
            "linux",
            "observability",
            "prometheus",
            "grafana",
            "site reliability",
            "devops",
            "cloudformation",
        ),
    ),
    ResumeCategory(
        key="cybersecurity",
        label="Cybersecurity",
        keywords=(
            "security",
            "cybersecurity",
            "penetration testing",
            "vulnerability",
            "siem",
            "soc",
            "incident response",
            "threat modeling",
            "iam",
            "zero trust",
            "owasp",
            "risk assessment",
            "compliance",
            "encryption",
        ),
    ),
    ResumeCategory(
        key="product_management",
        label="Product Management",
        keywords=(
            "product manager",
            "roadmap",
            "user stories",
            "stakeholders",
            "go-to-market",
            "market research",
            "prioritization",
            "analytics",
            "experimentation",
            "okr",
            "agile",
            "scrum",
            "customer discovery",
        ),
    ),
    ResumeCategory(
        key="finance_accounting",
        label="Finance / Accounting",
        keywords=(
            "accounting",
            "financial analysis",
            "forecasting",
            "budgeting",
            "audit",
            "gaap",
            "excel",
            "valuation",
            "portfolio",
            "risk management",
            "accounts payable",
            "accounts receivable",
            "cpa",
        ),
    ),
    ResumeCategory(
        key="sales_marketing",
        label="Sales / Marketing",
        keywords=(
            "sales",
            "marketing",
            "crm",
            "lead generation",
            "pipeline",
            "seo",
            "sem",
            "content strategy",
            "campaign",
            "brand",
            "account executive",
            "customer acquisition",
            "hubspot",
            "salesforce",
        ),
    ),
    ResumeCategory(
        key="healthcare",
        label="Healthcare",
        keywords=(
            "patient",
            "clinical",
            "nursing",
            "healthcare",
            "medical",
            "emr",
            "hipaa",
            "care plan",
            "pharmacy",
            "diagnosis",
            "hospital",
            "registered nurse",
            "public health",
        ),
    ),
)

_SUPPORTED_TEXT_EXTENSIONS = {".txt", ".md", ".csv"}
_SUPPORTED_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".tif", ".tiff", ".bmp"}
_WORD_RE = re.compile(r"[a-z0-9+#.]+")
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_PHONE_RE = re.compile(r"(?:\+?\d[\d\s().-]{7,}\d)")


def _module_available(module_name: str) -> bool:
    return importlib.util.find_spec(module_name) is not None


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text.casefold()).strip()


def _keyword_hits(normalized_text: str, keywords: tuple[str, ...]) -> dict[str, int]:
    hits: dict[str, int] = {}
    token_counts = Counter(_WORD_RE.findall(normalized_text))
    for keyword in keywords:
        normalized_keyword = keyword.casefold()
        if " " in normalized_keyword or "/" in normalized_keyword or "-" in normalized_keyword:
            count = normalized_text.count(normalized_keyword)
        else:
            count = token_counts[normalized_keyword]
        if count > 0:
            hits[keyword] = count
    return hits


def _confidence(best_score: int, total_score: int) -> float:
    if total_score == 0:
        return 0.0
    return round(best_score / total_score, 3)


def classify_resume_text(text: str) -> dict[str, Any]:
    """Classify resume content into the most likely professional category."""

    normalized_text = _normalize_text(text)
    scored_categories: list[dict[str, Any]] = []

    for category in CATEGORIES:
        hits = _keyword_hits(normalized_text, category.keywords)
        score = sum(hits.values())
        scored_categories.append(
            {
                "key": category.key,
                "label": category.label,
                "score": score,
                "matched_keywords": hits,
            }
        )

    scored_categories.sort(key=lambda item: (-item["score"], item["label"]))
    best = scored_categories[0]
    total_score = sum(item["score"] for item in scored_categories)
    top_category = best["key"] if best["score"] > 0 else "unknown"
    top_label = best["label"] if best["score"] > 0 else "Unknown"

    return {
        "top_category": top_category,
        "top_label": top_label,
        "confidence": _confidence(best["score"], total_score),
        "scores": scored_categories,
        "signals": {
            "emails": sorted(set(_EMAIL_RE.findall(text))),
            "phones": sorted(set(match.strip() for match in _PHONE_RE.findall(text))),
            "word_count": len(_WORD_RE.findall(normalized_text)),
        },
        "extracted_text_preview": text[:500],
    }


def _extract_pdf_text(content: bytes) -> str:
    if not _module_available("pypdf"):
        raise RuntimeError("PDF parsing requires the optional 'pypdf' package. Install requirements.txt first.")

    pypdf = importlib.import_module("pypdf")
    reader = pypdf.PdfReader(io.BytesIO(content))
    return "\n".join(page.extract_text() or "" for page in reader.pages).strip()


def _extract_image_text(content: bytes) -> str:
    missing = [module for module in ("PIL", "pytesseract") if not _module_available(module)]
    if missing:
        raise RuntimeError(
            "Image OCR requires Pillow, pytesseract, and the Tesseract system binary. "
            "Install requirements.txt and tesseract-ocr first."
        )

    image_module = importlib.import_module("PIL.Image")
    pytesseract = importlib.import_module("pytesseract")
    image = image_module.open(io.BytesIO(content))
    return str(pytesseract.image_to_string(image)).strip()


def extract_text_from_file(filename: str, content: bytes) -> str:
    """Extract text from a plain-text, PDF, or image resume file."""

    extension = Path(filename).suffix.casefold()
    if extension in _SUPPORTED_TEXT_EXTENSIONS:
        return content.decode("utf-8", errors="replace").strip()
    if extension == ".pdf":
        return _extract_pdf_text(content)
    if extension in _SUPPORTED_IMAGE_EXTENSIONS:
        return _extract_image_text(content)
    supported = sorted(_SUPPORTED_TEXT_EXTENSIONS | _SUPPORTED_IMAGE_EXTENSIONS | {".pdf"})
    raise ValueError(f"Unsupported resume file type '{extension}'. Supported extensions: {', '.join(supported)}")


def classify_resume_bytes(filename: str, content: bytes) -> dict[str, Any]:
    """Extract text from file bytes and classify the resume."""

    extracted_text = extract_text_from_file(filename, content)
    result = classify_resume_text(extracted_text)
    result["source"] = {"filename": filename, "extracted_characters": len(extracted_text)}
    return result


def decode_base64_file(file_base64: str) -> bytes:
    """Decode a base64 payload, accepting optional data-URI prefixes."""

    payload = file_base64.split(",", maxsplit=1)[-1]
    return base64.b64decode(payload, validate=True)
