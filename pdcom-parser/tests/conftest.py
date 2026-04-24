from __future__ import annotations

from pathlib import Path

import fitz
import pytest

FIXTURE = Path(__file__).parent / "fixtures" / "chene_bougeries_pdcom_2024.pdf"


@pytest.fixture(scope="session")
def fixture_pdf_path() -> Path:
    if not FIXTURE.exists():
        pytest.skip(f"Fixture PDF not present at {FIXTURE}. Run scripts/bootstrap_fixtures.py")
    return FIXTURE


@pytest.fixture(scope="session")
def fixture_pdf(fixture_pdf_path) -> fitz.Document:
    return fitz.open(fixture_pdf_path)


@pytest.fixture
def page42(fixture_pdf):
    return fixture_pdf[41]


@pytest.fixture
def page53(fixture_pdf):
    return fixture_pdf[52]


@pytest.fixture
def page64(fixture_pdf):
    return fixture_pdf[63]
