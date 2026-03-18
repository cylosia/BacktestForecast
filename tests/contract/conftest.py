"""Shared fixtures for contract tests."""

import pytest

from apps.api.app.main import app


@pytest.fixture
def fastapi_app():
    """Return the FastAPI app instance for contract tests."""
    return app
