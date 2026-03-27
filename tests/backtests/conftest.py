from __future__ import annotations

from collections.abc import Generator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from apps.api.app.dependencies import get_token_verifier
from apps.api.app.main import app
from backtestforecast.auth.verification import AuthenticatedPrincipal
from backtestforecast.db.session import get_db
from backtestforecast.models import User
from backtestforecast.security.rate_limits import get_rate_limiter
from tests.postgres_support import reset_database


@pytest.fixture()
def auth_headers() -> dict[str, str]:
    return {"Authorization": "Bearer test-token"}


@pytest.fixture()
def client(
    monkeypatch: pytest.MonkeyPatch,
    postgres_session_factory: sessionmaker[Session],
) -> Generator[TestClient, None, None]:
    reset_database(postgres_session_factory)
    factory = postgres_session_factory

    with factory() as seed_session:
        user = User(
            id=uuid4(),
            clerk_user_id="clerk_test_user",
            email="test@example.com",
            plan_tier="pro",
            created_at=datetime.now(UTC),
        )
        seed_session.add(user)
        seed_session.commit()

    def override_get_db() -> Generator[Session, None, None]:
        db = factory()
        try:
            yield db
        finally:
            db.close()

    def fake_verify(_token: str) -> AuthenticatedPrincipal:
        return AuthenticatedPrincipal(
            clerk_user_id="clerk_test_user",
            session_id="sess_test_123",
            email="test@example.com",
            claims={"sub": "clerk_test_user", "email": "test@example.com"},
        )

    _verifier = get_token_verifier()
    monkeypatch.setattr(_verifier, "verify_bearer_token", fake_verify)
    app.dependency_overrides[get_db] = override_get_db
    get_rate_limiter().reset()
    try:
        with TestClient(app, base_url="http://localhost") as test_client:
            yield test_client
    finally:
        get_rate_limiter().reset()
        app.dependency_overrides.clear()
