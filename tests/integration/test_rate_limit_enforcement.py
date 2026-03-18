"""Test that rate limiting actually returns 429 when the limit is exceeded.

Unlike other integration tests, this fixture does NOT reset the rate
limiter between requests, so the rate-limit middleware is exercised.
"""
from __future__ import annotations

import os
from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from apps.api.app.dependencies import get_db, token_verifier
from apps.api.app.main import app
from backtestforecast.auth.verification import AuthenticatedPrincipal
from backtestforecast.db.base import Base
from backtestforecast.security.rate_limits import get_rate_limiter


def _make_engine():
    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL is not set — integration tests require Postgres.")
    return create_engine(url)


@pytest.fixture()
def rate_limit_client(monkeypatch: pytest.MonkeyPatch) -> Generator[TestClient, None, None]:
    """Client that does NOT reset the rate limiter, so 429s are reachable."""
    engine = _make_engine()
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)

    def override_get_db() -> Generator[Session, None, None]:
        db = factory()
        try:
            yield db
        finally:
            db.close()

    def fake_verify(_token: str) -> AuthenticatedPrincipal:
        return AuthenticatedPrincipal(
            clerk_user_id="rate_limit_test_user",
            session_id="sess_rl_test",
            email="ratelimit@test.com",
            claims={"sub": "rate_limit_test_user", "email": "ratelimit@test.com"},
        )

    monkeypatch.setattr(token_verifier, "verify_bearer_token", fake_verify)
    app.dependency_overrides[get_db] = override_get_db
    try:
        with TestClient(app, base_url="http://localhost") as test_client:
            yield test_client
    finally:
        get_rate_limiter().reset()
        app.dependency_overrides.clear()
        Base.metadata.drop_all(engine)
        engine.dispose()


def test_rate_limit_returns_429(rate_limit_client: TestClient):
    """Exceed the rate limit and verify the server returns HTTP 429."""
    headers = {"Authorization": "Bearer test-token"}
    limiter = get_rate_limiter()
    limiter.reset()

    got_429 = False
    for _ in range(200):
        resp = rate_limit_client.get("/v1/meta", headers=headers)
        if resp.status_code == 429:
            got_429 = True
            break

    assert got_429, (
        "Expected a 429 response after exceeding the rate limit, "
        "but never received one in 200 requests."
    )
