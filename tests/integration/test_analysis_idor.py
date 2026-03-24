"""Tests for analysis cross-user isolation (IDOR)."""
from __future__ import annotations

import apps.api.app.dependencies as dependencies
from backtestforecast.auth.verification import AuthenticatedPrincipal
from backtestforecast.models import SymbolAnalysis, User


def _make_principal(clerk_id: str) -> AuthenticatedPrincipal:
    return AuthenticatedPrincipal(
        clerk_user_id=clerk_id,
        session_id=f"sess_{clerk_id}",
        email=f"{clerk_id}@test.com",
        claims={"sub": clerk_id, "email": f"{clerk_id}@test.com"},
    )


def test_user_b_cannot_see_user_a_analysis(client, auth_headers, db_session, monkeypatch):
    """User B should not be able to access User A's analysis."""
    user_a = User(clerk_user_id="clerk_user_a", email="a@test.com")
    db_session.add(user_a)
    db_session.commit()
    db_session.refresh(user_a)

    analysis = SymbolAnalysis(
        user_id=user_a.id,
        symbol="AAPL",
        status="succeeded",
    )
    db_session.add(analysis)
    db_session.commit()
    db_session.refresh(analysis)

    user_b = User(clerk_user_id="clerk_user_b", email="b@test.com")
    db_session.add(user_b)
    db_session.commit()

    def fake_get_current_user():
        return user_b

    monkeypatch.setattr(
        dependencies.get_token_verifier(),
        "verify_bearer_token",
        lambda _: _make_principal("clerk_user_b"),
    )

    resp = client.get(f"/v1/analysis/{analysis.id}", headers=auth_headers)
    assert resp.status_code == 404


def test_analysis_concurrency_limit_returns_429(client, auth_headers, db_session, monkeypatch):
    """Item 85: Creating more than 5 concurrent analyses must return 429."""
    client.get("/v1/me", headers=auth_headers)
    user = db_session.query(User).filter_by(clerk_user_id="clerk_test_user").first()
    assert user is not None

    for i in range(5):
        analysis = SymbolAnalysis(
            user_id=user.id,
            symbol=f"SYM{i}",
            status="running" if i % 2 == 0 else "queued",
        )
        db_session.add(analysis)
    db_session.commit()

    import apps.api.app.dispatch as dispatch_mod
    monkeypatch.setattr(dispatch_mod, "celery_app", type("FakeCelery", (), {
        "send_task": staticmethod(lambda *a, **kw: type("R", (), {"id": "fake"})()),
    })())

    resp = client.post(
        "/v1/analysis",
        json={"symbol": "OVERFLOW"},
        headers=auth_headers,
    )
    assert resp.status_code == 429, (
        f"Expected 429 for exceeding concurrency limit, got {resp.status_code}"
    )


def test_user_a_can_see_own_analysis(client, auth_headers, db_session, monkeypatch):
    """User A should be able to access their own analysis."""
    client.get("/v1/me", headers=auth_headers)
    user_a = db_session.query(User).filter_by(clerk_user_id="clerk_test_user").first()
    assert user_a is not None

    analysis = SymbolAnalysis(
        user_id=user_a.id,
        symbol="TSLA",
        status="succeeded",
    )
    db_session.add(analysis)
    db_session.commit()
    db_session.refresh(analysis)

    resp = client.get(f"/v1/analysis/{analysis.id}", headers=auth_headers)
    assert resp.status_code == 200
