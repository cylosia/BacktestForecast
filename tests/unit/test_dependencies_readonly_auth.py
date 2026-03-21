from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from starlette.requests import Request

from apps.api.app import dependencies as deps
from backtestforecast.errors import AuthenticationError


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "headers": [(b"authorization", b"Bearer test-token")],
            "path": "/v1/me",
            "client": ("127.0.0.1", 1234),
            "scheme": "http",
            "server": ("testserver", 80),
        }
    )


def test_readonly_auth_returns_existing_user_without_write_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    user = SimpleNamespace(id="user-1", clerk_user_id="clerk-user")
    repo = MagicMock()
    repo.get_by_clerk_user_id.return_value = user
    repo.sync_email_if_needed.return_value = True

    monkeypatch.setattr(deps, "get_token_verifier", lambda: SimpleNamespace(verify_bearer_token=lambda _token: SimpleNamespace(clerk_user_id="clerk-user", email="new@example.com")))
    monkeypatch.setattr(deps, "UserRepository", lambda _db: repo)

    create_session_called = False

    def _fail_create_session():
        nonlocal create_session_called
        create_session_called = True
        raise AssertionError("readonly auth should not open a write session")

    monkeypatch.setattr(deps, "create_session", _fail_create_session)

    resolved = deps.get_current_user_readonly(request=_request(), authorization="Bearer test-token", db=MagicMock())

    assert resolved is user
    assert create_session_called is False


def test_readonly_auth_rejects_missing_user_without_side_effects(monkeypatch: pytest.MonkeyPatch) -> None:
    repo = MagicMock()
    repo.get_by_clerk_user_id.return_value = None

    monkeypatch.setattr(deps, "get_token_verifier", lambda: SimpleNamespace(verify_bearer_token=lambda _token: SimpleNamespace(clerk_user_id="missing", email="missing@example.com")))
    monkeypatch.setattr(deps, "UserRepository", lambda _db: repo)
    monkeypatch.setattr(deps, "create_session", lambda: (_ for _ in ()).throw(AssertionError("should not create session")))

    with pytest.raises(AuthenticationError, match="User account not initialized"):
        deps.get_current_user_readonly(request=_request(), authorization="Bearer test-token", db=MagicMock())
