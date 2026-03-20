from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from apps.api.app.dependencies import get_current_user
from backtestforecast.auth.verification import AuthenticatedPrincipal
from backtestforecast.repositories.users import GetOrCreateUserResult


def _request() -> MagicMock:
    request = MagicMock()
    request.cookies = {}
    request.method = "GET"
    request.headers = {}
    request.client = MagicMock()
    request.client.host = "127.0.0.1"
    request.state = SimpleNamespace(request_id="req-1")
    return request


def test_get_current_user_skips_commit_for_unchanged_existing_user() -> None:
    request = _request()
    db = MagicMock()
    user = MagicMock()
    user.id = "user-id"
    user.clerk_user_id = "clerk_123"
    principal = AuthenticatedPrincipal(
        clerk_user_id="clerk_123",
        session_id=None,
        email="same@example.com",
        claims={},
    )
    repo = MagicMock()
    repo.get_or_create.return_value = GetOrCreateUserResult(user=user, was_persisted=False)

    with patch("apps.api.app.dependencies.get_token_verifier") as get_verifier, patch(
        "apps.api.app.dependencies.UserRepository", return_value=repo
    ), patch("apps.api.app.dependencies.structlog.contextvars.bind_contextvars"):
        get_verifier.return_value.verify_bearer_token.return_value = principal

        result = get_current_user(request, authorization="Bearer token", db=db)

    assert result is user
    db.commit.assert_not_called()
    db.refresh.assert_not_called()


def test_get_current_user_commits_for_new_or_updated_user() -> None:
    request = _request()
    db = MagicMock()
    user = MagicMock()
    user.id = "user-id"
    user.clerk_user_id = "clerk_123"
    principal = AuthenticatedPrincipal(
        clerk_user_id="clerk_123",
        session_id=None,
        email="new@example.com",
        claims={},
    )
    repo = MagicMock()
    repo.get_or_create.return_value = GetOrCreateUserResult(user=user, was_persisted=True)

    with patch("apps.api.app.dependencies.get_token_verifier") as get_verifier, patch(
        "apps.api.app.dependencies.UserRepository", return_value=repo
    ), patch("apps.api.app.dependencies.structlog.contextvars.bind_contextvars"):
        get_verifier.return_value.verify_bearer_token.return_value = principal

        result = get_current_user(request, authorization="Bearer token", db=db)

    assert result is user
    db.commit.assert_called_once_with()
    db.refresh.assert_called_once_with(user)
