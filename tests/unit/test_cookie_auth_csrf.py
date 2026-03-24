"""Verify cookie-based auth has CSRF protection."""
from __future__ import annotations

import inspect


def test_cookie_auth_requires_xrw_header():
    """Cookie auth for state-changing methods must require X-Requested-With."""
    from apps.api.app.dependencies import _resolve_current_user

    source = inspect.getsource(_resolve_current_user)
    assert "X-Requested-With" in source or "x-requested-with" in source, (
        "Cookie auth must check X-Requested-With header for CSRF protection"
    )
    assert "POST" in source and "PUT" in source and "PATCH" in source and "DELETE" in source, (
        "CSRF check must cover POST, PUT, PATCH, and DELETE methods"
    )


def test_cookie_auth_checks_origin():
    """Cookie auth must validate the Origin header against allowed origins."""
    from apps.api.app.dependencies import _resolve_current_user

    source = inspect.getsource(_resolve_current_user)
    assert "origin" in source.lower(), (
        "Cookie auth must check Origin header"
    )
    assert "_get_allowed_origins" in source, (
        "Origin must be checked against configured allowed origins"
    )
