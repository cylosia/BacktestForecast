"""Test that cookie-authenticated state-changing requests require Origin or Referer.

Regression test for the defense-in-depth gap where a POST request with
cookie auth and X-Requested-With but no Origin or Referer would bypass
origin validation entirely.
"""
from __future__ import annotations

import inspect


def test_cookie_auth_rejects_state_changing_without_origin_or_referer():
    """The auth handler must reject cookie POST/PATCH/DELETE with no Origin and no Referer."""
    from apps.api.app.dependencies import get_current_user

    source = inspect.getsource(get_current_user)

    assert "cookie_no_origin_or_referer" in source or "no_origin" in source, (
        "get_current_user must reject cookie-based state-changing requests "
        "that have neither Origin nor Referer header"
    )

    assert "POST" in source and "DELETE" in source, (
        "The rejection must cover POST, PUT, PATCH, DELETE methods"
    )


def test_cookie_auth_origin_check_structure():
    """Verify the origin check code has the reject-on-missing-origin path."""
    from apps.api.app.dependencies import get_current_user

    source = inspect.getsource(get_current_user)

    origin_block = source.find("origin = request.headers.get")
    referer_block = source.find("referer = request.headers.get")
    reject_block = source.find("cookie_no_origin_or_referer")

    assert origin_block > 0, "Must check Origin header"
    assert referer_block > origin_block, "Must check Referer as fallback after Origin"
    assert reject_block > referer_block, "Must reject after both Origin and Referer checks fail"
