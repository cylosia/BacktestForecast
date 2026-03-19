"""Verify that PUT requests via cookie auth require X-Requested-With header."""
from __future__ import annotations
from unittest.mock import MagicMock, patch
from types import SimpleNamespace

import pytest


class TestCookieCsrfPutMethod:
    def test_put_without_xrw_header_raises(self):
        from apps.api.app.dependencies import get_current_user
        from backtestforecast.errors import AuthenticationError

        request = MagicMock()
        request.cookies = {"__session": "valid-token"}
        request.method = "PUT"
        request.headers = {}
        request.client = MagicMock()
        request.client.host = "127.0.0.1"
        request.state = SimpleNamespace(request_id="test-req-id")

        db = MagicMock()

        with pytest.raises(AuthenticationError, match="X-Requested-With"):
            get_current_user(request, authorization=None, db=db)

    def test_put_with_xrw_header_passes_csrf_check(self):
        """PUT with X-Requested-With should pass the CSRF check (may fail on JWT verify later)."""
        from apps.api.app.dependencies import get_current_user
        from backtestforecast.errors import AuthenticationError

        request = MagicMock()
        request.cookies = {"__session": "valid-token"}
        request.method = "PUT"
        request.headers = {
            "x-requested-with": "XMLHttpRequest",
            "origin": "http://localhost:3000",
        }
        request.client = MagicMock()
        request.client.host = "127.0.0.1"
        request.state = SimpleNamespace(request_id="test-req-id")

        db = MagicMock()

        # Will fail at JWT verification, not CSRF
        with pytest.raises((AuthenticationError, Exception)) as exc_info:
            get_current_user(request, authorization=None, db=db)

        if isinstance(exc_info.value, AuthenticationError):
            assert "X-Requested-With" not in str(exc_info.value.message)
