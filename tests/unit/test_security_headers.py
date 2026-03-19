"""Verify security headers are applied to API responses."""
from __future__ import annotations

import inspect


def test_robots_tag_present():
    """API responses must include X-Robots-Tag to prevent indexing."""
    from backtestforecast.security.http import ApiSecurityHeadersMiddleware

    source = inspect.getsource(ApiSecurityHeadersMiddleware)
    assert "X-Robots-Tag" in source, "API must set X-Robots-Tag header"
    assert "noindex" in source, "X-Robots-Tag must include noindex"


def test_csp_header_present():
    """API responses must include Content-Security-Policy."""
    from backtestforecast.security.http import ApiSecurityHeadersMiddleware

    source = inspect.getsource(ApiSecurityHeadersMiddleware)
    assert "Content-Security-Policy" in source
    assert "frame-ancestors" in source


def test_hsts_in_production():
    """HSTS must be set for production environments."""
    from backtestforecast.security.http import ApiSecurityHeadersMiddleware

    source = inspect.getsource(ApiSecurityHeadersMiddleware)
    assert "Strict-Transport-Security" in source
    assert "is_production" in source.lower() or "_is_production" in source
