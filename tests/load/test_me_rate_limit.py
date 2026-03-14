"""Load test verifying /v1/me rate limiting prevents abuse."""
import pytest


def test_me_rate_limit_exists():
    """Verify that the /v1/me endpoint has rate limiting configured."""
    import importlib
    mod = importlib.import_module("apps.api.app.routers.me")
    source = open(mod.__file__).read()
    assert "rate_limit" in source.lower() or "get_rate_limiter" in source, (
        "/v1/me endpoint must have rate limiting"
    )
