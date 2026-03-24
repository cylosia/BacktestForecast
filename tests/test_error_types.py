from __future__ import annotations

from backtestforecast.errors import (
    AppError,
    AuthorizationError,
    FeatureLockedError,
    QuotaExceededError,
)


def test_quota_exceeded_error_properties() -> None:
    err = QuotaExceededError("Limit reached", current_tier="free")
    assert isinstance(err, AppError)
    assert err.code == "quota_exceeded"
    assert err.status_code == 403
    assert err.current_tier == "free"
    assert "Limit reached" in err.message


def test_feature_locked_error_properties() -> None:
    err = FeatureLockedError("Scanner requires Pro", required_tier="pro")
    assert isinstance(err, AppError)
    assert err.code == "feature_locked"
    assert err.status_code == 403
    assert err.required_tier == "pro"


def test_error_hierarchy() -> None:
    """Both new error types are subclasses of AppError."""
    assert issubclass(QuotaExceededError, AppError)
    assert issubclass(FeatureLockedError, AppError)
    # They are NOT AuthorizationError - they have distinct codes
    assert QuotaExceededError("x", current_tier="free").code != AuthorizationError().code
    assert FeatureLockedError("x", required_tier="pro").code != AuthorizationError().code
