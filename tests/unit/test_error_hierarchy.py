"""Tests for error type hierarchy and attributes."""
from __future__ import annotations

from backtestforecast.errors import (
    AppError,
    AppValidationError,
    AuthenticationError,
    AuthorizationError,
    ConfigurationError,
    ConflictError,
    DataUnavailableError,
    ExternalServiceError,
    FeatureLockedError,
    NotFoundError,
    QuotaExceededError,
    RateLimitError,
    ServiceUnavailableError,
)


class TestAppError:
    def test_base_error(self):
        err = AppError(code="test", message="msg", status_code=400)
        assert err.code == "test"
        assert err.message == "msg"
        assert err.status_code == 400
        assert str(err) == "msg"

    def test_authentication_error(self):
        err = AuthenticationError()
        assert err.status_code == 401
        assert err.code == "authentication_error"

    def test_authorization_error(self):
        err = AuthorizationError()
        assert err.status_code == 403

    def test_quota_exceeded_carries_tier(self):
        err = QuotaExceededError("limit reached", current_tier="pro")
        assert err.status_code == 403
        assert err.current_tier == "pro"

    def test_feature_locked_carries_required_tier(self):
        err = FeatureLockedError("upgrade needed", required_tier="premium")
        assert err.status_code == 403
        assert err.required_tier == "premium"

    def test_not_found_error(self):
        err = NotFoundError("gone")
        assert err.status_code == 404

    def test_conflict_error(self):
        err = ConflictError("conflict")
        assert err.status_code == 409

    def test_validation_error(self):
        err = AppValidationError("bad input")
        assert err.status_code == 422

    def test_data_unavailable_error(self):
        err = DataUnavailableError("no data")
        assert err.status_code == 422

    def test_configuration_error(self):
        err = ConfigurationError("misconfigured")
        assert err.status_code == 500

    def test_external_service_error(self):
        err = ExternalServiceError("upstream down")
        assert err.status_code == 502

    def test_rate_limit_error(self):
        err = RateLimitError()
        assert err.status_code == 429
        assert err.rate_limit_info is None

    def test_service_unavailable_error(self):
        err = ServiceUnavailableError()
        assert err.status_code == 503

    def test_all_are_app_error_subclasses(self):
        for cls in (
            AuthenticationError,
            AuthorizationError,
            QuotaExceededError,
            FeatureLockedError,
            ConfigurationError,
            AppValidationError,
            DataUnavailableError,
            ExternalServiceError,
            NotFoundError,
            RateLimitError,
            ConflictError,
            ServiceUnavailableError,
        ):
            assert issubclass(cls, AppError)
