from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = [
    "AppError",
    "AuthenticationError",
    "AuthorizationError",
    "QuotaExceededError",
    "FeatureLockedError",
    "ConfigurationError",
    "ValidationError",
    "DataUnavailableError",
    "ExternalServiceError",
    "NotFoundError",
    "RateLimitError",
    "ConflictError",
    "ServiceUnavailableError",
]


@dataclass(slots=True)
class AppError(Exception):
    code: str
    message: str
    status_code: int = 400

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)


class AuthenticationError(AppError):
    def __init__(self, message: str = "Authentication required.") -> None:
        super().__init__(code="authentication_error", message=message, status_code=401)


class AuthorizationError(AppError):
    def __init__(self, message: str = "You do not have access to this resource.") -> None:
        super().__init__(code="authorization_error", message=message, status_code=403)


# Note: AppError uses @dataclass(slots=True). These subclasses are NOT
# dataclasses themselves, so they get __dict__ and can set arbitrary
# instance attributes. Do NOT add @dataclass to these subclasses.
class QuotaExceededError(AppError):
    """Raised when a user hits a plan-based usage limit (e.g. monthly backtests).

    Uses 403 (Forbidden) rather than 429 (Too Many Requests) because this
    represents a plan-tier restriction, not a transient rate limit. Clients
    should direct users to upgrade, not retry.
    """

    def __init__(self, message: str, *, current_tier: str = "free") -> None:
        super().__init__(code="quota_exceeded", message=message, status_code=403)
        self.current_tier = current_tier


class FeatureLockedError(AppError):
    """Raised when a user tries to access a feature not included in their plan."""

    def __init__(self, message: str, *, required_tier: str = "pro") -> None:
        super().__init__(code="feature_locked", message=message, status_code=403)
        self.required_tier = required_tier


class ConfigurationError(AppError):
    def __init__(self, message: str) -> None:
        super().__init__(code="configuration_error", message=message, status_code=500)


class ValidationError(AppError):
    def __init__(self, message: str) -> None:
        super().__init__(code="validation_error", message=message, status_code=422)


class DataUnavailableError(AppError):
    def __init__(self, message: str) -> None:
        super().__init__(code="data_unavailable", message=message, status_code=503)


class ExternalServiceError(AppError):
    def __init__(self, message: str) -> None:
        super().__init__(code="external_service_error", message=message, status_code=502)


class NotFoundError(AppError):
    def __init__(self, message: str) -> None:
        super().__init__(code="not_found", message=message, status_code=404)


class RateLimitError(AppError):
    rate_limit_info: Any

    def __init__(self, message: str = "Rate limit exceeded. Please retry later.") -> None:
        super().__init__(code="rate_limited", message=message, status_code=429)
        self.rate_limit_info = None


class ConflictError(AppError):
    def __init__(self, message: str) -> None:
        super().__init__(code="conflict", message=message, status_code=409)


class ServiceUnavailableError(AppError):
    def __init__(self, message: str = "Service temporarily unavailable. Please retry later.") -> None:
        super().__init__(code="service_unavailable", message=message, status_code=503)
