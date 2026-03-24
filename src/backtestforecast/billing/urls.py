from __future__ import annotations

from urllib.parse import urljoin, urlparse

import structlog

from backtestforecast.errors import AppValidationError

_logger = structlog.get_logger("billing.urls")


def resolve_return_url(app_public_url: str, return_path: str | None) -> str:
    base_app_url = app_public_url.rstrip("/")
    default_url = f"{base_app_url}/app/settings/billing"
    if not return_path:
        return default_url

    if len(return_path) > 2048:
        _logger.warning(
            "billing.return_path_too_long",
            length=len(return_path),
            max=2048,
            msg="return_path exceeded 2048 characters; using default billing URL.",
        )
        return default_url

    if any(c in return_path for c in "\r\n"):
        raise AppValidationError("return_path must not contain CR/LF characters.")

    if return_path.startswith("//"):
        raise AppValidationError("return_path must not use protocol-relative URLs.")

    lower = return_path.lower().strip()
    _dangerous_schemes = ("javascript:", "data:", "vbscript:", "blob:", "file:")
    if any(lower.startswith(scheme) for scheme in _dangerous_schemes):
        raise AppValidationError("return_path must not use javascript:, data:, vbscript:, blob:, or file: schemes.")

    if return_path.startswith("http://") or return_path.startswith("https://"):
        requested = urlparse(return_path)
        allowed = urlparse(base_app_url)
        if (requested.scheme, requested.netloc) != (allowed.scheme, allowed.netloc):
            raise AppValidationError("return_path must stay within the BacktestForecast app origin.")
        return return_path

    base = base_app_url + "/"
    resolved = urljoin(base, return_path.lstrip("/"))
    parsed = urlparse(resolved)
    allowed = urlparse(base_app_url)
    if parsed.scheme not in ("http", "https"):
        raise AppValidationError("return_path must use http or https scheme.")
    if parsed.netloc != allowed.netloc:
        raise AppValidationError("return_path resolved to a disallowed origin.")
    return resolved
