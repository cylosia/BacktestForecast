from __future__ import annotations

from urllib.parse import urljoin, urlparse

from backtestforecast.errors import ValidationError


def resolve_return_url(app_public_url: str, return_path: str | None) -> str:
    base_app_url = app_public_url.rstrip("/")
    default_url = f"{base_app_url}/app/settings/billing"
    if not return_path:
        return default_url

    if any(c in return_path for c in "\r\n"):
        raise ValidationError("return_path must not contain CR/LF characters.")

    if return_path.startswith("//"):
        raise ValidationError("return_path must not use protocol-relative URLs.")

    lower = return_path.lower().strip()
    if lower.startswith("javascript:") or lower.startswith("data:") or lower.startswith("vbscript:"):
        raise ValidationError("return_path must not use javascript:, data:, or vbscript: schemes.")

    if return_path.startswith("http://") or return_path.startswith("https://"):
        requested = urlparse(return_path)
        allowed = urlparse(base_app_url)
        if (requested.scheme, requested.netloc) != (allowed.scheme, allowed.netloc):
            raise ValidationError("return_path must stay within the BacktestForecast app origin.")
        return return_path

    base = base_app_url + "/"
    resolved = urljoin(base, return_path.lstrip("/"))
    parsed = urlparse(resolved)
    allowed = urlparse(base_app_url)
    if parsed.scheme not in ("http", "https"):
        raise ValidationError("return_path must use http or https scheme.")
    if parsed.netloc != allowed.netloc:
        raise ValidationError("return_path resolved to a disallowed origin.")
    return resolved
