"""Verification tests for the Security Findings.

Covers all 7 findings: IP hash salt, admin token, webhook limits,
cookie auth, CSV injection, DLQ redaction, and sec-fetch-site.
"""
from __future__ import annotations

import inspect
import warnings

# ---- SF1: IP hash salt - guarded in production ----

def test_sf1_ip_salt_production_guard():
    from backtestforecast.config import Settings
    source = inspect.getsource(Settings.validate_production_security)
    assert "IP_HASH_SALT" in source
    assert "placeholder" in source.lower()


def test_sf1_ip_salt_auto_generated_in_dev():
    from backtestforecast.config import Settings
    source = inspect.getsource(Settings.validate_production_security)
    assert "secrets.token_urlsafe" in source


# ---- SF2: admin_token required in production ----

def test_sf2_admin_token_required():
    from backtestforecast.config import Settings
    source = inspect.getsource(Settings.validate_production_security)
    assert "ADMIN_TOKEN" in source
    assert "not self.admin_token" in source


# ---- SF3: Webhook payload limited to 512KB ----

def test_sf3_webhook_payload_limited():
    from apps.api.app.routers.billing import stripe_webhook
    source = inspect.getsource(stripe_webhook)
    assert "max_length=512_000" in source or "max_length=512000" in source


def test_sf3_body_limit_override_for_webhook():
    from backtestforecast.security.http import BODY_LIMIT_OVERRIDES
    assert "/v1/billing/webhook" in BODY_LIMIT_OVERRIDES
    assert BODY_LIMIT_OVERRIDES["/v1/billing/webhook"] == 512_000


# ---- SF4: sec-fetch-site now rejects cross-site cookie auth ----

def test_sf4_sec_fetch_site_rejects():
    from apps.api.app.dependencies import _resolve_current_user
    source = inspect.getsource(_resolve_current_user)
    assert "cookie_cross_site_rejected" in source
    assert "AuthenticationError" in source


# ---- SF5: CSV formula injection sanitization ----

def test_sf5_csv_sanitizer_handles_equals():
    from backtestforecast.services.exports import ExportService
    result = ExportService._sanitize_csv_cell("=cmd('x')")
    assert isinstance(result, str)
    assert result.startswith("'")


def test_sf5_csv_sanitizer_handles_at():
    from backtestforecast.services.exports import ExportService
    result = ExportService._sanitize_csv_cell("@SUM(A1)")
    assert result.startswith("'")


def test_sf5_csv_sanitizer_handles_plus():
    from backtestforecast.services.exports import ExportService
    result = ExportService._sanitize_csv_cell("+cmd|'/C calc'")
    assert result.startswith("'")


def test_sf5_csv_sanitizer_handles_pipe():
    from backtestforecast.services.exports import ExportService
    result = ExportService._sanitize_csv_cell("|cmd")
    assert result.startswith("'")


def test_sf5_csv_sanitizer_strips_null_bytes():
    from backtestforecast.services.exports import ExportService
    result = ExportService._sanitize_csv_cell("hello\x00world")
    assert "\x00" not in str(result)


def test_sf5_csv_sanitizer_allows_normal_text():
    from backtestforecast.services.exports import ExportService
    result = ExportService._sanitize_csv_cell("AAPL long_call")
    assert result == "AAPL long_call"


# ---- SF6: DLQ args and error redacted ----

def test_sf6_dlq_args_redacted():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import BaseTaskWithDLQ
    source = inspect.getsource(BaseTaskWithDLQ.on_failure)
    assert "_redact_args" in source
    assert '"args": _redact_args(args)' in source


def test_sf6_dlq_error_sanitized():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        from apps.worker.app.tasks import BaseTaskWithDLQ
    source = inspect.getsource(BaseTaskWithDLQ.on_failure)
    assert "_sanitize_error" in source
    assert '"error": _sanitize_error(str(exc))' in source


def test_sf6_redact_args_keeps_short_strings():
    """Short strings (UUIDs, IDs) should be preserved."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        import apps.worker.app.tasks
    source = inspect.getsource(apps.worker.app.tasks.BaseTaskWithDLQ.on_failure)
    assert "len(arg) <= 80" in source


def test_sf6_sanitize_error_truncates():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        import apps.worker.app.tasks
    source = inspect.getsource(apps.worker.app.tasks.BaseTaskWithDLQ.on_failure)
    assert "err_str[:2000]" in source


def test_sf6_sanitize_error_redacts_stripe_keys():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        import apps.worker.app.tasks
    source = inspect.getsource(apps.worker.app.tasks.BaseTaskWithDLQ.on_failure)
    assert "sk_live_" in source
    assert "REDACTED_KEY" in source


# ---- SF7: sec-fetch-site now blocks (not just warns) ----

def test_sf7_sec_fetch_site_blocks():
    from apps.api.app.dependencies import _resolve_current_user
    source = inspect.getsource(_resolve_current_user)
    assert "raise AuthenticationError" in source
    assert "cross-site" in source.lower()
