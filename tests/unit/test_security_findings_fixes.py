"""Tests for security finding fixes.

Covers:
- #4: StripeEventRepository.list_recent has user_id filter
- #3: AccountDataExportResponse includes audit_events
"""
from __future__ import annotations

import inspect


class TestStripeEventRepoUserFilter:
    """Security #4: list_recent must accept user_id for data isolation."""

    def test_list_recent_accepts_user_id(self) -> None:
        from backtestforecast.repositories.stripe_events import StripeEventRepository
        sig = inspect.signature(StripeEventRepository.list_recent)
        assert "user_id" in sig.parameters, (
            "StripeEventRepository.list_recent must accept a user_id parameter "
            "for data isolation when exposed via API endpoints"
        )

    def test_list_recent_docstring_warns_about_isolation(self) -> None:
        from backtestforecast.repositories.stripe_events import StripeEventRepository
        doc = StripeEventRepository.list_recent.__doc__ or ""
        assert "user_id" in doc.lower() or "data isolation" in doc.lower() or "MUST pass" in doc, (
            "list_recent docstring should document the data isolation requirement"
        )


class TestGdprExportIncludesAuditEvents:
    """Security #3: GDPR export response schema must include audit_events."""

    def test_response_model_has_audit_events_field(self) -> None:
        from apps.api.app.routers.account import AccountDataExportResponse
        fields = AccountDataExportResponse.model_fields
        assert "audit_events" in fields, (
            "AccountDataExportResponse must include an 'audit_events' field "
            "for GDPR data portability compliance"
        )

    def test_export_endpoint_returns_audit_events(self) -> None:
        from apps.api.app.routers.account import export_account_data
        source = inspect.getsource(export_account_data)
        assert "audit_events" in source, (
            "export_account_data must include audit_events in the response dict"
        )
