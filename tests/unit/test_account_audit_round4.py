"""Round 4 audit tests: Stripe cleanup metrics, audit metadata, GDPR export completeness."""
from __future__ import annotations

import inspect
import uuid
from unittest.mock import MagicMock


class TestStripeCleanupMetricAccuracy:
    """Finding 1: _cleanup_stripe must return accurate status for metrics."""

    def test_returns_client_unavailable_when_stripe_not_configured(self):
        from apps.api.app.routers.account import _cleanup_stripe

        billing = MagicMock()
        billing._get_stripe_client.side_effect = Exception("No config")

        result = _cleanup_stripe(billing, "sub_1", "cus_1", uuid.uuid4())
        assert result == "client_unavailable"

    def test_returns_ok_when_both_succeed(self):
        from apps.api.app.routers.account import _cleanup_stripe

        billing = MagicMock()
        client = MagicMock()
        billing._get_stripe_client.return_value = client

        result = _cleanup_stripe(billing, "sub_1", "cus_1", uuid.uuid4())
        assert result == "ok"
        client.subscriptions.cancel.assert_called_once_with("sub_1")
        client.customers.delete.assert_called_once_with("cus_1")

    def test_returns_partial_when_sub_fails_cust_ok(self):
        from apps.api.app.routers.account import _cleanup_stripe

        billing = MagicMock()
        client = MagicMock()
        client.subscriptions.cancel.side_effect = Exception("fail")
        billing._get_stripe_client.return_value = client

        result = _cleanup_stripe(billing, "sub_1", "cus_1", uuid.uuid4())
        assert result == "partial"

    def test_returns_failed_when_both_fail(self):
        from apps.api.app.routers.account import _cleanup_stripe

        billing = MagicMock()
        client = MagicMock()
        client.subscriptions.cancel.side_effect = Exception("fail")
        client.customers.delete.side_effect = Exception("fail")
        billing._get_stripe_client.return_value = client

        result = _cleanup_stripe(billing, "sub_1", "cus_1", uuid.uuid4())
        assert result == "failed"

    def test_returns_skipped_when_no_ids(self):
        from apps.api.app.routers.account import _cleanup_stripe

        billing = MagicMock()
        result = _cleanup_stripe(billing, None, None, uuid.uuid4())
        assert result == "skipped"
        billing._get_stripe_client.assert_not_called()


class TestAuditEventMetadataCompleteness:
    """Finding 2/5: Audit metadata must include user_id/clerk_user_id as strings
    so they survive the ON DELETE SET NULL CASCADE on the user_id FK column."""

    def test_delete_account_includes_user_ids_in_metadata(self):
        source = inspect.getsource(
            __import__("apps.api.app.routers.account", fromlist=["delete_account"]).delete_account
        )
        assert "deleted_user_id" in source, (
            "Account deletion audit metadata must include 'deleted_user_id' as a string "
            "to survive the ON DELETE SET NULL cascade on audit_events.user_id"
        )
        assert "clerk_user_id" in source, (
            "Account deletion audit metadata must include 'clerk_user_id' "
            "for cross-system tracing after the user row is deleted"
        )

    def test_delete_account_includes_stripe_cleanup_result(self):
        source = inspect.getsource(
            __import__("apps.api.app.routers.account", fromlist=["delete_account"]).delete_account
        )
        assert "stripe_cleanup_result" in source, (
            "Account deletion audit metadata must include the stripe_cleanup_result "
            "so operators can identify accounts that need manual Stripe reconciliation"
        )


class TestGDPRExportCompleteness:
    """Finding 3: GDPR export must include ALL user-owned data types."""

    def test_export_includes_all_six_data_types(self):
        source = inspect.getsource(
            __import__("apps.api.app.routers.account", fromlist=["export_account_data"]).export_account_data
        )
        required_repos = [
            ("BacktestRunRepository", "backtests"),
            ("BacktestTemplateRepository", "templates"),
            ("ScannerJobRepository", "scanner_jobs"),
            ("SweepJobRepository", "sweep_jobs"),
            ("ExportJobRepository", "export_jobs"),
            ("SymbolAnalysisRepository", "symbol_analyses"),
        ]
        for repo_name, section_name in required_repos:
            assert repo_name in source, (
                f"GDPR export is missing {repo_name} — "
                f"'{section_name}' data will not be included in the export"
            )

    def test_export_has_pagination_params(self):
        source = inspect.getsource(
            __import__("apps.api.app.routers.account", fromlist=["export_account_data"]).export_account_data
        )
        assert "limit" in source and "offset" in source, (
            "GDPR export must support pagination to prevent memory issues "
            "for users with many records"
        )

    def test_cleanup_stripe_never_silently_returns_without_status(self):
        """The old _cancel_stripe_resources returned silently on client errors,
        causing the caller to record stripe_cleanup='ok'. The new _cleanup_stripe
        must always return an explicit status string."""
        from apps.api.app.routers.account import _cleanup_stripe

        billing = MagicMock()
        billing._get_stripe_client.side_effect = Exception("unavailable")

        result = _cleanup_stripe(billing, "sub_1", "cus_1", uuid.uuid4())
        assert isinstance(result, str), "_cleanup_stripe must return str, not None"
        assert result != "ok", (
            "_cleanup_stripe must not return 'ok' when the Stripe client is unavailable"
        )
