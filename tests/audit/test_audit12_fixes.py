"""Tests for audit round 12 — remaining unfixed items."""
from __future__ import annotations

import inspect

import pytest


class TestExportDeleteCleansUpStorage:
    """Export delete_for_user must clean up S3 storage objects."""

    def test_delete_for_user_calls_storage_delete(self):
        source = self._get_source()
        assert "self._storage.delete" in source, (
            "delete_for_user must call self._storage.delete to clean up S3"
        )

    def test_delete_for_user_captures_storage_key_before_db_delete(self):
        source = self._get_source()
        key_capture = source.find("storage_key = export_job.storage_key")
        db_delete = source.find("self.session.delete(export_job)")
        assert key_capture != -1 and db_delete != -1, (
            "delete_for_user must capture storage_key before session.delete"
        )
        assert key_capture < db_delete, (
            "storage_key must be captured before the DB row is deleted"
        )

    @staticmethod
    def _get_source() -> str:
        from backtestforecast.services.exports import ExportService
        return inspect.getsource(ExportService.delete_for_user)


class TestEngineNaNGuards:
    """Backtest engine must guard against NaN position_value and entry_cost."""

    def test_nan_guard_on_position_value_before_resolve_exit(self):
        from backtestforecast.backtests.engine import OptionsBacktestEngine
        source = inspect.getsource(OptionsBacktestEngine.run)
        pv_guard = source.find("isfinite(position_value)")
        resolve_exit = source.find("_resolve_exit(")
        assert pv_guard != -1, "NaN guard on position_value must exist"
        assert resolve_exit != -1, "_resolve_exit call must exist"
        assert pv_guard < resolve_exit, (
            "NaN guard on position_value must come BEFORE _resolve_exit call"
        )

    def test_nan_guard_on_entry_cost_before_resolve_exit(self):
        from backtestforecast.backtests.engine import OptionsBacktestEngine
        source = inspect.getsource(OptionsBacktestEngine.run)
        ec_guard = source.find("isfinite(entry_cost)")
        resolve_exit = source.find("_resolve_exit(")
        assert ec_guard != -1, "NaN guard on entry_cost must exist"
        assert ec_guard < resolve_exit, (
            "NaN guard on entry_cost must come BEFORE _resolve_exit call"
        )


class TestStripeWebhookRetryRecovery:
    """Errored Stripe webhook events must be recoverable on retry."""

    def test_recover_stale_claim_includes_error_status(self):
        from backtestforecast.repositories.stripe_events import StripeEventRepository
        source = inspect.getsource(StripeEventRepository._recover_stale_claim)
        assert '"error"' in source, (
            "_recover_stale_claim must recover 'error' status events "
            "to allow Stripe webhook retries for transient failures"
        )
        assert '"processing"' in source, (
            "_recover_stale_claim must still recover 'processing' status (stale claims)"
        )


class TestSSESlotReleaseFallback:
    """SSE slot release must fallback to in-process decrement on Redis failure."""

    def test_release_sse_slot_has_in_process_fallback(self):
        from apps.api.app.routers.events import _release_sse_slot
        source = inspect.getsource(_release_sse_slot)
        assert "_release_sse_slot_in_process" in source, (
            "_release_sse_slot must call _release_sse_slot_in_process on Redis failure "
            "to prevent slot leaks for the full TTL duration"
        )
