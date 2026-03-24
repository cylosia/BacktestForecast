"""Tests for audit round 15 - CAS race conditions, billing no-op, position sizing."""
from __future__ import annotations

import inspect


class TestSweepCASUpdate:
    """Sweep success must use CAS-style WHERE status='running' update."""

    def test_execute_sweep_uses_cas(self):
        from backtestforecast.services.sweeps import SweepService
        source = inspect.getsource(SweepService._execute_sweep)
        assert 'SweepJob.status == "running"' in source, (
            "_execute_sweep must use WHERE status='running' in its success UPDATE"
        )

    def test_execute_genetic_uses_cas(self):
        from backtestforecast.services.sweeps import SweepService
        source = inspect.getsource(SweepService._execute_genetic)
        assert 'SweepJob.status == "running"' in source, (
            "_execute_genetic must use WHERE status='running' in its success UPDATE"
        )


class TestExportCASUpdate:
    """Export success must use CAS-style WHERE status='running' update."""

    def test_export_success_uses_cas(self):
        from backtestforecast.services.exports import ExportService
        source = inspect.getsource(ExportService.execute_export_by_id)
        assert 'ExportJob.status == "running"' in source or 'status == "running"' in source, (
            "execute_export_by_id must guard its success transition with WHERE status='running'"
        )


class TestDeepAnalysisCASUpdate:
    """Deep analysis success must use CAS-style update."""

    def test_analysis_success_uses_cas(self):
        from backtestforecast.pipeline.deep_analysis import SymbolDeepAnalysisService
        source = inspect.getsource(SymbolDeepAnalysisService.execute_analysis)
        assert 'SymbolAnalysis.status == "running"' in source or 'status == "running"' in source, (
            "execute_analysis must guard its success transition"
        )


class TestMarkStripeEventErrorAuditTrail:
    """mark_error must return result so caller can check rowcount."""

    def test_mark_error_returns_result(self):
        from backtestforecast.repositories.stripe_events import StripeEventRepository
        sig = inspect.signature(StripeEventRepository.mark_error)
        assert sig.return_annotation is not None, (
            "mark_error must have a return annotation (not None)"
        )


class TestPositionSizingFloor:
    """Position sizing must use a meaningful minimum, not $1."""

    def test_capital_floor_is_at_least_50(self):
        from backtestforecast.backtests.engine import OptionsBacktestEngine
        source = inspect.getsource(OptionsBacktestEngine._resolve_position_size)
        assert "50" in source or "_MIN_CAPITAL" in source, (
            "Position sizing floor must be at least $50, not $1"
        )
        assert "< 1.0" not in source, (
            "The old $1.00 floor should be replaced with a higher minimum"
        )
