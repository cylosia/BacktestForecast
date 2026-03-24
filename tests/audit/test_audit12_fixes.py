"""Behavioral regressions for audit round 12 follow-ups."""
from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest


class TestExportDeleteCleansUpStorage:
    def test_delete_for_user_calls_storage_delete_after_commit(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from backtestforecast.models import ExportJob
        from backtestforecast.services.exports import ExportService

        storage_calls: list[tuple[str, str]] = []
        lifecycle: list[str] = []

        export_job = ExportJob(
            id="8f3b0d5f-b4a0-4f66-9f03-3ddcf73d3e77",  # type: ignore[arg-type]
            user_id="5e1ab1bc-c7cc-4cf6-9212-236d683df1f1",  # type: ignore[arg-type]
            backtest_run_id="b4e5cc8f-a243-451a-a19e-2fc011e49979",  # type: ignore[arg-type]
            export_format="csv",
            status="succeeded",
            file_name="test.csv",
            mime_type="text/csv",
            storage_key="exports/run-1.csv",
            size_bytes=12,
        )

        class _ExportsRepo:
            def get_for_user(self, export_job_id, user_id):
                assert export_job_id == export_job.id
                assert user_id == export_job.user_id
                return export_job

        class _Storage:
            def delete(self, key: str) -> None:
                storage_calls.append(("delete", key))
                lifecycle.append("storage_delete")

        class _Session:
            def delete(self, obj) -> None:
                assert obj is export_job
                lifecycle.append("db_delete")

            def commit(self) -> None:
                lifecycle.append("db_commit")

        service = ExportService.__new__(ExportService)
        service.exports = _ExportsRepo()
        service._storage = _Storage()
        service.session = _Session()

        service.delete_for_user(export_job.id, export_job.user_id)

        assert storage_calls == [("delete", "exports/run-1.csv")]
        assert lifecycle == ["db_delete", "db_commit", "storage_delete"]


class TestEngineNaNGuards:
    @staticmethod
    def _config():
        from backtestforecast.backtests.types import BacktestConfig

        return BacktestConfig(
            symbol="AAPL",
            strategy_type="unit_test_nan_guard",
            start_date=date(2025, 1, 2),
            end_date=date(2025, 1, 3),
            target_dte=30,
            dte_tolerance_days=5,
            max_holding_days=10,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("1"),
            commission_per_contract=Decimal("0.65"),
            entry_rules=[],
        )

    @staticmethod
    def _bars():
        from backtestforecast.market_data.types import DailyBar

        return [
            DailyBar(date(2025, 1, 2), 100, 101, 99, 100, 1000),
            DailyBar(date(2025, 1, 3), 101, 102, 100, 101, 1000),
        ]

    @staticmethod
    def _position():
        from backtestforecast.backtests.types import OpenMultiLegPosition, OpenOptionLeg

        return OpenMultiLegPosition(
            display_ticker="O:AAPL250131C00100000",
            strategy_type="unit_test_nan_guard",
            underlying_symbol="AAPL",
            entry_date=date(2025, 1, 2),
            entry_index=0,
            quantity=1,
            dte_at_open=29,
            option_legs=[
                OpenOptionLeg(
                    ticker="O:AAPL250131C00100000",
                    contract_type="call",
                    side=1,
                    strike_price=100.0,
                    expiration_date=date(2025, 1, 31),
                    quantity_per_unit=1,
                    entry_mid=1.0,
                    last_mid=1.0,
                )
            ],
            capital_required_per_unit=100.0,
        )

    def _run_with_snapshot(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        snapshot_position_value: Decimal,
        entry_value_per_unit: Decimal,
    ) -> dict[str, object]:
        from backtestforecast.backtests import engine as engine_mod
        from backtestforecast.backtests.engine import OptionsBacktestEngine
        from backtestforecast.backtests.types import PositionSnapshot, TradeResult

        position = self._position()

        class _DummyStrategy:
            margin_warning_message = None

            def build_position(self, *_args, **_kwargs):
                return position

        class _DummyGateway:
            def list_contracts(self, *_args, **_kwargs):
                return []

            def get_quote(self, *_args, **_kwargs):
                return None

            def get_ex_dividend_dates(self, *_args, **_kwargs):
                return set()

        engine = OptionsBacktestEngine()
        captured: dict[str, object] = {}

        monkeypatch.setitem(engine_mod.STRATEGY_REGISTRY, "unit_test_nan_guard", _DummyStrategy())
        monkeypatch.setattr(
            engine,
            "_mark_position",
            lambda *_args, **_kwargs: PositionSnapshot(
                position_value=snapshot_position_value,
                position_missing_quote=False,
                missing_quote_tickers=(),
            ),
        )
        entry_values = iter([Decimal("1"), entry_value_per_unit])
        monkeypatch.setattr(engine, "_entry_value_per_unit", lambda *_args, **_kwargs: next(entry_values))
        monkeypatch.setattr(engine, "_resolve_position_size", lambda **_kwargs: 1)

        def _resolve_exit(**kwargs):
            captured["resolve_position_value"] = kwargs["position_value"]
            captured["resolve_entry_cost"] = kwargs["entry_cost"]
            return True, "test_exit"

        def _close_position(_position, _config, position_value, *_args, **_kwargs):
            captured["close_position_value"] = position_value
            return (
                TradeResult(
                    option_ticker="O:AAPL250131C00100000",
                    strategy_type="unit_test_nan_guard",
                    underlying_symbol="AAPL",
                    entry_date=date(2025, 1, 2),
                    exit_date=date(2025, 1, 3),
                    expiration_date=date(2025, 1, 31),
                    quantity=1,
                    dte_at_open=29,
                    holding_period_days=1,
                    entry_underlying_close=Decimal("100"),
                    exit_underlying_close=Decimal("101"),
                    entry_mid=Decimal("1"),
                    exit_mid=Decimal("1"),
                    gross_pnl=Decimal("0"),
                    net_pnl=Decimal("0"),
                    total_commissions=Decimal("0"),
                    entry_reason="entry",
                    exit_reason="test_exit",
                ),
                Decimal("0"),
            )

        monkeypatch.setattr(engine, "_resolve_exit", _resolve_exit)
        monkeypatch.setattr(engine, "_close_position", _close_position)

        engine.run(self._config(), self._bars(), set(), _DummyGateway())
        return captured

    def test_nan_guard_on_position_value_before_resolve_exit(self, monkeypatch: pytest.MonkeyPatch):
        captured = self._run_with_snapshot(
            monkeypatch,
            snapshot_position_value=Decimal("NaN"),
            entry_value_per_unit=Decimal("12"),
        )

        assert captured["resolve_position_value"] == 12.0
        assert captured["resolve_entry_cost"] == 12.0
        assert captured["close_position_value"] == Decimal("12")

    def test_nan_guard_on_entry_cost_before_resolve_exit(self, monkeypatch: pytest.MonkeyPatch):
        captured = self._run_with_snapshot(
            monkeypatch,
            snapshot_position_value=Decimal("15"),
            entry_value_per_unit=Decimal("NaN"),
        )

        assert captured["resolve_position_value"] == 0.0
        assert captured["resolve_entry_cost"] == 0.0
        assert captured["close_position_value"] == Decimal("0")


class TestStripeWebhookRetryRecovery:
    def test_recover_stale_claim_recovers_processing_and_error_statuses(self, db_session) -> None:
        from datetime import UTC, datetime, timedelta

        from backtestforecast.models import StripeEvent
        from backtestforecast.repositories.stripe_events import StripeEventRepository

        stale_time = datetime.now(UTC) - timedelta(minutes=20)
        processing = StripeEvent(
            stripe_event_id="evt_processing",
            event_type="customer.subscription.updated",
            livemode=False,
            idempotency_status="processing",
            created_at=stale_time,
        )
        errored = StripeEvent(
            stripe_event_id="evt_error",
            event_type="customer.subscription.updated",
            livemode=False,
            idempotency_status="error",
            created_at=stale_time,
        )
        processed = StripeEvent(
            stripe_event_id="evt_processed",
            event_type="customer.subscription.updated",
            livemode=False,
            idempotency_status="processed",
            created_at=stale_time,
        )
        db_session.add_all([processing, errored, processed])
        db_session.flush()

        repo = StripeEventRepository(db_session)

        assert repo._recover_stale_claim("evt_processing") is True
        assert repo._recover_stale_claim("evt_error") is True
        assert repo._recover_stale_claim("evt_processed") is False


class TestSSESlotReleaseFallback:
    @pytest.mark.anyio
    async def test_release_sse_slot_has_in_process_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from apps.api.app.routers import events

        released: list[str] = []

        async def _boom():
            raise RuntimeError("redis down")

        async def _fallback(user_id):
            released.append(str(user_id))

        monkeypatch.setattr(events, "_get_async_redis", _boom)
        monkeypatch.setattr(events, "_release_sse_slot_in_process", _fallback)

        user_id = "a2a8af6c-56dd-4a9b-bd64-1322b168f5bf"
        await events._release_sse_slot(user_id)  # type: ignore[arg-type]

        assert released == [user_id]
