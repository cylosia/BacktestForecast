from __future__ import annotations

import json
import threading
import time as _time
from dataclasses import dataclass

import structlog

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.types import BacktestConfig, BacktestExecutionResult
from backtestforecast.config import get_settings
from backtestforecast.domain.execution_parameters import ResolvedExecutionParameters
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.market_data.prefetch import OptionDataPrefetcher
from backtestforecast.market_data.prewarm import (
    prewarm_long_option_bundle,
    prewarm_targeted_option_bundle,
    supports_targeted_exact_quote_prewarm,
)
from backtestforecast.market_data.service import HistoricalDataBundle, MarketDataService
from backtestforecast.schemas.backtests import CreateBacktestRunRequest, StrategyType
from backtestforecast.services.risk_free_rate import (
    build_backtest_risk_free_rate_curve,
    resolve_backtest_risk_free_rate,
)

_logger = structlog.get_logger("services.backtest_execution")
_thread_local_execution_services = threading.local()


@dataclass(frozen=True, slots=True)
class _PrefetchPlan:
    mode: str
    signature: tuple[object, ...]
    include_quotes: bool
    warm_future_quotes: bool
    max_dates: int


@dataclass(frozen=True, slots=True)
class _PrefetchResult:
    mode: str
    summary: dict[str, object]
    skipped: bool = False


class BacktestExecutionService:
    """Orchestrates market data fetching and backtest engine execution.

    Thread safety: instances hold mutable state (_owns_market_data_service,
    market_data_service, engine). Do NOT share across threads without
    external synchronization. Create one instance per thread/task.
    """

    def __init__(
        self,
        market_data_service: MarketDataService | None = None,
        engine: OptionsBacktestEngine | None = None,
    ) -> None:
        self._owns_market_data_service = market_data_service is None
        self._closed = False
        self._market_data_service = market_data_service
        self.engine = engine or OptionsBacktestEngine()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_market_data_service and self._market_data_service is not None:
            self._market_data_service.close()
            self._market_data_service.client.close()

    def __enter__(self) -> BacktestExecutionService:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def market_data_service(self) -> MarketDataService:
        if self._market_data_service is None:
            if self._closed:
                raise RuntimeError("BacktestExecutionService has been closed and cannot be reused.")
            self._market_data_service = MarketDataService(MassiveClient())
        return self._market_data_service

    def execute_request(
        self,
        request: CreateBacktestRunRequest,
        bundle: HistoricalDataBundle | None = None,
        resolved_parameters: ResolvedExecutionParameters | None = None,
    ) -> BacktestExecutionResult:
        if self._closed:
            raise RuntimeError("BacktestExecutionService has been closed and cannot be reused.")
        total_start = _time.perf_counter()
        settings = get_settings()
        prepare_start = _time.perf_counter()
        resolved_bundle = bundle or self.market_data_service.prepare_backtest(request)
        prepare_ms = round((_time.perf_counter() - prepare_start) * 1000, 3)
        provider_client = self._market_data_service.client if self._market_data_service is not None else None
        prefetch_start = _time.perf_counter()
        self._maybe_prefetch_option_data(request, resolved_bundle, settings)
        prefetch_ms = round((_time.perf_counter() - prefetch_start) * 1000, 3)
        parameter_start = _time.perf_counter()
        parameters = resolved_parameters
        if parameters is None:
            resolved_risk_free_rate = resolve_backtest_risk_free_rate(
                request,
                client=provider_client,
            )
            parameters = ResolvedExecutionParameters.from_request_resolution(
                request,
                resolved_risk_free_rate,
            )
        resolved_risk_free_rate_curve = build_backtest_risk_free_rate_curve(
            request,
            default_rate=parameters.risk_free_rate or 0.0,
            client=provider_client,
        )
        parameter_ms = round((_time.perf_counter() - parameter_start) * 1000, 3)
        config = BacktestConfig(
            symbol=request.symbol,
            strategy_type=request.strategy_type.value,
            start_date=request.start_date,
            end_date=request.end_date,
            target_dte=request.target_dte,
            dte_tolerance_days=request.dte_tolerance_days,
            max_holding_days=request.max_holding_days,
            account_size=request.account_size,
            risk_per_trade_pct=request.risk_per_trade_pct,
            commission_per_contract=request.commission_per_contract,
            entry_rules=request.entry_rules,
            risk_free_rate=parameters.risk_free_rate or 0.0,
            risk_free_rate_curve=resolved_risk_free_rate_curve,
            dividend_yield=float(parameters.dividend_yield),
            slippage_pct=float(request.slippage_pct),
            strategy_overrides=request.strategy_overrides,
            custom_legs=request.custom_legs,
            profit_target_pct=float(request.profit_target_pct) if request.profit_target_pct is not None else None,
            stop_loss_pct=float(request.stop_loss_pct) if request.stop_loss_pct is not None else None,
        )
        engine_start = _time.perf_counter()
        result = self.engine.run(
            config=config,
            bars=resolved_bundle.bars,
            earnings_dates=resolved_bundle.earnings_dates,
            ex_dividend_dates=resolved_bundle.ex_dividend_dates,
            option_gateway=resolved_bundle.option_gateway,
        )
        engine_ms = round((_time.perf_counter() - engine_start) * 1000, 3)
        if resolved_bundle.warnings:
            result.warnings.extend(resolved_bundle.warnings)
        object.__setattr__(result, "data_source", resolved_bundle.data_source)
        staleness_start = _time.perf_counter()
        self._check_data_staleness(request.symbol, result, settings)
        staleness_ms = round((_time.perf_counter() - staleness_start) * 1000, 3)
        total_ms = round((_time.perf_counter() - total_start) * 1000, 3)
        _logger.info(
            "backtest.execute_timing",
            symbol=request.symbol,
            strategy_type=request.strategy_type.value,
            bars=len(resolved_bundle.bars),
            trade_dates=sum(1 for bar in resolved_bundle.bars if request.start_date <= bar.trade_date <= request.end_date),
            used_prepared_bundle=bundle is not None,
            data_source=resolved_bundle.data_source,
            prepare_ms=prepare_ms,
            prefetch_ms=prefetch_ms,
            parameter_ms=parameter_ms,
            engine_ms=engine_ms,
            staleness_ms=staleness_ms,
            total_ms=total_ms,
        )
        return result

    def prefetch_requests_with_shared_bundle(
        self,
        requests: list[CreateBacktestRunRequest],
        *,
        bundle: HistoricalDataBundle,
    ) -> dict[str, object]:
        settings = get_settings()
        aggregate: dict[str, object] = {
            "prefetch_count": 0,
            "skipped_count": 0,
            "dates_processed": 0,
            "contracts_fetched": 0,
            "quotes_fetched": 0,
            "errors": [],
            "requests": [],
        }
        for request in requests:
            result = self._maybe_prefetch_option_data(request, bundle, settings)
            if result is None:
                continue
            request_entry = {
                "strategy_type": request.strategy_type.value,
                "mode": result.mode,
                "skipped": result.skipped,
                **result.summary,
            }
            aggregate["requests"].append(request_entry)
            if result.skipped:
                aggregate["skipped_count"] = int(aggregate["skipped_count"]) + 1
                continue
            aggregate["prefetch_count"] = int(aggregate["prefetch_count"]) + 1
            aggregate["dates_processed"] = int(aggregate["dates_processed"]) + int(result.summary.get("dates_processed", 0))
            aggregate["contracts_fetched"] = int(aggregate["contracts_fetched"]) + int(result.summary.get("contracts_fetched", 0))
            aggregate["quotes_fetched"] = int(aggregate["quotes_fetched"]) + int(result.summary.get("quotes_fetched", 0))
            aggregate_errors = aggregate["errors"]
            if isinstance(aggregate_errors, list):
                aggregate_errors.extend(result.summary.get("errors", []))
        errors = aggregate.get("errors")
        if isinstance(errors, list):
            aggregate["errors"] = errors[:20]
        return aggregate

    def _maybe_prefetch_option_data(
        self,
        request: CreateBacktestRunRequest,
        bundle: HistoricalDataBundle,
        settings: object,
    ) -> _PrefetchResult | None:
        plan = self._build_prefetch_plan(request, bundle, settings)
        if plan is None:
            return None
        cached_summary = bundle.get_prefetch_summary(plan.signature)
        if cached_summary is not None or bundle.has_prefetched(plan.signature):
            _logger.info(
                "backtest.option_prefetch_skipped",
                symbol=request.symbol,
                mode=plan.mode,
                strategy_type=request.strategy_type.value,
                reason="bundle_already_warm",
            )
            return _PrefetchResult(
                mode=plan.mode,
                summary=dict(cached_summary or {}),
                skipped=True,
            )
        return self._run_prefetch_plan(request, bundle, plan, settings)

    def _build_prefetch_plan(
        self,
        request: CreateBacktestRunRequest,
        bundle: HistoricalDataBundle,
        settings: object,
    ) -> _PrefetchPlan | None:
        if not getattr(settings, "backtest_option_prefetch_enabled", True):
            return None
        trade_dates = [
            bar.trade_date for bar in bundle.bars
            if request.start_date <= bar.trade_date <= request.end_date
        ]
        if len(trade_dates) < getattr(settings, "backtest_prefetch_min_trade_dates", 10):
            return None
        max_dates = getattr(settings, "backtest_prefetch_max_dates", 6)
        if request.strategy_type in {StrategyType.LONG_CALL, StrategyType.LONG_PUT}:
            mode = "targeted_exact_quotes"
            override_signature = self._json_signature(
                self._long_option_override_signature(request)
            )
            signature: tuple[object, ...] = (
                mode,
                request.strategy_type.value,
                request.symbol,
                request.start_date.isoformat(),
                request.end_date.isoformat(),
                request.target_dte,
                request.dte_tolerance_days,
                request.max_holding_days,
                max_dates,
                override_signature,
            )
            return _PrefetchPlan(
                mode=mode,
                signature=signature,
                include_quotes=True,
                warm_future_quotes=True,
                max_dates=max_dates,
            )
        if supports_targeted_exact_quote_prewarm(request.strategy_type):
            mode = "targeted_strategy_exact_contracts"
            override_signature = self._json_signature(
                self._targeted_strategy_override_signature(request)
            )
            signature = (
                mode,
                request.strategy_type.value,
                request.symbol,
                request.start_date.isoformat(),
                request.end_date.isoformat(),
                request.target_dte,
                request.dte_tolerance_days,
                max_dates,
                override_signature,
            )
            return _PrefetchPlan(
                mode=mode,
                signature=signature,
                include_quotes=False,
                warm_future_quotes=False,
                max_dates=max_dates,
            )
        mode = "broad_contracts_only"
        signature = (
            mode,
            request.symbol,
            request.start_date.isoformat(),
            request.end_date.isoformat(),
            request.target_dte,
            request.dte_tolerance_days,
            max_dates,
        )
        return _PrefetchPlan(
            mode=mode,
            signature=signature,
            include_quotes=False,
            warm_future_quotes=False,
            max_dates=max_dates,
        )

    def _run_prefetch_plan(
        self,
        request: CreateBacktestRunRequest,
        bundle: HistoricalDataBundle,
        plan: _PrefetchPlan,
        settings: object,
    ) -> _PrefetchResult | None:
        try:
            if plan.mode == "targeted_exact_quotes":
                summary = prewarm_long_option_bundle(
                    request,
                    bundle=bundle,
                    include_quotes=plan.include_quotes,
                    max_dates=plan.max_dates,
                    warm_future_quotes=plan.warm_future_quotes,
                )
            elif plan.mode == "targeted_strategy_exact_contracts":
                summary = prewarm_targeted_option_bundle(
                    request,
                    bundle=bundle,
                    include_quotes=plan.include_quotes,
                    max_dates=plan.max_dates,
                    warm_future_quotes=plan.warm_future_quotes,
                )
            else:
                summary = OptionDataPrefetcher(
                    timeout_seconds=getattr(settings, "backtest_prefetch_timeout_seconds", 180),
                ).prefetch_for_symbol(
                    request.symbol,
                    bundle.bars,
                    request.start_date,
                    request.end_date,
                    request.target_dte,
                    request.dte_tolerance_days,
                    bundle.option_gateway,
                    include_quotes=plan.include_quotes,
                    max_dates=plan.max_dates,
                )
            summary_dict = summary.to_dict()
            bundle.remember_prefetch(plan.signature, summary_dict)
            _logger.info(
                "backtest.option_prefetch_completed",
                symbol=request.symbol,
                mode=plan.mode,
                summary=summary_dict,
            )
            return _PrefetchResult(mode=plan.mode, summary=summary_dict)
        except Exception:
            _logger.warning("backtest.option_prefetch_failed", symbol=request.symbol, exc_info=True)
            return None

    @staticmethod
    def _json_signature(value: object | None) -> str | None:
        if value is None:
            return None
        return json.dumps(value, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def _long_option_override_signature(request: CreateBacktestRunRequest) -> dict[str, object] | None:
        overrides = request.strategy_overrides
        if overrides is None:
            return None
        if request.strategy_type == StrategyType.LONG_CALL and overrides.long_call_strike is not None:
            return {"long_call_strike": overrides.long_call_strike.model_dump(mode="json", exclude_none=True)}
        if request.strategy_type == StrategyType.LONG_PUT and overrides.long_put_strike is not None:
            return {"long_put_strike": overrides.long_put_strike.model_dump(mode="json", exclude_none=True)}
        return None

    @staticmethod
    def _targeted_strategy_override_signature(request: CreateBacktestRunRequest) -> dict[str, object] | None:
        overrides = request.strategy_overrides
        if overrides is None:
            return None
        if request.strategy_type == StrategyType.CALENDAR_SPREAD:
            return {"calendar_contract_type": overrides.calendar_contract_type}
        return None

    def _check_data_staleness(
        self,
        symbol: str,
        result: BacktestExecutionResult,
        settings: object,
    ) -> None:
        """Add a warning to backtest results if cached option data is stale."""
        try:
            cache = getattr(self._market_data_service, '_redis_cache', None)
            if cache is None:
                return
            warn_age = getattr(settings, 'option_cache_warn_age_seconds', 259_200)
            age = cache.get_oldest_cache_age_seconds(symbol)
            if age is not None and age > warn_age:
                days = int(age / 86400)
                warning = {
                    "code": "stale_option_cache",
                    "message": (
                        f"Option data for {symbol} was cached {days} day(s) ago. "
                        f"Results may not reflect the most recent market conditions."
                    ),
                }
                if result.warnings is None:
                    result.warnings = [warning]
                else:
                    result.warnings.append(warning)
                _logger.warning(
                    "backtest.stale_cache",
                    symbol=symbol,
                    cache_age_seconds=round(age),
                    warn_threshold=warn_age,
                )
        except Exception:
            _logger.warning("backtest.staleness_check_failed", symbol=symbol, exc_info=True)


def get_thread_local_shared_execution_service() -> BacktestExecutionService:
    """Return a per-thread shared execution service for direct service paths.

    BacktestExecutionService is explicitly not thread-safe. This helper keeps
    one warmed instance per thread so repeated direct scan/sweep/backtest
    service calls in the same thread can reuse MarketDataService state without
    unsafe cross-thread sharing.
    """

    service = getattr(_thread_local_execution_services, "execution_service", None)
    if service is None or getattr(service, "_closed", False):
        service = BacktestExecutionService()
        _thread_local_execution_services.execution_service = service
    return service


def close_thread_local_shared_execution_service() -> None:
    service = getattr(_thread_local_execution_services, "execution_service", None)
    _thread_local_execution_services.execution_service = None
    if service is not None:
        service.close()
