from __future__ import annotations

from collections import OrderedDict
import inspect
import json
import threading
import time as _time
from datetime import timedelta
from dataclasses import dataclass
from decimal import Decimal

import structlog

from backtestforecast.backtests.engine import OptionsBacktestEngine
from backtestforecast.backtests.rules import EntryRuleEvaluator
from backtestforecast.backtests.types import BacktestConfig, BacktestExecutionResult
from backtestforecast.config import get_settings
from backtestforecast.domain.execution_parameters import ResolvedExecutionParameters
from backtestforecast.integrations.massive_client import MassiveClient
from backtestforecast.market_data.prefetch import OptionDataPrefetcher
from backtestforecast.market_data.prewarm import (
    prewarm_long_option_bundle,
    prewarm_targeted_option_bundle,
    supports_targeted_exact_quote_prewarm,
    targeted_exact_quote_prewarm_signature,
)
from backtestforecast.market_data.historical_gateway import HistoricalOptionGateway
from backtestforecast.market_data.service import HistoricalDataBundle, MarketDataService
from backtestforecast.schemas.backtests import CreateBacktestRunRequest, StrategyType
from backtestforecast.services.risk_free_rate import (
    build_backtest_risk_free_rate_curve,
    resolve_backtest_risk_free_rate,
)

_logger = structlog.get_logger("services.backtest_execution")
_thread_local_execution_services = threading.local()
_EXECUTION_INPUTS_CACHE_MAX = 256
_PREPARED_BUNDLE_CACHE_MAX = 16


@dataclass(frozen=True, slots=True)
class _PrefetchPlan:
    mode: str
    signature: tuple[object, ...]
    include_quotes: bool
    warm_future_quotes: bool
    max_dates: int
    trade_dates: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class _PrefetchResult:
    mode: str
    summary: dict[str, object]
    skipped: bool = False


@dataclass(frozen=True, slots=True)
class _ResolvedExecutionInputsCacheEntry:
    parameters: ResolvedExecutionParameters
    risk_free_rate_curve: object | None


@dataclass(frozen=True, slots=True)
class _PreparedBundleCoverage:
    warmup_trading_days: int
    forward_window_days: int
    earnings_days_before: int
    earnings_days_after: int


@dataclass(frozen=True, slots=True)
class _PreparedBundleCacheEntry:
    bundle: HistoricalDataBundle
    coverage: _PreparedBundleCoverage


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
        self._execution_inputs_cache: OrderedDict[
            tuple[object, ...],
            _ResolvedExecutionInputsCacheEntry,
        ] = OrderedDict()
        self._prepared_bundle_cache: OrderedDict[
            tuple[object, ...],
            _PreparedBundleCacheEntry,
        ] = OrderedDict()
        self._engine_supports_shared_entry_rule_cache = (
            "shared_entry_rule_cache" in inspect.signature(self.engine.run).parameters
        )
        self._engine_supports_exit_policy_variants = callable(
            getattr(self.engine, "run_exit_policy_variants", None)
        )

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
        risk_free_rate_curve: object | None = None,
    ) -> BacktestExecutionResult:
        if self._closed:
            raise RuntimeError("BacktestExecutionService has been closed and cannot be reused.")
        total_start = _time.perf_counter()
        settings = get_settings()
        prepare_start = _time.perf_counter()
        resolved_bundle, prepared_bundle_cache_hit = self._resolve_bundle_for_request(
            request,
            bundle=bundle,
        )
        prepare_ms = round((_time.perf_counter() - prepare_start) * 1000, 3)
        prefetch_start = _time.perf_counter()
        self._maybe_prefetch_option_data(request, resolved_bundle, settings)
        prefetch_ms = round((_time.perf_counter() - prefetch_start) * 1000, 3)
        parameter_start = _time.perf_counter()
        parameters, resolved_risk_free_rate_curve = self.resolve_execution_inputs(
            request,
            resolved_parameters=resolved_parameters,
            risk_free_rate_curve=risk_free_rate_curve,
        )
        parameter_ms = round((_time.perf_counter() - parameter_start) * 1000, 3)
        config = self._build_config(
            request=request,
            parameters=parameters,
            risk_free_rate_curve=resolved_risk_free_rate_curve,
        )
        engine_start = _time.perf_counter()
        engine_kwargs = {
            "config": config,
            "bars": resolved_bundle.bars,
            "earnings_dates": resolved_bundle.earnings_dates,
            "ex_dividend_dates": resolved_bundle.ex_dividend_dates,
            "option_gateway": resolved_bundle.option_gateway,
        }
        if self._engine_supports_shared_entry_rule_cache:
            engine_kwargs["shared_entry_rule_cache"] = resolved_bundle.entry_rule_cache
        result = self.engine.run(**engine_kwargs)
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
            prepared_bundle_cache_hit=prepared_bundle_cache_hit,
            data_source=resolved_bundle.data_source,
            prepare_ms=prepare_ms,
            prefetch_ms=prefetch_ms,
            parameter_ms=parameter_ms,
            engine_ms=engine_ms,
            staleness_ms=staleness_ms,
            total_ms=total_ms,
        )
        return result

    def execute_exit_policy_variants(
        self,
        request: CreateBacktestRunRequest,
        *,
        exit_policies: list[tuple[Decimal | None, Decimal | None]],
        bundle: HistoricalDataBundle | None = None,
        resolved_parameters: ResolvedExecutionParameters | None = None,
        risk_free_rate_curve: object | None = None,
    ) -> list[BacktestExecutionResult]:
        if self._closed:
            raise RuntimeError("BacktestExecutionService has been closed and cannot be reused.")
        if not exit_policies:
            return []
        if len(exit_policies) == 1:
            profit_target_pct, stop_loss_pct = exit_policies[0]
            variant_request = request.model_copy(
                update={
                    "profit_target_pct": profit_target_pct,
                    "stop_loss_pct": stop_loss_pct,
                }
            )
            return [
                self.execute_request(
                    variant_request,
                    bundle=bundle,
                    resolved_parameters=resolved_parameters,
                    risk_free_rate_curve=risk_free_rate_curve,
                )
            ]

        total_start = _time.perf_counter()
        settings = get_settings()
        prepare_start = _time.perf_counter()
        resolved_bundle, prepared_bundle_cache_hit = self._resolve_bundle_for_request(
            request,
            bundle=bundle,
        )
        prepare_ms = round((_time.perf_counter() - prepare_start) * 1000, 3)
        prefetch_start = _time.perf_counter()
        self._maybe_prefetch_option_data(request, resolved_bundle, settings)
        prefetch_ms = round((_time.perf_counter() - prefetch_start) * 1000, 3)
        parameter_start = _time.perf_counter()
        parameters, resolved_risk_free_rate_curve = self.resolve_execution_inputs(
            request,
            resolved_parameters=resolved_parameters,
            risk_free_rate_curve=risk_free_rate_curve,
        )
        parameter_ms = round((_time.perf_counter() - parameter_start) * 1000, 3)

        if not self._engine_supports_exit_policy_variants:
            results: list[BacktestExecutionResult] = []
            for profit_target_pct, stop_loss_pct in exit_policies:
                variant_request = request.model_copy(
                    update={
                        "profit_target_pct": profit_target_pct,
                        "stop_loss_pct": stop_loss_pct,
                    }
                )
                results.append(
                    self.execute_request(
                        variant_request,
                        bundle=resolved_bundle,
                        resolved_parameters=parameters,
                        risk_free_rate_curve=resolved_risk_free_rate_curve,
                    )
                )
            return results

        configs = [
            self._build_config(
                request=request.model_copy(
                    update={
                        "profit_target_pct": profit_target_pct,
                        "stop_loss_pct": stop_loss_pct,
                    }
                ),
                parameters=parameters,
                risk_free_rate_curve=resolved_risk_free_rate_curve,
            )
            for profit_target_pct, stop_loss_pct in exit_policies
        ]
        engine_start = _time.perf_counter()
        engine_kwargs = {
            "configs": configs,
            "bars": resolved_bundle.bars,
            "earnings_dates": resolved_bundle.earnings_dates,
            "ex_dividend_dates": resolved_bundle.ex_dividend_dates,
            "option_gateway": resolved_bundle.option_gateway,
        }
        if self._engine_supports_shared_entry_rule_cache:
            engine_kwargs["shared_entry_rule_cache"] = resolved_bundle.entry_rule_cache
        results = self.engine.run_exit_policy_variants(**engine_kwargs)
        engine_ms = round((_time.perf_counter() - engine_start) * 1000, 3)
        staleness_start = _time.perf_counter()
        for result in results:
            if resolved_bundle.warnings:
                result.warnings.extend(resolved_bundle.warnings)
            object.__setattr__(result, "data_source", resolved_bundle.data_source)
            self._check_data_staleness(request.symbol, result, settings)
        staleness_ms = round((_time.perf_counter() - staleness_start) * 1000, 3)
        total_ms = round((_time.perf_counter() - total_start) * 1000, 3)
        _logger.info(
            "backtest.execute_exit_variants_timing",
            symbol=request.symbol,
            strategy_type=request.strategy_type.value,
            variant_count=len(exit_policies),
            bars=len(resolved_bundle.bars),
            trade_dates=sum(1 for bar in resolved_bundle.bars if request.start_date <= bar.trade_date <= request.end_date),
            used_prepared_bundle=bundle is not None,
            prepared_bundle_cache_hit=prepared_bundle_cache_hit,
            data_source=resolved_bundle.data_source,
            prepare_ms=prepare_ms,
            prefetch_ms=prefetch_ms,
            parameter_ms=parameter_ms,
            engine_ms=engine_ms,
            staleness_ms=staleness_ms,
            total_ms=total_ms,
        )
        return results

    def _resolve_bundle_for_request(
        self,
        request: CreateBacktestRunRequest,
        *,
        bundle: HistoricalDataBundle | None,
    ) -> tuple[HistoricalDataBundle, bool]:
        if bundle is not None:
            return bundle, False
        cache_key = self._prepared_bundle_cache_key(request)
        required_coverage = self._prepared_bundle_coverage(request)
        cached_bundle = self._get_cached_prepared_bundle(
            cache_key,
            required_coverage=required_coverage,
        )
        if cached_bundle is not None:
            return self._build_execution_bundle(
                cached_bundle,
                request=request,
                coverage=required_coverage,
            ), True
        prepared_bundle = self.market_data_service.prepare_backtest(request)
        self._store_prepared_bundle(
            cache_key,
            _PreparedBundleCacheEntry(
                bundle=prepared_bundle,
                coverage=required_coverage,
            ),
        )
        return self._build_execution_bundle(
            prepared_bundle,
            request=request,
            coverage=required_coverage,
        ), False

    @staticmethod
    def _prepared_bundle_cache_key(
        request: CreateBacktestRunRequest,
    ) -> tuple[object, ...]:
        return (
            request.symbol,
            request.start_date,
            request.end_date,
        )

    @staticmethod
    def _prepared_bundle_coverage(
        request: CreateBacktestRunRequest,
    ) -> _PreparedBundleCoverage:
        avoid_rules = MarketDataService._collect_avoid_earnings_rules(request.entry_rules)
        return _PreparedBundleCoverage(
            warmup_trading_days=MarketDataService._resolve_warmup_trading_days(request),
            forward_window_days=max(
                request.max_holding_days,
                request.target_dte + request.dte_tolerance_days,
            ) + 45,
            earnings_days_before=max((rule.days_before for rule in avoid_rules), default=0),
            earnings_days_after=max((rule.days_after for rule in avoid_rules), default=0),
        )

    @staticmethod
    def _prepared_bundle_covers(
        cached_coverage: _PreparedBundleCoverage,
        *,
        required_coverage: _PreparedBundleCoverage,
    ) -> bool:
        return (
            cached_coverage.warmup_trading_days >= required_coverage.warmup_trading_days
            and cached_coverage.forward_window_days >= required_coverage.forward_window_days
            and cached_coverage.earnings_days_before >= required_coverage.earnings_days_before
            and cached_coverage.earnings_days_after >= required_coverage.earnings_days_after
        )

    def _get_cached_prepared_bundle(
        self,
        cache_key: tuple[object, ...],
        *,
        required_coverage: _PreparedBundleCoverage,
    ) -> HistoricalDataBundle | None:
        cached = self._prepared_bundle_cache.get(cache_key)
        if cached is None:
            return None
        if not self._prepared_bundle_covers(
            cached.coverage,
            required_coverage=required_coverage,
        ):
            return None
        self._prepared_bundle_cache.move_to_end(cache_key)
        return cached.bundle

    def _store_prepared_bundle(
        self,
        cache_key: tuple[object, ...],
        entry: _PreparedBundleCacheEntry,
    ) -> None:
        existing = self._prepared_bundle_cache.get(cache_key)
        if existing is not None and self._prepared_bundle_covers(
            existing.coverage,
            required_coverage=entry.coverage,
        ):
            self._prepared_bundle_cache.move_to_end(cache_key)
            return
        self._prepared_bundle_cache[cache_key] = entry
        self._prepared_bundle_cache.move_to_end(cache_key)
        while len(self._prepared_bundle_cache) > _PREPARED_BUNDLE_CACHE_MAX:
            self._prepared_bundle_cache.popitem(last=False)

    @staticmethod
    def _build_execution_bundle(
        bundle: HistoricalDataBundle,
        *,
        request: CreateBacktestRunRequest,
        coverage: _PreparedBundleCoverage,
    ) -> HistoricalDataBundle:
        sliced_bars = BacktestExecutionService._slice_bundle_bars(
            bundle.bars,
            request=request,
            coverage=coverage,
        )
        return HistoricalDataBundle(
            bars=sliced_bars,
            earnings_dates=bundle.earnings_dates,
            ex_dividend_dates=bundle.ex_dividend_dates,
            option_gateway=bundle.option_gateway,
            data_source=bundle.data_source,
            warnings=list(bundle.warnings) if bundle.warnings is not None else None,
            prefetched_signatures=bundle.prefetched_signatures,
            prefetched_summaries=bundle.prefetched_summaries,
            prefetch_lock=bundle.prefetch_lock,
            prefetch_inflight=bundle.prefetch_inflight,
        )

    @staticmethod
    def _slice_bundle_bars(
        bars: list[object],
        *,
        request: CreateBacktestRunRequest,
        coverage: _PreparedBundleCoverage,
    ) -> list[object]:
        if not bars:
            return []
        first_entry_index = next(
            (index for index, bar in enumerate(bars) if bar.trade_date >= request.start_date),
            None,
        )
        if first_entry_index is None:
            return list(bars)
        start_index = max(0, first_entry_index - coverage.warmup_trading_days)
        latest_required_trade_date = request.end_date + timedelta(days=coverage.forward_window_days)
        end_index = len(bars) - 1
        for index in range(len(bars) - 1, -1, -1):
            if bars[index].trade_date <= latest_required_trade_date:
                end_index = index
                break
        return list(bars[start_index:end_index + 1])

    def resolve_execution_inputs(
        self,
        request: CreateBacktestRunRequest,
        *,
        resolved_parameters: ResolvedExecutionParameters | None = None,
        risk_free_rate_curve: object | None = None,
    ) -> tuple[ResolvedExecutionParameters, object | None]:
        provider_client = self._market_data_service.client if self._market_data_service is not None else None
        cache_key = self._execution_inputs_cache_key(request)
        cached = self._get_cached_execution_inputs(cache_key)
        parameters = resolved_parameters
        if parameters is None and cached is not None:
            parameters = cached.parameters
        if parameters is None:
            resolved_risk_free_rate = resolve_backtest_risk_free_rate(
                request,
                client=provider_client,
            )
            parameters = ResolvedExecutionParameters.from_request_resolution(
                request,
                resolved_risk_free_rate,
            )
        resolved_curve = risk_free_rate_curve
        if resolved_curve is None and cached is not None:
            resolved_curve = cached.risk_free_rate_curve
        if resolved_curve is None:
            resolved_curve = build_backtest_risk_free_rate_curve(
                request,
                default_rate=parameters.risk_free_rate or 0.0,
                client=provider_client,
            )
        self._store_execution_inputs_cache(
            cache_key,
            _ResolvedExecutionInputsCacheEntry(
                parameters=parameters,
                risk_free_rate_curve=resolved_curve,
            ),
        )
        return parameters, resolved_curve

    @staticmethod
    def _execution_inputs_cache_key(
        request: CreateBacktestRunRequest,
    ) -> tuple[object, ...]:
        return (
            request.start_date,
            request.end_date,
            float(request.risk_free_rate) if request.risk_free_rate is not None else None,
            float(request.dividend_yield) if request.dividend_yield is not None else 0.0,
        )

    def _get_cached_execution_inputs(
        self,
        cache_key: tuple[object, ...],
    ) -> _ResolvedExecutionInputsCacheEntry | None:
        cached = self._execution_inputs_cache.get(cache_key)
        if cached is None:
            return None
        self._execution_inputs_cache.move_to_end(cache_key)
        return cached

    def _store_execution_inputs_cache(
        self,
        cache_key: tuple[object, ...],
        entry: _ResolvedExecutionInputsCacheEntry,
    ) -> None:
        self._execution_inputs_cache[cache_key] = entry
        self._execution_inputs_cache.move_to_end(cache_key)
        while len(self._execution_inputs_cache) > _EXECUTION_INPUTS_CACHE_MAX:
            self._execution_inputs_cache.popitem(last=False)

    @staticmethod
    def _build_config(
        *,
        request: CreateBacktestRunRequest,
        parameters: ResolvedExecutionParameters,
        risk_free_rate_curve: object | None,
    ) -> BacktestConfig:
        return BacktestConfig(
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
            risk_free_rate_curve=risk_free_rate_curve,
            dividend_yield=float(parameters.dividend_yield),
            slippage_pct=float(request.slippage_pct),
            strategy_overrides=request.strategy_overrides,
            custom_legs=request.custom_legs,
            profit_target_pct=float(request.profit_target_pct) if request.profit_target_pct is not None else None,
            stop_loss_pct=float(request.stop_loss_pct) if request.stop_loss_pct is not None else None,
        )

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
        wait_timeout_seconds = max(1, int(getattr(settings, "backtest_prefetch_timeout_seconds", 180)))
        while True:
            state, cached_summary, inflight_event = bundle.begin_prefetch(plan.signature)
            if state == "cached":
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
            if state == "wait":
                if inflight_event is not None and inflight_event.wait(timeout=wait_timeout_seconds):
                    continue
                _logger.warning(
                    "backtest.option_prefetch_wait_timed_out",
                    symbol=request.symbol,
                    mode=plan.mode,
                    strategy_type=request.strategy_type.value,
                    timeout_seconds=wait_timeout_seconds,
                )
                return None
            result: _PrefetchResult | None = None
            try:
                result = self._run_prefetch_plan(request, bundle, plan, settings)
            finally:
                bundle.end_prefetch(plan.signature, result.summary if result is not None else None)
            return result

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
        selected_trade_bars = self._select_prefetch_trade_bars(
            request,
            bundle=bundle,
            max_dates=max_dates,
        )
        selected_trade_dates = tuple(bar.trade_date for bar in selected_trade_bars)
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
                selected_trade_dates,
                override_signature,
            )
            return _PrefetchPlan(
                mode=mode,
                signature=signature,
                include_quotes=True,
                warm_future_quotes=True,
                max_dates=max_dates,
                trade_dates=selected_trade_dates,
            )
        if supports_targeted_exact_quote_prewarm(request.strategy_type):
            mode = "targeted_strategy_exact_contracts"
            override_signature = self._json_signature(
                targeted_exact_quote_prewarm_signature(request)
            )
            local_historical_gateway = isinstance(bundle.option_gateway, HistoricalOptionGateway)
            signature = (
                mode,
                request.symbol,
                request.start_date.isoformat(),
                request.end_date.isoformat(),
                request.target_dte,
                request.dte_tolerance_days,
                max_dates,
                local_historical_gateway,
                selected_trade_dates,
                override_signature,
            )
            return _PrefetchPlan(
                mode=mode,
                signature=signature,
                include_quotes=local_historical_gateway,
                warm_future_quotes=False,
                max_dates=max_dates,
                trade_dates=selected_trade_dates,
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
            selected_trade_dates,
        )
        return _PrefetchPlan(
            mode=mode,
            signature=signature,
            include_quotes=False,
            warm_future_quotes=False,
            max_dates=max_dates,
            trade_dates=selected_trade_dates,
        )

    def _run_prefetch_plan(
        self,
        request: CreateBacktestRunRequest,
        bundle: HistoricalDataBundle,
        plan: _PrefetchPlan,
        settings: object,
    ) -> _PrefetchResult | None:
        entry_trade_bars = self._resolve_prefetch_trade_bars(bundle, plan.trade_dates)
        try:
            if plan.mode == "targeted_exact_quotes":
                summary = prewarm_long_option_bundle(
                    request,
                    bundle=bundle,
                    include_quotes=plan.include_quotes,
                    max_dates=plan.max_dates,
                    warm_future_quotes=plan.warm_future_quotes,
                    entry_trade_bars=entry_trade_bars,
                )
            elif plan.mode == "targeted_strategy_exact_contracts":
                summary = prewarm_targeted_option_bundle(
                    request,
                    bundle=bundle,
                    include_quotes=plan.include_quotes,
                    max_dates=plan.max_dates,
                    warm_future_quotes=plan.warm_future_quotes,
                    entry_trade_bars=entry_trade_bars,
                )
            else:
                summary = OptionDataPrefetcher(
                    timeout_seconds=getattr(settings, "backtest_prefetch_timeout_seconds", 180),
                ).prefetch_for_symbol(
                    request.symbol,
                    entry_trade_bars or bundle.bars,
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
    def _resolve_prefetch_trade_bars(
        bundle: HistoricalDataBundle,
        trade_dates: tuple[object, ...],
    ) -> list[object]:
        if not trade_dates:
            return []
        by_date = {bar.trade_date: bar for bar in bundle.bars}
        return [by_date[trade_date] for trade_date in trade_dates if trade_date in by_date]

    def _select_prefetch_trade_bars(
        self,
        request: CreateBacktestRunRequest,
        *,
        bundle: HistoricalDataBundle,
        max_dates: int,
    ) -> list[object]:
        trade_bars = [
            bar for bar in bundle.bars
            if request.start_date <= bar.trade_date <= request.end_date
        ]
        if len(trade_bars) <= max_dates or not request.entry_rules:
            return trade_bars[:max_dates]

        try:
            evaluator = EntryRuleEvaluator(
                config=BacktestConfig(
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
                    strategy_overrides=request.strategy_overrides,
                    custom_legs=request.custom_legs,
                    profit_target_pct=float(request.profit_target_pct) if request.profit_target_pct is not None else None,
                    stop_loss_pct=float(request.stop_loss_pct) if request.stop_loss_pct is not None else None,
                ),
                bars=bundle.bars,
                earnings_dates=bundle.earnings_dates,
                option_gateway=bundle.option_gateway,
                shared_cache=bundle.entry_rule_cache,
            )
            mask = evaluator.build_entry_allowed_mask()
            selected = [
                bar
                for index, bar in enumerate(bundle.bars)
                if request.start_date <= bar.trade_date <= request.end_date and index < len(mask) and mask[index]
            ]
            if selected:
                return selected[:max_dates]
        except Exception:
            _logger.warning(
                "backtest.option_prefetch_signal_selection_failed",
                symbol=request.symbol,
                strategy_type=request.strategy_type.value,
                exc_info=True,
            )
        return trade_bars[:max_dates]

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
