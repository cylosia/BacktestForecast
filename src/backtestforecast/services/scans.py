from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Self
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backtestforecast.billing.entitlements import (
    ensure_forecasting_access,
    resolve_scanner_policy,
    validate_strategy_access,
)
from backtestforecast.errors import AppError, NotFoundError, ValidationError
from backtestforecast.forecasts.analog import HistoricalAnalogForecaster
from backtestforecast.market_data.service import HistoricalDataBundle
from backtestforecast.models import ScannerJob, ScannerRecommendation, User
from backtestforecast.repositories.scanner_jobs import ScannerJobRepository
from backtestforecast.scans.ranking import (
    HistoricalObservation,
    aggregate_historical_performance,
    build_ranking_breakdown,
    hash_payload,
    is_strategy_rule_set_compatible,
    recommendation_sort_key,
    rule_set_hash,
)
from backtestforecast.schemas.backtests import (
    BacktestSummaryResponse,
    BacktestTradeResponse,
    CreateBacktestRunRequest,
    EquityCurvePointResponse,
)
from backtestforecast.schemas.forecasts import ForecastEnvelopeResponse
from backtestforecast.schemas.scans import (
    CreateScannerJobRequest,
    HistoricalAnalogForecastResponse,
    ScannerJobListResponse,
    ScannerJobResponse,
    ScannerRecommendationListResponse,
    ScannerRecommendationResponse,
)
from backtestforecast.services.backtest_execution import BacktestExecutionService
from backtestforecast.services.backtests import to_decimal


class ScanService:
    def __init__(
        self,
        session: Session,
        execution_service: BacktestExecutionService | None = None,
        forecaster: HistoricalAnalogForecaster | None = None,
    ) -> None:
        self.session = session
        self._execution_service = execution_service
        self._forecaster = forecaster
        self.repository = ScannerJobRepository(session)

    @property
    def execution_service(self) -> BacktestExecutionService:
        if self._execution_service is None:
            self._execution_service = BacktestExecutionService()
        return self._execution_service

    def close(self) -> None:
        if self._execution_service is not None:
            self._execution_service.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    @property
    def forecaster(self) -> HistoricalAnalogForecaster:
        if self._forecaster is None:
            self._forecaster = HistoricalAnalogForecaster()
        return self._forecaster

    def create_job(self, user: User, payload: CreateScannerJobRequest) -> ScannerJob:
        policy = resolve_scanner_policy(user.plan_tier, payload.mode.value, user.subscription_status)
        validate_strategy_access(policy, [strategy.value for strategy in payload.strategy_types])
        self._validate_limits(policy, payload)

        candidate_count, compatibility_warnings = self._count_compatible_candidates(payload)
        if candidate_count <= 0:
            raise ValidationError("No compatible symbol/strategy/rule-set combinations were left after validation.")

        request_hash = self._request_hash(payload)
        if payload.idempotency_key:
            existing_by_key = self.repository.get_by_idempotency_key(user.id, payload.idempotency_key)
            if existing_by_key is not None:
                return existing_by_key

        recent_duplicate = self.repository.find_recent_duplicate(
            user.id,
            request_hash,
            payload.mode.value,
            since=datetime.now(UTC) - timedelta(minutes=10),
        )
        if recent_duplicate is not None:
            return recent_duplicate

        job = ScannerJob(
            user_id=user.id,
            name=payload.name,
            status="queued",
            mode=payload.mode.value,
            plan_tier_snapshot=user.plan_tier,
            job_kind="manual",
            request_hash=request_hash,
            idempotency_key=payload.idempotency_key,
            refresh_daily=payload.refresh_daily,
            refresh_priority=payload.refresh_priority,
            candidate_count=candidate_count,
            evaluated_candidate_count=0,
            recommendation_count=0,
            request_snapshot_json=payload.model_dump(mode="json"),
            warnings_json=compatibility_warnings,
            ranking_version="scanner-ranking-v1",
            engine_version="options-multileg-v2",
        )
        self.repository.add(job)
        self.session.commit()
        self.session.refresh(job)
        return job

    def run_job(self, job_id: UUID) -> ScannerJob:
        job = self.repository.get(job_id, for_update=True)
        if job is None:
            raise NotFoundError("Scanner job not found.")
        if job.status == "succeeded" and job.recommendation_count > 0:
            return job

        payload = CreateScannerJobRequest.model_validate(job.request_snapshot_json)
        self.repository.delete_recommendations(job.id)
        job.status = "running"
        job.started_at = datetime.now(UTC)
        job.completed_at = None
        job.error_code = None
        job.error_message = None
        job.recommendation_count = 0
        job.evaluated_candidate_count = 0
        job.warnings_json = []
        self.session.flush()

        compatibility_candidate_count, compatibility_warnings = self._count_compatible_candidates(payload)
        job.candidate_count = compatibility_candidate_count
        warnings: list[dict[str, Any]] = list(compatibility_warnings)
        candidates: list[dict[str, Any]] = []
        forecast_cache: dict[tuple[str, str], HistoricalAnalogForecastResponse] = {}

        bundle_cache = self._prepare_bundles(payload, warnings)
        historical_cache = self._batch_historical_performance(payload, job.created_at)

        for symbol in payload.symbols:
            for strategy in payload.strategy_types:
                for rule_set in payload.rule_sets:
                    if not is_strategy_rule_set_compatible(strategy.value, rule_set.entry_rules):
                        continue

                    request = CreateBacktestRunRequest(
                        symbol=symbol,
                        strategy_type=strategy,
                        start_date=payload.start_date,
                        end_date=payload.end_date,
                        target_dte=payload.target_dte,
                        dte_tolerance_days=payload.dte_tolerance_days,
                        max_holding_days=payload.max_holding_days,
                        account_size=payload.account_size,
                        risk_per_trade_pct=payload.risk_per_trade_pct,
                        commission_per_contract=payload.commission_per_contract,
                        entry_rules=rule_set.entry_rules,
                    )
                    candidate_rule_set_hash = rule_set_hash(rule_set.entry_rules)
                    try:
                        bundle = bundle_cache.get(symbol)
                        if bundle is None:
                            continue
                        execution_result = self.execution_service.execute_request(request, bundle=bundle)
                        forecast = forecast_cache.get((symbol, strategy.value))
                        if forecast is None:
                            forecast = self._forecast_for_bundle(
                                symbol=symbol,
                                strategy_type=strategy.value,
                                bars=bundle.bars,
                                horizon_days=min(payload.max_holding_days, payload.target_dte),
                            )
                            forecast_cache[(symbol, strategy.value)] = forecast
                        hist_key = (symbol, strategy.value, candidate_rule_set_hash)
                        historical = historical_cache.get(hist_key)
                        if historical is None:
                            historical = self._historical_performance(
                                symbol=symbol,
                                strategy_type=strategy.value,
                                candidate_rule_set_hash=candidate_rule_set_hash,
                                before=job.created_at,
                            )
                        ranking = build_ranking_breakdown(
                            execution_result=execution_result,
                            historical_performance=historical,
                            forecast=forecast,
                            strategy_type=strategy.value,
                            account_size=float(payload.account_size),
                        )
                        candidates.append(
                            {
                                "symbol": symbol,
                                "strategy_type": strategy.value,
                                "rule_set_name": rule_set.name,
                                "rule_set_hash": candidate_rule_set_hash,
                                "request_snapshot": request.model_dump(mode="json"),
                                "summary": self._serialize_summary(execution_result.summary),
                                "warnings": execution_result.warnings,
                                "trades": [self._serialize_trade(trade) for trade in execution_result.trades],
                                "equity_curve": [
                                    self._serialize_equity_point(point) for point in execution_result.equity_curve
                                ],
                                "historical": historical.model_dump(mode="json"),
                                "forecast": forecast.model_dump(mode="json"),
                                "ranking": ranking.model_dump(mode="json"),
                            }
                        )
                        job.evaluated_candidate_count += 1
                    except AppError as exc:
                        warnings.append(
                            {
                                "code": "candidate_failed",
                                "message": (
                                    f"{symbol} / {strategy.value} / {rule_set.name} "
                                    f"could not be evaluated ({exc.code})"
                                ),
                                "error_code": exc.code,
                            }
                        )
                    except Exception:  # pragma: no cover - safeguard
                        warnings.append(
                            {
                                "code": "candidate_failed_internal",
                                "message": (
                                    f"{symbol} / {strategy.value} / {rule_set.name} "
                                    f"failed with an unexpected error"
                                ),
                            }
                        )

        if not candidates:
            job.status = "failed"
            job.error_code = "scan_empty"
            job.error_message = "No scan combinations completed successfully."
            job.completed_at = datetime.now(UTC)
            job.warnings_json = warnings
            self.session.commit()
            return job

        sorted_candidates = sorted(
            candidates,
            key=lambda c: recommendation_sort_key(
                (
                    c["symbol"],
                    c["strategy_type"],
                    c["rule_set_name"],
                    self._ranking_response_model(c["ranking"]),
                )
            ),
        )
        rank_lookup = {
            (c["symbol"], c["strategy_type"], c["rule_set_name"]): idx + 1
            for idx, c in enumerate(sorted_candidates)
        }
        selected = sorted_candidates[: payload.max_recommendations]

        for candidate in selected:
            rank = rank_lookup[(candidate["symbol"], candidate["strategy_type"], candidate["rule_set_name"])]
            job.recommendations.append(
                ScannerRecommendation(
                    rank=rank,
                    score=to_decimal(float(candidate["ranking"]["final_score"])),
                    symbol=candidate["symbol"],
                    strategy_type=candidate["strategy_type"],
                    rule_set_name=candidate["rule_set_name"],
                    rule_set_hash=candidate["rule_set_hash"],
                    request_snapshot_json=candidate["request_snapshot"],
                    summary_json=candidate["summary"],
                    warnings_json=candidate["warnings"],
                    trades_json=candidate["trades"],
                    equity_curve_json=candidate["equity_curve"],
                    historical_performance_json=candidate["historical"],
                    forecast_json=candidate["forecast"],
                    ranking_features_json=candidate["ranking"],
                )
            )

        job.recommendation_count = len(selected)
        job.status = "succeeded"
        job.completed_at = datetime.now(UTC)
        job.warnings_json = warnings
        self.session.commit()
        self.session.refresh(job)
        return job

    def list_jobs(self, user: User, limit: int = 50) -> ScannerJobListResponse:
        jobs = self.repository.list_for_user(user.id, limit=limit)
        return ScannerJobListResponse(items=[self._to_job_response(job) for job in jobs])

    def get_job(self, user: User, job_id: UUID) -> ScannerJobResponse:
        job = self.repository.get_for_user(job_id, user.id)
        if job is None:
            raise NotFoundError("Scanner job not found.")
        return self._to_job_response(job)

    def get_recommendations(self, user: User, job_id: UUID) -> ScannerRecommendationListResponse:
        job = self.repository.get_for_user(job_id, user.id, include_recommendations=True)
        if job is None:
            raise NotFoundError("Scanner job not found.")
        return ScannerRecommendationListResponse(
            items=[self._to_recommendation_response(recommendation) for recommendation in job.recommendations]
        )

    def create_scheduled_refresh_jobs(self, limit: int = 25) -> list[ScannerJob]:
        created_jobs: list[ScannerJob] = []
        latest_sources: dict[tuple[UUID, str, str], ScannerJob] = {}
        for source in self.repository.list_refresh_sources(limit=200):
            key = (source.user_id, source.request_hash, source.mode)
            latest_sources.setdefault(key, source)

        refresh_day = datetime.now(UTC).date().isoformat()
        for source in list(latest_sources.values())[:limit]:
            refresh_key = f"{source.user_id}:{source.request_hash}:{refresh_day}:{source.mode}"
            job = ScannerJob(
                user_id=source.user_id,
                parent_job_id=source.id,
                name=source.name,
                status="queued",
                mode=source.mode,
                plan_tier_snapshot=source.plan_tier_snapshot,
                job_kind="scheduled_refresh",
                request_hash=source.request_hash,
                refresh_key=refresh_key,
                refresh_daily=source.refresh_daily,
                refresh_priority=source.refresh_priority,
                candidate_count=source.candidate_count,
                evaluated_candidate_count=0,
                recommendation_count=0,
                request_snapshot_json=source.request_snapshot_json,
                warnings_json=[],
                ranking_version=source.ranking_version,
                engine_version=source.engine_version,
            )
            try:
                nested = self.session.begin_nested()
                self.repository.add(job)
                nested.commit()
                self.session.refresh(job)
                created_jobs.append(job)
            except IntegrityError:
                nested.rollback()
                continue
        if created_jobs:
            self.session.commit()
        return created_jobs

    def build_forecast(
        self,
        *,
        user: User,
        symbol: str,
        strategy_type: str | None,
        horizon_days: int,
    ) -> ForecastEnvelopeResponse:
        ensure_forecasting_access(user.plan_tier, user.subscription_status)
        request = CreateBacktestRunRequest(
            symbol=symbol,
            strategy_type=(strategy_type or "long_call"),
            start_date=datetime.now(UTC).date() - timedelta(days=365),
            end_date=datetime.now(UTC).date() - timedelta(days=1),
            target_dte=max(horizon_days, 7),
            dte_tolerance_days=10,
            max_holding_days=horizon_days,
            account_size=Decimal("10000"),
            risk_per_trade_pct=Decimal("5"),
            commission_per_contract=Decimal("1"),
            entry_rules=[{"type": "rsi", "operator": "lte", "threshold": Decimal("40"), "period": 14}],
        )
        bundle = self.execution_service.market_data_service.prepare_backtest(request)
        forecast = self._forecast_for_bundle(
            symbol=symbol,
            strategy_type=strategy_type,
            bars=bundle.bars,
            horizon_days=horizon_days,
        )
        expected_move_abs_pct = max(
            abs(forecast.expected_return_low_pct),
            abs(forecast.expected_return_high_pct),
        )
        return ForecastEnvelopeResponse(
            forecast=forecast,
            expected_move_abs_pct=expected_move_abs_pct,
        )

    def _validate_limits(self, policy, payload: CreateScannerJobRequest) -> None:
        if len(payload.symbols) > policy.max_symbols:
            raise ValidationError(f"The selected scanner mode allows at most {policy.max_symbols} symbols.")
        if len(payload.strategy_types) > policy.max_strategies:
            raise ValidationError(f"The selected scanner mode allows at most {policy.max_strategies} strategies.")
        if len(payload.rule_sets) > policy.max_rule_sets:
            raise ValidationError(f"The selected scanner mode allows at most {policy.max_rule_sets} rule sets.")
        if payload.max_recommendations > policy.max_recommendations:
            raise ValidationError(
                f"The selected scanner mode allows at most {policy.max_recommendations} recommendations."
            )

    def _count_compatible_candidates(self, payload: CreateScannerJobRequest) -> tuple[int, list[dict[str, Any]]]:
        count = 0
        warnings: list[dict[str, Any]] = []
        for strategy in payload.strategy_types:
            for rule_set in payload.rule_sets:
                if not is_strategy_rule_set_compatible(strategy.value, rule_set.entry_rules):
                    warnings.append(
                        {
                            "code": "incompatible_combination",
                            "message": (
                                f"{strategy.value} is incompatible with the directional bias "
                                f"of rule set '{rule_set.name}'."
                            ),
                        }
                    )
                    continue
                count += len(payload.symbols)
        unique_warnings = {warning["message"]: warning for warning in warnings}
        return count, list(unique_warnings.values())

    def _prepare_bundles(
        self,
        payload: CreateScannerJobRequest,
        warnings: list[dict[str, Any]],
    ) -> dict[str, HistoricalDataBundle]:
        all_rules = [rule for rule_set in payload.rule_sets for rule in rule_set.entry_rules]
        representative = CreateBacktestRunRequest(
            symbol=payload.symbols[0],
            strategy_type=payload.strategy_types[0],
            start_date=payload.start_date,
            end_date=payload.end_date,
            target_dte=payload.target_dte,
            dte_tolerance_days=payload.dte_tolerance_days,
            max_holding_days=payload.max_holding_days,
            account_size=payload.account_size,
            risk_per_trade_pct=payload.risk_per_trade_pct,
            commission_per_contract=payload.commission_per_contract,
            entry_rules=all_rules or [{"type": "rsi", "operator": "lte", "threshold": "40", "period": 14}],
        )
        bundles: dict[str, HistoricalDataBundle] = {}
        for symbol in payload.symbols:
            try:
                req = representative.model_copy(update={"symbol": symbol})
                bundles[symbol] = self.execution_service.market_data_service.prepare_backtest(req)
            except AppError as exc:
                warnings.append({
                    "code": "symbol_data_unavailable",
                    "message": f"{symbol} could not be loaded ({exc.code})",
                    "error_code": exc.code,
                })
        return bundles

    def _batch_historical_performance(
        self,
        payload: CreateScannerJobRequest,
        before: datetime,
    ) -> dict[tuple[str, str, str], Any]:
        keys: list[tuple[str, str, str]] = []
        for symbol in payload.symbols:
            for strategy in payload.strategy_types:
                for rs in payload.rule_sets:
                    if not is_strategy_rule_set_compatible(strategy.value, rs.entry_rules):
                        continue
                    keys.append((symbol, strategy.value, rule_set_hash(rs.entry_rules)))

        if not keys:
            return {}

        raw = self.repository.batch_list_historical_recommendations(keys=keys, before=before)
        result: dict[tuple[str, str, str], Any] = {}
        for key, rows in raw.items():
            observations: list[HistoricalObservation] = []
            for recommendation, completed_at in rows:
                if completed_at is None:
                    continue
                summary = recommendation.summary_json or {}
                observations.append(
                    HistoricalObservation(
                        completed_at=completed_at,
                        win_rate=float(summary.get("win_rate", 0.0)),
                        total_roi_pct=float(summary.get("total_roi_pct", 0.0)),
                        total_net_pnl=float(summary.get("total_net_pnl", 0.0)),
                        max_drawdown_pct=float(summary.get("max_drawdown_pct", 0.0)),
                    )
                )
            result[key] = aggregate_historical_performance(observations, reference_time=before)
        return result

    def _historical_performance(
        self,
        *,
        symbol: str,
        strategy_type: str,
        candidate_rule_set_hash: str,
        before: datetime,
    ):
        observations: list[HistoricalObservation] = []
        for recommendation, completed_at in self.repository.list_historical_recommendations(
            symbol=symbol,
            strategy_type=strategy_type,
            rule_set_hash=candidate_rule_set_hash,
            before=before,
        ):
            if completed_at is None:
                continue
            summary = recommendation.summary_json or {}
            observations.append(
                HistoricalObservation(
                    completed_at=completed_at,
                    win_rate=float(summary.get("win_rate", 0.0)),
                    total_roi_pct=float(summary.get("total_roi_pct", 0.0)),
                    total_net_pnl=float(summary.get("total_net_pnl", 0.0)),
                    max_drawdown_pct=float(summary.get("max_drawdown_pct", 0.0)),
                )
            )
        return aggregate_historical_performance(observations, reference_time=before)

    def _forecast_for_bundle(
        self,
        *,
        symbol: str,
        strategy_type: str | None,
        bars,
        horizon_days: int,
    ) -> HistoricalAnalogForecastResponse:
        try:
            return self.forecaster.forecast(
                symbol=symbol,
                bars=bars,
                horizon_days=horizon_days,
                strategy_type=strategy_type,
            )
        except ValueError:
            fallback_date = bars[-1].trade_date if bars else datetime.now(UTC).date()
            return HistoricalAnalogForecastResponse(
                symbol=symbol,
                strategy_type=strategy_type,
                as_of_date=fallback_date,
                horizon_days=horizon_days,
                analog_count=0,
                expected_return_low_pct=Decimal("0"),
                expected_return_median_pct=Decimal("0"),
                expected_return_high_pct=Decimal("0"),
                positive_outcome_rate_pct=Decimal("0"),
                summary="Not enough analog history was available to build a bounded expected range for this symbol.",
                disclaimer=(
                    "This is a bounded probability range based on historical analogs "
                    "under similar daily-bar conditions. "
                    "It is not a prediction, certainty, or financial advice."
                ),
                analog_dates=[],
            )

    @staticmethod
    def _serialize_summary(summary) -> dict[str, Any]:
        return {
            "trade_count": summary.trade_count,
            "win_rate": float(to_decimal(summary.win_rate)),
            "total_roi_pct": float(to_decimal(summary.total_roi_pct)),
            "average_win_amount": float(to_decimal(summary.average_win_amount)),
            "average_loss_amount": float(to_decimal(summary.average_loss_amount)),
            "average_holding_period_days": float(to_decimal(summary.average_holding_period_days)),
            "average_dte_at_open": float(to_decimal(summary.average_dte_at_open)),
            "max_drawdown_pct": float(to_decimal(summary.max_drawdown_pct)),
            "total_commissions": float(to_decimal(summary.total_commissions)),
            "total_net_pnl": float(to_decimal(summary.total_net_pnl)),
            "starting_equity": float(to_decimal(summary.starting_equity)),
            "ending_equity": float(to_decimal(summary.ending_equity)),
        }

    @staticmethod
    def _serialize_trade(trade) -> dict[str, Any]:
        return {
            "option_ticker": trade.option_ticker,
            "strategy_type": trade.strategy_type,
            "underlying_symbol": trade.underlying_symbol,
            "entry_date": trade.entry_date.isoformat(),
            "exit_date": trade.exit_date.isoformat(),
            "expiration_date": trade.expiration_date.isoformat(),
            "quantity": trade.quantity,
            "dte_at_open": trade.dte_at_open,
            "holding_period_days": trade.holding_period_days,
            "entry_underlying_close": float(to_decimal(trade.entry_underlying_close)),
            "exit_underlying_close": float(to_decimal(trade.exit_underlying_close)),
            "entry_mid": float(to_decimal(trade.entry_mid)),
            "exit_mid": float(to_decimal(trade.exit_mid)),
            "gross_pnl": float(to_decimal(trade.gross_pnl)),
            "net_pnl": float(to_decimal(trade.net_pnl)),
            "total_commissions": float(to_decimal(trade.total_commissions)),
            "entry_reason": trade.entry_reason,
            "exit_reason": trade.exit_reason,
            "detail_json": trade.detail_json,
        }

    @staticmethod
    def _serialize_equity_point(point) -> dict[str, Any]:
        return {
            "trade_date": point.trade_date.isoformat(),
            "equity": float(to_decimal(point.equity)),
            "cash": float(to_decimal(point.cash)),
            "position_value": float(to_decimal(point.position_value)),
            "drawdown_pct": float(to_decimal(point.drawdown_pct)),
        }

    @staticmethod
    def _ranking_response_model(payload: dict[str, Any]):
        from backtestforecast.schemas.scans import RankingBreakdownResponse

        return RankingBreakdownResponse.model_validate(payload)

    @staticmethod
    def _request_hash(payload: CreateScannerJobRequest) -> str:
        base_payload = payload.model_dump(mode="json")
        base_payload.pop("name", None)
        base_payload.pop("idempotency_key", None)
        return hash_payload(base_payload)

    @staticmethod
    def _to_job_response(job: ScannerJob) -> ScannerJobResponse:
        return ScannerJobResponse(
            id=job.id,
            name=job.name,
            status=job.status,
            mode=job.mode,
            plan_tier_snapshot=job.plan_tier_snapshot,
            job_kind=job.job_kind,
            candidate_count=job.candidate_count,
            evaluated_candidate_count=job.evaluated_candidate_count,
            recommendation_count=job.recommendation_count,
            refresh_daily=job.refresh_daily,
            refresh_priority=job.refresh_priority,
            warnings=job.warnings_json,
            error_code=job.error_code,
            error_message=job.error_message,
            created_at=job.created_at,
            started_at=job.started_at,
            completed_at=job.completed_at,
        )

    @staticmethod
    def _to_recommendation_response(recommendation: ScannerRecommendation) -> ScannerRecommendationResponse:
        return ScannerRecommendationResponse(
            id=recommendation.id,
            rank=recommendation.rank,
            score=recommendation.score,
            symbol=recommendation.symbol,
            strategy_type=recommendation.strategy_type,
            rule_set_name=recommendation.rule_set_name,
            request_snapshot=recommendation.request_snapshot_json,
            summary=BacktestSummaryResponse.model_validate(recommendation.summary_json),
            warnings=recommendation.warnings_json,
            historical_performance=recommendation.historical_performance_json,
            forecast=recommendation.forecast_json,
            ranking_breakdown=recommendation.ranking_features_json,
            trades=[BacktestTradeResponse.model_validate(item) for item in recommendation.trades_json],
            equity_curve=[EquityCurvePointResponse.model_validate(item) for item in recommendation.equity_curve_json],
        )
