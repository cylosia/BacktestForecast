import type {
  AnalysisDetailResponse,
  AnalysisListResponse,
  BacktestRunDetailResponse,
  BacktestRunListResponse,
  BacktestRunStatusResponse,
  CheckoutSessionResponse,
  CompareBacktestsResponse,
  CurrentUserResponse,
  DailyPicksResponse,
  ExportJobResponse,
  ForecastEnvelopeResponse,
  MetaResponse,
  PipelineHistoryResponse,
  PortalSessionResponse,
  ScannerJobListResponse,
  ScannerJobResponse,
  ScannerRecommendationListResponse,
  StrategyCatalogResponse,
  SweepJobListResponse,
  SweepJobResponse,
  SweepResultListResponse,
  SymbolAnalysisSummary,
} from "@backtestforecast/api-client";
import type {
  MultiStepRunStatusResponse,
  MultiStepRunDetailResponse,
  MultiStepRunListResponse,
  MultiSymbolRunStatusResponse,
  MultiSymbolRunDetailResponse,
  MultiSymbolRunListResponse,
} from "@/lib/api/multi-workflow-types";

export interface PricingIntervalContract {
  price_id: string | null;
  unit_amount_usd: number | null;
  display_price: string;
  available: boolean;
}

export interface PricingPlanContract {
  tier: "free" | "pro" | "premium";
  title: string;
  headline: string;
  description: string;
  features: string[];
  monthly: PricingIntervalContract | null;
  yearly: PricingIntervalContract | null;
}

export interface PricingContractResponseShape {
  currency: string;
  checkout_authoritative: boolean;
  plans: PricingPlanContract[];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value != null && typeof value === "object" && !Array.isArray(value);
}

function assertString(value: unknown, label: string): void {
  if (typeof value !== "string") throw new Error(`${label} must be a string`);
}

function assertBoolean(value: unknown, label: string): void {
  if (typeof value !== "boolean") throw new Error(`${label} must be a boolean`);
}

function assertNumber(value: unknown, label: string): void {
  if (typeof value !== "number" || Number.isNaN(value)) throw new Error(`${label} must be a number`);
}

function assertArray(value: unknown, label: string): asserts value is unknown[] {
  if (!Array.isArray(value)) throw new Error(`${label} must be an array`);
}

function assertStringArray(value: unknown, label: string): void {
  assertArray(value, label);
  for (const [index, item] of value.entries()) {
    assertString(item, `${label}[${index}]`);
  }
}

function assertOptionalString(value: unknown, label: string): void {
  if (!(value == null || typeof value === "string")) throw new Error(`${label} must be null or a string`);
}

function assertOptionalNumber(value: unknown, label: string): void {
  if (!(value == null || (typeof value === "number" && !Number.isNaN(value)))) {
    throw new Error(`${label} must be null or a number`);
  }
}

function assertNumericLike(value: unknown, label: string): void {
  if (typeof value === "number" && !Number.isNaN(value)) return;
  if (typeof value === "string" && value.trim().length > 0) return;
  throw new Error(`${label} must be a number or numeric string`);
}

function assertOptionalNumericLike(value: unknown, label: string): void {
  if (value == null) return;
  assertNumericLike(value, label);
}

function assertOptionalBoolean(value: unknown, label: string): void {
  if (!(value == null || typeof value === "boolean")) {
    throw new Error(`${label} must be null or a boolean`);
  }
}

type PaginatedCollection = Record<string, unknown> & {
  items: unknown[];
  total: number;
  offset: number;
  limit: number;
  next_cursor?: string | null;
};

function assertPaginatedCollection(value: unknown, label: string): asserts value is PaginatedCollection {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertArray(value.items, `${label}.items`);
  assertNumber(value.total, `${label}.total`);
  assertNumber(value.offset, `${label}.offset`);
  assertNumber(value.limit, `${label}.limit`);
  if (!(value.next_cursor == null || typeof value.next_cursor === "string")) {
    throw new Error(`${label}.next_cursor must be null or a string`);
  }
}

function validateSummary(summary: unknown, label: string): void {
  if (!isRecord(summary)) throw new Error(`${label} must be an object`);
  assertNumber(summary.trade_count, `${label}.trade_count`);
  assertOptionalNumber(summary.decided_trades, `${label}.decided_trades`);
  assertNumericLike(summary.win_rate, `${label}.win_rate`);
  assertNumericLike(summary.total_roi_pct, `${label}.total_roi_pct`);
  assertNumericLike(summary.average_win_amount, `${label}.average_win_amount`);
  assertNumericLike(summary.average_loss_amount, `${label}.average_loss_amount`);
  assertNumericLike(summary.average_holding_period_days, `${label}.average_holding_period_days`);
  assertNumericLike(summary.average_dte_at_open, `${label}.average_dte_at_open`);
  assertNumericLike(summary.max_drawdown_pct, `${label}.max_drawdown_pct`);
  assertNumericLike(summary.total_commissions, `${label}.total_commissions`);
  assertNumericLike(summary.total_net_pnl, `${label}.total_net_pnl`);
  assertNumericLike(summary.starting_equity, `${label}.starting_equity`);
  assertNumericLike(summary.ending_equity, `${label}.ending_equity`);
  assertOptionalNumericLike(summary.profit_factor, `${label}.profit_factor`);
  assertOptionalNumericLike(summary.payoff_ratio, `${label}.payoff_ratio`);
  assertNumericLike(summary.expectancy, `${label}.expectancy`);
  assertOptionalNumericLike(summary.sharpe_ratio, `${label}.sharpe_ratio`);
  assertOptionalNumericLike(summary.sortino_ratio, `${label}.sortino_ratio`);
  assertOptionalNumericLike(summary.cagr_pct, `${label}.cagr_pct`);
  assertOptionalNumericLike(summary.calmar_ratio, `${label}.calmar_ratio`);
  assertNumber(summary.max_consecutive_wins, `${label}.max_consecutive_wins`);
  assertNumber(summary.max_consecutive_losses, `${label}.max_consecutive_losses`);
  assertOptionalNumericLike(summary.recovery_factor, `${label}.recovery_factor`);
}

function validateOptionalSummary(summary: unknown, label: string): void {
  if (summary == null) return;
  validateSummary(summary, label);
}

function validateWarningList(value: unknown, label: string): void {
  assertArray(value, label);
  for (const [index, item] of value.entries()) {
    if (!isRecord(item)) throw new Error(`${label}[${index}] must be an object`);
    assertString(item.code, `${label}[${index}].code`);
    assertString(item.message, `${label}[${index}].message`);
    if (!(item.severity == null || typeof item.severity === "string")) {
      throw new Error(`${label}[${index}].severity must be null or a string`);
    }
    if (!(item.error_code == null || typeof item.error_code === "string")) {
      throw new Error(`${label}[${index}].error_code must be null or a string`);
    }
  }
}

function validateFeatureAccess(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertString(value.plan_tier, `${label}.plan_tier`);
  assertOptionalNumber(value.monthly_backtest_quota, `${label}.monthly_backtest_quota`);
  assertOptionalNumber(value.history_days, `${label}.history_days`);
  assertNumber(value.history_item_limit, `${label}.history_item_limit`);
  assertNumber(value.side_by_side_comparison_limit, `${label}.side_by_side_comparison_limit`);
  assertBoolean(value.forecasting_access, `${label}.forecasting_access`);
  assertArray(value.export_formats, `${label}.export_formats`);
  assertArray(value.scanner_modes, `${label}.scanner_modes`);
  assertArray(value.scanner_basic_allowed_strategy_types, `${label}.scanner_basic_allowed_strategy_types`);
  assertArray(value.scanner_advanced_allowed_strategy_types, `${label}.scanner_advanced_allowed_strategy_types`);
  assertNumber(value.max_scanner_window_days, `${label}.max_scanner_window_days`);
  assertNumber(value.max_sweep_window_days, `${label}.max_sweep_window_days`);
  assertOptionalBoolean(value.cancel_at_period_end, `${label}.cancel_at_period_end`);
}

function validateMetaFeatures(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertBoolean(value.backtests, `${label}.backtests`);
  assertBoolean(value.scanner, `${label}.scanner`);
  assertBoolean(value.exports, `${label}.exports`);
  assertBoolean(value.forecasts, `${label}.forecasts`);
  assertBoolean(value.analysis, `${label}.analysis`);
  assertBoolean(value.daily_picks, `${label}.daily_picks`);
  assertBoolean(value.billing, `${label}.billing`);
  assertBoolean(value.sweeps, `${label}.sweeps`);
}

function validateUsageSummary(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertNumber(value.backtests_used_this_month, `${label}.backtests_used_this_month`);
  assertOptionalNumber(value.backtests_remaining_this_month, `${label}.backtests_remaining_this_month`);
}

function validatePipelineStats(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertNumber(value.symbols_screened, `${label}.symbols_screened`);
  assertNumber(value.symbols_after_screen, `${label}.symbols_after_screen`);
  assertNumber(value.pairs_generated, `${label}.pairs_generated`);
  assertNumber(value.quick_backtests_run, `${label}.quick_backtests_run`);
  assertNumber(value.full_backtests_run, `${label}.full_backtests_run`);
  assertNumber(value.recommendations_produced, `${label}.recommendations_produced`);
  assertOptionalNumber(value.duration_seconds, `${label}.duration_seconds`);
  assertOptionalString(value.completed_at, `${label}.completed_at`);
}

function validateDailyPickForecast(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertString(value.symbol, `${label}.symbol`);
  assertString(value.strategy_type, `${label}.strategy_type`);
  assertString(value.as_of_date, `${label}.as_of_date`);
  assertNumber(value.horizon_days, `${label}.horizon_days`);
  assertOptionalNumber(value.trading_days_used, `${label}.trading_days_used`);
  assertNumericLike(value.expected_return_low_pct, `${label}.expected_return_low_pct`);
  assertNumericLike(value.expected_return_median_pct, `${label}.expected_return_median_pct`);
  assertNumericLike(value.expected_return_high_pct, `${label}.expected_return_high_pct`);
  assertNumericLike(value.positive_outcome_rate_pct, `${label}.positive_outcome_rate_pct`);
  assertNumber(value.analog_count, `${label}.analog_count`);
  assertOptionalNumber(value.analogs_used, `${label}.analogs_used`);
  assertString(value.summary, `${label}.summary`);
  assertString(value.disclaimer, `${label}.disclaimer`);
  if (value.analog_dates != null) {
    assertStringArray(value.analog_dates, `${label}.analog_dates`);
  }
  assertOptionalNumber(value.analog_dates_shown, `${label}.analog_dates_shown`);
  assertOptionalNumber(value.analog_dates_total, `${label}.analog_dates_total`);
}

function validateForecastPayload(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertString(value.symbol, `${label}.symbol`);
  assertString(value.strategy_type, `${label}.strategy_type`);
  assertString(value.as_of_date, `${label}.as_of_date`);
  assertNumber(value.horizon_days, `${label}.horizon_days`);
  assertOptionalNumber(value.trading_days_used, `${label}.trading_days_used`);
  assertNumber(value.analog_count, `${label}.analog_count`);
  assertOptionalNumber(value.analogs_used, `${label}.analogs_used`);
  assertNumericLike(value.expected_return_low_pct, `${label}.expected_return_low_pct`);
  assertNumericLike(value.expected_return_median_pct, `${label}.expected_return_median_pct`);
  assertNumericLike(value.expected_return_high_pct, `${label}.expected_return_high_pct`);
  assertNumericLike(value.positive_outcome_rate_pct, `${label}.positive_outcome_rate_pct`);
  assertString(value.summary, `${label}.summary`);
  assertString(value.disclaimer, `${label}.disclaimer`);
  if (value.analog_dates != null) {
    assertStringArray(value.analog_dates, `${label}.analog_dates`);
  }
  assertOptionalNumber(value.analog_dates_shown, `${label}.analog_dates_shown`);
  assertOptionalNumber(value.analog_dates_total, `${label}.analog_dates_total`);
}

function validateMultiSymbolDefinition(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertString(value.symbol, `${label}.symbol`);
  assertNumericLike(value.risk_per_trade_pct, `${label}.risk_per_trade_pct`);
  assertNumber(value.max_open_positions, `${label}.max_open_positions`);
  assertOptionalNumericLike(value.capital_allocation_pct, `${label}.capital_allocation_pct`);
}

function validateMultiSymbolTrade(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertString(value.id, `${label}.id`);
  assertString(value.trade_group_id, `${label}.trade_group_id`);
  assertString(value.symbol, `${label}.symbol`);
  assertString(value.option_ticker, `${label}.option_ticker`);
  assertString(value.strategy_type, `${label}.strategy_type`);
  assertString(value.entry_date, `${label}.entry_date`);
  assertString(value.exit_date, `${label}.exit_date`);
  assertNumber(value.quantity, `${label}.quantity`);
  assertNumericLike(value.gross_pnl, `${label}.gross_pnl`);
  assertNumericLike(value.net_pnl, `${label}.net_pnl`);
  assertNumericLike(value.total_commissions, `${label}.total_commissions`);
  assertString(value.entry_reason, `${label}.entry_reason`);
  assertString(value.exit_reason, `${label}.exit_reason`);
}

function validateMultiSymbolRunListItem(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertString(value.id, `${label}.id`);
  assertOptionalString(value.name, `${label}.name`);
  assertString(value.status, `${label}.status`);
  assertString(value.created_at, `${label}.created_at`);
  assertOptionalString(value.completed_at, `${label}.completed_at`);
  assertStringArray(value.symbols, `${label}.symbols`);
  validateSummary(value.summary, `${label}.summary`);
}

function validateMultiStepRunListItem(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertString(value.id, `${label}.id`);
  assertOptionalString(value.name, `${label}.name`);
  assertString(value.symbol, `${label}.symbol`);
  assertString(value.workflow_type, `${label}.workflow_type`);
  assertString(value.status, `${label}.status`);
  assertString(value.created_at, `${label}.created_at`);
  assertOptionalString(value.completed_at, `${label}.completed_at`);
  validateSummary(value.summary, `${label}.summary`);
}

function validateAnalysisForecast(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  if (value.no_results_message != null) {
    assertString(value.no_results_message, `${label}.no_results_message`);
    return;
  }
  assertString(value.symbol, `${label}.symbol`);
  assertString(value.strategy_type, `${label}.strategy_type`);
  assertString(value.as_of_date, `${label}.as_of_date`);
  assertNumber(value.horizon_days, `${label}.horizon_days`);
  assertOptionalNumber(value.trading_days_used, `${label}.trading_days_used`);
  assertNumber(value.analog_count, `${label}.analog_count`);
  assertOptionalNumber(value.analogs_used, `${label}.analogs_used`);
  assertNumericLike(value.expected_return_low_pct, `${label}.expected_return_low_pct`);
  assertNumericLike(value.expected_return_median_pct, `${label}.expected_return_median_pct`);
  assertNumericLike(value.expected_return_high_pct, `${label}.expected_return_high_pct`);
  assertNumericLike(value.positive_outcome_rate_pct, `${label}.positive_outcome_rate_pct`);
  assertString(value.summary, `${label}.summary`);
  assertString(value.disclaimer, `${label}.disclaimer`);
  if (value.analog_dates != null) {
    assertStringArray(value.analog_dates, `${label}.analog_dates`);
  }
  assertOptionalNumber(value.analog_dates_shown, `${label}.analog_dates_shown`);
  assertOptionalNumber(value.analog_dates_total, `${label}.analog_dates_total`);
}

function validateRegimeDetail(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  if (value.regimes != null) {
    assertStringArray(value.regimes, `${label}.regimes`);
  }
  assertOptionalNumber(value.rsi_14, `${label}.rsi_14`);
  assertOptionalNumber(value.ema_8, `${label}.ema_8`);
  assertOptionalNumber(value.ema_21, `${label}.ema_21`);
  assertOptionalNumber(value.sma_50, `${label}.sma_50`);
  assertOptionalNumber(value.sma_200, `${label}.sma_200`);
  assertOptionalNumber(value.realized_vol_20, `${label}.realized_vol_20`);
  assertOptionalNumber(value.iv_rank_proxy, `${label}.iv_rank_proxy`);
  assertOptionalNumber(value.volume_ratio, `${label}.volume_ratio`);
  assertOptionalNumber(value.close_price, `${label}.close_price`);
}

function validateLandscapeCell(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertString(value.strategy_type, `${label}.strategy_type`);
  assertString(value.strategy_label, `${label}.strategy_label`);
  assertNumber(value.target_dte, `${label}.target_dte`);
  if (!(value.config == null || isRecord(value.config))) {
    throw new Error(`${label}.config must be null or an object`);
  }
  assertNumber(value.trade_count, `${label}.trade_count`);
  assertNumber(value.decided_trades, `${label}.decided_trades`);
  assertNumber(value.win_rate, `${label}.win_rate`);
  assertNumber(value.total_roi_pct, `${label}.total_roi_pct`);
  assertNumber(value.max_drawdown_pct, `${label}.max_drawdown_pct`);
  assertNumber(value.score, `${label}.score`);
}

function validateHistoricalPerformance(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertNumber(value.sample_count, `${label}.sample_count`);
  assertNumericLike(value.effective_sample_size, `${label}.effective_sample_size`);
  assertNumericLike(value.weighted_win_rate, `${label}.weighted_win_rate`);
  assertNumericLike(value.weighted_total_roi_pct, `${label}.weighted_total_roi_pct`);
  assertNumericLike(value.weighted_max_drawdown_pct, `${label}.weighted_max_drawdown_pct`);
  assertNumber(value.recency_half_life_days, `${label}.recency_half_life_days`);
  assertOptionalString(value.last_observed_at, `${label}.last_observed_at`);
}

function validateRankingBreakdown(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertNumericLike(value.current_performance_score, `${label}.current_performance_score`);
  assertNumericLike(value.historical_performance_score, `${label}.historical_performance_score`);
  assertNumericLike(value.forecast_alignment_score, `${label}.forecast_alignment_score`);
  assertNumericLike(value.final_score, `${label}.final_score`);
  assertStringArray(value.reasoning, `${label}.reasoning`);
}

function validateDailyPickItem(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertNumber(value.rank, `${label}.rank`);
  if (!(typeof value.score === "number" || typeof value.score === "string")) {
    throw new Error(`${label}.score must be a number or string`);
  }
  assertString(value.symbol, `${label}.symbol`);
  assertString(value.strategy_type, `${label}.strategy_type`);
  assertArray(value.regime_labels, `${label}.regime_labels`);
  if (!(typeof value.close_price === "number" || typeof value.close_price === "string")) {
    throw new Error(`${label}.close_price must be a number or string`);
  }
  assertNumber(value.target_dte, `${label}.target_dte`);
  if (!(value.config_snapshot == null || isRecord(value.config_snapshot))) {
    throw new Error(`${label}.config_snapshot must be null or an object`);
  }
  validateOptionalSummary(value.summary, `${label}.summary`);
  if (value.forecast != null) {
    validateDailyPickForecast(value.forecast, `${label}.forecast`);
  }
}

function validatePipelineHistoryItem(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertString(value.id, `${label}.id`);
  assertString(value.trade_date, `${label}.trade_date`);
  assertString(value.status, `${label}.status`);
  assertNumber(value.symbols_screened, `${label}.symbols_screened`);
  assertNumber(value.recommendations_produced, `${label}.recommendations_produced`);
  assertOptionalNumber(value.duration_seconds, `${label}.duration_seconds`);
  assertOptionalString(value.completed_at, `${label}.completed_at`);
  assertOptionalString(value.error_code, `${label}.error_code`);
  assertOptionalString(value.error_message, `${label}.error_message`);
}

function validateStrategyCatalogItem(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertString(value.strategy_type, `${label}.strategy_type`);
  assertString(value.label, `${label}.label`);
  assertString(value.short_description, `${label}.short_description`);
  assertString(value.category, `${label}.category`);
  assertString(value.bias, `${label}.bias`);
  assertNumber(value.leg_count, `${label}.leg_count`);
  assertString(value.min_tier, `${label}.min_tier`);
  assertString(value.max_loss_description, `${label}.max_loss_description`);
  assertOptionalString(value.notes, `${label}.notes`);
  assertArray(value.tags, `${label}.tags`);
}

function validatePricingInterval(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertOptionalString(value.price_id, `${label}.price_id`);
  assertOptionalNumber(value.unit_amount_usd, `${label}.unit_amount_usd`);
  assertString(value.display_price, `${label}.display_price`);
  assertBoolean(value.available, `${label}.available`);
}

function validatePricingPlan(value: unknown, label: string): void {
  if (!isRecord(value)) throw new Error(`${label} must be an object`);
  assertString(value.tier, `${label}.tier`);
  if (!["free", "pro", "premium"].includes(String(value.tier))) {
    throw new Error(`${label}.tier must be one of free, pro, premium`);
  }
  assertString(value.title, `${label}.title`);
  assertString(value.headline, `${label}.headline`);
  assertString(value.description, `${label}.description`);
  assertStringArray(value.features, `${label}.features`);
  if (!(value.monthly == null || isRecord(value.monthly))) {
    throw new Error(`${label}.monthly must be null or an object`);
  }
  if (!(value.yearly == null || isRecord(value.yearly))) {
    throw new Error(`${label}.yearly must be null or an object`);
  }
  if (value.monthly != null) {
    validatePricingInterval(value.monthly, `${label}.monthly`);
  }
  if (value.yearly != null) {
    validatePricingInterval(value.yearly, `${label}.yearly`);
  }
}

function validateBacktestRunLike(run: unknown, label: string): void {
  if (!isRecord(run)) throw new Error(`${label} must be an object`);
  assertString(run.id, `${label}.id`);
  assertString(run.symbol, `${label}.symbol`);
  assertString(run.strategy_type, `${label}.strategy_type`);
  validateSummary(run.summary, `${label}.summary`);
  assertString(run.summary_provenance, `${label}.summary_provenance`);
}

function validateRiskFreeRateCurvePoints(value: unknown, label: string): void {
  assertArray(value, label);
  for (const [index, item] of value.entries()) {
    if (!isRecord(item)) throw new Error(`${label}[${index}] must be an object`);
    assertString(item.trade_date, `${label}[${index}].trade_date`);
    assertNumericLike(item.rate, `${label}[${index}].rate`);
  }
}

function validateAnalysisSummaryLike(item: unknown, label: string): void {
  if (!isRecord(item)) throw new Error(`${label} must be an object`);
  assertString(item.id, `${label}.id`);
  assertString(item.symbol, `${label}.symbol`);
  assertString(item.status, `${label}.status`);
  assertString(item.stage, `${label}.stage`);
  assertNumber(item.strategies_tested, `${label}.strategies_tested`);
  assertNumber(item.configs_tested, `${label}.configs_tested`);
  assertNumber(item.top_results_count, `${label}.top_results_count`);
  assertString(item.created_at, `${label}.created_at`);
}

export function validateBacktestRunListResponse(data: unknown): BacktestRunListResponse {
  assertPaginatedCollection(data, "backtests");
  for (const [index, item] of data.items.entries()) {
    validateBacktestRunLike(item, `backtests.items[${index}]`);
  }
  return data as BacktestRunListResponse;
}

export function validateAnalysisListResponse(data: unknown): AnalysisListResponse {
  assertPaginatedCollection(data, "analysis_list");
  for (const [index, item] of data.items.entries()) {
    validateAnalysisSummaryLike(item, `analysis_list.items[${index}]`);
  }
  return data as AnalysisListResponse;
}

export function validateBacktestRunDetailResponse(data: unknown): BacktestRunDetailResponse {
  validateBacktestRunLike(data, "backtest");
  if (!isRecord(data)) throw new Error("backtest must be an object");
  validateWarningList(data.warnings, "backtest.warnings");
  assertArray(data.trades, "backtest.trades");
  assertArray(data.equity_curve, "backtest.equity_curve");
  assertBoolean(data.equity_curve_truncated, "backtest.equity_curve_truncated");
  assertNumber(data.trade_items_omitted, "backtest.trade_items_omitted");
  assertNumber(data.equity_curve_points_omitted, "backtest.equity_curve_points_omitted");
  assertOptionalNumericLike(data.risk_free_rate, "backtest.risk_free_rate");
  assertOptionalString(data.risk_free_rate_model, "backtest.risk_free_rate_model");
  if (data.risk_free_rate_curve_points != null) {
    validateRiskFreeRateCurvePoints(data.risk_free_rate_curve_points, "backtest.risk_free_rate_curve_points");
  }
  return data as BacktestRunDetailResponse;
}

export function validateCompareBacktestsResponse(data: unknown): CompareBacktestsResponse {
  if (!isRecord(data)) throw new Error("compare response must be an object");
  assertArray(data.items, "compare.items");
  assertBoolean(data.trades_truncated, "compare.trades_truncated");
  for (const [index, item] of data.items.entries()) {
    validateBacktestRunLike(item, `compare.items[${index}]`);
    if (!isRecord(item)) throw new Error(`compare.items[${index}] must be an object`);
    validateWarningList(item.warnings, `compare.items[${index}].warnings`);
    assertArray(item.trades, `compare.items[${index}].trades`);
    assertArray(item.equity_curve, `compare.items[${index}].equity_curve`);
    assertBoolean(item.equity_curve_truncated, `compare.items[${index}].equity_curve_truncated`);
    assertNumber(item.trade_items_omitted, `compare.items[${index}].trade_items_omitted`);
    assertNumber(item.equity_curve_points_omitted, `compare.items[${index}].equity_curve_points_omitted`);
    assertOptionalNumericLike(item.risk_free_rate, `compare.items[${index}].risk_free_rate`);
    assertOptionalString(item.risk_free_rate_model, `compare.items[${index}].risk_free_rate_model`);
    if (item.risk_free_rate_curve_points != null) {
      validateRiskFreeRateCurvePoints(
        item.risk_free_rate_curve_points,
        `compare.items[${index}].risk_free_rate_curve_points`,
      );
    }
  }
  return data as CompareBacktestsResponse;
}

export function validateAnalysisDetailResponse(data: unknown): AnalysisDetailResponse {
  validateAnalysisSummaryLike(data, "analysis");
  if (!isRecord(data)) throw new Error("analysis response must be an object");
  if (data.integrity_warnings != null) {
    assertStringArray(data.integrity_warnings, "analysis.integrity_warnings");
  }
  if (!(data.top_results == null || Array.isArray(data.top_results))) {
    throw new Error("analysis.top_results must be null or an array");
  }
  if (!(data.regime == null || isRecord(data.regime))) {
    throw new Error("analysis.regime must be null or an object");
  }
  if (!(data.landscape == null || Array.isArray(data.landscape))) {
    throw new Error("analysis.landscape must be null or an array");
  }
  if (!(data.forecast == null || isRecord(data.forecast))) {
    throw new Error("analysis.forecast must be null or an object");
  }
  if (data.forecast != null) {
    validateAnalysisForecast(data.forecast, "analysis.forecast");
  }
  if (data.regime != null) {
    validateRegimeDetail(data.regime, "analysis.regime");
  }
  if (Array.isArray(data.landscape)) {
    for (const [index, item] of data.landscape.entries()) {
      validateLandscapeCell(item, `analysis.landscape[${index}]`);
    }
  }
  if (Array.isArray(data.top_results)) {
    for (const [index, item] of data.top_results.entries()) {
      if (!isRecord(item)) throw new Error(`analysis.top_results[${index}] must be an object`);
      validateOptionalSummary(item.summary, `analysis.top_results[${index}].summary`);
      assertArray(item.trades, `analysis.top_results[${index}].trades`);
      assertArray(item.equity_curve, `analysis.top_results[${index}].equity_curve`);
      if (item.forecast != null) {
        validateAnalysisForecast(item.forecast, `analysis.top_results[${index}].forecast`);
      }
    }
  }
  return data as AnalysisDetailResponse;
}

export function validateBacktestRunStatusResponse(data: unknown): BacktestRunStatusResponse {
  if (!isRecord(data)) throw new Error("backtest status must be an object");
  assertString(data.id, "backtest_status.id");
  assertString(data.status, "backtest_status.status");
  return data as BacktestRunStatusResponse;
}

export function validateCheckoutSessionResponse(data: unknown): CheckoutSessionResponse {
  if (!isRecord(data)) throw new Error("checkout session must be an object");
  assertString(data.checkout_url, "checkout_session.checkout_url");
  assertString(data.session_id, "checkout_session.session_id");
  assertString(data.tier, "checkout_session.tier");
  assertString(data.billing_interval, "checkout_session.billing_interval");
  return data as CheckoutSessionResponse;
}

export function validatePortalSessionResponse(data: unknown): PortalSessionResponse {
  if (!isRecord(data)) throw new Error("portal session must be an object");
  assertString(data.portal_url, "portal_session.portal_url");
  return data as PortalSessionResponse;
}

export function validateExportJobResponse(data: unknown): ExportJobResponse {
  if (!isRecord(data)) throw new Error("export job must be an object");
  assertString(data.id, "export_job.id");
  assertString(data.backtest_run_id, "export_job.backtest_run_id");
  assertString(data.export_format, "export_job.export_format");
  assertString(data.status, "export_job.status");
  assertString(data.file_name, "export_job.file_name");
  assertString(data.mime_type, "export_job.mime_type");
  assertNumber(data.size_bytes, "export_job.size_bytes");
  assertString(data.created_at, "export_job.created_at");
  assertOptionalNumericLike(data.risk_free_rate, "export_job.risk_free_rate");
  assertOptionalString(data.risk_free_rate_source, "export_job.risk_free_rate_source");
  assertOptionalString(data.risk_free_rate_model, "export_job.risk_free_rate_model");
  if (data.risk_free_rate_curve_points != null) {
    validateRiskFreeRateCurvePoints(data.risk_free_rate_curve_points, "export_job.risk_free_rate_curve_points");
  }
  return data as ExportJobResponse;
}

export function validateForecastEnvelopeResponse(data: unknown): ForecastEnvelopeResponse {
  if (!isRecord(data)) throw new Error("forecast envelope must be an object");
  assertString(data.expected_move_abs_pct, "forecast.expected_move_abs_pct");
  validateForecastPayload(data.forecast, "forecast.forecast");
  assertString(data.probabilistic_note, "forecast.probabilistic_note");
  return data as ForecastEnvelopeResponse;
}

export function validateSymbolAnalysisSummary(data: unknown): SymbolAnalysisSummary {
  validateAnalysisSummaryLike(data, "analysis_summary");
  return data as SymbolAnalysisSummary;
}

export function validateCurrentUserResponse(data: unknown): CurrentUserResponse {
  if (!isRecord(data)) throw new Error("current user response must be an object");
  assertString(data.id, "current_user.id");
  assertString(data.clerk_user_id, "current_user.clerk_user_id");
  assertOptionalString(data.email, "current_user.email");
  assertString(data.plan_tier, "current_user.plan_tier");
  assertOptionalString(data.subscription_status, "current_user.subscription_status");
  assertOptionalString(data.subscription_billing_interval, "current_user.subscription_billing_interval");
  assertOptionalString(data.subscription_current_period_end, "current_user.subscription_current_period_end");
  assertBoolean(data.cancel_at_period_end, "current_user.cancel_at_period_end");
  assertString(data.created_at, "current_user.created_at");
  validateFeatureAccess(data.features, "current_user.features");
  validateUsageSummary(data.usage, "current_user.usage");
  return data as CurrentUserResponse;
}

export function validateMetaResponse(data: unknown): MetaResponse {
  if (!isRecord(data)) throw new Error("meta response must be an object");
  assertString(data.service, "meta.service");
  assertString(data.version, "meta.version");
  if (!(data.billing_enabled == null || typeof data.billing_enabled === "boolean")) {
    throw new Error("meta.billing_enabled must be null or a boolean");
  }
  assertOptionalString(data.environment, "meta.environment");
  assertOptionalString(data.daily_picks_schedule_utc, "meta.daily_picks_schedule_utc");
  if (data.features != null) {
    validateMetaFeatures(data.features, "meta.features");
  }
  return data as MetaResponse;
}

export function validateDailyPicksResponse(data: unknown): DailyPicksResponse {
  if (!isRecord(data)) throw new Error("daily picks response must be an object");
  assertString(data.status, "daily_picks.status");
  assertOptionalString(data.trade_date, "daily_picks.trade_date");
  assertOptionalString(data.pipeline_run_id, "daily_picks.pipeline_run_id");
  if (data.integrity_warnings != null) {
    assertStringArray(data.integrity_warnings, "daily_picks.integrity_warnings");
  }
  if (data.pipeline_stats != null) {
    validatePipelineStats(data.pipeline_stats, "daily_picks.pipeline_stats");
  }
  assertArray(data.items, "daily_picks.items");
  for (const [index, item] of data.items.entries()) {
    validateDailyPickItem(item, `daily_picks.items[${index}]`);
  }
  return data as DailyPicksResponse;
}

export function validatePipelineHistoryResponse(data: unknown): PipelineHistoryResponse {
  assertPaginatedCollection(data, "daily_picks.history");
  for (const [index, item] of data.items.entries()) {
    validatePipelineHistoryItem(item, `daily_picks.history.items[${index}]`);
  }
  return data as PipelineHistoryResponse;
}

export function validateScannerJobResponse(data: unknown): ScannerJobResponse {
  if (!isRecord(data)) throw new Error("scanner job must be an object");
  assertString(data.id, "scanner_job.id");
  assertString(data.status, "scanner_job.status");
  assertString(data.mode, "scanner_job.mode");
  assertNumber(data.candidate_count, "scanner_job.candidate_count");
  assertNumber(data.evaluated_candidate_count, "scanner_job.evaluated_candidate_count");
  assertNumber(data.recommendation_count, "scanner_job.recommendation_count");
  validateWarningList(data.warnings, "scanner_job.warnings");
  return data as ScannerJobResponse;
}

export function validateScannerJobListResponse(data: unknown): ScannerJobListResponse {
  assertPaginatedCollection(data, "scanner_jobs");
  for (const item of data.items) {
    validateScannerJobResponse(item);
  }
  return data as ScannerJobListResponse;
}

export function validateScannerRecommendationListResponse(data: unknown): ScannerRecommendationListResponse {
  if (!isRecord(data)) throw new Error("scanner recommendations must be an object");
  assertArray(data.items, "scanner_recommendations.items");
  assertNumber(data.total, "scanner_recommendations.total");
  assertNumber(data.offset, "scanner_recommendations.offset");
  assertNumber(data.limit, "scanner_recommendations.limit");
  for (const [index, item] of data.items.entries()) {
    if (!isRecord(item)) throw new Error(`scanner_recommendations.items[${index}] must be an object`);
    assertString(item.id, `scanner_recommendations.items[${index}].id`);
    assertString(item.symbol, `scanner_recommendations.items[${index}].symbol`);
    assertString(item.strategy_type, `scanner_recommendations.items[${index}].strategy_type`);
    validateSummary(item.summary, `scanner_recommendations.items[${index}].summary`);
    validateWarningList(item.warnings, `scanner_recommendations.items[${index}].warnings`);
    if (item.historical_performance != null) {
      validateHistoricalPerformance(
        item.historical_performance,
        `scanner_recommendations.items[${index}].historical_performance`,
      );
    }
    if (item.forecast != null) {
      validateForecastPayload(item.forecast, `scanner_recommendations.items[${index}].forecast`);
    }
    if (item.ranking_breakdown != null) {
      validateRankingBreakdown(item.ranking_breakdown, `scanner_recommendations.items[${index}].ranking_breakdown`);
    }
    assertArray(item.trades, `scanner_recommendations.items[${index}].trades`);
    assertArray(item.equity_curve, `scanner_recommendations.items[${index}].equity_curve`);
    assertBoolean(item.trades_truncated, `scanner_recommendations.items[${index}].trades_truncated`);
    assertNumber(item.trade_items_omitted, `scanner_recommendations.items[${index}].trade_items_omitted`);
    assertNumber(item.equity_curve_points_omitted, `scanner_recommendations.items[${index}].equity_curve_points_omitted`);
  }
  return data as ScannerRecommendationListResponse;
}

export function validateSweepJobResponse(data: unknown): SweepJobResponse {
  if (!isRecord(data)) throw new Error("sweep job must be an object");
  assertString(data.id, "sweep_job.id");
  assertString(data.status, "sweep_job.status");
  assertString(data.symbol, "sweep_job.symbol");
  assertString(data.mode, "sweep_job.mode");
  assertNumber(data.candidate_count, "sweep_job.candidate_count");
  assertNumber(data.evaluated_candidate_count, "sweep_job.evaluated_candidate_count");
  assertNumber(data.result_count, "sweep_job.result_count");
  validateWarningList(data.warnings, "sweep_job.warnings");
  return data as SweepJobResponse;
}

export function validateSweepJobListResponse(data: unknown): SweepJobListResponse {
  assertPaginatedCollection(data, "sweep_jobs");
  for (const item of data.items) {
    validateSweepJobResponse(item);
  }
  return data as SweepJobListResponse;
}

export function validateSweepResultListResponse(data: unknown): SweepResultListResponse {
  if (!isRecord(data)) throw new Error("sweep results must be an object");
  assertArray(data.items, "sweep_results.items");
  assertNumber(data.total, "sweep_results.total");
  assertNumber(data.offset, "sweep_results.offset");
  assertNumber(data.limit, "sweep_results.limit");
  for (const [index, item] of data.items.entries()) {
    if (!isRecord(item)) throw new Error(`sweep_results.items[${index}] must be an object`);
    assertString(item.id, `sweep_results.items[${index}].id`);
    assertString(item.strategy_type, `sweep_results.items[${index}].strategy_type`);
    validateSummary(item.summary, `sweep_results.items[${index}].summary`);
    validateWarningList(item.warnings, `sweep_results.items[${index}].warnings`);
    assertBoolean(item.trades_truncated, `sweep_results.items[${index}].trades_truncated`);
    assertNumber(item.trade_items_omitted, `sweep_results.items[${index}].trade_items_omitted`);
    assertNumber(item.equity_curve_points_omitted, `sweep_results.items[${index}].equity_curve_points_omitted`);
  }
  return data as SweepResultListResponse;
}

export function validateStrategyCatalogResponse(data: unknown): StrategyCatalogResponse {
  if (!isRecord(data)) throw new Error("strategy catalog must be an object");
  assertArray(data.groups, "strategy_catalog.groups");
  assertNumber(data.total_strategies, "strategy_catalog.total_strategies");
  assertOptionalString(data.user_tier, "strategy_catalog.user_tier");
  for (const [groupIndex, group] of data.groups.entries()) {
    if (!isRecord(group)) throw new Error(`strategy_catalog.groups[${groupIndex}] must be an object`);
    assertString(group.category, `strategy_catalog.groups[${groupIndex}].category`);
    assertString(group.category_label, `strategy_catalog.groups[${groupIndex}].category_label`);
    assertArray(group.strategies, `strategy_catalog.groups[${groupIndex}].strategies`);
    for (const [strategyIndex, strategy] of group.strategies.entries()) {
      validateStrategyCatalogItem(
        strategy,
        `strategy_catalog.groups[${groupIndex}].strategies[${strategyIndex}]`,
      );
    }
  }
  return data as StrategyCatalogResponse;
}

export function validateMultiSymbolRunListResponse(data: unknown): MultiSymbolRunListResponse {
  assertPaginatedCollection(data, "multi_symbol_runs");
  for (const [index, item] of data.items.entries()) {
    validateMultiSymbolRunListItem(item, `multi_symbol_runs.items[${index}]`);
  }
  return data as MultiSymbolRunListResponse;
}

export function validateMultiSymbolRunDetailResponse(data: unknown): MultiSymbolRunDetailResponse {
  if (!isRecord(data)) throw new Error("multi_symbol_run must be an object");
  assertString(data.id, "multi_symbol_run.id");
  assertOptionalString(data.name, "multi_symbol_run.name");
  assertString(data.status, "multi_symbol_run.status");
  assertString(data.start_date, "multi_symbol_run.start_date");
  assertString(data.end_date, "multi_symbol_run.end_date");
  assertString(data.created_at, "multi_symbol_run.created_at");
  assertOptionalString(data.started_at, "multi_symbol_run.started_at");
  assertOptionalString(data.completed_at, "multi_symbol_run.completed_at");
  validateWarningList(data.warnings, "multi_symbol_run.warnings");
  assertOptionalString(data.error_code, "multi_symbol_run.error_code");
  assertOptionalString(data.error_message, "multi_symbol_run.error_message");
  assertArray(data.symbols, "multi_symbol_run.symbols");
  for (const [index, symbol] of data.symbols.entries()) {
    validateMultiSymbolDefinition(symbol, `multi_symbol_run.symbols[${index}]`);
  }
  validateSummary(data.summary, "multi_symbol_run.summary");
  assertArray(data.symbol_summaries, "multi_symbol_run.symbol_summaries");
  for (const [index, item] of data.symbol_summaries.entries()) {
    if (!isRecord(item)) throw new Error(`multi_symbol_run.symbol_summaries[${index}] must be an object`);
    assertString(item.symbol, `multi_symbol_run.symbol_summaries[${index}].symbol`);
    validateSummary(item.summary, `multi_symbol_run.symbol_summaries[${index}].summary`);
  }
  assertArray(data.trade_groups, "multi_symbol_run.trade_groups");
  for (const [groupIndex, group] of data.trade_groups.entries()) {
    if (!isRecord(group)) throw new Error(`multi_symbol_run.trade_groups[${groupIndex}] must be an object`);
    assertString(group.id, `multi_symbol_run.trade_groups[${groupIndex}].id`);
    assertString(group.entry_date, `multi_symbol_run.trade_groups[${groupIndex}].entry_date`);
    assertOptionalString(group.exit_date, `multi_symbol_run.trade_groups[${groupIndex}].exit_date`);
    assertString(group.status, `multi_symbol_run.trade_groups[${groupIndex}].status`);
    assertArray(group.trades, `multi_symbol_run.trade_groups[${groupIndex}].trades`);
    for (const [tradeIndex, trade] of group.trades.entries()) {
      validateMultiSymbolTrade(trade, `multi_symbol_run.trade_groups[${groupIndex}].trades[${tradeIndex}]`);
    }
  }
  assertArray(data.equity_curve, "multi_symbol_run.equity_curve");
  if (!isRecord(data.symbol_equity_curves)) throw new Error("multi_symbol_run.symbol_equity_curves must be an object");
  return data as unknown as MultiSymbolRunDetailResponse;
}

export function validateMultiStepRunListResponse(data: unknown): MultiStepRunListResponse {
  assertPaginatedCollection(data, "multi_step_runs");
  for (const [index, item] of data.items.entries()) {
    validateMultiStepRunListItem(item, `multi_step_runs.items[${index}]`);
  }
  return data as MultiStepRunListResponse;
}

export function validateMultiStepRunDetailResponse(data: unknown): MultiStepRunDetailResponse {
  if (!isRecord(data)) throw new Error("multi_step_run must be an object");
  assertString(data.id, "multi_step_run.id");
  assertOptionalString(data.name, "multi_step_run.name");
  assertString(data.symbol, "multi_step_run.symbol");
  assertString(data.workflow_type, "multi_step_run.workflow_type");
  assertString(data.status, "multi_step_run.status");
  assertString(data.start_date, "multi_step_run.start_date");
  assertString(data.end_date, "multi_step_run.end_date");
  assertString(data.created_at, "multi_step_run.created_at");
  assertOptionalString(data.started_at, "multi_step_run.started_at");
  assertOptionalString(data.completed_at, "multi_step_run.completed_at");
  validateWarningList(data.warnings, "multi_step_run.warnings");
  assertOptionalString(data.error_code, "multi_step_run.error_code");
  assertOptionalString(data.error_message, "multi_step_run.error_message");
  validateSummary(data.summary, "multi_step_run.summary");
  assertArray(data.steps, "multi_step_run.steps");
  for (const [index, item] of data.steps.entries()) {
    if (!isRecord(item)) throw new Error(`multi_step_run.steps[${index}] must be an object`);
    assertNumber(item.step_number, `multi_step_run.steps[${index}].step_number`);
    assertString(item.name, `multi_step_run.steps[${index}].name`);
    assertString(item.action, `multi_step_run.steps[${index}].action`);
    assertString(item.status, `multi_step_run.steps[${index}].status`);
    assertOptionalString(item.triggered_at, `multi_step_run.steps[${index}].triggered_at`);
    assertOptionalString(item.executed_at, `multi_step_run.steps[${index}].executed_at`);
    assertOptionalString(item.failure_reason, `multi_step_run.steps[${index}].failure_reason`);
  }
  assertArray(data.events, "multi_step_run.events");
  for (const [index, item] of data.events.entries()) {
    if (!isRecord(item)) throw new Error(`multi_step_run.events[${index}] must be an object`);
    assertNumber(item.step_number, `multi_step_run.events[${index}].step_number`);
    assertString(item.event_type, `multi_step_run.events[${index}].event_type`);
    assertString(item.event_at, `multi_step_run.events[${index}].event_at`);
    assertOptionalString(item.message, `multi_step_run.events[${index}].message`);
  }
  assertArray(data.trades, "multi_step_run.trades");
  for (const [index, item] of data.trades.entries()) {
    if (!isRecord(item)) throw new Error(`multi_step_run.trades[${index}] must be an object`);
    assertString(item.id, `multi_step_run.trades[${index}].id`);
    assertNumber(item.step_number, `multi_step_run.trades[${index}].step_number`);
    assertString(item.option_ticker, `multi_step_run.trades[${index}].option_ticker`);
    assertString(item.strategy_type, `multi_step_run.trades[${index}].strategy_type`);
    assertString(item.entry_date, `multi_step_run.trades[${index}].entry_date`);
    assertString(item.exit_date, `multi_step_run.trades[${index}].exit_date`);
    assertNumber(item.quantity, `multi_step_run.trades[${index}].quantity`);
    assertNumericLike(item.gross_pnl, `multi_step_run.trades[${index}].gross_pnl`);
    assertNumericLike(item.net_pnl, `multi_step_run.trades[${index}].net_pnl`);
  }
  assertArray(data.equity_curve, "multi_step_run.equity_curve");
  return data as unknown as MultiStepRunDetailResponse;
}

export function validateMultiSymbolRunStatusResponse(data: unknown): MultiSymbolRunStatusResponse {
  if (!isRecord(data)) throw new Error("multi_symbol_run_status must be an object");
  assertString(data.id, "multi_symbol_run_status.id");
  assertString(data.status, "multi_symbol_run_status.status");
  assertOptionalString(data.started_at, "multi_symbol_run_status.started_at");
  assertOptionalString(data.completed_at, "multi_symbol_run_status.completed_at");
  assertOptionalString(data.error_code, "multi_symbol_run_status.error_code");
  assertOptionalString(data.error_message, "multi_symbol_run_status.error_message");
  return data as unknown as MultiSymbolRunStatusResponse;
}

export function validateMultiStepRunStatusResponse(data: unknown): MultiStepRunStatusResponse {
  if (!isRecord(data)) throw new Error("multi_step_run_status must be an object");
  assertString(data.id, "multi_step_run_status.id");
  assertString(data.status, "multi_step_run_status.status");
  assertOptionalString(data.started_at, "multi_step_run_status.started_at");
  assertOptionalString(data.completed_at, "multi_step_run_status.completed_at");
  assertOptionalString(data.error_code, "multi_step_run_status.error_code");
  assertOptionalString(data.error_message, "multi_step_run_status.error_message");
  return data as unknown as MultiStepRunStatusResponse;
}

export function validatePricingContractResponse(data: unknown): PricingContractResponseShape {
  if (!isRecord(data)) throw new Error("pricing contract must be an object");
  assertString(data.currency, "pricing.currency");
  assertBoolean(data.checkout_authoritative, "pricing.checkout_authoritative");
  assertArray(data.plans, "pricing.plans");
  for (const [index, plan] of data.plans.entries()) {
    validatePricingPlan(plan, `pricing.plans[${index}]`);
  }
  return data as unknown as PricingContractResponseShape;
}
