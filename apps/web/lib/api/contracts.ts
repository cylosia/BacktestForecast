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
  assertNumber(summary.win_rate, `${label}.win_rate`);
  assertNumber(summary.total_roi_pct, `${label}.total_roi_pct`);
  assertNumber(summary.max_drawdown_pct, `${label}.max_drawdown_pct`);
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
  assertOptionalNumber(value.expected_return_median_pct, `${label}.expected_return_median_pct`);
  assertOptionalNumber(value.positive_outcome_rate_pct, `${label}.positive_outcome_rate_pct`);
  assertOptionalNumber(value.analog_count, `${label}.analog_count`);
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
  return data as BacktestRunDetailResponse;
}

export function validateCompareBacktestsResponse(data: unknown): CompareBacktestsResponse {
  if (!isRecord(data)) throw new Error("compare response must be an object");
  assertArray(data.items, "compare.items");
  assertBoolean(data.trades_truncated, "compare.trades_truncated");
  for (const [index, item] of data.items.entries()) {
    validateBacktestRunLike(item, `compare.items[${index}]`);
    if (!isRecord(item)) throw new Error(`compare.items[${index}] must be an object`);
    assertArray(item.trades, `compare.items[${index}].trades`);
    assertArray(item.equity_curve, `compare.items[${index}].equity_curve`);
    assertBoolean(item.equity_curve_truncated, `compare.items[${index}].equity_curve_truncated`);
    assertNumber(item.trade_items_omitted, `compare.items[${index}].trade_items_omitted`);
    assertNumber(item.equity_curve_points_omitted, `compare.items[${index}].equity_curve_points_omitted`);
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
  if (Array.isArray(data.top_results)) {
    for (const [index, item] of data.top_results.entries()) {
    if (!isRecord(item)) throw new Error(`analysis.top_results[${index}] must be an object`);
      validateOptionalSummary(item.summary, `analysis.top_results[${index}].summary`);
      assertArray(item.trades, `analysis.top_results[${index}].trades`);
      assertArray(item.equity_curve, `analysis.top_results[${index}].equity_curve`);
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
  return data as ExportJobResponse;
}

export function validateForecastEnvelopeResponse(data: unknown): ForecastEnvelopeResponse {
  if (!isRecord(data)) throw new Error("forecast envelope must be an object");
  assertString(data.expected_move_abs_pct, "forecast.expected_move_abs_pct");
  if (!isRecord(data.forecast)) throw new Error("forecast.forecast must be an object");
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
