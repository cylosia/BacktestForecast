export type NumericValue = number | string;

export type StrategyType =
  | "long_call"
  | "long_put"
  | "covered_call"
  | "cash_secured_put"
  | "bull_call_debit_spread"
  | "bear_put_debit_spread"
  | "bull_put_credit_spread"
  | "bear_call_credit_spread"
  | "iron_condor"
  | "long_straddle"
  | "long_strangle"
  | "calendar_spread"
  | "butterfly"
  | "wheel_strategy"
  | "poor_mans_covered_call"
  | "ratio_call_backspread"
  | "ratio_put_backspread"
  | "collar"
  | "diagonal_spread"
  | "double_diagonal"
  | "short_straddle"
  | "short_strangle"
  | "covered_strangle"
  | "synthetic_put"
  | "reverse_conversion"
  | "jade_lizard"
  | "iron_butterfly"
  | "custom_2_leg"
  | "custom_3_leg"
  | "custom_4_leg"
  | "custom_5_leg"
  | "custom_6_leg"
  | "custom_8_leg"
  | "naked_call"
  | "naked_put";
export type RunStatus = "queued" | "running" | "succeeded" | "failed";
export type ComparisonOperator = "lt" | "lte" | "gt" | "gte";
export type CrossoverDirection = "bullish" | "bearish";
export type MovingAverageRuleType = "sma_crossover" | "ema_crossover";
export type PlanTier = "free" | "pro" | "premium";
export type BillingInterval = "monthly" | "yearly";
export type ExportFormat = "csv" | "pdf";

export interface FeatureAccessResponse {
  plan_tier: PlanTier;
  monthly_backtest_quota: number | null;
  history_days: number | null;
  history_item_limit: number;
  side_by_side_comparison_limit: number;
  forecasting_access: boolean;
  export_formats: ExportFormat[];
  scanner_modes: Array<"basic" | "advanced">;
}

export interface UsageSummaryResponse {
  backtests_used_this_month: number;
  backtests_remaining_this_month: number | null;
}

export interface CurrentUserResponse {
  id: string;
  clerk_user_id: string;
  email: string | null;
  plan_tier: PlanTier;
  subscription_status: string | null;
  subscription_billing_interval: BillingInterval | null;
  subscription_current_period_end: string | null;
  cancel_at_period_end: boolean;
  created_at: string;
  features: FeatureAccessResponse;
  usage: UsageSummaryResponse;
}

export interface RsiRule {
  type: "rsi";
  operator: ComparisonOperator;
  threshold: number;
  period: number;
}

export interface MovingAverageCrossoverRule {
  type: MovingAverageRuleType;
  fast_period: number;
  slow_period: number;
  direction: CrossoverDirection;
}

export interface MacdRule {
  type: "macd";
  fast_period: number;
  slow_period: number;
  signal_period: number;
  direction: CrossoverDirection;
}

export interface BollingerBandsRule {
  type: "bollinger_bands";
  period: number;
  num_std: number;
  operator: ComparisonOperator;
  band: "upper" | "lower";
}

export interface IvRankRule {
  type: "iv_rank";
  operator: ComparisonOperator;
  threshold: number;
}

export interface IvPercentileRule {
  type: "iv_percentile";
  operator: ComparisonOperator;
  threshold: number;
}

export interface VolumeSpikeRule {
  type: "volume_spike";
  lookback_days: number;
  spike_multiplier: number;
}

export type SupportResistanceMode =
  | "near_support"
  | "near_resistance"
  | "breakout_above_resistance"
  | "breakdown_below_support";

export interface SupportResistanceRule {
  type: "support_resistance";
  mode: SupportResistanceMode;
  lookback_days: number;
  tolerance_pct: number;
}

export interface AvoidEarningsRule {
  type: "avoid_earnings";
  days_before: number;
  days_after: number;
}

export type EntryRule =
  | RsiRule
  | MovingAverageCrossoverRule
  | MacdRule
  | BollingerBandsRule
  | IvRankRule
  | IvPercentileRule
  | VolumeSpikeRule
  | SupportResistanceRule
  | AvoidEarningsRule;

export interface CustomLegDefinition {
  asset_type: "option" | "stock";
  contract_type?: "call" | "put" | null;
  side: "long" | "short";
  strike_offset: number;
  expiration_offset: number;
  quantity_ratio: number;
}

export type StrikeSelectionMode = "nearest_otm" | "pct_from_spot" | "atm_offset_steps" | "delta_target";
export type SpreadWidthMode = "strike_steps" | "dollar_width" | "pct_width";

export interface StrikeSelection {
  mode: StrikeSelectionMode;
  value?: number | null;
}

export interface SpreadWidthConfig {
  mode: SpreadWidthMode;
  value: number;
}

export interface StrategyOverrides {
  short_call_strike?: StrikeSelection | null;
  short_put_strike?: StrikeSelection | null;
  long_call_strike?: StrikeSelection | null;
  long_put_strike?: StrikeSelection | null;
  spread_width?: SpreadWidthConfig | null;
}

export interface CreateBacktestRunRequest {
  symbol: string;
  strategy_type: StrategyType;
  start_date: string;
  end_date: string;
  target_dte: number;
  dte_tolerance_days: number;
  max_holding_days: number;
  account_size: number;
  risk_per_trade_pct: number;
  commission_per_contract: number;
  entry_rules: EntryRule[];
  idempotency_key?: string;
  custom_legs?: CustomLegDefinition[];
  strategy_overrides?: StrategyOverrides | null;
}

export interface BacktestSummaryResponse {
  trade_count: number;
  win_rate: NumericValue;
  total_roi_pct: NumericValue;
  average_win_amount: NumericValue;
  average_loss_amount: NumericValue;
  average_holding_period_days: NumericValue;
  average_dte_at_open: NumericValue;
  max_drawdown_pct: NumericValue;
  total_commissions: NumericValue;
  total_net_pnl: NumericValue;
  starting_equity: NumericValue;
  ending_equity: NumericValue;
}

export interface BacktestTradeResponse {
  id: string | null;
  option_ticker: string;
  strategy_type: string;
  underlying_symbol: string;
  entry_date: string;
  exit_date: string;
  expiration_date: string;
  quantity: number;
  dte_at_open: number;
  holding_period_days: number;
  entry_underlying_close: NumericValue;
  exit_underlying_close: NumericValue;
  entry_mid: NumericValue;
  exit_mid: NumericValue;
  gross_pnl: NumericValue;
  net_pnl: NumericValue;
  total_commissions: NumericValue;
  entry_reason: string;
  exit_reason: string;
  detail_json?: Record<string, unknown>;
}

export interface EquityCurvePointResponse {
  trade_date: string;
  equity: NumericValue;
  cash: NumericValue;
  position_value: NumericValue;
  drawdown_pct: NumericValue;
}

export interface BacktestRunHistoryItemResponse {
  id: string;
  symbol: string;
  strategy_type: string;
  status: RunStatus;
  date_from: string;
  date_to: string;
  target_dte: number;
  max_holding_days: number;
  created_at: string;
  completed_at: string | null;
  summary: BacktestSummaryResponse;
}

export interface BacktestRunDetailResponse {
  id: string;
  symbol: string;
  strategy_type: string;
  status: RunStatus;
  date_from: string;
  date_to: string;
  target_dte: number;
  dte_tolerance_days: number;
  max_holding_days: number;
  account_size: NumericValue;
  risk_per_trade_pct: NumericValue;
  commission_per_contract: NumericValue;
  engine_version: string;
  data_source: string;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  warnings: Array<Record<string, NumericValue | string | null>>;
  error_code?: string | null;
  error_message?: string | null;
  summary: BacktestSummaryResponse;
  trades: BacktestTradeResponse[];
  equity_curve: EquityCurvePointResponse[];
}

export interface BacktestRunListResponse {
  items: BacktestRunHistoryItemResponse[];
}

export interface CompareBacktestsRequest {
  run_ids: string[];
}

export interface CompareBacktestsResponse {
  items: BacktestRunDetailResponse[];
  comparison_limit: number;
}

export interface CreateCheckoutSessionRequest {
  tier: Exclude<PlanTier, "free">;
  billing_interval: BillingInterval;
}

export interface CheckoutSessionResponse {
  session_id: string;
  checkout_url: string;
  tier: Exclude<PlanTier, "free">;
  billing_interval: BillingInterval;
  expires_at: string | null;
}

export interface CreatePortalSessionRequest {
  return_path?: string;
}

export interface PortalSessionResponse {
  portal_url: string;
}

export interface CreateExportRequest {
  run_id: string;
  format: ExportFormat;
  idempotency_key?: string;
}

export interface ExportJobResponse {
  id: string;
  run_id: string;
  export_format: ExportFormat;
  status: string;
  file_name: string;
  mime_type: string;
  size_bytes: number;
  created_at: string;
  completed_at: string | null;
}

export interface ApiErrorPayload {
  error?: {
    code?: string;
    message?: string;
    request_id?: string;
  };
}

// --- Templates ---

export interface TemplateConfig {
  strategy_type: StrategyType;
  target_dte: number;
  dte_tolerance_days: number;
  max_holding_days: number;
  account_size: number;
  risk_per_trade_pct: number;
  commission_per_contract: number;
  entry_rules: EntryRule[];
  default_symbol?: string | null;
}

export interface CreateTemplateRequest {
  name: string;
  description?: string | null;
  config: TemplateConfig;
}

export interface UpdateTemplateRequest {
  name?: string;
  description?: string | null;
  config?: TemplateConfig;
}

export interface TemplateResponse {
  id: string;
  name: string;
  description: string | null;
  strategy_type: string;
  config: TemplateConfig;
  created_at: string;
  updated_at: string;
}

export interface TemplateListResponse {
  items: TemplateResponse[];
  total: number;
  template_limit: number | null;
}

// --- Scanner ---

export type ScannerMode = "basic" | "advanced";
export type ScannerJobStatus = "queued" | "running" | "succeeded" | "failed";

export interface RuleSetDefinition {
  name: string;
  entry_rules: EntryRule[];
}

export interface CreateScannerJobRequest {
  name?: string | null;
  mode: ScannerMode;
  symbols: string[];
  strategy_types: string[];
  rule_sets: RuleSetDefinition[];
  start_date: string;
  end_date: string;
  target_dte: number;
  dte_tolerance_days: number;
  max_holding_days: number;
  account_size: number;
  risk_per_trade_pct: number;
  commission_per_contract: number;
  max_recommendations: number;
  refresh_daily?: boolean;
  idempotency_key?: string;
}

export interface ScannerJobResponse {
  id: string;
  name: string | null;
  status: ScannerJobStatus;
  mode: ScannerMode;
  plan_tier_snapshot: string;
  job_kind: string;
  candidate_count: number;
  evaluated_candidate_count: number;
  recommendation_count: number;
  refresh_daily: boolean;
  refresh_priority: number;
  warnings: Array<Record<string, unknown>>;
  error_code: string | null;
  error_message: string | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
}

export interface ScannerJobListResponse {
  items: ScannerJobResponse[];
}

export interface HistoricalPerformanceResponse {
  sample_count: number;
  weighted_win_rate: NumericValue;
  weighted_total_roi_pct: NumericValue;
  weighted_total_net_pnl: NumericValue;
  weighted_max_drawdown_pct: NumericValue;
  recency_half_life_days: number;
  last_observed_at: string | null;
}

export interface ForecastResponse {
  symbol: string;
  strategy_type: string | null;
  as_of_date: string;
  horizon_days: number;
  analog_count: number;
  expected_return_low_pct: NumericValue;
  expected_return_median_pct: NumericValue;
  expected_return_high_pct: NumericValue;
  positive_outcome_rate_pct: NumericValue;
  summary: string;
  disclaimer: string;
}

export interface RankingBreakdownResponse {
  current_performance_score: NumericValue;
  historical_performance_score: NumericValue;
  forecast_alignment_score: NumericValue;
  final_score: NumericValue;
  reasoning: string[];
}

export interface ScannerRecommendationResponse {
  id: string;
  rank: number;
  score: NumericValue;
  symbol: string;
  strategy_type: string;
  rule_set_name: string;
  request_snapshot: Record<string, unknown>;
  summary: BacktestSummaryResponse;
  warnings: Array<Record<string, unknown>>;
  historical_performance: HistoricalPerformanceResponse;
  forecast: ForecastResponse;
  ranking_breakdown: RankingBreakdownResponse;
  trades: BacktestTradeResponse[];
  equity_curve: EquityCurvePointResponse[];
}

export interface ScannerRecommendationListResponse {
  items: ScannerRecommendationResponse[];
}

// --- Strategy Catalog ---

export interface StrategyCatalogItem {
  strategy_type: StrategyType;
  label: string;
  short_description: string;
  category: string;
  bias: string;
  leg_count: number;
  min_tier: string;
  max_loss_description: string;
  notes: string;
  tags: string[];
}

export interface StrategyCatalogGroup {
  category: string;
  category_label: string;
  strategies: StrategyCatalogItem[];
}

export interface StrategyCatalogResponse {
  groups: StrategyCatalogGroup[];
  total_strategies: number;
}

// --- Forecast ---

export interface ForecastEnvelopeResponse {
  forecast: ForecastResponse;
  probabilistic_note: string;
  expected_move_abs_pct: NumericValue;
}

// --- Daily Picks (Nightly Pipeline) ---

export interface DailyPickItem {
  rank: number;
  score: number;
  symbol: string;
  strategy_type: string;
  regime_labels: string[];
  close_price: number;
  target_dte: number;
  config_snapshot: Record<string, unknown>;
  summary: Record<string, unknown>;
  forecast: Record<string, unknown>;
}

export interface PipelineStats {
  symbols_screened: number;
  symbols_after_screen: number;
  pairs_generated: number;
  quick_backtests_run: number;
  full_backtests_run: number;
  recommendations_produced: number;
  duration_seconds: number | null;
  completed_at: string | null;
}

export interface DailyPicksResponse {
  trade_date: string | null;
  pipeline_run_id: string | null;
  status: string;
  items: DailyPickItem[];
  pipeline_stats: PipelineStats | null;
}

// --- Symbol Deep Analysis ---

export interface SymbolAnalysisSummary {
  id: string;
  symbol: string;
  status: string;
  stage: string;
  close_price: number | null;
  strategies_tested: number;
  configs_tested: number;
  top_results_count: number;
  duration_seconds: number | null;
  error_message: string | null;
  created_at: string | null;
  completed_at: string | null;
}

export interface RegimeDetail {
  regimes: string[];
  rsi_14: number | null;
  ema_8: number | null;
  ema_21: number | null;
  sma_50: number | null;
  sma_200: number | null;
  realized_vol_20: number | null;
  iv_rank_proxy: number | null;
  volume_ratio: number | null;
  close_price: number;
}

export interface LandscapeCell {
  strategy_type: string;
  strategy_label: string;
  target_dte: number;
  config: Record<string, unknown>;
  trade_count: number;
  win_rate: number;
  total_roi_pct: number;
  max_drawdown_pct: number;
  score: number;
}

export interface AnalysisTopResult {
  rank: number;
  strategy_type: string;
  strategy_label: string;
  target_dte: number;
  config: Record<string, unknown>;
  summary: Record<string, unknown>;
  trades: Record<string, unknown>[];
  equity_curve: Record<string, unknown>[];
  forecast: Record<string, unknown>;
  score: number;
}

export interface SymbolAnalysisFullResponse extends SymbolAnalysisSummary {
  regime: RegimeDetail;
  landscape: LandscapeCell[];
  top_results: AnalysisTopResult[];
  forecast: Record<string, unknown>;
}
