import type { components } from "./schema";

// ---------------------------------------------------------------------------
// Enum / literal union types (from OpenAPI schema)
// The OpenAPI codegen step (Fix #35) ensures these stay in sync with the
// backend. Regenerate via: npx openapi-typescript openapi.snapshot.json -o src/schema.d.ts
// ---------------------------------------------------------------------------

export type StrategyType = components["schemas"]["StrategyType"];
export type RunStatus = components["schemas"]["JobStatus"];
export type ComparisonOperator = components["schemas"]["ComparisonOperator"];
export type CrossoverDirection = components["schemas"]["CrossoverDirection"];
export type BillingInterval = components["schemas"]["BillingInterval"];
export type ExportFormat = components["schemas"]["ExportFormat"];
export type ScannerMode = components["schemas"]["ScannerMode"];
export type ScannerJobStatus = components["schemas"]["JobStatus"];
export type StrikeSelectionMode = components["schemas"]["StrikeSelectionMode"];
export type SpreadWidthMode = components["schemas"]["SpreadWidthMode"];
export type SupportResistanceMode = components["schemas"]["SupportResistanceMode"];
export type BollingerBand = components["schemas"]["BollingerBand"];

export type MovingAverageRuleType = "sma_crossover" | "ema_crossover";
export type PlanTier = "free" | "pro" | "premium";

// ---------------------------------------------------------------------------
// Entry rule types
// ---------------------------------------------------------------------------

export type RsiRule = components["schemas"]["RsiRule-Output"];
export type MovingAverageCrossoverRule = components["schemas"]["MovingAverageCrossoverRule"];
export type MacdRule = components["schemas"]["MacdRule"];
export type BollingerBandsRule = components["schemas"]["BollingerBandsRule-Output"];
export type IvRankRule = components["schemas"]["IvRankRule-Output"];
export type IvPercentileRule = components["schemas"]["IvPercentileRule-Output"];
export type VolumeSpikeRule = components["schemas"]["VolumeSpikeRule-Output"];
export type SupportResistanceRule = components["schemas"]["SupportResistanceRule-Output"];
export type AvoidEarningsRule = components["schemas"]["AvoidEarningsRule"];

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

// ---------------------------------------------------------------------------
// Strategy configuration types
// ---------------------------------------------------------------------------

export type CustomLegDefinition = components["schemas"]["CustomLegDefinition"];
export type StrikeSelection = components["schemas"]["StrikeSelection"];
export type SpreadWidthConfig = components["schemas"]["SpreadWidthConfig"];
export type StrategyOverrides = components["schemas"]["StrategyOverrides"];

// ---------------------------------------------------------------------------
// User / billing
// ---------------------------------------------------------------------------

export type FeatureAccessResponse = components["schemas"]["FeatureAccessResponse"];
export type UsageSummaryResponse = components["schemas"]["UsageSummaryResponse"];
export type CurrentUserResponse = components["schemas"]["CurrentUserResponse"];
export type CreateCheckoutSessionRequest = components["schemas"]["CreateCheckoutSessionRequest"];
export type CheckoutSessionResponse = components["schemas"]["CheckoutSessionResponse"];
export type CreatePortalSessionRequest = components["schemas"]["CreatePortalSessionRequest"];
export type PortalSessionResponse = components["schemas"]["PortalSessionResponse"];

// ---------------------------------------------------------------------------
// Backtest types
// ---------------------------------------------------------------------------

export type CreateBacktestRunRequest = components["schemas"]["CreateBacktestRunRequest"];
export type BacktestSummaryResponse = components["schemas"]["BacktestSummaryResponse"];
export type BacktestTradeResponse = components["schemas"]["BacktestTradeResponse"];
export type EquityCurvePointResponse = components["schemas"]["EquityCurvePointResponse"];
export type BacktestRunHistoryItemResponse = components["schemas"]["BacktestRunHistoryItemResponse"];
export type BacktestRunDetailResponse = components["schemas"]["BacktestRunDetailResponse"];
export type BacktestRunStatusResponse = components["schemas"]["BacktestRunStatusResponse"];
export type BacktestRunListResponse = components["schemas"]["BacktestRunListResponse"];
export type CompareBacktestsRequest = components["schemas"]["CompareBacktestsRequest"];
export type CompareBacktestsResponse = components["schemas"]["CompareBacktestsResponse"];

// ---------------------------------------------------------------------------
// Export types
// ---------------------------------------------------------------------------

export type CreateExportRequest = components["schemas"]["CreateExportRequest"];
export type ExportJobResponse = components["schemas"]["ExportJobResponse"];

// ---------------------------------------------------------------------------
// Template types
// ---------------------------------------------------------------------------

export type TemplateConfig = components["schemas"]["TemplateConfig-Output"];
export type CreateTemplateRequest = components["schemas"]["CreateTemplateRequest"];
export type UpdateTemplateRequest = components["schemas"]["UpdateTemplateRequest"];
export type TemplateResponse = components["schemas"]["TemplateResponse"];
export type TemplateListResponse = components["schemas"]["TemplateListResponse"];

// ---------------------------------------------------------------------------
// Scanner types
// ---------------------------------------------------------------------------

export type RuleSetDefinition = components["schemas"]["RuleSetDefinition"];
export type CreateScannerJobRequest = components["schemas"]["CreateScannerJobRequest"];
export type ScannerJobResponse = components["schemas"]["ScannerJobResponse"];
export type ScannerJobListResponse = components["schemas"]["ScannerJobListResponse"];
export type HistoricalPerformanceResponse = components["schemas"]["HistoricalPerformanceResponse"];
export type RankingBreakdownResponse = components["schemas"]["RankingBreakdownResponse"];
export type ScannerRecommendationResponse = components["schemas"]["ScannerRecommendationResponse"];
export type ScannerRecommendationListResponse = components["schemas"]["ScannerRecommendationListResponse"];

// ---------------------------------------------------------------------------
// Forecast types
// ---------------------------------------------------------------------------

export type ForecastResponse = components["schemas"]["HistoricalAnalogForecastResponse"];
export type ForecastEnvelopeResponse = components["schemas"]["ForecastEnvelopeResponse"];

// ---------------------------------------------------------------------------
// Strategy catalog types
// ---------------------------------------------------------------------------

export type StrategyCatalogItem = components["schemas"]["StrategyCatalogItemResponse"];
export type StrategyCatalogGroup = components["schemas"]["StrategyCatalogGroupResponse"];
export type StrategyCatalogResponse = components["schemas"]["StrategyCatalogResponse"];

// ---------------------------------------------------------------------------
// Daily picks / pipeline types
// ---------------------------------------------------------------------------

export type DailyPickItem = components["schemas"]["DailyPickItemResponse"];
export type PipelineStats = components["schemas"]["PipelineStatsResponse"];
export type DailyPicksResponse = components["schemas"]["DailyPicksResponse"];
export type PipelineHistoryItemResponse = components["schemas"]["PipelineHistoryItemResponse"];
export type PipelineHistoryResponse = components["schemas"]["PipelineHistoryResponse"];

// ---------------------------------------------------------------------------
// Analysis types
//
// These types are now derived from the generated OpenAPI schema.  After
// regenerating the client (e.g. via openapi-typescript), RegimeDetail,
// LandscapeCell and AnalysisTopResult will be available as proper schemas
// rather than Record<string, unknown>.
// ---------------------------------------------------------------------------

export type SymbolAnalysisSummary = components["schemas"]["AnalysisSummaryResponse"];
export type AnalysisDetailResponse = components["schemas"]["AnalysisDetailResponse"];
export type AnalysisListResponse = components["schemas"]["AnalysisListResponse"];

export type RegimeDetail = components["schemas"]["RegimeDetail"];
export type LandscapeCell = components["schemas"]["LandscapeCell"];
export type AnalysisTopResult = components["schemas"]["AnalysisTopResult"];

export type SymbolAnalysisFullResponse = AnalysisDetailResponse;

// ---------------------------------------------------------------------------
// Sweep types
//
// TODO: These types are manually defined. Sweep endpoints are not yet in the
// OpenAPI export. Add sweep endpoints to OpenAPI (Fix #35) and regenerate
// via `npx openapi-typescript openapi.snapshot.json -o src/schema.d.ts` to
// derive these from the schema. Until then, any backend schema change must be
// mirrored here. Run `python scripts/check_contract_drift.py` to verify alignment.
// NOTE: SweepJobResponse fields `prefetch_summary` and `warnings` match the
// Pydantic *field* names (not the aliases `prefetch_summary_json` /
// `warnings_json`), because FastAPI serializes by field name when
// `populate_by_name=True`.
// ---------------------------------------------------------------------------

/** Sweep mode: grid (exhaustive) or genetic (evolutionary optimization). */
export type SweepMode = "grid" | "genetic";

export interface ParameterSnapshotJson {
  strategy_type?: string;
  mode?: "grid" | "genetic";
  delta?: number | null;
  width_mode?: string | null;
  width_value?: number | string | null;
  entry_rule_set_name?: string | null;
  exit_rule_set_name?: string | null;
  profit_target_pct?: number | null;
  stop_loss_pct?: number | null;
  custom_legs?: {
    contract_type?: string;
    asset_type?: string;
    side?: string;
    strike_offset?: number;
    expiration_offset?: number;
    quantity_ratio?: number | string;
  }[];
  num_legs?: number;
  generations_run?: number;
  total_evaluations?: number;
}

export interface SweepResultResponse {
  id: string;
  rank: number;
  score: string;
  strategy_type: string;
  delta: number | null;
  width_mode: string | null;
  width_value: string | null;
  entry_rule_set_name: string;
  exit_rule_set_name: string | null;
  profit_target_pct: number | null;
  stop_loss_pct: number | null;
  parameter_snapshot_json: ParameterSnapshotJson;
  summary: BacktestSummaryResponse;
  warnings: Record<string, unknown>[];
  trades_json: Record<string, unknown>[];
  equity_curve: EquityCurvePointResponse[];
  trades_truncated: boolean;
}

export interface SweepResultListResponse {
  items: SweepResultResponse[];
  total: number;
  offset: number;
  limit: number;
}

export interface SweepJobResponse {
  id: string;
  status: RunStatus;
  mode: "grid" | "genetic";
  symbol: string;
  engine_version: string | null;
  plan_tier_snapshot: string;
  candidate_count: number;
  evaluated_candidate_count: number;
  result_count: number;
  prefetch_summary: Record<string, unknown> | null;
  warnings: Record<string, unknown>[];
  request_snapshot: Record<string, unknown>;
  error_code: string | null;
  error_message: string | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
}

export interface SweepJobListResponse {
  items: SweepJobResponse[];
  total: number;
  offset: number;
  limit: number;
}

export interface GeneticSweepConfig {
  num_legs: number;
  population_size?: number;
  max_generations?: number;
  tournament_size?: number;
  crossover_rate?: number;
  mutation_rate?: number;
  elitism_count?: number;
  max_workers?: number;
  max_stale_generations?: number;
}

export interface CreateSweepRequest {
  mode?: "grid" | "genetic";
  symbol: string;
  strategy_types: string[];
  start_date: string;
  end_date: string;
  target_dte: number;
  dte_tolerance_days?: number;
  max_holding_days: number;
  account_size: number;
  risk_per_trade_pct: number;
  commission_per_contract: number;
  entry_rule_sets: { name: string; entry_rules: EntryRule[] }[];
  delta_grid?: { value: number }[];
  width_grid?: { mode: string; value: number }[];
  exit_rule_sets?: { name: string; profit_target_pct?: number; stop_loss_pct?: number }[];
  genetic_config?: GeneticSweepConfig;
  max_results?: number;
  slippage_pct?: number;
  idempotency_key?: string;
}
