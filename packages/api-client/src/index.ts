import type { components } from "./schema";

// ---------------------------------------------------------------------------
// Enum / literal union types (from OpenAPI schema)
// The OpenAPI codegen step (Fix #35) ensures these stay in sync with the
// backend. Regenerate via: npx openapi-typescript openapi.snapshot.json -o src/schema.d.ts
// ---------------------------------------------------------------------------

export type StrategyType = components["schemas"]["StrategyType"];
export type RunStatus = components["schemas"]["RunJobStatus"];
export type ComparisonOperator = components["schemas"]["ComparisonOperator"];
export type CrossoverDirection = components["schemas"]["CrossoverDirection"];
export type BillingInterval = components["schemas"]["BillingInterval"];
export type ExportFormat = components["schemas"]["ExportFormat"];
export type ScannerMode = components["schemas"]["ScannerMode"];
export type ScannerJobStatus = components["schemas"]["RunJobStatus"];
export type StrikeSelectionMode = components["schemas"]["StrikeSelectionMode"];
export type SpreadWidthMode = components["schemas"]["SpreadWidthMode"];
export type SupportResistanceMode = components["schemas"]["SupportResistanceMode"];
export type BollingerBand = components["schemas"]["BollingerBand"];

export type MovingAverageRuleType = "sma_crossover" | "ema_crossover";
export type PlanTier = components["schemas"]["PlanTier"];

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
export type BacktestRunListResponse = components["schemas"]["BacktestRunListResponse"] & {
  next_cursor?: string | null;
};
export type CompareBacktestsRequest = components["schemas"]["CompareBacktestsRequest"];
export type CompareBacktestsResponse = components["schemas"]["CompareBacktestsResponse"];

// ---------------------------------------------------------------------------
// Export types
// ---------------------------------------------------------------------------

export type CreateExportRequest = components["schemas"]["CreateExportRequest"];
export type ExportJobResponse = components["schemas"]["ExportJobResponse"];
export type ExportJobListResponse = components["schemas"]["ExportJobListResponse"];

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
export type ScannerJobListResponse = components["schemas"]["ScannerJobListResponse"] & {
  next_cursor?: string | null;
};
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
export type AnalysisListResponse = components["schemas"]["AnalysisListResponse"] & {
  next_cursor?: string | null;
};

export type RegimeDetail = components["schemas"]["RegimeDetail"];
export type LandscapeCell = components["schemas"]["LandscapeCell"];
export type AnalysisTopResult = components["schemas"]["AnalysisTopResult"];

export type SymbolAnalysisFullResponse = AnalysisDetailResponse;

// ---------------------------------------------------------------------------
// Sweep types (derived from generated OpenAPI schema)
// ---------------------------------------------------------------------------

export type SweepMode = components["schemas"]["SweepMode"];
export type SweepJobResponse = components["schemas"]["SweepJobResponse"];
export type SweepJobListResponse = components["schemas"]["SweepJobListResponse"] & {
  next_cursor?: string | null;
};
export type SweepJobStatusResponse = components["schemas"]["SweepJobStatusResponse"];
export type SweepResultResponse = components["schemas"]["SweepResultResponse"];
export type SweepResultListResponse = components["schemas"]["SweepResultListResponse"];
export type CreateSweepRequest = components["schemas"]["CreateSweepRequest"];
export type GeneticSweepConfig = components["schemas"]["GeneticSweepConfig"];
