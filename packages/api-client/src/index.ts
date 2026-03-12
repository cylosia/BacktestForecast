import type { components } from "./schema";

// ---------------------------------------------------------------------------
// Enum / literal union types
// ---------------------------------------------------------------------------

export type StrategyType = components["schemas"]["StrategyType"];
export type RunStatus = components["schemas"]["RunStatus"];
export type ComparisonOperator = components["schemas"]["ComparisonOperator"];
export type CrossoverDirection = components["schemas"]["CrossoverDirection"];
export type BillingInterval = components["schemas"]["BillingInterval"];
export type ExportFormat = components["schemas"]["ExportFormat"];
export type ScannerMode = components["schemas"]["ScannerMode"];
export type ScannerJobStatus = components["schemas"]["ScannerJobStatus"];
export type StrikeSelectionMode = components["schemas"]["StrikeSelectionMode"];
export type SpreadWidthMode = components["schemas"]["SpreadWidthMode"];
export type SupportResistanceMode = components["schemas"]["SupportResistanceMode"];
export type BollingerBand = components["schemas"]["BollingerBand"];

export type MovingAverageRuleType = "sma_crossover" | "ema_crossover";
export type PlanTier = "free" | "pro" | "premium";

// ---------------------------------------------------------------------------
// Entry rule types
// ---------------------------------------------------------------------------

export type RsiRule = components["schemas"]["RsiRule"];
export type MovingAverageCrossoverRule = components["schemas"]["MovingAverageCrossoverRule"];
export type MacdRule = components["schemas"]["MacdRule"];
export type BollingerBandsRule = components["schemas"]["BollingerBandsRule"];
export type IvRankRule = components["schemas"]["IvRankRule"];
export type IvPercentileRule = components["schemas"]["IvPercentileRule"];
export type VolumeSpikeRule = components["schemas"]["VolumeSpikeRule"];
export type SupportResistanceRule = components["schemas"]["SupportResistanceRule"];
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

export type TemplateConfig = components["schemas"]["TemplateConfig"];
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

// ---------------------------------------------------------------------------
// Analysis types
//
// The generated AnalysisDetailResponse uses Record<string, unknown> for nested
// objects (regime, landscape, top_results). The interfaces below provide
// stronger typing for these structures based on the known API contract.
// ---------------------------------------------------------------------------

export type SymbolAnalysisSummary = components["schemas"]["AnalysisSummaryResponse"];
export type AnalysisDetailResponse = components["schemas"]["AnalysisDetailResponse"];
export type AnalysisListResponse = components["schemas"]["AnalysisListResponse"];

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
