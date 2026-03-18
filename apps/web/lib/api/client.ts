import { apiDownload, apiRequest } from "@/lib/api/shared";
import type {
  BacktestRunDetailResponse,
  BacktestRunStatusResponse,
  CheckoutSessionResponse,
  CompareBacktestsResponse,
  CreateBacktestRunRequest,
  CreateCheckoutSessionRequest,
  CreateExportRequest,
  CreatePortalSessionRequest,
  CreateScannerJobRequest,
  CreateTemplateRequest,
  ExportJobResponse,
  ForecastEnvelopeResponse,
  PortalSessionResponse,
  ScannerJobResponse,
  ScannerRecommendationListResponse,
  SymbolAnalysisFullResponse,
  SymbolAnalysisSummary,
  TemplateListResponse,
  TemplateResponse,
} from "@backtestforecast/api-client";

export async function createBacktestRun(
  token: string,
  payload: CreateBacktestRunRequest,
  signal?: AbortSignal,
): Promise<BacktestRunDetailResponse> {
  return apiRequest<BacktestRunDetailResponse>("/v1/backtests", token, {
    method: "POST",
    body: JSON.stringify(payload),
    signal,
  });
}

export async function fetchBacktestRunStatus(
  token: string,
  runId: string,
  signal?: AbortSignal,
): Promise<BacktestRunStatusResponse> {
  return apiRequest<BacktestRunStatusResponse>(`/v1/backtests/${encodeURIComponent(runId)}/status`, token, signal ? { signal } : undefined);
}

export async function compareBacktests(
  token: string,
  runIds: string[],
  signal?: AbortSignal,
): Promise<CompareBacktestsResponse> {
  return apiRequest<CompareBacktestsResponse>("/v1/backtests/compare", token, {
    method: "POST",
    body: JSON.stringify({ run_ids: runIds }),
    signal,
  });
}

export async function createCheckoutSession(
  token: string,
  payload: CreateCheckoutSessionRequest,
): Promise<CheckoutSessionResponse> {
  return apiRequest<CheckoutSessionResponse>("/v1/billing/checkout-session", token, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function createPortalSession(
  token: string,
  payload: Partial<CreatePortalSessionRequest> = {},
): Promise<PortalSessionResponse> {
  const body: Record<string, unknown> = {};
  if (payload.return_path != null) {
    body.return_path = payload.return_path;
  }
  return apiRequest<PortalSessionResponse>("/v1/billing/portal-session", token, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function createExport(
  token: string,
  payload: CreateExportRequest,
  signal?: AbortSignal,
): Promise<ExportJobResponse> {
  return apiRequest<ExportJobResponse>("/v1/exports", token, {
    method: "POST",
    body: JSON.stringify(payload),
    signal,
  });
}

export async function downloadExport(token: string, exportJobId: string, signal?: AbortSignal): Promise<Response> {
  return apiDownload(`/v1/exports/${encodeURIComponent(exportJobId)}`, token, signal ? { signal } : undefined);
}

export async function fetchExportStatus(
  token: string,
  exportJobId: string,
  signal?: AbortSignal,
): Promise<ExportJobResponse> {
  return apiRequest<ExportJobResponse>(`/v1/exports/${encodeURIComponent(exportJobId)}/status`, token, signal ? { signal } : undefined);
}

export async function createTemplate(
  token: string,
  payload: CreateTemplateRequest,
): Promise<TemplateResponse> {
  return apiRequest<TemplateResponse>("/v1/templates", token, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function updateTemplate(
  token: string,
  templateId: string,
  payload: { name?: string; description?: string },
  signal?: AbortSignal,
): Promise<TemplateResponse> {
  return apiRequest<TemplateResponse>(`/v1/templates/${encodeURIComponent(templateId)}`, token, {
    method: "PATCH",
    body: JSON.stringify(payload),
    signal,
  });
}

export async function deleteTemplate(token: string, templateId: string, signal?: AbortSignal): Promise<void> {
  await apiRequest<void>(`/v1/templates/${encodeURIComponent(templateId)}`, token, {
    method: "DELETE",
    signal,
  });
}

export async function createScannerJob(
  token: string,
  payload: CreateScannerJobRequest,
  signal?: AbortSignal,
): Promise<ScannerJobResponse> {
  return apiRequest<ScannerJobResponse>("/v1/scans", token, {
    method: "POST",
    body: JSON.stringify(payload),
    signal,
  });
}

export async function fetchScannerJob(
  token: string,
  jobId: string,
  signal?: AbortSignal,
): Promise<ScannerJobResponse> {
  return apiRequest<ScannerJobResponse>(`/v1/scans/${encodeURIComponent(jobId)}`, token, signal ? { signal } : undefined);
}

export async function fetchScannerRecommendations(
  token: string,
  jobId: string,
): Promise<ScannerRecommendationListResponse> {
  return apiRequest<ScannerRecommendationListResponse>(`/v1/scans/${encodeURIComponent(jobId)}/recommendations`, token);
}

// TODO: These types should be generated from the OpenAPI schema via
// packages/api-client. Manually kept in sync until sweep endpoints are
// added to the schema generation pipeline.
export interface SweepJobResponse {
  id: string;
  status: string;
  symbol: string;
  candidate_count: number;
  evaluated_candidate_count: number;
  result_count: number;
  prefetch_summary: Record<string, unknown> | null;
  warnings: Record<string, unknown>[];
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
  summary: Record<string, unknown>;
  warnings: Record<string, unknown>[];
  trades_json: Record<string, unknown>[];
  equity_curve: { date: string; equity: number }[];
}

export interface SweepResultListResponse {
  items: SweepResultResponse[];
}

export async function createSweepJob(
  token: string,
  payload: Record<string, unknown>,
  signal?: AbortSignal,
): Promise<SweepJobResponse> {
  return apiRequest<SweepJobResponse>("/v1/sweeps", token, {
    method: "POST",
    body: JSON.stringify(payload),
    signal,
  });
}

export async function fetchSweepJob(
  token: string,
  jobId: string,
  signal?: AbortSignal,
): Promise<SweepJobResponse> {
  return apiRequest<SweepJobResponse>(`/v1/sweeps/${encodeURIComponent(jobId)}`, token, signal ? { signal } : undefined);
}

export async function fetchForecast(
  token: string,
  ticker: string,
  options?: { strategyType?: string; horizonDays?: number; signal?: AbortSignal },
): Promise<ForecastEnvelopeResponse> {
  const params = new URLSearchParams();
  if (options?.strategyType != null) params.set("strategy_type", options.strategyType);
  if (options?.horizonDays != null) params.set("horizon_days", String(options.horizonDays));
  const qs = params.toString();
  return apiRequest<ForecastEnvelopeResponse>(
    `/v1/forecasts/${encodeURIComponent(ticker)}${qs ? `?${qs}` : ""}`,
    token,
    options?.signal ? { signal: options.signal } : undefined,
  );
}

export async function createSymbolAnalysis(
  token: string,
  symbol: string,
  idempotencyKey?: string,
  signal?: AbortSignal,
): Promise<SymbolAnalysisSummary> {
  return apiRequest<SymbolAnalysisSummary>("/v1/analysis", token, {
    method: "POST",
    body: JSON.stringify({ symbol, idempotency_key: idempotencyKey }),
    signal,
  });
}

export async function fetchAnalysisStatus(
  token: string,
  analysisId: string,
  signal?: AbortSignal,
): Promise<SymbolAnalysisSummary> {
  return apiRequest<SymbolAnalysisSummary>(`/v1/analysis/${encodeURIComponent(analysisId)}/status`, token, signal ? { signal } : undefined);
}

export async function fetchAnalysisFull(
  token: string,
  analysisId: string,
  signal?: AbortSignal,
): Promise<SymbolAnalysisFullResponse> {
  return apiRequest<SymbolAnalysisFullResponse>(`/v1/analysis/${encodeURIComponent(analysisId)}`, token, signal ? { signal } : undefined);
}
