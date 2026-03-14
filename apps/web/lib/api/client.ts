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
  UpdateTemplateRequest,
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

export async function fetchBacktestRun(
  token: string,
  runId: string,
  signal?: AbortSignal,
): Promise<BacktestRunDetailResponse> {
  return apiRequest<BacktestRunDetailResponse>(`/v1/backtests/${encodeURIComponent(runId)}`, token, signal ? { signal } : undefined);
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
  payload: UpdateTemplateRequest,
): Promise<TemplateResponse> {
  return apiRequest<TemplateResponse>(`/v1/templates/${encodeURIComponent(templateId)}`, token, {
    method: "PATCH",
    body: JSON.stringify(payload),
  });
}

export async function deleteTemplate(token: string, templateId: string, signal?: AbortSignal): Promise<void> {
  await apiRequest<void>(`/v1/templates/${encodeURIComponent(templateId)}`, token, {
    method: "DELETE",
    signal,
  });
}

export async function fetchTemplates(token: string, signal?: AbortSignal): Promise<TemplateListResponse> {
  return apiRequest<TemplateListResponse>("/v1/templates", token, signal ? { signal } : undefined);
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
