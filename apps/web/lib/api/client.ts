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
): Promise<BacktestRunDetailResponse> {
  return apiRequest<BacktestRunDetailResponse>("/v1/backtests", token, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function fetchBacktestRun(
  token: string,
  runId: string,
): Promise<BacktestRunDetailResponse> {
  return apiRequest<BacktestRunDetailResponse>(`/v1/backtests/${runId}`, token);
}

export async function fetchBacktestRunStatus(
  token: string,
  runId: string,
): Promise<BacktestRunStatusResponse> {
  return apiRequest<BacktestRunStatusResponse>(`/v1/backtests/${runId}/status`, token);
}

export async function compareBacktests(
  token: string,
  runIds: string[],
): Promise<CompareBacktestsResponse> {
  return apiRequest<CompareBacktestsResponse>("/v1/backtests/compare", token, {
    method: "POST",
    body: JSON.stringify({ run_ids: runIds }),
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
  payload: CreatePortalSessionRequest = { return_path: null },
): Promise<PortalSessionResponse> {
  return apiRequest<PortalSessionResponse>("/v1/billing/portal-session", token, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function createExport(
  token: string,
  payload: CreateExportRequest,
): Promise<ExportJobResponse> {
  return apiRequest<ExportJobResponse>("/v1/exports", token, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function downloadExport(token: string, exportJobId: string): Promise<Response> {
  return apiDownload(`/v1/exports/${exportJobId}`, token);
}

export async function fetchExportStatus(
  token: string,
  exportJobId: string,
): Promise<ExportJobResponse> {
  return apiRequest<ExportJobResponse>(`/v1/exports/${exportJobId}/status`, token);
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
  return apiRequest<TemplateResponse>(`/v1/templates/${templateId}`, token, {
    method: "PATCH",
    body: JSON.stringify(payload),
  });
}

export async function deleteTemplate(token: string, templateId: string): Promise<void> {
  await apiRequest<void>(`/v1/templates/${templateId}`, token, {
    method: "DELETE",
  });
}

export async function fetchTemplates(token: string): Promise<TemplateListResponse> {
  return apiRequest<TemplateListResponse>("/v1/templates", token);
}

export async function createScannerJob(
  token: string,
  payload: CreateScannerJobRequest,
): Promise<ScannerJobResponse> {
  return apiRequest<ScannerJobResponse>("/v1/scans", token, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function fetchScannerJob(
  token: string,
  jobId: string,
): Promise<ScannerJobResponse> {
  return apiRequest<ScannerJobResponse>(`/v1/scans/${jobId}`, token);
}

export async function fetchScannerRecommendations(
  token: string,
  jobId: string,
): Promise<ScannerRecommendationListResponse> {
  return apiRequest<ScannerRecommendationListResponse>(`/v1/scans/${jobId}/recommendations`, token);
}

export async function fetchForecast(
  token: string,
  ticker: string,
  options?: { strategyType?: string; horizonDays?: number },
): Promise<ForecastEnvelopeResponse> {
  const params = new URLSearchParams();
  if (options?.strategyType != null) params.set("strategy_type", options.strategyType);
  if (options?.horizonDays != null) params.set("horizon_days", String(options.horizonDays));
  const qs = params.toString();
  return apiRequest<ForecastEnvelopeResponse>(
    `/v1/forecasts/${encodeURIComponent(ticker)}${qs ? `?${qs}` : ""}`,
    token,
  );
}

export async function createSymbolAnalysis(
  token: string,
  symbol: string,
  idempotencyKey?: string,
): Promise<SymbolAnalysisSummary> {
  return apiRequest<SymbolAnalysisSummary>("/v1/analysis", token, {
    method: "POST",
    body: JSON.stringify({ symbol, idempotency_key: idempotencyKey }),
  });
}

export async function fetchAnalysisStatus(
  token: string,
  analysisId: string,
): Promise<SymbolAnalysisSummary> {
  return apiRequest<SymbolAnalysisSummary>(`/v1/analysis/${analysisId}/status`, token);
}

export async function fetchAnalysisFull(
  token: string,
  analysisId: string,
): Promise<SymbolAnalysisFullResponse> {
  return apiRequest<SymbolAnalysisFullResponse>(`/v1/analysis/${analysisId}`, token);
}
