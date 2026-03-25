import { apiDownload, apiRequest, validatedApiRequest } from "@/lib/api/shared";
import {
  validateAnalysisDetailResponse,
  validateBacktestRunDetailResponse,
  validateBacktestRunStatusResponse,
  validateCheckoutSessionResponse,
  validateCompareBacktestsResponse,
  validateExportJobResponse,
  validateForecastEnvelopeResponse,
  validateMultiStepRunDetailResponse,
  validateMultiStepRunStatusResponse,
  validateMultiSymbolRunDetailResponse,
  validateMultiSymbolRunStatusResponse,
  validatePortalSessionResponse,
  validateScannerJobResponse,
  validateScannerRecommendationListResponse,
  validateSymbolAnalysisSummary,
  validateSweepJobResponse,
} from "@/lib/api/contracts";
import type {
  CreateMultiStepRunRequest,
  CreateMultiSymbolRunRequest,
  MultiStepRunDetailResponse,
  MultiStepRunStatusResponse,
  MultiSymbolRunDetailResponse,
  MultiSymbolRunStatusResponse,
} from "@/lib/api/multi-workflow-types";
import { validateTemplateResponse } from "@/lib/templates/contracts";
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
  CreateSweepRequest,
  CreateTemplateRequest,
  UpdateTemplateRequest,
  ExportJobResponse,
  ForecastEnvelopeResponse,
  PortalSessionResponse,
  ScannerJobResponse,
  ScannerRecommendationListResponse,
  SweepJobResponse,
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
  return validatedApiRequest<BacktestRunDetailResponse>("/v1/backtests", token, validateBacktestRunDetailResponse, {
    method: "POST",
    body: JSON.stringify(payload),
    signal,
  });
}

export async function createMultiSymbolRun(
  token: string,
  payload: CreateMultiSymbolRunRequest,
  signal?: AbortSignal,
): Promise<MultiSymbolRunDetailResponse> {
  return validatedApiRequest<MultiSymbolRunDetailResponse>("/v1/multi-symbol-backtests", token, validateMultiSymbolRunDetailResponse, {
    method: "POST",
    body: JSON.stringify(payload),
    signal,
  });
}

export async function createMultiStepRun(
  token: string,
  payload: CreateMultiStepRunRequest,
  signal?: AbortSignal,
): Promise<MultiStepRunDetailResponse> {
  return validatedApiRequest<MultiStepRunDetailResponse>("/v1/multi-step-backtests", token, validateMultiStepRunDetailResponse, {
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
  return validatedApiRequest<BacktestRunStatusResponse>(
    `/v1/backtests/${encodeURIComponent(runId)}/status`,
    token,
    validateBacktestRunStatusResponse,
    signal ? { signal } : undefined,
  );
}

export async function fetchMultiSymbolRunStatus(
  token: string,
  runId: string,
  signal?: AbortSignal,
): Promise<MultiSymbolRunStatusResponse> {
  return validatedApiRequest<MultiSymbolRunStatusResponse>(
    `/v1/multi-symbol-backtests/${encodeURIComponent(runId)}/status`,
    token,
    validateMultiSymbolRunStatusResponse,
    signal ? { signal } : undefined,
  );
}

export async function fetchMultiStepRunStatus(
  token: string,
  runId: string,
  signal?: AbortSignal,
): Promise<MultiStepRunStatusResponse> {
  return validatedApiRequest<MultiStepRunStatusResponse>(
    `/v1/multi-step-backtests/${encodeURIComponent(runId)}/status`,
    token,
    validateMultiStepRunStatusResponse,
    signal ? { signal } : undefined,
  );
}

export async function compareBacktests(
  token: string,
  runIds: string[],
  signal?: AbortSignal,
): Promise<CompareBacktestsResponse> {
  if (runIds.length < 2 || runIds.length > 8) {
    throw new Error("compareBacktests requires between 2 and 8 run IDs.");
  }
  if (new Set(runIds).size !== runIds.length) {
    throw new Error("compareBacktests requires unique run IDs.");
  }
  return validatedApiRequest<CompareBacktestsResponse>("/v1/backtests/compare", token, validateCompareBacktestsResponse, {
    method: "POST",
    body: JSON.stringify({ run_ids: runIds }),
    signal,
  });
}

export async function createCheckoutSession(
  token: string,
  payload: CreateCheckoutSessionRequest,
): Promise<CheckoutSessionResponse> {
  return validatedApiRequest<CheckoutSessionResponse>("/v1/billing/checkout-session", token, validateCheckoutSessionResponse, {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function createPortalSession(
  token: string,
  payload?: Pick<CreatePortalSessionRequest, "return_path">,
): Promise<PortalSessionResponse> {
  const body: Record<string, unknown> = {};
  if (payload?.return_path != null) {
    body.return_path = payload.return_path;
  }
  return validatedApiRequest<PortalSessionResponse>("/v1/billing/portal-session", token, validatePortalSessionResponse, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

export async function createExport(
  token: string,
  payload: CreateExportRequest,
  signal?: AbortSignal,
): Promise<ExportJobResponse> {
  return validatedApiRequest<ExportJobResponse>("/v1/exports", token, validateExportJobResponse, {
    method: "POST",
    body: JSON.stringify(payload),
    signal,
  });
}

export async function downloadExport(token: string, exportJobId: string, signal?: AbortSignal): Promise<Response> {
  return apiDownload(`/v1/exports/${encodeURIComponent(exportJobId)}`, token, {
    ...(signal ? { signal } : {}),
    timeoutMs: 120_000,
  });
}

export async function fetchExportStatus(
  token: string,
  exportJobId: string,
  signal?: AbortSignal,
): Promise<ExportJobResponse> {
  return validatedApiRequest<ExportJobResponse>(
    `/v1/exports/${encodeURIComponent(exportJobId)}/status`,
    token,
    validateExportJobResponse,
    signal ? { signal } : undefined,
  );
}

export async function createTemplate(
  token: string,
  payload: CreateTemplateRequest,
  signal?: AbortSignal,
): Promise<TemplateResponse> {
  return validatedApiRequest<TemplateResponse>("/v1/templates", token, validateTemplateResponse, {
    method: "POST",
    body: JSON.stringify(payload),
    signal,
  });
}

export async function updateTemplate(
  token: string,
  templateId: string,
  payload: UpdateTemplateRequest,
  signal?: AbortSignal,
): Promise<TemplateResponse> {
  return validatedApiRequest<TemplateResponse>(
    `/v1/templates/${encodeURIComponent(templateId)}`,
    token,
    validateTemplateResponse,
    {
      method: "PATCH",
      body: JSON.stringify(payload),
      signal,
    },
  );
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
  return validatedApiRequest<ScannerJobResponse>("/v1/scans", token, validateScannerJobResponse, {
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
  return validatedApiRequest<ScannerJobResponse>(
    `/v1/scans/${encodeURIComponent(jobId)}`,
    token,
    validateScannerJobResponse,
    signal ? { signal } : undefined,
  );
}

export async function fetchScannerRecommendations(
  token: string,
  jobId: string,
  signal?: AbortSignal,
): Promise<ScannerRecommendationListResponse> {
  return validatedApiRequest<ScannerRecommendationListResponse>(
    `/v1/scans/${encodeURIComponent(jobId)}/recommendations`,
    token,
    validateScannerRecommendationListResponse,
    signal ? { signal } : undefined,
  );
}

export async function createSweepJob(
  token: string,
  payload: CreateSweepRequest,
  signal?: AbortSignal,
): Promise<SweepJobResponse> {
  return validatedApiRequest<SweepJobResponse>("/v1/sweeps", token, validateSweepJobResponse, {
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
  return validatedApiRequest<SweepJobResponse>(
    `/v1/sweeps/${encodeURIComponent(jobId)}`,
    token,
    validateSweepJobResponse,
    signal ? { signal } : undefined,
  );
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
  return validatedApiRequest<ForecastEnvelopeResponse>(
    `/v1/forecasts/${encodeURIComponent(ticker)}${qs ? `?${qs}` : ""}`,
    token,
    validateForecastEnvelopeResponse,
    options?.signal ? { signal: options.signal } : undefined,
  );
}

export async function createSymbolAnalysis(
  token: string,
  symbol: string,
  idempotencyKey?: string,
  signal?: AbortSignal,
): Promise<SymbolAnalysisSummary> {
  return validatedApiRequest<SymbolAnalysisSummary>("/v1/analysis", token, validateSymbolAnalysisSummary, {
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
  return validatedApiRequest<SymbolAnalysisSummary>(
    `/v1/analysis/${encodeURIComponent(analysisId)}/status`,
    token,
    validateSymbolAnalysisSummary,
    signal ? { signal } : undefined,
  );
}

export async function fetchAnalysisFull(
  token: string,
  analysisId: string,
  signal?: AbortSignal,
): Promise<SymbolAnalysisFullResponse> {
  return validatedApiRequest<SymbolAnalysisFullResponse>(
    `/v1/analysis/${encodeURIComponent(analysisId)}`,
    token,
    validateAnalysisDetailResponse,
    signal ? { signal } : undefined,
  );
}

export async function deleteAccount(
  token: string,
  signal?: AbortSignal,
): Promise<void> {
  await apiRequest<void>("/v1/account/me", token, {
    method: "DELETE",
    headers: { "X-Confirm-Delete": "permanently-delete-my-account" },
    signal,
  });
}
