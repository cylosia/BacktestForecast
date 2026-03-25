import { auth } from "@clerk/nextjs/server";
import { cache } from "react";
import { validatedApiRequest } from "@/lib/api/shared";
import {
  validateAnalysisDetailResponse,
  validateAnalysisListResponse,
  validateBacktestRunListResponse,
  validateBacktestRunDetailResponse,
  validateCompareBacktestsResponse,
  validateCurrentUserResponse,
  validateDailyPicksResponse,
  validateMetaResponse,
  validateMultiStepRunDetailResponse,
  validateMultiStepRunListResponse,
  validateMultiStepRunStatusResponse,
  validateMultiSymbolRunDetailResponse,
  validateMultiSymbolRunListResponse,
  validateMultiSymbolRunStatusResponse,
  validatePipelineHistoryResponse,
  validateScannerJobListResponse,
  validateScannerJobResponse,
  validateScannerRecommendationListResponse,
  validateStrategyCatalogResponse,
  validateSweepJobListResponse,
  validateSweepJobResponse,
  validateSweepResultListResponse,
} from "@/lib/api/contracts";
import { buildCursorPaginatedPath, buildPaginatedListPath } from "@/lib/api/pagination";
import type {
  MultiStepRunDetailResponse,
  MultiStepRunListResponse,
  MultiStepRunStatusResponse,
  MultiSymbolRunDetailResponse,
  MultiSymbolRunListResponse,
  MultiSymbolRunStatusResponse,
} from "@/lib/api/multi-workflow-types";
import { validateTemplateListResponse } from "@/lib/templates/contracts";
import type {
  AnalysisDetailResponse,
  AnalysisListResponse,
  BacktestRunDetailResponse,
  BacktestRunListResponse,
  CompareBacktestsResponse,
  CurrentUserResponse,
  DailyPicksResponse,
  PipelineHistoryResponse,
  ScannerJobListResponse,
  ScannerJobResponse,
  ScannerRecommendationListResponse,
  StrategyCatalogResponse,
  SweepJobListResponse,
  SweepJobResponse,
  SweepResultListResponse,
  TemplateListResponse,
} from "@backtestforecast/api-client";


const getServerToken = cache(async (): Promise<string> => {
  const { isAuthenticated, getToken, redirectToSignIn } = await auth();

  if (!isAuthenticated) {
    redirectToSignIn();
    throw new Error("Redirecting to sign-in.");
  }

  const token = await getToken();
  if (!token) {
    redirectToSignIn();
    throw new Error("Missing Clerk session token.");
  }

  return token;
});

const loadCurrentUser = cache(async (token: string): Promise<CurrentUserResponse> => {
  return validatedApiRequest<CurrentUserResponse>("/v1/me", token, validateCurrentUserResponse, { cache: "no-store" });
});

export async function getCurrentUser(): Promise<CurrentUserResponse> {
  return loadCurrentUser(await getServerToken());
}

export interface RuntimeMetaResponse {
  service: string;
  version: string;
  billing_enabled?: boolean | null;
  environment?: string | null;
  daily_picks_schedule_utc?: string | null;
  features?: CurrentUserResponse["features"] extends infer _T ? Record<string, boolean> | null : Record<string, boolean> | null;
}

export const getMeta = cache(async (): Promise<RuntimeMetaResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<RuntimeMetaResponse>("/v1/meta", token, validateMetaResponse, { cache: "no-store" });
});

export const getBacktestHistory = cache(async (limit = 50, offset = 0, cursor?: string | null): Promise<BacktestRunListResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<BacktestRunListResponse>(
    buildPaginatedListPath("/v1/backtests", limit, offset, 100, cursor),
    token,
    validateBacktestRunListResponse,
    { cache: "no-store" },
  );
});

export const getBacktestRun = cache(async (runId: string): Promise<BacktestRunDetailResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<BacktestRunDetailResponse>(
    `/v1/backtests/${encodeURIComponent(runId)}`,
    token,
    validateBacktestRunDetailResponse,
    { cache: "no-store" },
  );
});

export const getMultiSymbolBacktestHistory = cache(async (limit = 50, offset = 0, cursor?: string | null): Promise<MultiSymbolRunListResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<MultiSymbolRunListResponse>(
    buildPaginatedListPath("/v1/multi-symbol-backtests", limit, offset, 100, cursor),
    token,
    validateMultiSymbolRunListResponse,
    { cache: "no-store" },
  );
});

export const getMultiSymbolBacktestRun = cache(async (runId: string): Promise<MultiSymbolRunDetailResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<MultiSymbolRunDetailResponse>(
    `/v1/multi-symbol-backtests/${encodeURIComponent(runId)}`,
    token,
    validateMultiSymbolRunDetailResponse,
    { cache: "no-store" },
  );
});

export const getMultiSymbolBacktestRunStatus = cache(async (runId: string): Promise<MultiSymbolRunStatusResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<MultiSymbolRunStatusResponse>(
    `/v1/multi-symbol-backtests/${encodeURIComponent(runId)}/status`,
    token,
    validateMultiSymbolRunStatusResponse,
    { cache: "no-store" },
  );
});

export const getMultiStepBacktestHistory = cache(async (limit = 50, offset = 0, cursor?: string | null): Promise<MultiStepRunListResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<MultiStepRunListResponse>(
    buildPaginatedListPath("/v1/multi-step-backtests", limit, offset, 100, cursor),
    token,
    validateMultiStepRunListResponse,
    { cache: "no-store" },
  );
});

export const getMultiStepBacktestRun = cache(async (runId: string): Promise<MultiStepRunDetailResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<MultiStepRunDetailResponse>(
    `/v1/multi-step-backtests/${encodeURIComponent(runId)}`,
    token,
    validateMultiStepRunDetailResponse,
    { cache: "no-store" },
  );
});

export const getMultiStepBacktestRunStatus = cache(async (runId: string): Promise<MultiStepRunStatusResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<MultiStepRunStatusResponse>(
    `/v1/multi-step-backtests/${encodeURIComponent(runId)}/status`,
    token,
    validateMultiStepRunStatusResponse,
    { cache: "no-store" },
  );
});

export const getTemplates = cache(async (): Promise<TemplateListResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<TemplateListResponse>(
    "/v1/templates",
    token,
    validateTemplateListResponse,
    { cache: "no-store" },
  );
});

export async function compareBacktests(runIds: string[]): Promise<CompareBacktestsResponse> {
  if (runIds.length < 2 || runIds.length > 8) {
    throw new Error("compareBacktests requires between 2 and 8 run IDs.");
  }
  if (new Set(runIds).size !== runIds.length) {
    throw new Error("compareBacktests requires unique run IDs.");
  }
  const token = await getServerToken();
  return validatedApiRequest<CompareBacktestsResponse>("/v1/backtests/compare", token, validateCompareBacktestsResponse, {
    method: "POST",
    body: JSON.stringify({ run_ids: runIds }),
    cache: "no-store",
  });
}

export const getScannerJobs = cache(async (limit = 50, offset = 0, cursor?: string | null): Promise<ScannerJobListResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<ScannerJobListResponse>(
    buildPaginatedListPath("/v1/scans", limit, offset, 50, cursor),
    token,
    validateScannerJobListResponse,
    { cache: "no-store" },
  );
});

export const getScannerJob = cache(async (jobId: string): Promise<ScannerJobResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<ScannerJobResponse>(
    `/v1/scans/${encodeURIComponent(jobId)}`,
    token,
    validateScannerJobResponse,
    { cache: "no-store" },
  );
});

export const getScannerRecommendations = cache(async (jobId: string): Promise<ScannerRecommendationListResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<ScannerRecommendationListResponse>(
    `/v1/scans/${encodeURIComponent(jobId)}/recommendations`,
    token,
    validateScannerRecommendationListResponse,
    { cache: "no-store" },
  );
});

export const getSweepJobs = cache(async (limit = 50, offset = 0, cursor?: string | null): Promise<SweepJobListResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<SweepJobListResponse>(
    buildPaginatedListPath("/v1/sweeps", limit, offset, 50, cursor),
    token,
    validateSweepJobListResponse,
    { cache: "no-store" },
  );
});

export const getSweepJob = cache(async (jobId: string): Promise<SweepJobResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<SweepJobResponse>(
    `/v1/sweeps/${encodeURIComponent(jobId)}`,
    token,
    validateSweepJobResponse,
    { cache: "no-store" },
  );
});

export const getSweepResults = cache(async (jobId: string): Promise<SweepResultListResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<SweepResultListResponse>(
    `/v1/sweeps/${encodeURIComponent(jobId)}/results`,
    token,
    validateSweepResultListResponse,
    { cache: "no-store" },
  );
});

export const getStrategyCatalog = cache(async (): Promise<StrategyCatalogResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<StrategyCatalogResponse>(
    "/v1/strategy-catalog",
    token,
    validateStrategyCatalogResponse,
    { cache: "no-store" },
  );
});

export const getDailyPicks = cache(async (): Promise<DailyPicksResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<DailyPicksResponse>("/v1/daily-picks", token, validateDailyPicksResponse, {
    cache: "no-store",
  });
});

export const getAnalysisHistory = cache(async (limit = 10, offset = 0, cursor?: string | null) => {
  const token = await getServerToken();
  return validatedApiRequest<AnalysisListResponse>(
    buildPaginatedListPath("/v1/analysis", limit, offset, 50, cursor),
    token,
    validateAnalysisListResponse,
    { cache: "no-store" },
  );
});

export const getAnalysisDetail = cache(async (analysisId: string): Promise<AnalysisDetailResponse> => {
  const token = await getServerToken();
  return validatedApiRequest<AnalysisDetailResponse>(
    `/v1/analysis/${encodeURIComponent(analysisId)}`,
    token,
    validateAnalysisDetailResponse,
    { cache: "no-store" },
  );
});

export const getDailyPicksHistory = cache(async (limit = 10, cursor?: string | null) => {
  const token = await getServerToken();
  return validatedApiRequest<PipelineHistoryResponse>(
    buildCursorPaginatedPath("/v1/daily-picks/history", limit, 30, cursor),
    token,
    validatePipelineHistoryResponse,
    { cache: "no-store" },
  );
});
