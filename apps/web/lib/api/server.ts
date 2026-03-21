import { auth } from "@clerk/nextjs/server";
import { cache } from "react";
import { apiRequest } from "@/lib/api/shared";
import { buildCursorPaginatedPath, buildPaginatedListPath } from "@/lib/api/pagination";
import type {
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

export const getCurrentUser = cache(async (): Promise<CurrentUserResponse> => {
  const token = await getServerToken();
  const user = await apiRequest<CurrentUserResponse>("/v1/me", token, { cache: "no-store" });
  if (!user || typeof user.id !== "string" || typeof user.plan_tier !== "string") {
    throw new Error("Invalid user response shape from API");
  }
  return user;
});

export const getBacktestHistory = cache(async (limit = 50, offset = 0, cursor?: string | null): Promise<BacktestRunListResponse> => {
  const token = await getServerToken();
  return apiRequest<BacktestRunListResponse>(
    buildPaginatedListPath("/v1/backtests", limit, offset, 100, cursor),
    token,
    { cache: "no-store" },
  );
});

export const getBacktestRun = cache(async (runId: string): Promise<BacktestRunDetailResponse> => {
  const token = await getServerToken();
  return apiRequest<BacktestRunDetailResponse>(`/v1/backtests/${encodeURIComponent(runId)}`, token, { cache: "no-store" });
});

export const getTemplates = cache(async (): Promise<TemplateListResponse> => {
  const token = await getServerToken();
  return apiRequest<TemplateListResponse>("/v1/templates", token, { cache: "no-store" });
});

export async function compareBacktests(runIds: string[]): Promise<CompareBacktestsResponse> {
  if (runIds.length < 2 || runIds.length > 8) {
    throw new Error("compareBacktests requires between 2 and 8 run IDs.");
  }
  if (new Set(runIds).size !== runIds.length) {
    throw new Error("compareBacktests requires unique run IDs.");
  }
  const token = await getServerToken();
  return apiRequest<CompareBacktestsResponse>("/v1/backtests/compare", token, {
    method: "POST",
    body: JSON.stringify({ run_ids: runIds }),
    cache: "no-store",
  });
}

export const getScannerJobs = cache(async (limit = 50, offset = 0, cursor?: string | null): Promise<ScannerJobListResponse> => {
  const token = await getServerToken();
  return apiRequest<ScannerJobListResponse>(
    buildPaginatedListPath("/v1/scans", limit, offset, 50, cursor),
    token,
    { cache: "no-store" },
  );
});

export const getScannerJob = cache(async (jobId: string): Promise<ScannerJobResponse> => {
  const token = await getServerToken();
  return apiRequest<ScannerJobResponse>(`/v1/scans/${encodeURIComponent(jobId)}`, token, { cache: "no-store" });
});

export const getScannerRecommendations = cache(async (jobId: string): Promise<ScannerRecommendationListResponse> => {
  const token = await getServerToken();
  return apiRequest<ScannerRecommendationListResponse>(`/v1/scans/${encodeURIComponent(jobId)}/recommendations`, token, { cache: "no-store" });
});

export const getSweepJobs = cache(async (limit = 50, offset = 0, cursor?: string | null): Promise<SweepJobListResponse> => {
  const token = await getServerToken();
  return apiRequest<SweepJobListResponse>(
    buildPaginatedListPath("/v1/sweeps", limit, offset, 50, cursor),
    token,
    { cache: "no-store" },
  );
});

export const getSweepJob = cache(async (jobId: string): Promise<SweepJobResponse> => {
  const token = await getServerToken();
  return apiRequest<SweepJobResponse>(`/v1/sweeps/${encodeURIComponent(jobId)}`, token, { cache: "no-store" });
});

export const getSweepResults = cache(async (jobId: string): Promise<SweepResultListResponse> => {
  const token = await getServerToken();
  return apiRequest<SweepResultListResponse>(`/v1/sweeps/${encodeURIComponent(jobId)}/results`, token, { cache: "no-store" });
});

export const getStrategyCatalog = cache(async (): Promise<StrategyCatalogResponse> => {
  const token = await getServerToken();
  return apiRequest<StrategyCatalogResponse>("/v1/strategy-catalog", token, { cache: "no-store" });
});

export const getDailyPicks = cache(async (): Promise<DailyPicksResponse> => {
  const token = await getServerToken();
  return apiRequest<DailyPicksResponse>("/v1/daily-picks", token, { cache: "no-store" });
});

export const getAnalysisHistory = cache(async (limit = 10, offset = 0, cursor?: string | null) => {
  const token = await getServerToken();
  return apiRequest<AnalysisListResponse>(
    buildPaginatedListPath("/v1/analysis", limit, offset, 50, cursor),
    token,
    { cache: "no-store" },
  );
});

export const getDailyPicksHistory = cache(async (limit = 10, cursor?: string | null) => {
  const token = await getServerToken();
  return apiRequest<PipelineHistoryResponse>(
    buildCursorPaginatedPath("/v1/daily-picks/history", limit, 30, cursor),
    token,
    { cache: "no-store" },
  );
});
