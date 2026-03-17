import { auth } from "@clerk/nextjs/server";
import { apiRequest } from "@/lib/api/shared";
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
  TemplateListResponse,
} from "@backtestforecast/api-client";

async function getServerToken(): Promise<string> {
  const { isAuthenticated, getToken, redirectToSignIn } = await auth();

  if (!isAuthenticated) {
    redirectToSignIn();
    throw new Error("Authentication required.");
  }

  const token = await getToken();
  if (!token) {
    redirectToSignIn();
    throw new Error("Missing Clerk session token.");
  }

  return token;
}

export async function getCurrentUser(): Promise<CurrentUserResponse> {
  const token = await getServerToken();
  return apiRequest<CurrentUserResponse>("/v1/me", token, { cache: "no-store" });
}

export async function getBacktestHistory(limit = 50, offset = 0): Promise<BacktestRunListResponse> {
  const token = await getServerToken();
  const safeLimit = Math.max(1, Math.min(limit, 100));
  const safeOffset = Math.max(0, offset);
  return apiRequest<BacktestRunListResponse>(`/v1/backtests?limit=${safeLimit}&offset=${safeOffset}`, token, { cache: "no-store" });
}

export async function getBacktestRun(runId: string): Promise<BacktestRunDetailResponse> {
  const token = await getServerToken();
  return apiRequest<BacktestRunDetailResponse>(`/v1/backtests/${encodeURIComponent(runId)}`, token, { cache: "no-store" });
}

export async function getTemplates(): Promise<TemplateListResponse> {
  const token = await getServerToken();
  return apiRequest<TemplateListResponse>("/v1/templates", token, { cache: "no-store" });
}

export async function compareBacktests(runIds: string[]): Promise<CompareBacktestsResponse> {
  const token = await getServerToken();
  return apiRequest<CompareBacktestsResponse>("/v1/backtests/compare", token, {
    method: "POST",
    body: JSON.stringify({ run_ids: runIds }),
    cache: "no-store",
  });
}

export async function getScannerJobs(limit = 50, offset = 0): Promise<ScannerJobListResponse> {
  const safeLimit = Math.max(1, Math.min(limit, 50));
  const safeOffset = Math.max(0, offset);
  const token = await getServerToken();
  return apiRequest<ScannerJobListResponse>(`/v1/scans?limit=${safeLimit}&offset=${safeOffset}`, token, { cache: "no-store" });
}

export async function getScannerJob(jobId: string): Promise<ScannerJobResponse> {
  const token = await getServerToken();
  return apiRequest<ScannerJobResponse>(`/v1/scans/${encodeURIComponent(jobId)}`, token, { cache: "no-store" });
}

export async function getScannerRecommendations(jobId: string): Promise<ScannerRecommendationListResponse> {
  const token = await getServerToken();
  return apiRequest<ScannerRecommendationListResponse>(`/v1/scans/${encodeURIComponent(jobId)}/recommendations`, token, { cache: "no-store" });
}

export async function getStrategyCatalog(): Promise<StrategyCatalogResponse> {
  const token = await getServerToken();
  return apiRequest<StrategyCatalogResponse>("/v1/strategy-catalog", token, { cache: "no-store" });
}

export async function getDailyPicks(): Promise<DailyPicksResponse> {
  const token = await getServerToken();
  return apiRequest<DailyPicksResponse>("/v1/daily-picks", token, { cache: "no-store" });
}

export async function getAnalysisHistory(limit = 10, offset = 0) {
  const token = await getServerToken();
  return apiRequest<AnalysisListResponse>(`/v1/analysis?limit=${limit}&offset=${offset}`, token, { cache: "no-store" });
}

export async function getDailyPicksHistory(limit = 10) {
  const token = await getServerToken();
  return apiRequest<PipelineHistoryResponse>(`/v1/daily-picks/history?limit=${limit}`, token, { cache: "no-store" });
}
