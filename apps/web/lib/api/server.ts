import { auth } from "@clerk/nextjs/server";
import { apiRequest } from "@/lib/api/shared";
import type {
  BacktestRunDetailResponse,
  BacktestRunListResponse,
  CompareBacktestsResponse,
  CurrentUserResponse,
  DailyPicksResponse,
  ScannerJobListResponse,
  ScannerJobResponse,
  ScannerRecommendationListResponse,
  StrategyCatalogResponse,
  TemplateListResponse,
} from "@/lib/backtests/types";

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

export async function getBacktestHistory(limit = 50): Promise<BacktestRunListResponse> {
  const token = await getServerToken();
  return apiRequest<BacktestRunListResponse>(`/v1/backtests?limit=${limit}`, token, { cache: "no-store" });
}

export async function getBacktestRun(runId: string): Promise<BacktestRunDetailResponse> {
  const token = await getServerToken();
  return apiRequest<BacktestRunDetailResponse>(`/v1/backtests/${runId}`, token, { cache: "no-store" });
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

export async function getScannerJobs(limit = 50): Promise<ScannerJobListResponse> {
  const token = await getServerToken();
  return apiRequest<ScannerJobListResponse>(`/v1/scans?limit=${limit}`, token, { cache: "no-store" });
}

export async function getScannerJob(jobId: string): Promise<ScannerJobResponse> {
  const token = await getServerToken();
  return apiRequest<ScannerJobResponse>(`/v1/scans/${jobId}`, token, { cache: "no-store" });
}

export async function getScannerRecommendations(jobId: string): Promise<ScannerRecommendationListResponse> {
  const token = await getServerToken();
  return apiRequest<ScannerRecommendationListResponse>(`/v1/scans/${jobId}/recommendations`, token, { cache: "no-store" });
}

export async function getStrategyCatalog(): Promise<StrategyCatalogResponse> {
  const token = await getServerToken();
  return apiRequest<StrategyCatalogResponse>("/v1/strategy-catalog", token, { cache: "no-store" });
}

export async function getDailyPicks(): Promise<DailyPicksResponse> {
  const token = await getServerToken();
  return apiRequest<DailyPicksResponse>("/v1/daily-picks", token, { cache: "no-store" });
}
