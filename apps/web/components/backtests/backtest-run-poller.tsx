"use client";

import { useCallback, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Loader2 } from "lucide-react";
import { fetchBacktestRun } from "@/lib/api/client";
import { isTerminalStatus, statusLabel } from "@/lib/backtests/format";
import type { BacktestRunDetailResponse, RunStatus } from "@backtestforecast/api-client";
import { usePolling } from "@/hooks/use-polling";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

const POLL_INTERVAL_MS = 2_000;
const MAX_POLLS = 120;

export function BacktestRunPoller({
  runId,
  initialStatus,
}: {
  runId: string;
  initialStatus: RunStatus | string;
}) {
  const router = useRouter();
  const { getToken } = useAuth();
  const [status, setStatus] = useState<RunStatus | string>(initialStatus);

  const fetcher = useCallback(async () => {
    const token = await getToken();
    if (!token) throw new Error("No token");
    return fetchBacktestRun(token, runId);
  }, [getToken, runId]);

  const { status: pollStatus, attempts } = usePolling<BacktestRunDetailResponse>({
    fetcher,
    onComplete: () => router.refresh(),
    onProgress: (run) => setStatus(run.status),
    isComplete: (run) => isTerminalStatus(run.status),
    interval: POLL_INTERVAL_MS,
    maxAttempts: MAX_POLLS,
    autoStart: !isTerminalStatus(initialStatus),
  });

  if (isTerminalStatus(status)) {
    return null;
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-3">
          <Loader2 className="h-5 w-5 animate-spin text-primary" />
          Backtest {statusLabel(status).toLowerCase()}
        </CardTitle>
        <CardDescription>
          {status === "queued"
            ? "Your backtest is queued and will begin processing shortly."
            : "Your backtest is running. Results will appear automatically when complete."}
        </CardDescription>
      </CardHeader>
      {pollStatus === "timeout" || pollStatus === "error" ? (
        <CardContent>
          <p className="text-sm text-muted-foreground">
            {pollStatus === "error"
              ? "Something went wrong while checking status. Refresh the page to see the latest results."
              : `Polling timed out after ${attempts} attempts. Refresh the page to check the latest status.`}
          </p>
        </CardContent>
      ) : null}
    </Card>
  );
}
