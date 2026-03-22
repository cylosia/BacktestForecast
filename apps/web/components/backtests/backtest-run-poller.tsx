"use client";

import { useCallback, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Loader2 } from "lucide-react";
import { fetchBacktestRunStatus } from "@/lib/api/client";
import { isTerminalStatus, statusLabel } from "@/lib/backtests/format";
import type { BacktestRunStatusResponse, RunStatus } from "@backtestforecast/api-client";
import { useSSE } from "@/hooks/use-sse";
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
  const [diagnosticCode, setDiagnosticCode] = useState<string | null>(null);

  const fetcher = useCallback(async (signal: AbortSignal) => {
    const token = await getToken();
    if (!token) throw new Error("No token");
    return fetchBacktestRunStatus(token, runId, signal);
  }, [getToken, runId]);

  const { status: sseStatus } = useSSE<BacktestRunStatusResponse>({
    resourceType: "backtests",
    resourceId: runId,
    onProgress: (data) => {
      const s = data.status as RunStatus | string;
      if (s) setStatus(s);
      if (typeof data.error_code === "string") setDiagnosticCode(data.error_code);
    },
    onComplete: () => router.refresh(),
    isTerminal: isTerminalStatus,
    autoStart: !isTerminalStatus(initialStatus),
    pollingFallback: {
      fetcher,
      onComplete: () => router.refresh(),
      onProgress: (run) => {
        setStatus(run.status);
        setDiagnosticCode(run.error_code ?? null);
      },
      isComplete: (run) => isTerminalStatus(run.status),
      interval: POLL_INTERVAL_MS,
      maxAttempts: MAX_POLLS,
    },
  });

  if (isTerminalStatus(status)) {
    return null;
  }

  const queueMessage = diagnosticCode === "dispatch_stuck"
    ? "This backtest is still queued because dispatch is being repaired automatically. You can leave this page open or check history in a moment."
    : diagnosticCode === "dispatch_delayed"
      ? "This backtest is still queued longer than expected, but worker capacity may still pick it up shortly."
      : "Your backtest is queued and will begin processing shortly.";

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-3">
          <Loader2 className="h-5 w-5 animate-spin text-primary" />
          Backtest {statusLabel(status).toLowerCase()}
        </CardTitle>
        <CardDescription>
          {status === "queued"
            ? queueMessage
            : "Your backtest is running. Results will appear automatically when complete."}
        </CardDescription>
      </CardHeader>
      {sseStatus === "error" ? (
        <CardContent>
          <p className="text-sm text-muted-foreground">
            Something went wrong while checking status. Refresh the page to see the latest results.
          </p>
        </CardContent>
      ) : null}
    </Card>
  );
}
