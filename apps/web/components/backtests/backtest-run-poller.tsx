"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Loader2 } from "lucide-react";
import { fetchBacktestRun } from "@/lib/api/client";
import { isTerminalStatus, statusLabel } from "@/lib/backtests/format";
import type { BacktestRunDetailResponse, RunStatus } from "@/lib/backtests/types";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

const POLL_INTERVAL_MS = 2_000;
const MAX_POLLS = 120; // ~4 minutes max

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
  const [pollCount, setPollCount] = useState(0);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const poll = useCallback(async () => {
    try {
      const token = await getToken();
      if (!token) return;

      const run = await fetchBacktestRun(token, runId);
      setStatus(run.status);

      if (isTerminalStatus(run.status)) {
        // Refresh the server component to show full results
        router.refresh();
        return;
      }

      setPollCount((count) => {
        if (count + 1 >= MAX_POLLS) return count + 1;
        timerRef.current = setTimeout(poll, POLL_INTERVAL_MS);
        return count + 1;
      });
    } catch {
      // On error, keep trying a few more times
      setPollCount((count) => {
        if (count + 1 >= MAX_POLLS) return count + 1;
        timerRef.current = setTimeout(poll, POLL_INTERVAL_MS * 2);
        return count + 1;
      });
    }
  }, [getToken, runId, router]);

  useEffect(() => {
    if (!isTerminalStatus(initialStatus)) {
      timerRef.current = setTimeout(poll, POLL_INTERVAL_MS);
    }

    return () => {
      if (timerRef.current) clearTimeout(timerRef.current);
    };
  }, [initialStatus, poll]);

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
      {pollCount >= MAX_POLLS ? (
        <CardContent>
          <p className="text-sm text-muted-foreground">
            Polling timed out after {MAX_POLLS} attempts. Refresh the page to check the latest
            status.
          </p>
        </CardContent>
      ) : null}
    </Card>
  );
}
