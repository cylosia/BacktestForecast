"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Loader2 } from "lucide-react";
import { fetchScannerJob } from "@/lib/api/client";
import { isTerminalStatus, statusLabel } from "@/lib/backtests/format";
import type { ScannerJobStatus } from "@/lib/backtests/types";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

const POLL_INTERVAL_MS = 3_000;
const MAX_POLLS = 200; // ~10 minutes max

export function ScannerJobPoller({
  jobId,
  initialStatus,
  candidateCount,
}: {
  jobId: string;
  initialStatus: ScannerJobStatus;
  candidateCount: number;
}) {
  const router = useRouter();
  const { getToken } = useAuth();
  const [status, setStatus] = useState<ScannerJobStatus | string>(initialStatus);
  const [evaluated, setEvaluated] = useState(0);
  const [pollCount, setPollCount] = useState(0);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const poll = useCallback(async () => {
    try {
      const token = await getToken();
      if (!token) return;

      const job = await fetchScannerJob(token, jobId);
      setStatus(job.status);
      setEvaluated(job.evaluated_candidate_count);

      if (isTerminalStatus(job.status)) {
        router.refresh();
        return;
      }

      setPollCount((count) => {
        if (count + 1 >= MAX_POLLS) return count + 1;
        timerRef.current = setTimeout(poll, POLL_INTERVAL_MS);
        return count + 1;
      });
    } catch {
      setPollCount((count) => {
        if (count + 1 >= MAX_POLLS) return count + 1;
        timerRef.current = setTimeout(poll, POLL_INTERVAL_MS * 2);
        return count + 1;
      });
    }
  }, [getToken, jobId, router]);

  useEffect(() => {
    if (!isTerminalStatus(initialStatus)) {
      timerRef.current = setTimeout(poll, POLL_INTERVAL_MS);
    }
    return () => {
      if (timerRef.current) clearTimeout(timerRef.current);
    };
  }, [initialStatus, poll]);

  if (isTerminalStatus(status)) return null;

  const pct = candidateCount > 0 ? Math.round((evaluated / candidateCount) * 100) : 0;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-3">
          <Loader2 className="h-5 w-5 animate-spin text-primary" />
          Scanner {statusLabel(status).toLowerCase()}
        </CardTitle>
        <CardDescription>
          {status === "queued"
            ? "Your scan is queued and will start processing shortly."
            : `Evaluating candidates — ${evaluated} of ${candidateCount} complete (${pct}%).`}
        </CardDescription>
      </CardHeader>
      {candidateCount > 0 && status === "running" ? (
        <CardContent>
          <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
            <div
              className="h-full rounded-full bg-primary transition-all duration-500"
              style={{ width: `${pct}%` }}
            />
          </div>
        </CardContent>
      ) : null}
      {pollCount >= MAX_POLLS ? (
        <CardContent>
          <p className="text-sm text-muted-foreground">
            Polling timed out. Refresh the page to check the latest status.
          </p>
        </CardContent>
      ) : null}
    </Card>
  );
}
