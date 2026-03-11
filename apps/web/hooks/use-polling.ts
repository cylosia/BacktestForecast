"use client";

import { useCallback, useEffect, useRef, useState } from "react";

export type PollingStatus = "idle" | "polling" | "done" | "timeout" | "error";

export interface UsePollingOptions<T> {
  /** Async function that fetches the latest state. */
  fetcher: () => Promise<T>;
  /** Called once when `isComplete` returns true. */
  onComplete: (result: T) => void;
  /** Return true when the resource has reached a terminal state. */
  isComplete: (result: T) => boolean;
  /** Milliseconds between polls. */
  interval: number;
  /** Maximum number of attempts before giving up. */
  maxAttempts: number;
  /** Optional callback invoked with each intermediate result. */
  onProgress?: (result: T) => void;
  /** If true, polling starts automatically on mount. Default false. */
  autoStart?: boolean;
}

export interface UsePollingReturn {
  status: PollingStatus;
  /** Begin (or restart) the polling loop. */
  start: () => void;
  /** Number of polls executed so far. */
  attempts: number;
}

/**
 * Generic polling hook that replaces the four bespoke polling
 * implementations across backtest-run-poller, scanner-job-poller,
 * export-actions, and symbol-analysis-launcher.
 */
export function usePolling<T>({
  fetcher,
  onComplete,
  isComplete,
  interval,
  maxAttempts,
  onProgress,
  autoStart = false,
}: UsePollingOptions<T>): UsePollingReturn {
  const [status, setStatus] = useState<PollingStatus>(autoStart ? "polling" : "idle");
  const [attempts, setAttempts] = useState(0);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mountedRef = useRef(true);

  const cancel = useCallback(() => {
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }
  }, []);

  const poll = useCallback(async () => {
    if (!mountedRef.current) return;

    try {
      const result = await fetcher();
      if (!mountedRef.current) return;

      onProgress?.(result);

      if (isComplete(result)) {
        setStatus("done");
        onComplete(result);
        return;
      }

      setAttempts((prev) => {
        const next = prev + 1;
        if (next >= maxAttempts) {
          setStatus("timeout");
          return next;
        }
        timerRef.current = setTimeout(poll, interval);
        return next;
      });
    } catch {
      if (!mountedRef.current) return;
      setAttempts((prev) => {
        const next = prev + 1;
        if (next >= maxAttempts) {
          setStatus("error");
          return next;
        }
        timerRef.current = setTimeout(poll, interval * 2);
        return next;
      });
    }
  }, [fetcher, isComplete, onComplete, onProgress, interval, maxAttempts]);

  const start = useCallback(() => {
    cancel();
    setAttempts(0);
    setStatus("polling");
    timerRef.current = setTimeout(poll, interval);
  }, [cancel, poll, interval]);

  useEffect(() => {
    mountedRef.current = true;
    if (autoStart) {
      start();
    }
    return () => {
      mountedRef.current = false;
      cancel();
    };
    // Only run on mount/unmount
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return { status, start, attempts };
}
