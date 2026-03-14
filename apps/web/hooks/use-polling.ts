"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError } from "@/lib/api/shared";

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

  const fetcherRef = useRef(fetcher);
  const onCompleteRef = useRef(onComplete);
  const isCompleteRef = useRef(isComplete);
  const onProgressRef = useRef(onProgress);
  useEffect(() => { fetcherRef.current = fetcher; }, [fetcher]);
  useEffect(() => { onCompleteRef.current = onComplete; }, [onComplete]);
  useEffect(() => { isCompleteRef.current = isComplete; }, [isComplete]);
  useEffect(() => { onProgressRef.current = onProgress; }, [onProgress]);

  const attemptsRef = useRef(0);

  const poll = useCallback(async () => {
    if (!mountedRef.current) return;

    try {
      const result = await fetcherRef.current();
      if (!mountedRef.current) return;

      onProgressRef.current?.(result);

      if (isCompleteRef.current(result)) {
        setStatus("done");
        onCompleteRef.current(result);
        return;
      }

      const next = attemptsRef.current + 1;
      attemptsRef.current = next;
      setAttempts(next);
      if (next >= maxAttempts) {
        setStatus("timeout");
      } else {
        timerRef.current = setTimeout(poll, interval);
      }
    } catch (err) {
      if (!mountedRef.current) return;
      if (err instanceof ApiError && (err.status === 401 || err.status === 403)) {
        setStatus("error");
        return;
      }
      const next = attemptsRef.current + 1;
      attemptsRef.current = next;
      setAttempts(next);
      if (next >= maxAttempts) {
        setStatus("error");
      } else {
        timerRef.current = setTimeout(poll, interval * 2);
      }
    }
  }, [interval, maxAttempts]);

  const start = useCallback(() => {
    cancel();
    attemptsRef.current = 0;
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
