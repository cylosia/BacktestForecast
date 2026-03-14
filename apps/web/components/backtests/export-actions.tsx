"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { useAuth } from "@clerk/nextjs";
import { Download, FileText, Loader2, Sheet } from "lucide-react";
import { createExport, downloadExport, fetchExportStatus } from "@/lib/api/client";
import { ApiError } from "@/lib/api/shared";
import type { ExportFormat } from "@backtestforecast/api-client";
import { isPlanLimitError, UpgradePrompt } from "@/components/billing/upgrade-prompt";
import { Button } from "@/components/ui/button";

const POLL_INTERVAL_MS = 1_500;
const MAX_POLLS = 40;
const BLOB_REVOKE_DELAY_MS = 5_000;

function iconForFormat(format: ExportFormat) {
  return format === "pdf" ? FileText : Sheet;
}

function labelForFormat(format: ExportFormat, busy: boolean) {
  if (busy) return format === "pdf" ? "Generating PDF…" : "Generating CSV…";
  return format === "pdf" ? "Export PDF" : "Export CSV";
}

function triggerBlobDownload(
  blob: Blob,
  fileName: string,
  blobUrlsRef: React.MutableRefObject<string[]>,
  revokeTimersRef: React.MutableRefObject<ReturnType<typeof setTimeout>[]>,
): void {
  const blobUrl = window.URL.createObjectURL(blob);
  blobUrlsRef.current.push(blobUrl);
  const anchor = document.createElement("a");
  anchor.href = blobUrl;
  anchor.download = fileName;
  anchor.style.display = "none";
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  const timer = setTimeout(() => {
    window.URL.revokeObjectURL(blobUrl);
    blobUrlsRef.current = blobUrlsRef.current.filter((u) => u !== blobUrl);
    revokeTimersRef.current = revokeTimersRef.current.filter((t) => t !== timer);
  }, BLOB_REVOKE_DELAY_MS);
  revokeTimersRef.current.push(timer);
}

export function ExportActions({
  runId,
  formats,
}: {
  runId: string;
  formats: ExportFormat[];
}) {
  const { getToken } = useAuth();
  const [busyFormat, setBusyFormat] = useState<ExportFormat | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [errorCode, setErrorCode] = useState<string | undefined>();
  const mountedRef = useRef(true);
  const abortRef = useRef<AbortController | null>(null);
  const revokeTimersRef = useRef<ReturnType<typeof setTimeout>[]>([]);
  const blobUrlsRef = useRef<string[]>([]);

  useEffect(() => {
    return () => {
      mountedRef.current = false;
      abortRef.current?.abort();
      revokeTimersRef.current.forEach(clearTimeout);
      revokeTimersRef.current = [];
      blobUrlsRef.current.forEach((url) => window.URL.revokeObjectURL(url));
      blobUrlsRef.current = [];
    };
  }, []);

  const fetchAndDownload = useCallback(
    async (token: string, exportJobId: string, fileName: string, signal: AbortSignal) => {
      const response = await downloadExport(token, exportJobId, signal);
      if (signal.aborted) return;
      const blob = await response.blob();
      if (signal.aborted || !mountedRef.current) return;
      triggerBlobDownload(blob, fileName, blobUrlsRef, revokeTimersRef);
    },
    [],
  );

  const pollAndDownload = useCallback(
    async (token: string, exportJobId: string, fileName: string, signal: AbortSignal) => {
      let consecutiveErrors = 0;
      for (let attempt = 0; attempt < MAX_POLLS; attempt++) {
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(resolve, POLL_INTERVAL_MS);
          signal.addEventListener("abort", () => { clearTimeout(timer); reject(signal.reason); }, { once: true });
        }).catch(() => {});
        if (signal.aborted || !mountedRef.current) return;

        let result;
        try {
          result = await fetchExportStatus(token, exportJobId, signal);
          consecutiveErrors = 0;
        } catch (err) {
          if (err instanceof ApiError && (err.status === 401 || err.status === 403)) {
            throw err;
          }
          consecutiveErrors++;
          if (consecutiveErrors >= 3) {
            throw new Error("Export status check failed repeatedly. Please try again.");
          }
          continue;
        }

        if (signal.aborted || !mountedRef.current) return;

        if (result.status === "succeeded") {
          await fetchAndDownload(token, exportJobId, fileName, signal);
          return;
        }

        if (result.status === "failed") {
          throw new Error(result.error_message || "Export generation failed on the server.");
        }
      }
      throw new Error("Export is still processing. Please try downloading from the history later.");
    },
    [fetchAndDownload],
  );

  async function handleExport(format: ExportFormat) {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setBusyFormat(format);
    setMessage(null);
    setErrorCode(undefined);

    try {
      const token = await getToken();
      if (!token) {
        throw new Error("Your session token could not be loaded. Please sign in again.");
      }

      const exportJob = await createExport(token, {
        run_id: runId,
        format,
        idempotency_key: `${runId}:${format}:${Date.now()}`,
      });

      if (controller.signal.aborted) return;

      if (exportJob.status === "succeeded") {
        await fetchAndDownload(token, exportJob.id, exportJob.file_name, controller.signal);
      } else {
        await pollAndDownload(token, exportJob.id, exportJob.file_name, controller.signal);
      }
    } catch (error) {
      if (controller.signal.aborted) return;
      const nextMessage =
        error instanceof ApiError
          ? error.message
          : error instanceof Error
            ? error.message
            : "Export could not be completed.";
      const code = error instanceof ApiError ? error.code : undefined;
      setMessage(nextMessage);
      setErrorCode(code);
    } finally {
      setBusyFormat(null);
    }
  }

  if (formats.length === 0) {
    return null;
  }

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap gap-2">
        {formats.map((format) => {
          const Icon = iconForFormat(format);
          const isBusy = busyFormat === format;
          return (
            <Button
              disabled={busyFormat !== null}
              key={format}
              onClick={() => handleExport(format)}
              type="button"
              variant="outline"
            >
              {isBusy ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Icon className="h-4 w-4" />
              )}
              {labelForFormat(format, isBusy)}
            </Button>
          );
        })}
      </div>
      {message && isPlanLimitError(errorCode) ? (
        <UpgradePrompt message={message} />
      ) : message ? (
        <p className="flex items-center gap-2 text-sm text-destructive">
          <Download className="h-4 w-4" />
          {message}
        </p>
      ) : null}
    </div>
  );
}
