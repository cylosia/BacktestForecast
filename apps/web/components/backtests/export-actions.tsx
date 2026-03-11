"use client";

import { useCallback, useState } from "react";
import { useAuth } from "@clerk/nextjs";
import { Download, FileText, Loader2, Sheet } from "lucide-react";
import { createExport, downloadExport, fetchExportStatus } from "@/lib/api/client";
import { ApiError } from "@/lib/api/shared";
import type { ExportFormat } from "@/lib/backtests/types";
import { isPlanLimitError, UpgradePrompt } from "@/components/billing/upgrade-prompt";
import { Button } from "@/components/ui/button";

const POLL_INTERVAL_MS = 1_500;
const MAX_POLLS = 40; // ~60 seconds

function iconForFormat(format: ExportFormat) {
  return format === "pdf" ? FileText : Sheet;
}

function labelForFormat(format: ExportFormat, busy: boolean) {
  if (busy) return format === "pdf" ? "Generating PDF…" : "Generating CSV…";
  return format === "pdf" ? "Export PDF" : "Export CSV";
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

  const pollAndDownload = useCallback(
    async (token: string, exportJobId: string, fileName: string) => {
      for (let i = 0; i < MAX_POLLS; i++) {
        await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
        const status = await fetchExportStatus(token, exportJobId);

        if (status.status === "succeeded") {
          const response = await downloadExport(token, exportJobId);
          const blob = await response.blob();
          const blobUrl = window.URL.createObjectURL(blob);
          const anchor = document.createElement("a");
          anchor.href = blobUrl;
          anchor.download = fileName;
          anchor.click();
          window.URL.revokeObjectURL(blobUrl);
          return;
        }

        if (status.status === "failed") {
          throw new Error("Export generation failed on the server.");
        }
      }
      throw new Error("Export is still processing. Please try downloading from the history later.");
    },
    [],
  );

  async function handleExport(format: ExportFormat) {
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

      if (exportJob.status === "succeeded") {
        // Already complete (idempotent hit or instant generation)
        const response = await downloadExport(token, exportJob.id);
        const blob = await response.blob();
        const blobUrl = window.URL.createObjectURL(blob);
        const anchor = document.createElement("a");
        anchor.href = blobUrl;
        anchor.download = exportJob.file_name;
        anchor.click();
        window.URL.revokeObjectURL(blobUrl);
      } else {
        // Queued or running — poll until ready
        await pollAndDownload(token, exportJob.id, exportJob.file_name);
      }
    } catch (error) {
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
