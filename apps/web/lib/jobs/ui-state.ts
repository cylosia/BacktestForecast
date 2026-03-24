import type {
  BacktestRunDetailResponse,
  CompareBacktestsResponse,
  ExportJobResponse,
} from "@backtestforecast/api-client";

export function isTradePayloadPartial(totalTrades: number | null | undefined, returnedTrades: number): boolean {
  return typeof totalTrades === "number" && totalTrades > returnedTrades;
}

export function getBacktestPartialDataMessages(run: BacktestRunDetailResponse): string[] {
  const messages: string[] = [];
  if (isTradePayloadPartial(run.summary?.trade_count, run.trades.length)) {
    messages.push(
      "The API returned only a partial trade list for this run. Summary metrics still come from the stored full-run aggregates.",
    );
  }
  if (run.equity_curve_truncated) {
    messages.push(
      "The API returned only a partial equity curve for this run, so the chart is not a complete visual history.",
    );
  }
  return messages;
}

export function getComparePartialDataMessages(data: CompareBacktestsResponse): string[] {
  const messages: string[] = [];
  if (data.trades_truncated) {
    messages.push(
      "Detailed trade rows are partial for at least one compared run. Comparison metrics still use persisted full-run aggregates.",
    );
  }
  if (data.items.some((run) => run.equity_curve_truncated)) {
    messages.push(
      "One or more equity curves were truncated by the API, so the overlay chart is only a partial visual sample.",
    );
  }
  return messages;
}

export function getCancellationMessage(resourceLabel: string, errorCode?: string | null): string {
  switch (errorCode) {
    case "cancelled_by_support":
      return `This ${resourceLabel} was cancelled by support before completion. Start a new ${resourceLabel} if you still need results.`;
    case "subscription_revoked":
      return `This ${resourceLabel} was cancelled because billing access changed before it finished. Restore access and start a new ${resourceLabel} if needed.`;
    case "cancelled_by_user":
      return `This ${resourceLabel} was cancelled before completion. Start a new ${resourceLabel} if you still want fresh results.`;
    default:
      return `This ${resourceLabel} was cancelled before completion, so any in-progress work was discarded. Start a new ${resourceLabel} if you still need results.`;
  }
}

export function getExportTerminalMessage(job: Pick<ExportJobResponse, "status" | "error_message">): string {
  if (job.status === "cancelled") {
    return "Export was cancelled before the file was generated. Start a new export if you still need the file.";
  }
  if (job.status === "expired") {
    return "Export expired before download. Generate a fresh export to retrieve the file again.";
  }
  return job.error_message || "Export generation failed on the server.";
}
