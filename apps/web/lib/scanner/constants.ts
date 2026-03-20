export const MIN_SCANNER_WINDOW_DAYS = 30;
export const MAX_SCANNER_WINDOW_DAYS = 730;

export function getScannerWindowTooShortError(): string {
  return `Scanner window must be at least ${MIN_SCANNER_WINDOW_DAYS} days for meaningful results`;
}

export function getScannerWindowTooLongError(maxDays: number = MAX_SCANNER_WINDOW_DAYS): string {
  return `scanner window exceeds the configured maximum of ${maxDays} days`;
}

export function getScannerWindowHelpText(maxDays: number = MAX_SCANNER_WINDOW_DAYS): string {
  return `Choose a scan window between ${MIN_SCANNER_WINDOW_DAYS} and ${maxDays} days to match API limits.`;
}
