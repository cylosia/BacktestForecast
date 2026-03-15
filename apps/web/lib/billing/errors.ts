const PLAN_ERROR_CODES = new Set(["quota_exceeded", "feature_locked"]);

/**
 * Returns true if an API error code indicates a plan-limit issue that
 * can be resolved by upgrading. Use this to decide whether to show
 * an UpgradePrompt instead of a generic error message.
 */
export function isPlanLimitError(code: string | undefined): boolean {
  return code !== undefined && PLAN_ERROR_CODES.has(code);
}
