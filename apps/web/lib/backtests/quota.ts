import type { CurrentUserResponse, PlanTier } from "@backtestforecast/api-client";

const KNOWN_TIERS = new Set<string>(["free", "pro", "premium"]);

export interface BacktestQuota {
  tier: PlanTier;
  limit: number | null;
  used: number;
  remaining: number | null;
  reached: boolean;
}

export function buildBacktestQuota(user: CurrentUserResponse): BacktestQuota {
  const rawTier = user.plan_tier as string;
  if (!KNOWN_TIERS.has(rawTier)) {
    console.warn(`[buildBacktestQuota] Unknown plan_tier: "${rawTier}". Defaulting to "free".`);
  }
  const tier: PlanTier = KNOWN_TIERS.has(rawTier) ? (rawTier as PlanTier) : ("free" as PlanTier);
  const limit = user.features.monthly_backtest_quota ?? null;
  const used = user.usage.backtests_used_this_month;
  const remaining = user.usage.backtests_remaining_this_month ?? null;

  return {
    tier,
    limit,
    used,
    remaining,
    reached: limit !== null ? used >= limit : false,
  };
}
