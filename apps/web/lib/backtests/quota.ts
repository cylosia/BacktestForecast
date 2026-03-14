import type { CurrentUserResponse, PlanTier } from "@backtestforecast/api-client";

export interface BacktestQuota {
  tier: PlanTier;
  limit: number | null;
  used: number;
  remaining: number | null;
  reached: boolean;
}

export function buildBacktestQuota(user: CurrentUserResponse): BacktestQuota {
  const limit = user.features.monthly_backtest_quota ?? null;
  const used = user.usage.backtests_used_this_month;
  const remaining = user.usage.backtests_remaining_this_month ?? null;

  return {
    tier: user.plan_tier as PlanTier,
    limit,
    used,
    remaining,
    reached: limit !== null ? used >= limit : false,
  };
}
