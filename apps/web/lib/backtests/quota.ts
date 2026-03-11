import type { CurrentUserResponse, PlanTier } from "@/lib/backtests/types";

export const FREE_BACKTEST_LIMIT = 5;

export interface BacktestQuota {
  tier: PlanTier;
  limit: number | null;
  used: number;
  remaining: number | null;
  reached: boolean;
}

export function buildBacktestQuota(user: CurrentUserResponse): BacktestQuota {
  const limit = user.features.monthly_backtest_quota;
  const used = user.usage.backtests_used_this_month;
  const remaining = user.usage.backtests_remaining_this_month;

  return {
    tier: user.plan_tier,
    limit,
    used,
    remaining,
    reached: limit !== null ? used >= limit : false,
  };
}
