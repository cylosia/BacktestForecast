import { describe, expect, it } from "vitest";

import {
  validateCurrentUserResponse,
  validateDailyPicksResponse,
  validateMetaResponse,
  validatePipelineHistoryResponse,
  validatePricingContractResponse,
  validateStrategyCatalogResponse,
} from "@/lib/api/contracts";

describe("api runtime contracts", () => {
  it("rejects current-user payloads missing nested feature access required by authenticated pages", () => {
    expect(() =>
      validateCurrentUserResponse({
        id: "user-1",
        clerk_user_id: "clerk-user-1",
        email: "user@test.com",
        plan_tier: "pro",
        subscription_status: "active",
        subscription_billing_interval: "monthly",
        subscription_current_period_end: "2026-04-01T00:00:00Z",
        cancel_at_period_end: false,
        created_at: "2026-03-23T00:00:00Z",
        usage: {
          backtests_used_this_month: 1,
          backtests_remaining_this_month: 9,
        },
      }),
    ).toThrow(/current_user\.features/i);
  });

  it("accepts fully shaped current-user payloads", () => {
    expect(() =>
      validateCurrentUserResponse({
        id: "user-1",
        clerk_user_id: "clerk-user-1",
        email: "user@test.com",
        plan_tier: "pro",
        subscription_status: "active",
        subscription_billing_interval: "monthly",
        subscription_current_period_end: "2026-04-01T00:00:00Z",
        cancel_at_period_end: false,
        created_at: "2026-03-23T00:00:00Z",
        features: {
          plan_tier: "pro",
          monthly_backtest_quota: 10,
          history_days: 365,
          history_item_limit: 100,
          side_by_side_comparison_limit: 8,
          forecasting_access: true,
          export_formats: ["csv"],
          scanner_modes: ["basic"],
          scanner_basic_allowed_strategy_types: ["long_call"],
          scanner_advanced_allowed_strategy_types: ["long_call"],
          max_scanner_window_days: 365,
          max_sweep_window_days: 365,
          cancel_at_period_end: false,
        },
        usage: {
          backtests_used_this_month: 1,
          backtests_remaining_this_month: 9,
        },
      }),
    ).not.toThrow();
  });

  it("rejects malformed meta feature flags", () => {
    expect(() =>
      validateMetaResponse({
        service: "api",
        version: "1.0.0",
        billing_enabled: true,
        environment: "test",
        daily_picks_schedule_utc: "06:00 UTC",
        features: {
          forecasting_access: "yes",
        },
      }),
    ).toThrow(/meta\.features/i);
  });

  it("accepts valid backend meta payloads with boolean feature flags", () => {
    expect(() =>
      validateMetaResponse({
        service: "api",
        version: "1.0.0",
        billing_enabled: true,
        environment: "test",
        daily_picks_schedule_utc: "06:15 UTC",
        features: {
          backtests: true,
          scanner: true,
          exports: true,
          forecasts: true,
          analysis: true,
          daily_picks: true,
          billing: true,
          sweeps: true,
        },
      }),
    ).not.toThrow();
  });

  it("rejects malformed nested daily-picks payloads", () => {
    expect(() =>
      validateDailyPicksResponse({
        status: "ok",
        trade_date: "2026-03-23",
        pipeline_run_id: "run-1",
        pipeline_stats: {
          symbols_screened: 100,
          symbols_after_screen: 50,
          pairs_generated: 25,
          quick_backtests_run: 10,
          full_backtests_run: 5,
          recommendations_produced: 3,
          duration_seconds: 12,
          completed_at: "2026-03-23T06:00:00Z",
        },
        items: [
          {
            rank: 1,
            score: "0.9",
            symbol: "AAPL",
            strategy_type: "long_call",
            regime_labels: ["bullish"],
            close_price: "150.0",
            target_dte: 30,
            config_snapshot: {},
            summary: {
              trade_count: 10,
              win_rate: 60,
              total_roi_pct: 12,
              max_drawdown_pct: 5,
            },
            forecast: "bad",
          },
        ],
      }),
    ).toThrow(/daily_picks\.items\[0\]\.forecast/i);
  });

  it("accepts paginated daily-picks history payloads including error codes", () => {
    expect(() =>
      validatePipelineHistoryResponse({
        items: [
          {
            id: "b860d23c-6d75-492d-961a-2e3cb56713d6",
            trade_date: "2026-03-23",
            status: "failed",
            symbols_screened: 100,
            recommendations_produced: 0,
            duration_seconds: 12.5,
            completed_at: "2026-03-23T06:15:00Z",
            error_code: "pipeline_failed",
            error_message: "Upstream market data fetch failed",
          },
        ],
        total: 1,
        offset: 0,
        limit: 10,
        next_cursor: null,
      }),
    ).not.toThrow();
  });

  it("rejects malformed strategy catalog items", () => {
    expect(() =>
      validateStrategyCatalogResponse({
        groups: [
          {
            category: "directional",
            category_label: "Directional",
            strategies: [
              {
                strategy_type: "long_call",
                label: "Long Call",
                short_description: "Bullish call",
                category: "directional",
                bias: "bullish",
                leg_count: "one",
                min_tier: "pro",
                max_loss_description: "Premium paid",
                notes: "",
                tags: ["bullish"],
              },
            ],
          },
        ],
        total_strategies: 1,
        user_tier: "pro",
      }),
    ).toThrow(/strategy_catalog\.groups\[0\]\.strategies\[0\]\.leg_count/i);
  });

  it("rejects malformed pricing contract payloads used by the public pricing page", () => {
    expect(() =>
      validatePricingContractResponse({
        currency: "USD",
        checkout_authoritative: true,
        plans: [
          {
            tier: "pro",
            title: "Pro",
            headline: "Best for active users",
            description: "Advanced workflows",
            features: ["Backtests"],
            monthly: {
              price_id: "price_123",
              unit_amount_usd: "2900",
              display_price: "$29/mo",
              available: true,
            },
            yearly: null,
          },
        ],
      }),
    ).toThrow(/pricing\.plans\[0\]\.monthly\.unit_amount_usd/i);
  });
});
