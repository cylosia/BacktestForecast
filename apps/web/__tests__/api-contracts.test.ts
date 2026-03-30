import { describe, expect, it } from "vitest";

import {
  validateAnalysisDetailResponse,
  validateBacktestRunDetailResponse,
  validateCompareBacktestsResponse,
  validateCurrentUserResponse,
  validateDailyPicksResponse,
  validateExportJobResponse,
  validateForecastEnvelopeResponse,
  validateMetaResponse,
  validatePipelineHistoryResponse,
  validatePricingContractResponse,
  validateScannerRecommendationListResponse,
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
          multi_symbol_backtests: false,
          multi_step_backtests: false,
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
              decided_trades: 9,
              win_rate: 60,
              total_roi_pct: 12,
              average_win_amount: 100,
              average_loss_amount: -40,
              average_holding_period_days: 12,
              average_dte_at_open: 30,
              max_drawdown_pct: 5,
              total_commissions: 15,
              total_net_pnl: 1200,
              starting_equity: 10000,
              ending_equity: 11200,
              profit_factor: "1.8",
              payoff_ratio: "2.5",
              expectancy: 80,
              sharpe_ratio: "1.4",
              sortino_ratio: "1.9",
              cagr_pct: "12.0",
              calmar_ratio: "1.1",
              max_consecutive_wins: 4,
              max_consecutive_losses: 2,
              recovery_factor: "1.7",
            },
            forecast: "bad",
          },
        ],
      }),
    ).toThrow(/daily_picks\.items\[0\]\.forecast/i);
  });

  it("rejects partial daily-picks forecast payloads that drift from the backend contract", () => {
    expect(() =>
      validateDailyPicksResponse({
        status: "ok",
        trade_date: "2026-03-23",
        pipeline_run_id: "run-1",
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
              decided_trades: 9,
              win_rate: 60,
              total_roi_pct: 12,
              average_win_amount: 100,
              average_loss_amount: -40,
              average_holding_period_days: 12,
              average_dte_at_open: 30,
              max_drawdown_pct: 5,
              total_commissions: 15,
              total_net_pnl: 1200,
              starting_equity: 10000,
              ending_equity: 11200,
              profit_factor: "1.8",
              payoff_ratio: "2.5",
              expectancy: 80,
              sharpe_ratio: "1.4",
              sortino_ratio: "1.9",
              cagr_pct: "12.0",
              calmar_ratio: "1.1",
              max_consecutive_wins: 4,
              max_consecutive_losses: 2,
              recovery_factor: "1.7",
            },
            forecast: {
              symbol: "AAPL",
              as_of_date: "2026-03-23",
              horizon_days: 21,
              expected_return_median_pct: "3.5",
              positive_outcome_rate_pct: "62.0",
              analog_count: 24,
            },
          },
        ],
      }),
    ).toThrow(/daily_picks\.items\[0\]\.forecast\.(strategy_type|expected_return_low_pct|summary|disclaimer)/i);
  });

  it("accepts fully shaped daily-picks forecast payloads", () => {
    expect(() =>
      validateDailyPicksResponse({
        status: "ok",
        trade_date: "2026-03-23",
        pipeline_run_id: "run-1",
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
              decided_trades: 9,
              win_rate: 60,
              total_roi_pct: 12,
              average_win_amount: 100,
              average_loss_amount: -40,
              average_holding_period_days: 12,
              average_dte_at_open: 30,
              max_drawdown_pct: 5,
              total_commissions: 15,
              total_net_pnl: 1200,
              starting_equity: 10000,
              ending_equity: 11200,
              profit_factor: "1.8",
              payoff_ratio: "2.5",
              expectancy: 80,
              sharpe_ratio: "1.4",
              sortino_ratio: "1.9",
              cagr_pct: "12.0",
              calmar_ratio: "1.1",
              max_consecutive_wins: 4,
              max_consecutive_losses: 2,
              recovery_factor: "1.7",
            },
            forecast: {
              symbol: "AAPL",
              strategy_type: "long_call",
              as_of_date: "2026-03-23",
              horizon_days: 21,
              trading_days_used: 15,
              expected_return_low_pct: "-1.2",
              expected_return_median_pct: "3.5",
              expected_return_high_pct: "7.8",
              positive_outcome_rate_pct: "62.0",
              analog_count: 24,
              analogs_used: 18,
              summary: "Most analogs finished moderately higher over the holding window.",
              disclaimer: "Historical analogs are research outputs, not predictions.",
            },
          },
        ],
      }),
    ).not.toThrow();
  });

  it("accepts daily-picks integrity warnings emitted for omitted malformed payloads", () => {
    expect(() =>
      validateDailyPicksResponse({
        status: "ok",
        trade_date: "2026-03-23",
        pipeline_run_id: "run-1",
        integrity_warnings: ["Stored daily-picks forecast payload was malformed and has been omitted."],
        items: [],
      }),
    ).not.toThrow();
  });

  it("rejects malformed nested analysis forecast payloads", () => {
    expect(() =>
      validateAnalysisDetailResponse({
        id: "analysis-1",
        symbol: "AAPL",
        status: "succeeded",
        stage: "forecast",
        strategies_tested: 4,
        configs_tested: 12,
        top_results_count: 1,
        created_at: "2026-03-23T06:00:00Z",
        top_results: [
          {
            rank: 1,
            strategy_type: "long_call",
            strategy_label: "Long Call",
            target_dte: 30,
            max_holding_days: 21,
            config: {},
            score: 12.4,
            summary: {
              trade_count: 10,
              decided_trades: 9,
              win_rate: 60,
              total_roi_pct: 12,
              average_win_amount: 100,
              average_loss_amount: -40,
              average_holding_period_days: 12,
              average_dte_at_open: 30,
              max_drawdown_pct: 5,
              total_commissions: 15,
              total_net_pnl: 1200,
              starting_equity: 10000,
              ending_equity: 11200,
              profit_factor: "1.8",
              payoff_ratio: "2.5",
              expectancy: 80,
              sharpe_ratio: "1.4",
              sortino_ratio: "1.9",
              cagr_pct: "12.0",
              calmar_ratio: "1.1",
              max_consecutive_wins: 4,
              max_consecutive_losses: 2,
              recovery_factor: "1.7",
            },
            trades: [],
            equity_curve: [],
            forecast: {
              symbol: "AAPL",
              strategy_type: "long_call",
              as_of_date: "2026-03-23",
              horizon_days: 21,
              expected_return_median_pct: "3.5",
            },
          },
        ],
      }),
    ).toThrow(/analysis\.top_results\[0\]\.forecast\.(strategy_type|analog_count|expected_return_low_pct|summary|disclaimer)/i);
  });

  it("rejects standalone forecast envelopes with incomplete nested forecast payloads", () => {
    expect(() =>
      validateForecastEnvelopeResponse({
        expected_move_abs_pct: "4.2",
        probabilistic_note: "Historical analogs only.",
        forecast: {
          expected_return_low_pct: "-1.0",
          expected_return_median_pct: "2.0",
          expected_return_high_pct: "5.0",
        },
      }),
    ).toThrow(/forecast\.forecast\.(symbol|strategy_type|analog_count|summary|disclaimer)/i);
  });

  it("rejects scanner recommendations with malformed nested forecast and ranking breakdown payloads", () => {
    expect(() =>
      validateScannerRecommendationListResponse({
        items: [
          {
            id: "rec-1",
            symbol: "AAPL",
            strategy_type: "long_call",
            summary: {
              trade_count: 10,
              decided_trades: 9,
              win_rate: 60,
              total_roi_pct: 12,
              average_win_amount: 100,
              average_loss_amount: -40,
              average_holding_period_days: 12,
              average_dte_at_open: 30,
              max_drawdown_pct: 5,
              total_commissions: 15,
              total_net_pnl: 1200,
              starting_equity: 10000,
              ending_equity: 11200,
              profit_factor: "1.8",
              payoff_ratio: "2.5",
              expectancy: 80,
              sharpe_ratio: "1.4",
              sortino_ratio: "1.9",
              cagr_pct: "12.0",
              calmar_ratio: "1.1",
              max_consecutive_wins: 4,
              max_consecutive_losses: 2,
              recovery_factor: "1.7",
            },
            warnings: [],
            trades: [],
            equity_curve: [],
            trades_truncated: false,
            trade_items_omitted: 0,
            equity_curve_points_omitted: 0,
            forecast: {
              symbol: "AAPL",
              expected_return_median_pct: "2.0",
            },
            ranking_breakdown: {
              final_score: "12.0",
              reasoning: [],
            },
          },
        ],
        total: 1,
        offset: 0,
        limit: 20,
      }),
    ).toThrow(/scanner_recommendations\.items\[0\]\.(forecast|ranking_breakdown)\./i);
  });

  it("rejects scanner recommendations with malformed historical-performance payloads", () => {
    expect(() =>
      validateScannerRecommendationListResponse({
        items: [
          {
            id: "rec-1",
            symbol: "AAPL",
            strategy_type: "long_call",
            summary: {
              trade_count: 10,
              decided_trades: 9,
              win_rate: 60,
              total_roi_pct: 12,
              average_win_amount: 100,
              average_loss_amount: -40,
              average_holding_period_days: 12,
              average_dte_at_open: 30,
              max_drawdown_pct: 5,
              total_commissions: 15,
              total_net_pnl: 1200,
              starting_equity: 10000,
              ending_equity: 11200,
              profit_factor: "1.8",
              payoff_ratio: "2.5",
              expectancy: 80,
              sharpe_ratio: "1.4",
              sortino_ratio: "1.9",
              cagr_pct: "12.0",
              calmar_ratio: "1.1",
              max_consecutive_wins: 4,
              max_consecutive_losses: 2,
              recovery_factor: "1.7",
            },
            warnings: [],
            trades: [],
            equity_curve: [],
            trades_truncated: false,
            trade_items_omitted: 0,
            equity_curve_points_omitted: 0,
            historical_performance: {
              sample_count: 12,
              effective_sample_size: "9.1",
              weighted_win_rate: "58.0",
              weighted_total_roi_pct: "11.5",
              weighted_max_drawdown_pct: "4.2",
              recency_half_life_days: "180",
            },
          },
        ],
        total: 1,
        offset: 0,
        limit: 20,
      }),
    ).toThrow(/scanner_recommendations\.items\[0\]\.historical_performance\.recency_half_life_days/i);
  });

  it("rejects malformed analysis regime and landscape payloads", () => {
    expect(() =>
      validateAnalysisDetailResponse({
        id: "analysis-1",
        symbol: "AAPL",
        status: "succeeded",
        stage: "forecast",
        strategies_tested: 5,
        configs_tested: 20,
        top_results_count: 3,
        created_at: "2026-03-23T06:00:00Z",
        integrity_warnings: [],
        regime: {
          regimes: ["bullish"],
          close_price: "150.0",
        },
        landscape: [
          {
            strategy_type: "long_call",
            strategy_label: "Long Call",
            target_dte: 30,
            config: {},
            trade_count: 10,
            decided_trades: 9,
            win_rate: 60,
            total_roi_pct: 12,
            max_drawdown_pct: 5,
            score: 0.9,
          },
        ],
        top_results: [],
      }),
    ).toThrow(/analysis\.regime\.close_price/i);
  });

  it("rejects malformed forecast analog-date audit fields", () => {
    expect(() =>
      validateForecastEnvelopeResponse({
        expected_move_abs_pct: "4.2",
        probabilistic_note: "Derived from analogs.",
        forecast: {
          symbol: "AAPL",
          strategy_type: "long_call",
          as_of_date: "2026-03-23",
          horizon_days: 21,
          analog_count: 24,
          expected_return_low_pct: "1.2",
          expected_return_median_pct: "3.5",
          expected_return_high_pct: "6.1",
          positive_outcome_rate_pct: "62.0",
          summary: "Supportive analogs.",
          disclaimer: "Research only.",
          analog_dates: [123],
        },
      }),
    ).toThrow(/forecast\.forecast\.analog_dates\[0\]/i);
  });

  it("rejects malformed risk-free-rate curve audit metadata on backtest detail responses", () => {
    expect(() =>
      validateBacktestRunDetailResponse({
        id: "run-1",
        symbol: "AAPL",
        strategy_type: "long_call",
        summary_provenance: "persisted_run_aggregates",
        summary: {
          trade_count: 10,
          decided_trades: 9,
          win_rate: "60",
          total_roi_pct: "12",
          average_win_amount: "100",
          average_loss_amount: "-40",
          average_holding_period_days: "12",
          average_dte_at_open: "30",
          max_drawdown_pct: "5",
          total_commissions: "15",
          total_net_pnl: "1200",
          starting_equity: "10000",
          ending_equity: "11200",
          profit_factor: "1.8",
          payoff_ratio: "2.5",
          expectancy: "80",
          sharpe_ratio: "1.4",
          sortino_ratio: "1.9",
          cagr_pct: "12.0",
          calmar_ratio: "1.1",
          max_consecutive_wins: 4,
          max_consecutive_losses: 2,
          recovery_factor: "1.7",
        },
        warnings: [],
        trades: [],
        equity_curve: [],
        equity_curve_truncated: false,
        trade_items_omitted: 0,
        equity_curve_points_omitted: 0,
        risk_free_rate: "0.045",
        risk_free_rate_model: "curve_default",
        risk_free_rate_curve_points: [{ trade_date: "2026-03-23", rate: {} }],
      }),
    ).toThrow(/backtest\.risk_free_rate_curve_points\[0\]\.rate/i);
  });

  it("rejects malformed compare-item risk-free-rate audit metadata", () => {
    expect(() =>
      validateCompareBacktestsResponse({
        items: [
          {
            id: "run-1",
            symbol: "AAPL",
            strategy_type: "long_call",
            summary_provenance: "persisted_run_aggregates",
            summary: {
              trade_count: 10,
              decided_trades: 9,
              win_rate: "60",
              total_roi_pct: "12",
              average_win_amount: "100",
              average_loss_amount: "-40",
              average_holding_period_days: "12",
              average_dte_at_open: "30",
              max_drawdown_pct: "5",
              total_commissions: "15",
              total_net_pnl: "1200",
              starting_equity: "10000",
              ending_equity: "11200",
              profit_factor: "1.8",
              payoff_ratio: "2.5",
              expectancy: "80",
              sharpe_ratio: "1.4",
              sortino_ratio: "1.9",
              cagr_pct: "12.0",
              calmar_ratio: "1.1",
              max_consecutive_wins: 4,
              max_consecutive_losses: 2,
              recovery_factor: "1.7",
            },
            warnings: [],
            trades: [],
            equity_curve: [],
            equity_curve_truncated: false,
            trade_items_omitted: 0,
            equity_curve_points_omitted: 0,
            risk_free_rate: "0.045",
            risk_free_rate_model: "curve_default",
            risk_free_rate_curve_points: [{ trade_date: "2026-03-23", rate: {} }],
          },
        ],
        trades_truncated: false,
      }),
    ).toThrow(/compare\.items\[0\]\.risk_free_rate_curve_points\[0\]\.rate/i);
  });

  it("rejects malformed risk-free-rate curve audit metadata on export responses", () => {
    expect(() =>
      validateExportJobResponse({
        id: "export-1",
        backtest_run_id: "run-1",
        export_format: "csv",
        status: "succeeded",
        file_name: "aapl.csv",
        mime_type: "text/csv",
        size_bytes: 1234,
        created_at: "2026-03-23T06:00:00Z",
        risk_free_rate: "0.045",
        risk_free_rate_source: "massive_treasury",
        risk_free_rate_model: "curve_default",
        risk_free_rate_curve_points: [{ trade_date: 123, rate: "0.04" }],
      }),
    ).toThrow(/export_job\.risk_free_rate_curve_points\[0\]\.trade_date/i);
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
