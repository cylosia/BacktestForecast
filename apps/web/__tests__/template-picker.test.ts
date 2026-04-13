import { describe, it, expect } from "vitest";
import type { TemplateResponse } from "@backtestforecast/api-client";
import { formValuesToTemplateConfig } from "@/lib/templates/format";
import { getDefaultBacktestFormValues } from "@/lib/backtests/validation";
import {
  templateToFormValues,
  isValidTemplateConfig,
} from "@/lib/templates/parse";

const VALID_TEMPLATE: TemplateResponse = {
  id: "tmpl-1",
  name: "Conservative CSP",
  description: null,
  strategy_type: "cash_secured_put",
  config_json: {
    strategy_type: "cash_secured_put",
    target_dte: 45,
    dte_tolerance_days: 10,
    max_holding_days: 30,
    account_size: "50000",
    risk_per_trade_pct: "2",
    commission_per_contract: "0.65",
    entry_rules: [],
  },
  created_at: "2025-01-01T00:00:00Z",
  updated_at: "2025-01-01T00:00:00Z",
};

describe("isValidTemplateConfig", () => {
  it("returns true for a valid config object", () => {
    expect(isValidTemplateConfig(VALID_TEMPLATE.config_json)).toBe(true);
  });

  it("returns false for null", () => {
    expect(isValidTemplateConfig(null)).toBe(false);
  });

  it("returns false for a config missing required fields", () => {
    expect(isValidTemplateConfig({ strategy_type: "long_call" })).toBe(false);
  });

  it("accepts string account_size (API returns Decimal as string)", () => {
    const config = { ...VALID_TEMPLATE.config_json, account_size: "50000" };
    expect(isValidTemplateConfig(config)).toBe(true);
  });
});

describe("templateToFormValues", () => {
  it("reads config from config_json and maps to form values", () => {
    const patch = templateToFormValues(VALID_TEMPLATE);
    expect(patch).not.toBeNull();
    expect(patch!.strategyType).toBe("cash_secured_put");
    expect(patch!.targetDte).toBe("45");
    expect(patch!.dteToleranceDays).toBe("10");
    expect(patch!.maxHoldingDays).toBe("30");
    expect(patch!.accountSize).toBe("50000");
    expect(patch!.riskPerTradePct).toBe("2");
    expect(patch!.commissionPerContract).toBe("0.65");
  });

  it("disables RSI and MA by default when template has no entry rules", () => {
    const patch = templateToFormValues(VALID_TEMPLATE);
    expect(patch!.rsiEnabled).toBe(false);
    expect(patch!.movingAverageEnabled).toBe(false);
  });

  it("enables RSI when template has an RSI entry rule", () => {
    const template: TemplateResponse = {
      ...VALID_TEMPLATE,
      config_json: {
        ...VALID_TEMPLATE.config_json,
        entry_rules: [{ type: "rsi", operator: "lt", threshold: "30", period: 14 }],
      },
    };
    const patch = templateToFormValues(template);
    expect(patch!.rsiEnabled).toBe(true);
    expect(patch!.rsiOperator).toBe("lt");
    expect(patch!.rsiThreshold).toBe("30");
    expect(patch!.rsiPeriod).toBe("14");
  });

  it("replaces form values when switching templates", () => {
    const secondTemplate: TemplateResponse = {
      ...VALID_TEMPLATE,
      id: "tmpl-2",
      strategy_type: "iron_condor",
      config_json: {
        ...VALID_TEMPLATE.config_json,
        strategy_type: "iron_condor",
        target_dte: 21,
      },
    };
    const first = templateToFormValues(VALID_TEMPLATE);
    const second = templateToFormValues(secondTemplate);
    expect(first!.strategyType).toBe("cash_secured_put");
    expect(second!.strategyType).toBe("iron_condor");
    expect(first!.targetDte).not.toBe(second!.targetDte);
  });

  it("accepts custom_7_leg templates", () => {
    const template: TemplateResponse = {
      ...VALID_TEMPLATE,
      strategy_type: "custom_7_leg",
      config_json: {
        ...VALID_TEMPLATE.config_json,
        strategy_type: "custom_7_leg",
      },
    };
    const patch = templateToFormValues(template);
    expect(patch).not.toBeNull();
    expect(patch!.strategyType).toBe("custom_7_leg");
  });

  it("rehydrates non-RSI indicator rules instead of silently dropping them", () => {
    const template: TemplateResponse = {
      ...VALID_TEMPLATE,
      config_json: {
        ...VALID_TEMPLATE.config_json,
        entry_rules: [
          { type: "macd", direction: "bullish", fast_period: 12, slow_period: 26, signal_period: 9 },
          { type: "bollinger_bands", band: "lower", operator: "lt", period: 20, standard_deviations: "2" },
          { type: "iv_rank", operator: "gt", threshold: "50", lookback_days: 252 },
          { type: "iv_percentile", operator: "gt", threshold: "55", lookback_days: 252 },
          { type: "volume_spike", operator: "gte", multiplier: "2", lookback_period: 20 },
          { type: "support_resistance", mode: "near_support", lookback_period: 20, tolerance_pct: "1.0" },
          { type: "avoid_earnings", days_before: 3, days_after: 1 },
        ],
      },
    };
    const patch = templateToFormValues(template);
    expect(patch).not.toBeNull();
    expect(patch!.macdEnabled).toBe(true);
    expect(patch!.macdDirection).toBe("bullish");
    expect(patch!.bollingerEnabled).toBe(true);
    expect(patch!.ivRankEnabled).toBe(true);
    expect(patch!.ivPercentileEnabled).toBe(true);
    expect(patch!.volumeSpikeEnabled).toBe(true);
    expect(patch!.supportResistanceEnabled).toBe(true);
    expect(patch!.supportResistanceMode).toBe("near_support");
    expect(patch!.avoidEarningsEnabled).toBe(true);
  });

  it("rehydrates put calendar template overrides", () => {
    const template: TemplateResponse = {
      ...VALID_TEMPLATE,
      strategy_type: "calendar_spread",
      config_json: {
        ...VALID_TEMPLATE.config_json,
        strategy_type: "calendar_spread",
        strategy_overrides: {
          calendar_contract_type: "put",
        },
      },
    };
    const patch = templateToFormValues(template);
    expect(patch).not.toBeNull();
    expect(patch!.strategyType).toBe("put_calendar_spread");
    expect(patch!.calendarContractType).toBe("put");
  });

  it("rehydrates generic indicator rules into advancedRules", () => {
    const template: TemplateResponse = {
      ...VALID_TEMPLATE,
      config_json: {
        ...VALID_TEMPLATE.config_json,
        entry_rules: [
          {
            type: "indicator_series_cross",
            direction: "crosses_above",
            left_series: { indicator: "close" },
            right_series: { indicator: "ema", period: 21 },
          },
        ],
      },
    };
    const patch = templateToFormValues(template);
    expect(patch).not.toBeNull();
    expect(patch!.advancedRules).toHaveLength(1);
    expect(patch!.advancedRules[0]?.type).toBe("indicator_series_cross");
    expect(patch!.advancedRules[0]?.leftSeries.indicator).toBe("close");
    expect(patch!.advancedRules[0]?.rightSeries.indicator).toBe("ema");
    expect(patch!.advancedRules[0]?.rightSeries.period).toBe("21");
  });

  it("returns null for a template with invalid config", () => {
    const bad = { ...VALID_TEMPLATE, config_json: {} as never };
    const patch = templateToFormValues(bad);
    expect(patch).toBeNull();
  });
});

describe("formValuesToTemplateConfig", () => {
  it("preserves non-RSI rules for put calendar templates without legacy overrides", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      strategyType: "put_calendar_spread" as const,
      rsiEnabled: false,
      macdEnabled: true,
      bollingerEnabled: true,
      ivRankEnabled: true,
      ivPercentileEnabled: true,
      volumeSpikeEnabled: true,
      supportResistanceEnabled: true,
      avoidEarningsEnabled: true,
    };

    const config = formValuesToTemplateConfig(values);

    expect(config.strategy_type).toBe("put_calendar_spread");
    expect(config.strategy_overrides).toBeUndefined();
    expect(config.entry_rules).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ type: "macd", fast_period: 12, slow_period: 26, signal_period: 9 }),
        expect.objectContaining({ type: "bollinger_bands", standard_deviations: 2 }),
        expect.objectContaining({ type: "iv_rank", threshold: 50 }),
        expect.objectContaining({ type: "iv_percentile", threshold: 50 }),
        expect.objectContaining({ type: "volume_spike", lookback_period: 20 }),
        expect.objectContaining({ type: "support_resistance", mode: "near_support", lookback_period: 20 }),
        expect.objectContaining({ type: "avoid_earnings", days_before: 3, days_after: 1 }),
      ]),
    );
  });

  it("preserves advanced generic rules when saving templates", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      rsiEnabled: false,
      advancedRules: [
        {
          id: "rule_1",
          type: "indicator_persistence" as const,
          series: {
            indicator: "mfi" as const,
            period: "14",
            fastPeriod: "12",
            slowPeriod: "26",
            signalPeriod: "9",
            standardDeviations: "2",
            band: "lower" as const,
            lookbackDays: "252",
            lookbackPeriod: "20",
            kPeriod: "14",
            dPeriod: "3",
            smoothK: "3",
          },
          leftSeries: {
            indicator: "close" as const,
            period: "14",
            fastPeriod: "12",
            slowPeriod: "26",
            signalPeriod: "9",
            standardDeviations: "2",
            band: "lower" as const,
            lookbackDays: "252",
            lookbackPeriod: "20",
            kPeriod: "14",
            dPeriod: "3",
            smoothK: "3",
          },
          rightSeries: {
            indicator: "ema" as const,
            period: "21",
            fastPeriod: "12",
            slowPeriod: "26",
            signalPeriod: "9",
            standardDeviations: "2",
            band: "lower" as const,
            lookbackDays: "252",
            lookbackPeriod: "20",
            kPeriod: "14",
            dPeriod: "3",
            smoothK: "3",
          },
          operator: "gte" as const,
          direction: "crosses_above" as const,
          level: "55",
          bars: "5",
        },
      ],
    };

    const config = formValuesToTemplateConfig(values);

    expect(config.entry_rules).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: "indicator_persistence",
          operator: "gte",
          level: 55,
          bars: 5,
          series: expect.objectContaining({
            indicator: "mfi",
            period: 14,
          }),
        }),
      ]),
    );
  });
});
