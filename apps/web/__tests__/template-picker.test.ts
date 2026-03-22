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
  config: {
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
    expect(isValidTemplateConfig(VALID_TEMPLATE.config)).toBe(true);
  });

  it("returns false for null", () => {
    expect(isValidTemplateConfig(null)).toBe(false);
  });

  it("returns false for a config missing required fields", () => {
    expect(isValidTemplateConfig({ strategy_type: "long_call" })).toBe(false);
  });

  it("accepts string account_size (API returns Decimal as string)", () => {
    const config = { ...VALID_TEMPLATE.config, account_size: "50000" };
    expect(isValidTemplateConfig(config)).toBe(true);
  });
});

describe("templateToFormValues", () => {
  it("reads config from config key and maps to form values", () => {
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
      config: {
        ...VALID_TEMPLATE.config,
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
      config: {
        ...VALID_TEMPLATE.config,
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
      config: {
        ...VALID_TEMPLATE.config,
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
      config: {
        ...VALID_TEMPLATE.config,
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
      config: {
        ...VALID_TEMPLATE.config,
        strategy_type: "calendar_spread",
        strategy_overrides: {
          calendar_contract_type: "put",
        },
      },
    };
    const patch = templateToFormValues(template);
    expect(patch).not.toBeNull();
    expect(patch!.strategyType).toBe("calendar_spread");
    expect(patch!.calendarContractType).toBe("put");
  });

  it("returns null for a template with invalid config", () => {
    const bad = { ...VALID_TEMPLATE, config: {} as never };
    const patch = templateToFormValues(bad);
    expect(patch).toBeNull();
  });
});

describe("formValuesToTemplateConfig", () => {
  it("preserves non-RSI rules and calendar overrides when saving templates", () => {
    const values = {
      ...getDefaultBacktestFormValues(),
      strategyType: "calendar_spread" as const,
      calendarContractType: "put" as const,
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

    expect(config.strategy_overrides).toEqual({ calendar_contract_type: "put" });
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
});
