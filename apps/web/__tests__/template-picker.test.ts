import { describe, it, expect } from "vitest";
import {
  formValuesToTemplateConfig,
  templateToFormValues,
  isValidTemplateConfig,
} from "@/lib/templates/parse";
import {
  getDefaultBacktestFormValues,
  type BacktestFormValues,
} from "@/lib/backtests/validation";
import type { TemplateResponse } from "@backtestforecast/api-client";

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
    account_size: 50000,
    risk_per_trade_pct: 2,
    commission_per_contract: 0.65,
    entry_rules: [],
  },
  created_at: "2025-01-01T00:00:00Z",
  updated_at: "2025-01-01T00:00:00Z",
};

function toTemplateResponse(values: BacktestFormValues, name = "Round-trip"): TemplateResponse {
  return {
    id: `${name.toLowerCase()}-id`,
    name,
    description: null,
    strategy_type: values.strategyType,
    config_json: formValuesToTemplateConfig(values),
    created_at: "2025-01-01T00:00:00Z",
    updated_at: "2025-01-01T00:00:00Z",
  };
}

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
  it("reads config from config_json key and maps to form values", () => {
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
        entry_rules: [{ type: "rsi", operator: "lt", threshold: 30, period: 14 }],
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

  it("returns null for a template with invalid config_json", () => {
    const bad = { ...VALID_TEMPLATE, config_json: {} as never };
    const patch = templateToFormValues(bad);
    expect(patch).toBeNull();
  });
});

describe("template save/apply round-trips", () => {
  it("round-trips advanced strategy risk controls and optional exits", () => {
    const values: BacktestFormValues = {
      ...getDefaultBacktestFormValues(),
      symbol: "QQQ",
      strategyType: "iron_condor",
      targetDte: "45",
      dteToleranceDays: "7",
      maxHoldingDays: "15",
      accountSize: "25000",
      riskPerTradePct: "1.5",
      commissionPerContract: "0.5",
      slippagePct: "0.25",
      riskFreeRate: "0.038",
      profitTargetEnabled: true,
      profitTargetPct: "35",
      stopLossEnabled: true,
      stopLossPct: "18",
      movingAverageEnabled: true,
      movingAverageType: "ema_crossover",
      fastPeriod: "21",
      slowPeriod: "55",
      crossoverDirection: "bullish",
      macdEnabled: true,
      macdFastPeriod: "8",
      macdSlowPeriod: "21",
      macdSignalPeriod: "5",
      macdDirection: "bullish",
      bollingerEnabled: true,
      bollingerPeriod: "18",
      bollingerStdDev: "2.5",
      bollingerBand: "lower",
      bollingerOperator: "lt",
      ivPercentileEnabled: true,
      ivPercentileOperator: "gt",
      ivPercentileThreshold: "70",
      volumeSpikeEnabled: true,
      volumeSpikeMultiplier: "3",
      volumeSpikePeriod: "25",
      avoidEarningsEnabled: true,
      avoidEarningsDaysBefore: "5",
      avoidEarningsDaysAfter: "2",
      strategyOverrides: {
        short_call_strike: { mode: "delta_target", value: 20 },
        short_put_strike: { mode: "delta_target", value: 18 },
        spread_width: { mode: "dollar_width", value: 10 },
      },
    };

    const template = toTemplateResponse(values, "Advanced");
    const patch = templateToFormValues(template);

    expect(template.config_json.profit_target_pct).toBe(35);
    expect(template.config_json.stop_loss_pct).toBe(18);
    expect(template.config_json.slippage_pct).toBe(0.25);
    expect(template.config_json.risk_free_rate).toBe(0.038);
    expect(template.config_json.strategy_overrides).toEqual(values.strategyOverrides);
    expect(patch).toMatchObject({
      symbol: "QQQ",
      strategyType: "iron_condor",
      slippagePct: "0.25",
      riskFreeRate: "0.038",
      profitTargetEnabled: true,
      profitTargetPct: "35",
      stopLossEnabled: true,
      stopLossPct: "18",
      movingAverageEnabled: true,
      macdEnabled: true,
      bollingerEnabled: true,
      ivPercentileEnabled: true,
      volumeSpikeEnabled: true,
      avoidEarningsEnabled: true,
      strategyOverrides: values.strategyOverrides,
    });
  });

  it("round-trips custom-leg strategies without dropping leg definitions", () => {
    const values: BacktestFormValues = {
      ...getDefaultBacktestFormValues(),
      symbol: "SPY",
      strategyType: "custom_3_leg",
      targetDte: "60",
      dteToleranceDays: "4",
      maxHoldingDays: "20",
      slippagePct: "0.1",
      riskFreeRate: "0.04",
      rsiEnabled: true,
      rsiOperator: "lt",
      rsiThreshold: "30",
      rsiPeriod: "10",
      customLegs: [
        { asset_type: "option", contract_type: "call", side: "long", strike_offset: -1, expiration_offset: 1, quantity_ratio: 1 },
        { asset_type: "option", contract_type: "call", side: "short", strike_offset: 0, expiration_offset: 0, quantity_ratio: 2 },
        { asset_type: "stock", contract_type: null, side: "long", strike_offset: 0, expiration_offset: 0, quantity_ratio: 0.5 },
      ],
    };

    const template = toTemplateResponse(values, "Custom Legs");
    const patch = templateToFormValues(template);

    expect(template.config_json.custom_legs).toEqual(values.customLegs);
    expect(patch).toMatchObject({
      strategyType: "custom_3_leg",
      slippagePct: "0.1",
      riskFreeRate: "0.04",
      rsiEnabled: true,
      customLegs: values.customLegs,
    });
  });
});
