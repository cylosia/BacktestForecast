import { describe, it, expect } from "vitest";
import {
  templateToFormValues,
  isValidTemplateConfig,
} from "@/lib/templates/parse";
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
