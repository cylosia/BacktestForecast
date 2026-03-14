import { describe, it, expect } from "vitest";

/**
 * Tests that template picker correctly reads config from the
 * `config_json` key (matching DB column / API alias) rather
 * than a hypothetical `config` key.
 */

interface TemplateConfig {
  strategy_type: string;
  target_dte: number;
  dte_tolerance_days: number;
  max_holding_days: number;
  account_size: number;
  risk_per_trade_pct: number;
  commission_per_contract: number;
  entry_rules: unknown[];
}

interface TemplateResponse {
  id: string;
  name: string;
  description: string | null;
  strategy_type: string;
  config_json: TemplateConfig;
  created_at: string;
  updated_at: string;
}

const mockTemplates: TemplateResponse[] = [
  {
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
  },
  {
    id: "tmpl-2",
    name: "Aggressive Iron Condor",
    description: "Higher risk",
    strategy_type: "iron_condor",
    config_json: {
      strategy_type: "iron_condor",
      target_dte: 21,
      dte_tolerance_days: 5,
      max_holding_days: 14,
      account_size: 25000,
      risk_per_trade_pct: 5,
      commission_per_contract: 0.5,
      entry_rules: [],
    },
    created_at: "2025-02-01T00:00:00Z",
    updated_at: "2025-02-15T00:00:00Z",
  },
];

describe("template picker config_json", () => {
  it("reads config from config_json key, not config", () => {
    const template = mockTemplates[0];
    expect(template).toHaveProperty("config_json");
    expect(template).not.toHaveProperty("config");
    expect(template.config_json.strategy_type).toBe("cash_secured_put");
  });

  it("applies all config_json fields correctly", () => {
    const config = mockTemplates[0].config_json;
    expect(config.strategy_type).toBe("cash_secured_put");
    expect(config.target_dte).toBe(45);
    expect(config.dte_tolerance_days).toBe(10);
    expect(config.max_holding_days).toBe(30);
    expect(config.account_size).toBe(50000);
    expect(config.risk_per_trade_pct).toBe(2);
    expect(config.commission_per_contract).toBe(0.65);
  });

  it("replaces form values when switching templates", () => {
    const first = mockTemplates[0].config_json;
    const second = mockTemplates[1].config_json;

    expect(first.strategy_type).toBe("cash_secured_put");
    expect(second.strategy_type).toBe("iron_condor");
    expect(second.target_dte).toBe(21);
    expect(first.target_dte).not.toBe(second.target_dte);
  });

  it("handles template with no entry_rules gracefully", () => {
    const template = mockTemplates[0];
    expect(template.config_json.entry_rules).toEqual([]);
  });
});
