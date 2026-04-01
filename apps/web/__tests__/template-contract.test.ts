import { describe, expect, it } from "vitest";
import type { TemplateListResponse, TemplateResponse } from "@backtestforecast/api-client";
import { validateTemplateListResponse, validateTemplateResponse } from "@/lib/templates/contracts";

const TEMPLATE: TemplateResponse = {
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

describe("template response contract", () => {
  it("accepts a valid template response", () => {
    expect(validateTemplateResponse(TEMPLATE)).toEqual(TEMPLATE);
  });

  it("rejects a template response with invalid config", () => {
    expect(() =>
      validateTemplateResponse({
        ...TEMPLATE,
        config_json: { strategy_type: "cash_secured_put" },
      }),
    ).toThrow(/template\.config_json/i);
  });

  it("accepts a valid template list response", () => {
    const payload: TemplateListResponse = {
      items: [TEMPLATE],
      total: 1,
      template_limit: 5,
    };
    expect(validateTemplateListResponse(payload)).toEqual(payload);
  });

  it("rejects a template list response with malformed items", () => {
    expect(() =>
      validateTemplateListResponse({
        items: [{ ...TEMPLATE, name: 42 }],
        total: 1,
      }),
    ).toThrow(/items\[0\]/i);
  });

  it("uses the typed config_json field and rejects legacy-only payloads", () => {
    expect(validateTemplateResponse(TEMPLATE)).toEqual(TEMPLATE);
    expect(() =>
      validateTemplateResponse({
        ...TEMPLATE,
        config_json: undefined,
        config: TEMPLATE.config_json,
      }),
    ).toThrow(/template\.config_json/i);
  });

  it("matches the expected template contract snapshot", () => {
    expect({
      templateKeys: Object.keys(TEMPLATE).sort(),
      configKeys: Object.keys(TEMPLATE.config_json).sort(),
    }).toMatchInlineSnapshot(`
      {
        "configKeys": [
          "account_size",
          "commission_per_contract",
          "dte_tolerance_days",
          "entry_rules",
          "max_holding_days",
          "risk_per_trade_pct",
          "strategy_type",
          "target_dte",
        ],
        "templateKeys": [
          "config_json",
          "created_at",
          "description",
          "id",
          "name",
          "strategy_type",
          "updated_at",
        ],
      }
    `);
  });
});
