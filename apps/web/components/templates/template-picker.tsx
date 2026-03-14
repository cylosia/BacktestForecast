"use client";

import { FileText } from "lucide-react";
import type { TemplateConfig, TemplateResponse } from "@backtestforecast/api-client";
import { strategyLabel } from "@/lib/backtests/format";
import type { BacktestFormValues } from "@/lib/backtests/validation";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

function isValidTemplateConfig(obj: unknown): obj is TemplateConfig {
  if (!obj || typeof obj !== "object") return false;
  const record = obj as Record<string, unknown>;
  return (
    typeof record.strategy_type === "string" &&
    typeof record.target_dte === "number" &&
    typeof record.dte_tolerance_days === "number" &&
    typeof record.max_holding_days === "number" &&
    (typeof record.account_size === "number" || typeof record.account_size === "string") &&
    (typeof record.risk_per_trade_pct === "number" || typeof record.risk_per_trade_pct === "string") &&
    (typeof record.commission_per_contract === "number" || typeof record.commission_per_contract === "string")
  );
}

export function templateToFormValues(template: TemplateResponse): Partial<BacktestFormValues> | null {
  const config = template.config_json;
  if (!isValidTemplateConfig(config)) return null;
  const typed = config;
  const patch: Partial<BacktestFormValues> = {
    strategyType: typed.strategy_type as BacktestFormValues["strategyType"],
    targetDte: String(typed.target_dte),
    dteToleranceDays: String(typed.dte_tolerance_days),
    maxHoldingDays: String(typed.max_holding_days),
    accountSize: String(typed.account_size),
    riskPerTradePct: String(typed.risk_per_trade_pct),
    commissionPerContract: String(typed.commission_per_contract),
    rsiEnabled: false,
    movingAverageEnabled: false,
  };

  if (typed.default_symbol) {
    patch.symbol = typed.default_symbol;
  }

  for (const rule of typed.entry_rules ?? []) {
    if (rule.type === "rsi") {
      patch.rsiEnabled = true;
      patch.rsiOperator = rule.operator as BacktestFormValues["rsiOperator"];
      patch.rsiThreshold = String(rule.threshold);
      patch.rsiPeriod = String(rule.period);
    }
    if (rule.type === "sma_crossover" || rule.type === "ema_crossover") {
      patch.movingAverageEnabled = true;
      patch.movingAverageType = rule.type as BacktestFormValues["movingAverageType"];
      patch.fastPeriod = String(rule.fast_period);
      patch.slowPeriod = String(rule.slow_period);
      patch.crossoverDirection = rule.direction as BacktestFormValues["crossoverDirection"];
    }
  }

  return patch;
}

export function TemplatePicker({
  templates,
  onApply,
}: {
  templates: TemplateResponse[];
  onApply: (patch: Partial<BacktestFormValues>) => void;
}) {
  if (templates.length === 0) {
    return null;
  }

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="text-base">Apply a template</CardTitle>
        <CardDescription>Pre-fill the form from a saved configuration.</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap gap-2">
          {templates.map((template) => (
            <button
              key={template.id}
              type="button"
              className="inline-flex items-center gap-2 rounded-lg border border-border/70 bg-background px-3 py-2 text-sm font-medium transition-colors hover:bg-accent hover:text-accent-foreground"
              onClick={() => {
                const patch = templateToFormValues(template);
                if (patch) onApply(patch);
              }}
            >
              <FileText className="h-3.5 w-3.5 text-muted-foreground" />
              <span>{template.name}</span>
              <span className="text-xs text-muted-foreground">
                {strategyLabel(template.strategy_type)}
              </span>
            </button>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
