"use client";

import { FileText } from "lucide-react";
import type { TemplateConfig, TemplateResponse } from "@backtestforecast/api-client";
import { strategyLabel } from "@/lib/backtests/format";
import type { BacktestFormValues } from "@/lib/backtests/validation";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

export function templateToFormValues(template: TemplateResponse): Partial<BacktestFormValues> | null {
  const config = template.config as Record<string, unknown> | undefined;
  if (!config || typeof config !== "object") return null;
  const requiredFields = [
    "strategy_type",
    "target_dte",
    "dte_tolerance_days",
    "max_holding_days",
    "account_size",
    "risk_per_trade_pct",
    "commission_per_contract",
  ] as const;
  for (const field of requiredFields) {
    if (!(field in config)) return null;
  }
  const typed = config as unknown as TemplateConfig;
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
