"use client";

import { FileText } from "lucide-react";
import type { TemplateResponse } from "@/lib/backtests/types";
import { strategyLabel } from "@/lib/backtests/format";
import type { BacktestFormValues } from "@/lib/backtests/validation";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

export function templateToFormValues(template: TemplateResponse): Partial<BacktestFormValues> {
  const config = template.config;
  const patch: Partial<BacktestFormValues> = {
    strategyType: config.strategy_type as BacktestFormValues["strategyType"],
    targetDte: String(config.target_dte),
    dteToleranceDays: String(config.dte_tolerance_days),
    maxHoldingDays: String(config.max_holding_days),
    accountSize: String(config.account_size),
    riskPerTradePct: String(config.risk_per_trade_pct),
    commissionPerContract: String(config.commission_per_contract),
    rsiEnabled: false,
    movingAverageEnabled: false,
  };

  if (config.default_symbol) {
    patch.symbol = config.default_symbol;
  }

  for (const rule of config.entry_rules ?? []) {
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
              onClick={() => onApply(templateToFormValues(template))}
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
