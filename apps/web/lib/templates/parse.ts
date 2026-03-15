import type { TemplateConfig, TemplateResponse } from "@backtestforecast/api-client";
import type { BacktestFormValues } from "@/lib/backtests/validation";

export function isValidTemplateConfig(obj: unknown): obj is TemplateConfig {
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
