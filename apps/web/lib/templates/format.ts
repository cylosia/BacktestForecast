import type { CreateTemplateRequest, StrategyType } from "@backtestforecast/api-client";
import { validateBacktestForm, type BacktestFormValues } from "@/lib/backtests/validation";

export function formValuesToTemplateConfig(
  values: BacktestFormValues,
): CreateTemplateRequest["config"] {
  const { payload } = validateBacktestForm(values);
  if (!payload) {
    throw new Error("Template config requires a valid backtest configuration.");
  }

  return {
    strategy_type: payload.strategy_type as StrategyType,
    target_dte: payload.target_dte,
    dte_tolerance_days: payload.dte_tolerance_days,
    max_holding_days: payload.max_holding_days,
    account_size: payload.account_size,
    risk_per_trade_pct: payload.risk_per_trade_pct,
    commission_per_contract: payload.commission_per_contract,
    entry_rules: payload.entry_rules,
    default_symbol: payload.symbol || null,
    strategy_overrides: payload.strategy_overrides,
  };
}
