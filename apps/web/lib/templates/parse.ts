import type {
  TemplateConfig,
  TemplateResponse,
} from "@backtestforecast/api-client";
import type { BacktestFormValues } from "@/lib/backtests/validation";

const KNOWN_STRATEGY_TYPES = new Set([
  "long_call",
  "long_put",
  "covered_call",
  "cash_secured_put",
  "bull_call_debit_spread",
  "bear_put_debit_spread",
  "bull_put_credit_spread",
  "bear_call_credit_spread",
  "iron_condor",
  "long_straddle",
  "long_strangle",
  "calendar_spread",
  "butterfly",
  "wheel_strategy",
  "poor_mans_covered_call",
  "ratio_call_backspread",
  "ratio_put_backspread",
  "collar",
  "diagonal_spread",
  "double_diagonal",
  "short_straddle",
  "short_strangle",
  "covered_strangle",
  "synthetic_put",
  "reverse_conversion",
  "jade_lizard",
  "iron_butterfly",
  "custom_2_leg",
  "custom_3_leg",
  "custom_4_leg",
  "custom_5_leg",
  "custom_6_leg",
  "custom_8_leg",
  "naked_call",
  "naked_put",
]);

const KNOWN_RULE_TYPES = new Set([
  "rsi",
  "sma_crossover",
  "ema_crossover",
  "macd",
  "bollinger_bands",
  "iv_rank",
  "iv_percentile",
  "volume_spike",
  "support_resistance",
  "avoid_earnings",
]);

export function isValidTemplateConfig(obj: unknown): obj is TemplateConfig {
  if (!obj || typeof obj !== "object") return false;
  const record = obj as Record<string, unknown>;
  if ("entry_rules" in record && !Array.isArray(record.entry_rules))
    return false;
  return (
    typeof record.strategy_type === "string" &&
    typeof record.target_dte === "number" &&
    typeof record.dte_tolerance_days === "number" &&
    typeof record.max_holding_days === "number" &&
    (typeof record.account_size === "number" ||
      typeof record.account_size === "string") &&
    (typeof record.risk_per_trade_pct === "number" ||
      typeof record.risk_per_trade_pct === "string") &&
    (typeof record.commission_per_contract === "number" ||
      typeof record.commission_per_contract === "string") &&
    (!("calendar_contract_type" in record) ||
      record.calendar_contract_type === "call" ||
      record.calendar_contract_type === "put")
  );
}

export function templateToFormValues(
  template: TemplateResponse,
): Partial<BacktestFormValues> | null {
  const config = template.config_json;
  if (!isValidTemplateConfig(config)) return null;
  const typed = config;

  if (!KNOWN_STRATEGY_TYPES.has(typed.strategy_type)) {
    return null;
  }

  const patch: Partial<BacktestFormValues> = {
    strategyType: typed.strategy_type as BacktestFormValues["strategyType"],
    targetDte: String(typed.target_dte),
    dteToleranceDays: String(typed.dte_tolerance_days),
    maxHoldingDays: String(typed.max_holding_days),
    accountSize: String(typed.account_size),
    riskPerTradePct: String(typed.risk_per_trade_pct),
    commissionPerContract: String(typed.commission_per_contract),
    calendarContractType: typed.calendar_contract_type ?? "call",
    rsiEnabled: false,
    movingAverageEnabled: false,
  };

  if (typed.default_symbol) {
    patch.symbol = typed.default_symbol;
  }

  const allRules = typed.entry_rules ?? [];
  let skippedRuleCount = 0;

  for (const rule of allRules) {
    if (!KNOWN_RULE_TYPES.has(rule.type)) {
      skippedRuleCount++;
      continue;
    }
    if (rule.type === "rsi") {
      patch.rsiEnabled = true;
      const validOperators = ["lt", "lte", "gt", "gte"] as const;
      if (
        validOperators.includes(
          rule.operator as (typeof validOperators)[number],
        )
      ) {
        patch.rsiOperator = rule.operator as BacktestFormValues["rsiOperator"];
      }
      patch.rsiThreshold = String(rule.threshold);
      patch.rsiPeriod = String(rule.period);
    }
    if (rule.type === "sma_crossover" || rule.type === "ema_crossover") {
      patch.movingAverageEnabled = true;
      const validTypes = ["sma_crossover", "ema_crossover"] as const;
      if (validTypes.includes(rule.type as (typeof validTypes)[number])) {
        patch.movingAverageType =
          rule.type as BacktestFormValues["movingAverageType"];
      }
      patch.fastPeriod = String(rule.fast_period);
      patch.slowPeriod = String(rule.slow_period);
      const validDirections = ["bullish", "bearish"] as const;
      if (
        validDirections.includes(
          rule.direction as (typeof validDirections)[number],
        )
      ) {
        patch.crossoverDirection =
          rule.direction as BacktestFormValues["crossoverDirection"];
      }
    }
  }

  if (skippedRuleCount > 0) {
    console.warn(
      `Template "${template.name ?? template.id}" has ${skippedRuleCount} unrecognized entry rule type(s) that were skipped.`,
    );
  }

  return patch;
}
