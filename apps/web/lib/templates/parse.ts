import type { TemplateConfig, TemplateResponse } from "@backtestforecast/api-client";
import { draftFromEntryRule, isGenericEntryRuleType } from "@/lib/backtests/advanced-rules";
import type { BacktestFormValues } from "@/lib/backtests/validation";

const KNOWN_STRATEGY_TYPES = new Set([
  "long_call", "long_put", "covered_call", "cash_secured_put",
  "bull_call_debit_spread", "bear_put_debit_spread",
  "bull_put_credit_spread", "bear_call_credit_spread",
  "iron_condor", "long_straddle", "long_strangle",
  "calendar_spread", "put_calendar_spread", "butterfly", "wheel_strategy",
  "poor_mans_covered_call", "ratio_call_backspread", "ratio_put_backspread",
  "collar", "diagonal_spread", "double_diagonal",
  "short_straddle", "short_strangle", "covered_strangle",
  "synthetic_put", "reverse_conversion", "jade_lizard", "iron_butterfly",
  "custom_2_leg", "custom_3_leg", "custom_4_leg",
  "custom_5_leg", "custom_6_leg", "custom_7_leg", "custom_8_leg",
  "naked_call", "naked_put",
]);

const KNOWN_RULE_TYPES = new Set([
  "rsi", "sma_crossover", "ema_crossover", "macd",
  "bollinger_bands", "iv_rank", "iv_percentile",
  "volume_spike", "support_resistance", "avoid_earnings",
  "indicator_threshold", "indicator_trend", "indicator_level_cross",
  "indicator_series_cross", "indicator_persistence",
]);

export function isValidTemplateConfig(obj: unknown): obj is TemplateConfig {
  if (!obj || typeof obj !== "object") return false;
  const record = obj as Record<string, unknown>;
  if ("entry_rules" in record && !Array.isArray(record.entry_rules)) return false;
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

  if (!KNOWN_STRATEGY_TYPES.has(typed.strategy_type)) {
    return null;
  }

  const strategyType = (
    typed.strategy_type === "calendar_spread" && typed.strategy_overrides?.calendar_contract_type === "put"
      ? "put_calendar_spread"
      : typed.strategy_type
  ) as BacktestFormValues["strategyType"];

  const patch: Partial<BacktestFormValues> = {
    strategyType,
    targetDte: String(typed.target_dte),
    dteToleranceDays: String(typed.dte_tolerance_days),
    maxHoldingDays: String(typed.max_holding_days),
    accountSize: String(typed.account_size),
    riskPerTradePct: String(typed.risk_per_trade_pct),
    commissionPerContract: String(typed.commission_per_contract),
    rsiEnabled: false,
    movingAverageEnabled: false,
    macdEnabled: false,
    bollingerEnabled: false,
    ivRankEnabled: false,
    ivPercentileEnabled: false,
    volumeSpikeEnabled: false,
    supportResistanceEnabled: false,
    avoidEarningsEnabled: false,
    calendarContractType: strategyType === "put_calendar_spread" ? "put" : "call",
    advancedRules: [],
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
    if (isGenericEntryRuleType(rule.type)) {
      const advancedRule = draftFromEntryRule(rule);
      if (advancedRule) {
        patch.advancedRules!.push(advancedRule);
      }
      continue;
    }
    if (rule.type === "rsi") {
      patch.rsiEnabled = true;
      const validOperators = ["lt", "lte", "gt", "gte"] as const;
      if (validOperators.includes(rule.operator as typeof validOperators[number])) {
        patch.rsiOperator = rule.operator as BacktestFormValues["rsiOperator"];
      }
      patch.rsiThreshold = String(rule.threshold);
      patch.rsiPeriod = String(rule.period);
    }
    if (rule.type === "sma_crossover" || rule.type === "ema_crossover") {
      patch.movingAverageEnabled = true;
      const validTypes = ["sma_crossover", "ema_crossover"] as const;
      if (validTypes.includes(rule.type as typeof validTypes[number])) {
        patch.movingAverageType = rule.type as BacktestFormValues["movingAverageType"];
      }
      patch.fastPeriod = String(rule.fast_period);
      patch.slowPeriod = String(rule.slow_period);
      const validDirections = ["bullish", "bearish"] as const;
      if (validDirections.includes(rule.direction as typeof validDirections[number])) {
        patch.crossoverDirection = rule.direction as BacktestFormValues["crossoverDirection"];
      }
    }
    if (rule.type === "macd") {
      patch.macdEnabled = true;
      patch.macdFastPeriod = String(rule.fast_period);
      patch.macdSlowPeriod = String(rule.slow_period);
      patch.macdSignalPeriod = String(rule.signal_period);
      const validDirections = ["bullish", "bearish"] as const;
      if (validDirections.includes(rule.direction as typeof validDirections[number])) {
        patch.macdDirection = rule.direction as BacktestFormValues["macdDirection"];
      }
    }
    if (rule.type === "bollinger_bands") {
      patch.bollingerEnabled = true;
      patch.bollingerPeriod = String(rule.period);
      patch.bollingerStdDev = String(rule.standard_deviations);
      const validBands = ["lower", "middle", "upper"] as const;
      if (validBands.includes(rule.band as typeof validBands[number])) {
        patch.bollingerBand = rule.band as BacktestFormValues["bollingerBand"];
      }
      const validOperators = ["lt", "lte", "gt", "gte"] as const;
      if (validOperators.includes(rule.operator as typeof validOperators[number])) {
        patch.bollingerOperator = rule.operator as BacktestFormValues["bollingerOperator"];
      }
    }
    if (rule.type === "iv_rank") {
      patch.ivRankEnabled = true;
      const validOperators = ["lt", "lte", "gt", "gte"] as const;
      if (validOperators.includes(rule.operator as typeof validOperators[number])) {
        patch.ivRankOperator = rule.operator as BacktestFormValues["ivRankOperator"];
      }
      patch.ivRankThreshold = String(rule.threshold);
    }
    if (rule.type === "iv_percentile") {
      patch.ivPercentileEnabled = true;
      const validOperators = ["lt", "lte", "gt", "gte"] as const;
      if (validOperators.includes(rule.operator as typeof validOperators[number])) {
        patch.ivPercentileOperator = rule.operator as BacktestFormValues["ivPercentileOperator"];
      }
      patch.ivPercentileThreshold = String(rule.threshold);
    }
    if (rule.type === "volume_spike") {
      patch.volumeSpikeEnabled = true;
      patch.volumeSpikeMultiplier = String(rule.multiplier);
      patch.volumeSpikePeriod = String(rule.lookback_period);
    }
    if (rule.type === "support_resistance") {
      patch.supportResistanceEnabled = true;
      patch.supportResistanceMode = String(rule.mode);
      patch.supportResistancePeriod = String(rule.lookback_period);
    }
    if (rule.type === "avoid_earnings") {
      patch.avoidEarningsEnabled = true;
      patch.avoidEarningsDaysBefore = String(rule.days_before);
      patch.avoidEarningsDaysAfter = String(rule.days_after);
    }
  }

  if (skippedRuleCount > 0) {
    console.warn(
      `Template "${template.name ?? template.id}" has ${skippedRuleCount} unrecognized entry rule type(s) that were skipped.`
    );
  }

  return patch;
}
