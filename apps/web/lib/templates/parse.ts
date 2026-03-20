import type {
  CreateTemplateRequest,
  EntryRule,
  StrategyType,
  TemplateConfig,
  TemplateResponse,
} from "@backtestforecast/api-client";
import type { BacktestFormValues } from "@/lib/backtests/validation";

const KNOWN_STRATEGY_TYPES = new Set([
  "long_call", "long_put", "covered_call", "cash_secured_put",
  "bull_call_debit_spread", "bear_put_debit_spread",
  "bull_put_credit_spread", "bear_call_credit_spread",
  "iron_condor", "long_straddle", "long_strangle",
  "calendar_spread", "butterfly", "wheel_strategy",
  "poor_mans_covered_call", "ratio_call_backspread", "ratio_put_backspread",
  "collar", "diagonal_spread", "double_diagonal",
  "short_straddle", "short_strangle", "covered_strangle",
  "synthetic_put", "reverse_conversion", "jade_lizard", "iron_butterfly",
  "custom_2_leg", "custom_3_leg", "custom_4_leg",
  "custom_5_leg", "custom_6_leg", "custom_7_leg", "custom_8_leg",
  "naked_call", "naked_put",
]);

export function isValidTemplateConfig(obj: unknown): obj is TemplateConfig {
  if (!obj || typeof obj !== "object") return false;
  const record = obj as Record<string, unknown>;
  if ("entry_rules" in record && !Array.isArray(record.entry_rules)) return false;
  if ("custom_legs" in record && record.custom_legs != null && !Array.isArray(record.custom_legs)) return false;
  return (
    typeof record.strategy_type === "string" &&
    typeof record.target_dte === "number" &&
    typeof record.dte_tolerance_days === "number" &&
    typeof record.max_holding_days === "number" &&
    (typeof record.account_size === "number" || typeof record.account_size === "string") &&
    (typeof record.risk_per_trade_pct === "number" || typeof record.risk_per_trade_pct === "string") &&
    (typeof record.commission_per_contract === "number" || typeof record.commission_per_contract === "string") &&
    (record.slippage_pct === undefined || typeof record.slippage_pct === "number" || typeof record.slippage_pct === "string") &&
    (record.risk_free_rate === undefined || record.risk_free_rate === null || typeof record.risk_free_rate === "number" || typeof record.risk_free_rate === "string")
  );
}

export function formValuesToTemplateConfig(
  values: BacktestFormValues,
): CreateTemplateRequest["config"] {
  const entryRules: EntryRule[] = [];

  if (values.rsiEnabled) {
    entryRules.push({
      type: "rsi",
      operator: values.rsiOperator,
      threshold: Number(values.rsiThreshold),
      period: Number(values.rsiPeriod),
    });
  }

  if (values.movingAverageEnabled) {
    entryRules.push({
      type: values.movingAverageType as "sma_crossover" | "ema_crossover",
      fast_period: Number(values.fastPeriod),
      slow_period: Number(values.slowPeriod),
      direction: values.crossoverDirection,
    });
  }

  if (values.macdEnabled) {
    entryRules.push({
      type: "macd",
      fast_period: Number(values.macdFastPeriod),
      slow_period: Number(values.macdSlowPeriod),
      signal_period: Number(values.macdSignalPeriod),
      direction: values.macdDirection,
    } as EntryRule);
  }

  if (values.bollingerEnabled) {
    entryRules.push({
      type: "bollinger_bands",
      band: values.bollingerBand as any,
      operator: values.bollingerOperator,
      period: Number(values.bollingerPeriod),
      standard_deviations: Number(values.bollingerStdDev),
    } as EntryRule);
  }

  if (values.ivRankEnabled) {
    entryRules.push({
      type: "iv_rank",
      operator: values.ivRankOperator,
      threshold: Number(values.ivRankThreshold),
    } as EntryRule);
  }

  if (values.ivPercentileEnabled) {
    entryRules.push({
      type: "iv_percentile",
      operator: values.ivPercentileOperator,
      threshold: Number(values.ivPercentileThreshold),
    } as EntryRule);
  }

  if (values.volumeSpikeEnabled) {
    entryRules.push({
      type: "volume_spike",
      multiplier: Number(values.volumeSpikeMultiplier),
      lookback_period: Number(values.volumeSpikePeriod),
    } as EntryRule);
  }

  if (values.supportResistanceEnabled) {
    entryRules.push({
      type: "support_resistance",
      mode: values.supportResistanceMode as any,
      lookback_period: Number(values.supportResistancePeriod),
    } as EntryRule);
  }

  if (values.avoidEarningsEnabled) {
    entryRules.push({
      type: "avoid_earnings",
      days_before: Number(values.avoidEarningsDaysBefore),
      days_after: Number(values.avoidEarningsDaysAfter),
    } as EntryRule);
  }

  return {
    strategy_type: values.strategyType as StrategyType,
    target_dte: Number(values.targetDte),
    dte_tolerance_days: Number(values.dteToleranceDays),
    max_holding_days: Number(values.maxHoldingDays),
    account_size: Number(values.accountSize),
    risk_per_trade_pct: Number(values.riskPerTradePct),
    commission_per_contract: Number(values.commissionPerContract),
    entry_rules: entryRules,
    default_symbol: values.symbol || null,
    slippage_pct: Number(values.slippagePct),
    risk_free_rate: values.riskFreeRate === "" ? null : Number(values.riskFreeRate),
    profit_target_pct: values.profitTargetEnabled ? Number(values.profitTargetPct) : null,
    stop_loss_pct: values.stopLossEnabled ? Number(values.stopLossPct) : null,
    strategy_overrides: values.strategyOverrides ?? null,
    custom_legs: values.customLegs ?? null,
  };
}

export function templateToFormValues(template: TemplateResponse): Partial<BacktestFormValues> | null {
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
    slippagePct: typed.slippage_pct != null ? String(typed.slippage_pct) : "0",
    riskFreeRate: typed.risk_free_rate != null ? String(typed.risk_free_rate) : "0.045",
    profitTargetEnabled: typed.profit_target_pct != null,
    profitTargetPct: typed.profit_target_pct != null ? String(typed.profit_target_pct) : "50",
    stopLossEnabled: typed.stop_loss_pct != null,
    stopLossPct: typed.stop_loss_pct != null ? String(typed.stop_loss_pct) : "20",
    rsiEnabled: false,
    movingAverageEnabled: false,
    macdEnabled: false,
    bollingerEnabled: false,
    ivRankEnabled: false,
    ivPercentileEnabled: false,
    volumeSpikeEnabled: false,
    supportResistanceEnabled: false,
    avoidEarningsEnabled: false,
    strategyOverrides: typed.strategy_overrides ?? null,
    customLegs: typed.custom_legs ?? null,
  };

  if (typed.default_symbol) {
    patch.symbol = typed.default_symbol;
  }

  const allRules = typed.entry_rules ?? [];
  let skippedRuleCount = 0;

  for (const rule of allRules) {
    switch (rule.type) {
      case "rsi":
        patch.rsiEnabled = true;
        patch.rsiOperator = rule.operator as BacktestFormValues["rsiOperator"];
        patch.rsiThreshold = String(rule.threshold);
        patch.rsiPeriod = String(rule.period);
        break;
      case "sma_crossover":
      case "ema_crossover":
        patch.movingAverageEnabled = true;
        patch.movingAverageType = rule.type as BacktestFormValues["movingAverageType"];
        patch.fastPeriod = String(rule.fast_period);
        patch.slowPeriod = String(rule.slow_period);
        patch.crossoverDirection = rule.direction as BacktestFormValues["crossoverDirection"];
        break;
      case "macd":
        patch.macdEnabled = true;
        patch.macdFastPeriod = String(rule.fast_period);
        patch.macdSlowPeriod = String(rule.slow_period);
        patch.macdSignalPeriod = String(rule.signal_period);
        patch.macdDirection = rule.direction as BacktestFormValues["macdDirection"];
        break;
      case "bollinger_bands":
        patch.bollingerEnabled = true;
        patch.bollingerPeriod = String(rule.period);
        patch.bollingerStdDev = String(rule.standard_deviations);
        patch.bollingerBand = rule.band as BacktestFormValues["bollingerBand"];
        patch.bollingerOperator = rule.operator as BacktestFormValues["bollingerOperator"];
        break;
      case "iv_rank":
        patch.ivRankEnabled = true;
        patch.ivRankOperator = rule.operator as BacktestFormValues["ivRankOperator"];
        patch.ivRankThreshold = String(rule.threshold);
        break;
      case "iv_percentile":
        patch.ivPercentileEnabled = true;
        patch.ivPercentileOperator = rule.operator as BacktestFormValues["ivPercentileOperator"];
        patch.ivPercentileThreshold = String(rule.threshold);
        break;
      case "volume_spike":
        patch.volumeSpikeEnabled = true;
        patch.volumeSpikeMultiplier = String(rule.multiplier);
        patch.volumeSpikePeriod = String(rule.lookback_period);
        break;
      case "support_resistance":
        patch.supportResistanceEnabled = true;
        patch.supportResistanceMode = rule.mode;
        patch.supportResistancePeriod = String(rule.lookback_period);
        break;
      case "avoid_earnings":
        patch.avoidEarningsEnabled = true;
        patch.avoidEarningsDaysBefore = String(rule.days_before);
        patch.avoidEarningsDaysAfter = String(rule.days_after);
        break;
      default:
        skippedRuleCount++;
    }
  }

  if (skippedRuleCount > 0) {
    console.warn(
      `Template "${template.name ?? template.id}" has ${skippedRuleCount} unrecognized entry rule type(s) that were skipped.`
    );
  }

  return patch;
}
