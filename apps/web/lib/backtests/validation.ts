import type {
  BollingerBand,
  ComparisonOperator,
  CreateBacktestRunRequest,
  CrossoverDirection,
  MovingAverageRuleType,
  StrategyType,
} from "@backtestforecast/api-client";
import type { ValidationFieldError } from "@/lib/api/shared";
import { currentEasternDate, daysAgoET } from "@/lib/utils";
import {
  ACCOUNT_SIZE_MAX,
  ACCOUNT_SIZE_MIN,
  DTE_TOLERANCE_MAX,
  MAX_HOLDING_DAYS_MAX,
  MAX_HOLDING_DAYS_MIN,
  TARGET_DTE_MAX,
  TARGET_DTE_MIN,
  TICKER_RE,
} from "@/lib/validation-constants";

export interface BacktestFormValues {
  symbol: string;
  strategyType: StrategyType;
  startDate: string;
  endDate: string;
  targetDte: string;
  dteToleranceDays: string;
  maxHoldingDays: string;
  accountSize: string;
  riskPerTradePct: string;
  commissionPerContract: string;
  rsiEnabled: boolean;
  rsiOperator: ComparisonOperator;
  rsiThreshold: string;
  rsiPeriod: string;
  movingAverageEnabled: boolean;
  movingAverageType: MovingAverageRuleType;
  fastPeriod: string;
  slowPeriod: string;
  crossoverDirection: CrossoverDirection;
  macdEnabled: boolean;
  macdFastPeriod: string;
  macdSlowPeriod: string;
  macdSignalPeriod: string;
  macdDirection: CrossoverDirection;
  bollingerEnabled: boolean;
  bollingerPeriod: string;
  bollingerStdDev: string;
  bollingerBand: BollingerBand;
  bollingerOperator: ComparisonOperator;
  ivRankEnabled: boolean;
  ivRankOperator: ComparisonOperator;
  ivRankThreshold: string;
  avoidEarningsEnabled: boolean;
  avoidEarningsDaysBefore: string;
  avoidEarningsDaysAfter: string;
  slippagePct: string;
  profitTargetEnabled: boolean;
  profitTargetPct: string;
  stopLossEnabled: boolean;
  stopLossPct: string;
  riskFreeRate: string;
  ivPercentileEnabled: boolean;
  ivPercentileOperator: ComparisonOperator;
  ivPercentileThreshold: string;
  volumeSpikeEnabled: boolean;
  volumeSpikeMultiplier: string;
  volumeSpikePeriod: string;
  supportResistanceEnabled: boolean;
  supportResistanceMode: string;
  supportResistancePeriod: string;
  calendarContractType: "call" | "put";
}

export type BacktestFormErrors = Partial<Record<keyof BacktestFormValues | "form", string>>;

const BACKTEST_FIELD_NAME_MAP: Record<string, keyof BacktestFormValues> = {
  symbol: "symbol",
  strategy_type: "strategyType",
  start_date: "startDate",
  end_date: "endDate",
  target_dte: "targetDte",
  dte_tolerance_days: "dteToleranceDays",
  max_holding_days: "maxHoldingDays",
  account_size: "accountSize",
  risk_per_trade_pct: "riskPerTradePct",
  commission_per_contract: "commissionPerContract",
  slippage_pct: "slippagePct",
  profit_target_pct: "profitTargetPct",
  stop_loss_pct: "stopLossPct",
  risk_free_rate: "riskFreeRate",
};

export function mapBacktestFieldErrors(fieldErrors: ValidationFieldError[] | undefined): BacktestFormErrors {
  if (!fieldErrors || fieldErrors.length === 0) {
    return {};
  }

  const mapped: BacktestFormErrors = {};
  for (const fieldError of fieldErrors) {
    const loc = fieldError.loc ?? [];
    const candidate = [...loc].reverse().find((part) => part !== "body");
    if (!candidate) {
      continue;
    }
    const field = BACKTEST_FIELD_NAME_MAP[candidate];
    if (!field || mapped[field]) {
      continue;
    }
    mapped[field] = fieldError.msg ?? "Invalid value.";
  }
  return mapped;
}

export function getDefaultBacktestFormValues(): BacktestFormValues {
  return {
    symbol: "SPY",
    strategyType: "long_call",
    startDate: daysAgoET(365),
    endDate: daysAgoET(0),
    targetDte: "30",
    dteToleranceDays: "5",
    maxHoldingDays: "10",
    accountSize: "10000",
    riskPerTradePct: "2",
    commissionPerContract: "0.65",
    rsiEnabled: true,
    rsiOperator: "lt",
    rsiThreshold: "35",
    rsiPeriod: "14",
    movingAverageEnabled: false,
    movingAverageType: "sma_crossover",
    fastPeriod: "20",
    slowPeriod: "50",
    crossoverDirection: "bullish",
    macdEnabled: false,
    macdFastPeriod: "12",
    macdSlowPeriod: "26",
    macdSignalPeriod: "9",
    macdDirection: "bullish",
    bollingerEnabled: false,
    bollingerPeriod: "20",
    bollingerStdDev: "2",
    bollingerBand: "lower",
    bollingerOperator: "lt",
    ivRankEnabled: false,
    ivRankOperator: "gt",
    ivRankThreshold: "50",
    avoidEarningsEnabled: false,
    avoidEarningsDaysBefore: "3",
    avoidEarningsDaysAfter: "1",
    slippagePct: "0",
    profitTargetEnabled: false,
    profitTargetPct: "50",
    stopLossEnabled: false,
    stopLossPct: "20",
    riskFreeRate: "0.045",
    ivPercentileEnabled: false,
    ivPercentileOperator: "gt",
    ivPercentileThreshold: "50",
    volumeSpikeEnabled: false,
    volumeSpikeMultiplier: "2",
    volumeSpikePeriod: "20",
    supportResistanceEnabled: false,
    supportResistanceMode: "near_support",
    supportResistancePeriod: "20",
    calendarContractType: "call",
  };
}

const NUMERIC_RE = /^-?\d+(\.\d+)?$/;

function isFiniteNumber(value: string): boolean {
  const trimmed = value.trim();
  if (!trimmed) return false;
  if (!NUMERIC_RE.test(trimmed)) return false;
  return Number.isFinite(Number(trimmed));
}

function parseNumber(value: string): number {
  return Number(value);
}

export function validateBacktestForm(values: BacktestFormValues): {
  errors: BacktestFormErrors;
  payload?: CreateBacktestRunRequest;
} {
  const errors: BacktestFormErrors = {};
  const normalizedSymbol = values.symbol.trim().toUpperCase();

  if (!normalizedSymbol) {
    errors.symbol = "Symbol is required.";
  } else if (normalizedSymbol.length > 16) {
    errors.symbol = "Symbol must be 16 characters or fewer.";
  } else if (!TICKER_RE.test(normalizedSymbol)) {
    errors.symbol = "Symbol must start with a letter and may contain letters, digits, dots, slashes, ^ or -.";
  }

  const datePattern = /^\d{4}-\d{2}-\d{2}$/;

  if (!values.startDate || !values.startDate.trim()) {
    errors.startDate = "Start date is required.";
  } else if (!datePattern.test(values.startDate) || Number.isNaN(Date.parse(values.startDate))) {
    errors.startDate = "Start date must be a valid date (YYYY-MM-DD).";
  }

  if (!values.endDate || !values.endDate.trim()) {
    errors.endDate = "End date is required.";
  } else if (!datePattern.test(values.endDate) || Number.isNaN(Date.parse(values.endDate))) {
    errors.endDate = "End date must be a valid date (YYYY-MM-DD).";
  }

  if (!errors.startDate && !errors.endDate && new Date(values.startDate) >= new Date(values.endDate)) {
    errors.endDate = "End date must be later than start date.";
  }

  if (!errors.endDate) {
    const todayEt = currentEasternDate();
    if (values.endDate > todayEt) {
      errors.endDate = "End date cannot be in the future (US Eastern time).";
    }
  }

  if (!errors.startDate && !errors.endDate) {
    const start = new Date(values.startDate);
    const end = new Date(values.endDate);
    const diffDays = (end.getTime() - start.getTime()) / (1000 * 60 * 60 * 24);
    if (diffDays > 1825) {
      errors.endDate = "Date range cannot exceed 5 years (1825 days).";
    }
  }

  const numericChecks: Array<{
    key: keyof BacktestFormValues;
    min?: number;
    max?: number;
    exclusiveMin?: boolean;
    integer?: boolean;
    label: string;
  }> = [
    { key: "targetDte", min: TARGET_DTE_MIN, max: TARGET_DTE_MAX, integer: true, label: "Target DTE" },
    { key: "dteToleranceDays", min: 0, max: DTE_TOLERANCE_MAX, integer: true, label: "DTE tolerance" },
    { key: "maxHoldingDays", min: MAX_HOLDING_DAYS_MIN, max: MAX_HOLDING_DAYS_MAX, integer: true, label: "Max holding days" },
    { key: "accountSize", min: ACCOUNT_SIZE_MIN, max: ACCOUNT_SIZE_MAX, label: "Account size" },
    { key: "riskPerTradePct", min: 0, max: 100, exclusiveMin: true, label: "Risk per trade" },
    { key: "commissionPerContract", min: 0, max: 100, label: "Commission per contract" },
  ];

  for (const check of numericChecks) {
    const rawValue = values[check.key];
    if (typeof rawValue !== "string" || !isFiniteNumber(rawValue)) {
      errors[check.key] = `${check.label} must be a valid number.`;
      continue;
    }

    const parsed = parseNumber(rawValue);
    if (check.integer && !Number.isInteger(parsed)) {
      errors[check.key] = `${check.label} must be a whole number.`;
      continue;
    }
    if (typeof check.min === "number") {
      const tooLow = check.exclusiveMin ? parsed <= check.min : parsed < check.min;
      if (tooLow) {
        errors[check.key] = `${check.label} must be ${check.exclusiveMin ? "greater than" : "at least"} ${check.min}.`;
      }
    }

    if (typeof check.max === "number" && parsed > check.max) {
      errors[check.key] = `${check.label} must be ${check.max} or less.`;
    }
  }

  const targetDte = parseNumber(values.targetDte);
  const dteTol = parseNumber(values.dteToleranceDays);
  if (!errors.dteToleranceDays && !errors.targetDte && targetDte > 0 && dteTol >= targetDte) {
    errors.dteToleranceDays = "DTE tolerance must be less than target DTE.";
  }

  const entryRules: CreateBacktestRunRequest["entry_rules"] = [];
  const hasEnabledEntryRule = [
    values.rsiEnabled,
    values.movingAverageEnabled,
    values.macdEnabled,
    values.bollingerEnabled,
    values.ivRankEnabled,
    values.avoidEarningsEnabled,
    values.ivPercentileEnabled,
    values.volumeSpikeEnabled,
    values.supportResistanceEnabled,
  ].some(Boolean);

  if (values.rsiEnabled) {
    if (!isFiniteNumber(values.rsiThreshold)) {
      errors.rsiThreshold = "RSI threshold must be a valid number.";
    }

    if (!isFiniteNumber(values.rsiPeriod)) {
      errors.rsiPeriod = "RSI period must be a valid number.";
    }

    const threshold = parseNumber(values.rsiThreshold);
    const period = parseNumber(values.rsiPeriod);

    if (Number.isFinite(threshold) && (threshold < 0 || threshold > 100)) {
      errors.rsiThreshold = "RSI threshold must be between 0 and 100.";
    }

    if (Number.isFinite(period) && !Number.isInteger(period)) {
      errors.rsiPeriod = "RSI period must be a whole number.";
    } else if (Number.isFinite(period) && (period < 2 || period > 100)) {
      errors.rsiPeriod = "RSI period must be between 2 and 100.";
    }

    if (!errors.rsiThreshold && !errors.rsiPeriod) {
      entryRules.push({
        type: "rsi",
        operator: values.rsiOperator,
        threshold,
        period,
      });
    }
  }

  if (values.movingAverageEnabled) {
    if (!isFiniteNumber(values.fastPeriod)) {
      errors.fastPeriod = "Fast period must be a valid number.";
    }

    if (!isFiniteNumber(values.slowPeriod)) {
      errors.slowPeriod = "Slow period must be a valid number.";
    }

    const fastPeriod = parseNumber(values.fastPeriod);
    const slowPeriod = parseNumber(values.slowPeriod);

    if (Number.isFinite(fastPeriod) && (fastPeriod < 2 || fastPeriod > 200)) {
      errors.fastPeriod = "Fast period must be between 2 and 200.";
    }

    if (Number.isFinite(slowPeriod) && (slowPeriod < 3 || slowPeriod > 400)) {
      errors.slowPeriod = "Slow period must be between 3 and 400.";
    }

    if (Number.isFinite(fastPeriod) && Number.isFinite(slowPeriod) && fastPeriod >= slowPeriod) {
      errors.slowPeriod = "Slow period must be greater than fast period.";
    }

    if (!errors.fastPeriod && !errors.slowPeriod) {
      entryRules.push({
        type: values.movingAverageType,
        fast_period: fastPeriod,
        slow_period: slowPeriod,
        direction: values.crossoverDirection,
      });
    }
  }

  if (values.macdEnabled) {
    const mFast = parseNumber(values.macdFastPeriod);
    const mSlow = parseNumber(values.macdSlowPeriod);
    const mSignal = parseNumber(values.macdSignalPeriod);
    if (!isFiniteNumber(values.macdFastPeriod) || mFast < 2 || mFast > 100) {
      errors.macdFastPeriod = "MACD fast period must be between 2 and 100.";
    }
    if (!isFiniteNumber(values.macdSlowPeriod) || mSlow < 3 || mSlow > 200) {
      errors.macdSlowPeriod = "MACD slow period must be between 3 and 200.";
    }
    if (!isFiniteNumber(values.macdSignalPeriod) || mSignal < 2 || mSignal > 100) {
      errors.macdSignalPeriod = "MACD signal period must be between 2 and 100.";
    }
    if (!errors.macdFastPeriod && !errors.macdSlowPeriod && Number.isFinite(mFast) && Number.isFinite(mSlow) && mFast >= mSlow) {
      errors.macdSlowPeriod = "MACD slow period must be greater than fast period.";
    }
    if (!errors.macdFastPeriod && !errors.macdSlowPeriod && !errors.macdSignalPeriod) {
      entryRules.push({
        type: "macd",
        fast_period: mFast,
        slow_period: mSlow,
        signal_period: mSignal,
        direction: values.macdDirection,
      } as any);
    }
  }

  if (values.bollingerEnabled) {
    const bPeriod = parseNumber(values.bollingerPeriod);
    const bStdDev = parseNumber(values.bollingerStdDev);
    if (!isFiniteNumber(values.bollingerPeriod) || bPeriod < 5 || bPeriod > 200) {
      errors.bollingerPeriod = "Bollinger period must be between 5 and 200.";
    }
    if (!isFiniteNumber(values.bollingerStdDev) || bStdDev < 0.5 || bStdDev > 5) {
      errors.bollingerStdDev = "Std deviations must be between 0.5 and 5.";
    }
    if (!errors.bollingerPeriod && !errors.bollingerStdDev) {
      entryRules.push({
        type: "bollinger_bands",
        period: bPeriod,
        standard_deviations: bStdDev,
        band: values.bollingerBand,
        operator: values.bollingerOperator,
      } as any);
    }
  }

  if (values.ivRankEnabled) {
    const ivThreshold = parseNumber(values.ivRankThreshold);
    if (!isFiniteNumber(values.ivRankThreshold) || ivThreshold < 0 || ivThreshold > 100) {
      errors.ivRankThreshold = "IV Rank threshold must be between 0 and 100.";
    }
    if (!errors.ivRankThreshold) {
      entryRules.push({
        type: "iv_rank",
        operator: values.ivRankOperator,
        threshold: ivThreshold,
      } as any);
    }
  }

  if (values.avoidEarningsEnabled) {
    const daysBefore = parseNumber(values.avoidEarningsDaysBefore);
    const daysAfter = parseNumber(values.avoidEarningsDaysAfter);
    if (!isFiniteNumber(values.avoidEarningsDaysBefore) || daysBefore < 0 || daysBefore > 30) {
      errors.avoidEarningsDaysBefore = "Days before must be between 0 and 30.";
    }
    if (!isFiniteNumber(values.avoidEarningsDaysAfter) || daysAfter < 0 || daysAfter > 30) {
      errors.avoidEarningsDaysAfter = "Days after must be between 0 and 30.";
    }
    if (!errors.avoidEarningsDaysBefore && !errors.avoidEarningsDaysAfter && daysBefore === 0 && daysAfter === 0) {
      errors.avoidEarningsDaysAfter = "At least one earnings buffer must be greater than 0.";
    }
    if (!errors.avoidEarningsDaysBefore && !errors.avoidEarningsDaysAfter) {
      entryRules.push({
        type: "avoid_earnings",
        days_before: daysBefore,
        days_after: daysAfter,
      } as any);
    }
  }

  if (values.ivPercentileEnabled) {
    const ivPctThreshold = parseNumber(values.ivPercentileThreshold);
    if (!isFiniteNumber(values.ivPercentileThreshold) || ivPctThreshold < 0 || ivPctThreshold > 100) {
      errors.ivPercentileThreshold = "IV Percentile threshold must be between 0 and 100.";
    }
    if (!errors.ivPercentileThreshold) {
      entryRules.push({
        type: "iv_percentile",
        operator: values.ivPercentileOperator,
        threshold: ivPctThreshold,
      } as any);
    }
  }

  if (values.volumeSpikeEnabled) {
    const vsMultiplier = parseNumber(values.volumeSpikeMultiplier);
    const vsPeriod = parseNumber(values.volumeSpikePeriod);
    if (!isFiniteNumber(values.volumeSpikeMultiplier) || vsMultiplier < 1 || vsMultiplier > 20) {
      errors.volumeSpikeMultiplier = "Volume spike multiplier must be between 1 and 20.";
    }
    if (!isFiniteNumber(values.volumeSpikePeriod) || vsPeriod < 5 || vsPeriod > 100) {
      errors.volumeSpikePeriod = "Volume spike period must be between 5 and 100.";
    }
    if (!errors.volumeSpikeMultiplier && !errors.volumeSpikePeriod) {
      entryRules.push({
        type: "volume_spike",
        multiplier: vsMultiplier,
        lookback_period: vsPeriod,
      } as any);
    }
  }

  if (values.supportResistanceEnabled) {
    const srPeriod = parseNumber(values.supportResistancePeriod);
    if (!isFiniteNumber(values.supportResistancePeriod) || srPeriod < 5 || srPeriod > 200) {
      errors.supportResistancePeriod = "Support/resistance period must be between 5 and 200.";
    }
    if (!errors.supportResistancePeriod) {
      entryRules.push({
        type: "support_resistance",
        mode: values.supportResistanceMode,
        lookback_period: srPeriod,
      } as any);
    }
  }

  if (!hasEnabledEntryRule) {
    errors.form = "At least one valid entry rule must be configured.";
  }

  const slippage = parseNumber(values.slippagePct);
  if (!isFiniteNumber(values.slippagePct) || slippage < 0 || slippage > 5) {
    errors.slippagePct = "Slippage must be between 0 and 5%.";
  }

  if (values.profitTargetEnabled) {
    const pt = parseNumber(values.profitTargetPct);
    if (!isFiniteNumber(values.profitTargetPct) || pt < 1 || pt > 500) {
      errors.profitTargetPct = "Profit target must be between 1 and 500%.";
    }
  }

  if (values.stopLossEnabled) {
    const sl = parseNumber(values.stopLossPct);
    if (!isFiniteNumber(values.stopLossPct) || sl < 1 || sl > 100) {
      errors.stopLossPct = "Stop loss must be between 1 and 100%.";
    }
  }

  const rfr = parseNumber(values.riskFreeRate);
  if (!isFiniteNumber(values.riskFreeRate) || rfr < 0 || rfr > 0.20) {
    errors.riskFreeRate = "Risk-free rate must be between 0 and 0.20 (20%).";
  }

  if (Object.keys(errors).length > 0) {
    return { errors };
  }

  const payload: CreateBacktestRunRequest & { profit_target_pct?: number; stop_loss_pct?: number } = {
    symbol: normalizedSymbol,
    strategy_type: values.strategyType,
    start_date: values.startDate,
    end_date: values.endDate,
    target_dte: parseNumber(values.targetDte),
    dte_tolerance_days: parseNumber(values.dteToleranceDays),
    max_holding_days: parseNumber(values.maxHoldingDays),
    account_size: parseNumber(values.accountSize),
    risk_per_trade_pct: parseNumber(values.riskPerTradePct),
    commission_per_contract: parseNumber(values.commissionPerContract),
    entry_rules: entryRules,
    slippage_pct: slippage,
    risk_free_rate: rfr,
  };
  if (values.strategyType === "calendar_spread" && values.calendarContractType === "put") {
    payload.strategy_overrides = {
      calendar_contract_type: "put",
    };
  }
  if (values.profitTargetEnabled) {
    payload.profit_target_pct = parseNumber(values.profitTargetPct);
  }
  if (values.stopLossEnabled) {
    payload.stop_loss_pct = parseNumber(values.stopLossPct);
  }

  return { errors, payload };
}
