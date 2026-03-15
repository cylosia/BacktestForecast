import type {
  ComparisonOperator,
  CreateBacktestRunRequest,
  CrossoverDirection,
  MovingAverageRuleType,
  StrategyType,
} from "@backtestforecast/api-client";

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
}

export type BacktestFormErrors = Partial<Record<keyof BacktestFormValues | "form", string>>;

export function getDefaultBacktestFormValues(): BacktestFormValues {
  return {
    symbol: "SPY",
    strategyType: "long_call",
    startDate: daysAgo(365),
    endDate: daysAgo(0),
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
  };
}

function daysAgo(days: number): string {
  const date = new Date();
  date.setUTCDate(date.getUTCDate() - days);
  return date.toISOString().slice(0, 10);
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
  } else if (!/^[A-Z0-9./^]{1,16}$/.test(normalizedSymbol)) {
    errors.symbol = "Symbol may only contain letters, digits, dots, slashes, or ^.";
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

  if (!errors.startDate && !errors.endDate && values.startDate >= values.endDate) {
    errors.endDate = "End date must be later than start date.";
  }

  const numericChecks: Array<{
    key: keyof BacktestFormValues;
    min?: number;
    max?: number;
    exclusiveMin?: boolean;
    integer?: boolean;
    label: string;
  }> = [
    { key: "targetDte", min: 7, max: 365, integer: true, label: "Target DTE" },
    { key: "dteToleranceDays", min: 0, max: 60, integer: true, label: "DTE tolerance" },
    { key: "maxHoldingDays", min: 1, max: 120, integer: true, label: "Max holding days" },
    { key: "accountSize", min: 100, max: 100_000_000, label: "Account size" },
    { key: "riskPerTradePct", min: 0, max: 100, exclusiveMin: true, label: "Risk per trade" },
    { key: "commissionPerContract", min: 0, max: 1000, label: "Commission per contract" },
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

  if (entryRules.length === 0 && !values.rsiEnabled && !values.movingAverageEnabled) {
    errors.form = "At least one entry rule (RSI or moving average) must be enabled.";
  }

  if (Object.keys(errors).length > 0) {
    return { errors };
  }

  return {
    errors,
    payload: {
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
      idempotency_key: crypto.randomUUID(),
    },
  };
}
