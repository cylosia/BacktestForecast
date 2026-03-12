import type {
  ComparisonOperator,
  CreateBacktestRunRequest,
  CrossoverDirection,
  MovingAverageRuleType,
  StrategyType,
} from "@/lib/backtests/types";

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
  date.setDate(date.getDate() - days);
  return date.toISOString().slice(0, 10);
}

function isFiniteNumber(value: string): boolean {
  if (!value.trim()) {
    return false;
  }

  return Number.isFinite(Number(value));
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
  }

  if (!values.startDate) {
    errors.startDate = "Start date is required.";
  }

  if (!values.endDate) {
    errors.endDate = "End date is required.";
  }

  if (values.startDate && values.endDate && values.startDate >= values.endDate) {
    errors.endDate = "End date must be later than start date.";
  }

  const numericChecks: Array<{
    key: keyof BacktestFormValues;
    min?: number;
    max?: number;
    exclusiveMin?: boolean;
    label: string;
  }> = [
    { key: "targetDte", min: 7, max: 365, label: "Target DTE" },
    { key: "dteToleranceDays", min: 0, max: 60, label: "DTE tolerance" },
    { key: "maxHoldingDays", min: 1, max: 120, label: "Max holding days" },
    { key: "accountSize", min: 0, exclusiveMin: true, label: "Account size" },
    { key: "riskPerTradePct", min: 0, max: 100, exclusiveMin: true, label: "Risk per trade" },
    { key: "commissionPerContract", min: 0, label: "Commission per contract" },
  ];

  for (const check of numericChecks) {
    const rawValue = values[check.key];
    if (typeof rawValue !== "string" || !isFiniteNumber(rawValue)) {
      errors[check.key] = `${check.label} must be a valid number.`;
      continue;
    }

    const parsed = parseNumber(rawValue);
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

    if (Number.isFinite(period) && (period < 2 || period > 100)) {
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
