import type {
  BollingerBand,
  ComparisonOperator,
  EntryRule,
  EntryRuleInput,
  IndicatorCrossDirection,
  IndicatorSeries,
  IndicatorSeriesInput,
  IndicatorTrendDirection,
} from "@backtestforecast/api-client";

export type AdvancedIndicatorType =
  | "close"
  | "rsi"
  | "sma"
  | "ema"
  | "macd_line"
  | "macd_signal"
  | "macd_histogram"
  | "bollinger_band"
  | "iv_rank"
  | "iv_percentile"
  | "volume_ratio"
  | "cci"
  | "roc"
  | "mfi"
  | "stochastic_k"
  | "stochastic_d"
  | "adx"
  | "williams_r";

export type AdvancedRuleType =
  | "indicator_threshold"
  | "indicator_trend"
  | "indicator_level_cross"
  | "indicator_series_cross"
  | "indicator_persistence";

export interface EditableIndicatorSeries {
  indicator: AdvancedIndicatorType;
  period: string;
  fastPeriod: string;
  slowPeriod: string;
  signalPeriod: string;
  standardDeviations: string;
  band: BollingerBand;
  lookbackDays: string;
  lookbackPeriod: string;
  kPeriod: string;
  dPeriod: string;
  smoothK: string;
}

export interface EditableAdvancedRule {
  id: string;
  type: AdvancedRuleType;
  series: EditableIndicatorSeries;
  leftSeries: EditableIndicatorSeries;
  rightSeries: EditableIndicatorSeries;
  operator: ComparisonOperator;
  direction: IndicatorTrendDirection | IndicatorCrossDirection;
  level: string;
  bars: string;
}

export const ADVANCED_RULE_OPTIONS: Array<{ value: AdvancedRuleType; label: string }> = [
  { value: "indicator_threshold", label: "Indicator vs level" },
  { value: "indicator_trend", label: "Indicator rising or falling" },
  { value: "indicator_level_cross", label: "Indicator crosses a level" },
  { value: "indicator_series_cross", label: "Indicator crosses another series" },
  { value: "indicator_persistence", label: "Indicator stays above or below" },
];

export const ADVANCED_INDICATOR_OPTIONS: Array<{ value: AdvancedIndicatorType; label: string }> = [
  { value: "close", label: "Close" },
  { value: "rsi", label: "RSI" },
  { value: "sma", label: "SMA" },
  { value: "ema", label: "EMA" },
  { value: "macd_line", label: "MACD line" },
  { value: "macd_signal", label: "MACD signal" },
  { value: "macd_histogram", label: "MACD histogram" },
  { value: "bollinger_band", label: "Bollinger band" },
  { value: "iv_rank", label: "IV rank" },
  { value: "iv_percentile", label: "IV percentile" },
  { value: "volume_ratio", label: "Volume ratio" },
  { value: "cci", label: "CCI" },
  { value: "roc", label: "ROC" },
  { value: "mfi", label: "MFI" },
  { value: "stochastic_k", label: "Stochastic %K" },
  { value: "stochastic_d", label: "Stochastic %D" },
  { value: "adx", label: "ADX" },
  { value: "williams_r", label: "Williams %R" },
];

function newDraftId(): string {
  return `rule_${Math.random().toString(36).slice(2, 10)}`;
}

export function createDefaultIndicatorSeries(
  indicator: AdvancedIndicatorType = "rsi",
): EditableIndicatorSeries {
  return {
    indicator,
    period: indicator === "roc" ? "10" : indicator === "cci" ? "20" : "14",
    fastPeriod: "12",
    slowPeriod: "26",
    signalPeriod: "9",
    standardDeviations: "2",
    band: "lower",
    lookbackDays: "252",
    lookbackPeriod: "20",
    kPeriod: "14",
    dPeriod: "3",
    smoothK: "3",
  };
}

export function createDefaultAdvancedRule(
  type: AdvancedRuleType = "indicator_threshold",
): EditableAdvancedRule {
  return {
    id: newDraftId(),
    type,
    series: createDefaultIndicatorSeries("rsi"),
    leftSeries: createDefaultIndicatorSeries("close"),
    rightSeries: createDefaultIndicatorSeries("ema"),
    operator: "gte",
    direction: type === "indicator_trend" ? "rising" : "crosses_above",
    level: "50",
    bars: "3",
  };
}

function toEditableSeries(spec: IndicatorSeries | IndicatorSeriesInput): EditableIndicatorSeries {
  const base = createDefaultIndicatorSeries(spec.indicator as AdvancedIndicatorType);
  switch (spec.indicator) {
    case "close":
      return base;
    case "rsi":
    case "sma":
    case "ema":
    case "cci":
    case "roc":
    case "mfi":
    case "adx":
    case "williams_r":
      return { ...base, period: String(spec.period) };
    case "macd_line":
    case "macd_signal":
    case "macd_histogram":
      return {
        ...base,
        fastPeriod: String(spec.fast_period),
        slowPeriod: String(spec.slow_period),
        signalPeriod: String(spec.signal_period),
      };
    case "bollinger_band":
      return {
        ...base,
        band: spec.band,
        period: String(spec.period),
        standardDeviations: String(spec.standard_deviations),
      };
    case "iv_rank":
    case "iv_percentile":
      return { ...base, lookbackDays: String(spec.lookback_days) };
    case "volume_ratio":
      return { ...base, lookbackPeriod: String(spec.lookback_period) };
    case "stochastic_k":
    case "stochastic_d":
      return {
        ...base,
        kPeriod: String(spec.k_period),
        dPeriod: String(spec.d_period),
        smoothK: String(spec.smooth_k),
      };
  }
}

export function isGenericEntryRuleType(type: string): type is AdvancedRuleType {
  return (
    type === "indicator_threshold"
    || type === "indicator_trend"
    || type === "indicator_level_cross"
    || type === "indicator_series_cross"
    || type === "indicator_persistence"
  );
}

export function draftFromEntryRule(rule: EntryRule | EntryRuleInput): EditableAdvancedRule | null {
  if (!isGenericEntryRuleType(rule.type)) {
    return null;
  }

  switch (rule.type) {
    case "indicator_series_cross":
      return {
        id: newDraftId(),
        type: rule.type,
        series: createDefaultIndicatorSeries("rsi"),
        leftSeries: toEditableSeries(rule.left_series),
        rightSeries: toEditableSeries(rule.right_series),
        operator: "gte",
        direction: rule.direction,
        level: "0",
        bars: "3",
      };
    case "indicator_trend":
      return {
        id: newDraftId(),
        type: rule.type,
        series: toEditableSeries(rule.series),
        leftSeries: createDefaultIndicatorSeries("close"),
        rightSeries: createDefaultIndicatorSeries("ema"),
        operator: "gte",
        direction: rule.direction,
        level: "0",
        bars: String(rule.bars),
      };
    case "indicator_threshold":
      return {
        id: newDraftId(),
        type: rule.type,
        series: toEditableSeries(rule.series),
        leftSeries: createDefaultIndicatorSeries("close"),
        rightSeries: createDefaultIndicatorSeries("ema"),
        operator: rule.operator,
        direction: "crosses_above",
        level: String(rule.level),
        bars: "3",
      };
    case "indicator_level_cross":
      return {
        id: newDraftId(),
        type: rule.type,
        series: toEditableSeries(rule.series),
        leftSeries: createDefaultIndicatorSeries("close"),
        rightSeries: createDefaultIndicatorSeries("ema"),
        operator: "gte",
        direction: rule.direction,
        level: String(rule.level),
        bars: "3",
      };
    case "indicator_persistence":
      return {
        id: newDraftId(),
        type: rule.type,
        series: toEditableSeries(rule.series),
        leftSeries: createDefaultIndicatorSeries("close"),
        rightSeries: createDefaultIndicatorSeries("ema"),
        operator: rule.operator,
        direction: "crosses_above",
        level: String(rule.level),
        bars: String(rule.bars),
      };
  }
}

function parseNumberInRange(
  value: string,
  label: string,
  min: number,
  max: number,
  integer = true,
): { value?: number; error?: string } {
  const trimmed = value.trim();
  if (!trimmed) {
    return { error: `${label} is required.` };
  }
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed)) {
    return { error: `${label} must be a valid number.` };
  }
  if (integer && !Number.isInteger(parsed)) {
    return { error: `${label} must be a whole number.` };
  }
  if (parsed < min || parsed > max) {
    return { error: `${label} must be between ${min} and ${max}.` };
  }
  return { value: parsed };
}

function parseDecimalInRange(
  value: string,
  label: string,
  min: number,
  max: number,
  exclusiveMin = false,
): { value?: number; error?: string } {
  const trimmed = value.trim();
  if (!trimmed) {
    return { error: `${label} is required.` };
  }
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed)) {
    return { error: `${label} must be a valid number.` };
  }
  const tooLow = exclusiveMin ? parsed <= min : parsed < min;
  if (tooLow || parsed > max) {
    return { error: `${label} must be ${exclusiveMin ? "greater than" : "between"} ${min}${exclusiveMin ? "" : ` and ${max}`}${exclusiveMin ? ` and at most ${max}` : ""}.` };
  }
  return { value: parsed };
}

export function buildIndicatorSeriesInput(
  draft: EditableIndicatorSeries,
): { series?: IndicatorSeriesInput; error?: string } {
  switch (draft.indicator) {
    case "close":
      return { series: { indicator: "close" } };
    case "rsi": {
      const parsed = parseNumberInRange(draft.period, "RSI period", 2, 100);
      return parsed.error ? { error: parsed.error } : { series: { indicator: "rsi", period: parsed.value! } };
    }
    case "sma": {
      const parsed = parseNumberInRange(draft.period, "SMA period", 2, 400);
      return parsed.error ? { error: parsed.error } : { series: { indicator: "sma", period: parsed.value! } };
    }
    case "ema": {
      const parsed = parseNumberInRange(draft.period, "EMA period", 2, 400);
      return parsed.error ? { error: parsed.error } : { series: { indicator: "ema", period: parsed.value! } };
    }
    case "macd_line":
    case "macd_signal":
    case "macd_histogram": {
      const fast = parseNumberInRange(draft.fastPeriod, "MACD fast period", 2, 100);
      if (fast.error) return { error: fast.error };
      const slow = parseNumberInRange(draft.slowPeriod, "MACD slow period", 3, 200);
      if (slow.error) return { error: slow.error };
      const signal = parseNumberInRange(draft.signalPeriod, "MACD signal period", 2, 100);
      if (signal.error) return { error: signal.error };
      if (fast.value! >= slow.value!) {
        return { error: "MACD slow period must be greater than fast period." };
      }
      return {
        series: {
          indicator: draft.indicator,
          fast_period: fast.value!,
          slow_period: slow.value!,
          signal_period: signal.value!,
        },
      };
    }
    case "bollinger_band": {
      const period = parseNumberInRange(draft.period, "Bollinger period", 5, 200);
      if (period.error) return { error: period.error };
      const stdDev = parseDecimalInRange(draft.standardDeviations, "Bollinger standard deviations", 0, 5, true);
      if (stdDev.error) return { error: stdDev.error };
      return {
        series: {
          indicator: "bollinger_band",
          band: draft.band,
          period: period.value!,
          standard_deviations: stdDev.value!,
        },
      };
    }
    case "iv_rank":
    case "iv_percentile": {
      const lookback = parseNumberInRange(draft.lookbackDays, "IV lookback days", 20, 756);
      return lookback.error ? { error: lookback.error } : { series: { indicator: draft.indicator, lookback_days: lookback.value! } };
    }
    case "volume_ratio": {
      const lookback = parseNumberInRange(draft.lookbackPeriod, "Volume lookback period", 2, 252);
      return lookback.error ? { error: lookback.error } : { series: { indicator: "volume_ratio", lookback_period: lookback.value! } };
    }
    case "cci": {
      const period = parseNumberInRange(draft.period, "CCI period", 2, 252);
      return period.error ? { error: period.error } : { series: { indicator: "cci", period: period.value! } };
    }
    case "roc": {
      const period = parseNumberInRange(draft.period, "ROC period", 1, 252);
      return period.error ? { error: period.error } : { series: { indicator: "roc", period: period.value! } };
    }
    case "mfi": {
      const period = parseNumberInRange(draft.period, "MFI period", 2, 252);
      return period.error ? { error: period.error } : { series: { indicator: "mfi", period: period.value! } };
    }
    case "stochastic_k":
    case "stochastic_d": {
      const k = parseNumberInRange(draft.kPeriod, "Stochastic %K period", 2, 252);
      if (k.error) return { error: k.error };
      const d = parseNumberInRange(draft.dPeriod, "Stochastic %D period", 1, 100);
      if (d.error) return { error: d.error };
      const smoothK = parseNumberInRange(draft.smoothK, "Stochastic smoothing", 1, 100);
      if (smoothK.error) return { error: smoothK.error };
      return {
        series: {
          indicator: draft.indicator,
          k_period: k.value!,
          d_period: d.value!,
          smooth_k: smoothK.value!,
        },
      };
    }
    case "adx": {
      const period = parseNumberInRange(draft.period, "ADX period", 2, 252);
      return period.error ? { error: period.error } : { series: { indicator: "adx", period: period.value! } };
    }
    case "williams_r": {
      const period = parseNumberInRange(draft.period, "Williams %R period", 2, 252);
      return period.error ? { error: period.error } : { series: { indicator: "williams_r", period: period.value! } };
    }
  }
}

export function buildAdvancedRuleInput(
  draft: EditableAdvancedRule,
): { rule?: EntryRuleInput; error?: string } {
  if (draft.type === "indicator_series_cross") {
    const left = buildIndicatorSeriesInput(draft.leftSeries);
    if (left.error) return { error: left.error };
    const right = buildIndicatorSeriesInput(draft.rightSeries);
    if (right.error) return { error: right.error };
    if (JSON.stringify(left.series) === JSON.stringify(right.series)) {
      return { error: "Crossed series must be different." };
    }
    return {
      rule: {
        type: "indicator_series_cross",
        left_series: left.series!,
        right_series: right.series!,
        direction: draft.direction as IndicatorCrossDirection,
      },
    };
  }

  const series = buildIndicatorSeriesInput(draft.series);
  if (series.error) return { error: series.error };

  if (draft.type === "indicator_trend") {
    const bars = parseNumberInRange(draft.bars, "Trend bars", 2, 50);
    if (bars.error) return { error: bars.error };
    return {
      rule: {
        type: "indicator_trend",
        series: series.series!,
        direction: draft.direction as IndicatorTrendDirection,
        bars: bars.value!,
      },
    };
  }

  if (draft.type === "indicator_level_cross") {
    const level = parseDecimalInRange(draft.level, "Cross level", Number.NEGATIVE_INFINITY, Number.POSITIVE_INFINITY);
    if (level.error) return { error: level.error };
    return {
      rule: {
        type: "indicator_level_cross",
        series: series.series!,
        direction: draft.direction as IndicatorCrossDirection,
        level: level.value!,
      },
    };
  }

  const level = parseDecimalInRange(draft.level, "Threshold level", Number.NEGATIVE_INFINITY, Number.POSITIVE_INFINITY);
  if (level.error) return { error: level.error };

  if (draft.type === "indicator_threshold") {
    return {
      rule: {
        type: "indicator_threshold",
        series: series.series!,
        operator: draft.operator,
        level: level.value!,
      },
    };
  }

  const bars = parseNumberInRange(draft.bars, "Persistence bars", 2, 50);
  if (bars.error) return { error: bars.error };
  return {
    rule: {
      type: "indicator_persistence",
      series: series.series!,
      operator: draft.operator,
      level: level.value!,
      bars: bars.value!,
    },
  };
}
