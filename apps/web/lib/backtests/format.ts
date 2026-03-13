import type {
  RunStatus,
  StrategyType,
} from "@backtestforecast/api-client";

export type NumericValue = number | string;

const currencyFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 2,
});

const numberFormatter = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 2,
});

const percentFormatter = new Intl.NumberFormat("en-US", {
  maximumFractionDigits: 2,
  minimumFractionDigits: 0,
});

export function toNumber(value: NumericValue): number {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : 0;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

export function formatCurrency(value: NumericValue): string {
  return currencyFormatter.format(toNumber(value));
}

export function formatPercent(value: NumericValue): string {
  return `${percentFormatter.format(toNumber(value))}%`;
}

export function formatNumber(value: NumericValue): string {
  return numberFormatter.format(toNumber(value));
}

const dateFormatter = new Intl.DateTimeFormat("en-US", {
  month: "short",
  day: "numeric",
  year: "numeric",
});

const dateTimeFormatter = new Intl.DateTimeFormat("en-US", {
  month: "short",
  day: "numeric",
  year: "numeric",
  hour: "numeric",
  minute: "2-digit",
});

export function formatDate(value: string | null | undefined): string {
  if (!value) return "—";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "—";
  return dateFormatter.format(d);
}

export function formatDateTime(value: string | null | undefined): string {
  if (!value) return "—";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "—";
  return dateTimeFormatter.format(d);
}

export function strategyLabel(strategy: string | StrategyType): string {
  switch (strategy) {
    case "long_call":
      return "Long Call";
    case "long_put":
      return "Long Put";
    case "covered_call":
      return "Covered Call";
    case "cash_secured_put":
      return "Cash-Secured Put";
    case "bull_call_debit_spread":
      return "Bull Call Spread";
    case "bear_put_debit_spread":
      return "Bear Put Spread";
    case "bull_put_credit_spread":
      return "Bull Put Credit Spread";
    case "bear_call_credit_spread":
      return "Bear Call Credit Spread";
    case "iron_condor":
      return "Iron Condor";
    case "long_straddle":
      return "Long Straddle";
    case "long_strangle":
      return "Long Strangle";
    case "calendar_spread":
      return "Calendar Spread";
    case "butterfly":
      return "Butterfly Spread";
    case "wheel_strategy":
      return "Wheel Strategy";
    case "poor_mans_covered_call":
      return "Poor Man's Covered Call";
    case "ratio_call_backspread":
      return "Ratio Call Backspread";
    case "ratio_put_backspread":
      return "Ratio Put Backspread";
    case "collar":
      return "Collar";
    case "diagonal_spread":
      return "Diagonal Spread";
    case "double_diagonal":
      return "Double Diagonal";
    case "short_straddle":
      return "Short Straddle";
    case "short_strangle":
      return "Short Strangle";
    case "covered_strangle":
      return "Covered Strangle";
    case "synthetic_put":
      return "Synthetic Put";
    case "reverse_conversion":
      return "Reverse Conversion";
    case "jade_lizard":
      return "Jade Lizard";
    case "iron_butterfly":
      return "Iron Butterfly";
    case "custom_2_leg":
      return "Custom 2-Leg";
    case "custom_3_leg":
      return "Custom 3-Leg";
    case "custom_4_leg":
      return "Custom 4-Leg";
    case "custom_5_leg":
      return "Custom 5-Leg";
    case "custom_6_leg":
      return "Custom 6-Leg";
    case "custom_8_leg":
      return "Custom 8-Leg";
    case "naked_call":
      return "Naked Call";
    case "naked_put":
      return "Naked Put";
    case "sma_crossover":
      return "SMA crossover";
    case "ema_crossover":
      return "EMA crossover";
    default:
      return strategy.replace(/_/g, " ");
  }
}

export function statusLabel(status: RunStatus | string): string {
  switch (status) {
    case "queued":
      return "Queued";
    case "running":
      return "Running";
    case "succeeded":
      return "Completed";
    case "failed":
      return "Failed";
    case "cancelled":
      return "Cancelled";
    default:
      return status;
  }
}

export function isTerminalStatus(status: RunStatus | string): boolean {
  return status === "succeeded" || status === "failed" || status === "cancelled";
}
