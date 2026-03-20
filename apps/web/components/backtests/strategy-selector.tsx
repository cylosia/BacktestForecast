import { useEffect, useRef } from "react";
import { ChevronDown } from "lucide-react";
import type { StrategyCatalogGroup, StrategyType } from "@backtestforecast/api-client";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";

/**
 * Minimal fallback labels used only while the backend catalog endpoint
 * has not yet responded.  The authoritative metadata (description, bias,
 * tier, max-loss, etc.) lives in the backend catalog.
 */
const FALLBACK_LABELS: Record<string, string> = {
  long_call: "Long Call",
  long_put: "Long Put",
  naked_call: "Naked Call",
  naked_put: "Naked Put",
  covered_call: "Covered Call",
  cash_secured_put: "Cash-Secured Put",
  wheel_strategy: "Wheel Strategy",
  collar: "Collar",
  covered_strangle: "Covered Strangle",
  bull_call_debit_spread: "Bull Call Spread",
  bear_put_debit_spread: "Bear Put Spread",
  bull_put_credit_spread: "Bull Put Credit Spread",
  bear_call_credit_spread: "Bear Call Credit Spread",
  iron_condor: "Iron Condor",
  long_straddle: "Long Straddle",
  long_strangle: "Long Strangle",
  calendar_spread: "Call Calendar Spread",
  butterfly: "Butterfly Spread",
  short_straddle: "Short Straddle",
  short_strangle: "Short Strangle",
  iron_butterfly: "Iron Butterfly",
  poor_mans_covered_call: "Poor Man's Covered Call",
  diagonal_spread: "Diagonal Spread",
  double_diagonal: "Double Diagonal",
  ratio_call_backspread: "Ratio Call Backspread",
  ratio_put_backspread: "Ratio Put Backspread",
  synthetic_put: "Synthetic Put",
  reverse_conversion: "Reverse Conversion",
  jade_lizard: "Jade Lizard",
};

function buildFallbackGroups(): StrategyCatalogGroup[] {
  const stub = (key: string): StrategyCatalogGroup["strategies"][number] => ({
    strategy_type: key as StrategyType,
    label: FALLBACK_LABELS[key] ?? key,
    short_description: "",
    category: "single_leg",
    bias: "neutral",
    leg_count: 1,
    min_tier: "free",
    max_loss_description: "",
    notes: "",
    tags: [],
  });

  return [
    {
      category: "all",
      category_label: "All strategies",
      strategies: Object.keys(FALLBACK_LABELS).map(stub),
    },
  ];
}

function biasColor(bias: string) {
  switch (bias) {
    case "bullish":
      return "text-emerald-600 dark:text-emerald-400";
    case "bearish":
      return "text-red-500 dark:text-red-400";
    default:
      return "text-muted-foreground";
  }
}

function biasLabel(bias: string) {
  switch (bias) {
    case "bullish":
      return "Bullish";
    case "bearish":
      return "Bearish";
    case "neutral":
      return "Neutral";
    default:
      return bias;
  }
}

function tierBadge(minTier: string) {
  if (minTier === "premium") {
    return (
      <span className="inline-flex items-center rounded-full bg-amber-500/10 px-1.5 py-0.5 text-[10px] font-medium text-amber-600 dark:text-amber-400">
        Premium
      </span>
    );
  }
  if (minTier === "pro") {
    return (
      <span className="inline-flex items-center rounded-full bg-primary/10 px-1.5 py-0.5 text-[10px] font-medium text-primary">
        Pro
      </span>
    );
  }
  return null;
}

export function StrategySelector({
  value,
  error,
  onChange,
  catalogGroups,
}: {
  value: StrategyType;
  error?: string;
  onChange: (value: StrategyType) => void;
  catalogGroups?: StrategyCatalogGroup[];
}) {
  const usingFallback = !catalogGroups || catalogGroups.length === 0;
  const groups = usingFallback ? buildFallbackGroups() : catalogGroups;

  const warnedRef = useRef(false);
  useEffect(() => {
    if (usingFallback && !warnedRef.current) {
      warnedRef.current = true;
      console.warn(
        "[StrategySelector] Backend catalog unavailable; using label-only fallback. " +
        "Strategy descriptions, bias, and tier metadata are not displayed.",
      );
    }
  }, [usingFallback]);

  return (
    <div className="space-y-2">
      <Label htmlFor="strategyType">Strategy</Label>
      <div className="relative">
        <select
          id="strategyType"
          className="flex h-10 w-full appearance-none rounded-lg border border-input bg-background px-3 py-2 pr-10 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
          value={value}
          aria-invalid={!!error}
          aria-describedby={error ? "strategyType-error" : undefined}
          onChange={(e) => onChange(e.target.value as StrategyType)}
        >
          {groups.map((group) => (
            <optgroup key={group.category} label={group.category_label}>
              {group.strategies.map((strategy) => (
                <option key={strategy.strategy_type} value={strategy.strategy_type}>
                  {strategy.label}
                  {(strategy.min_tier ?? "free") !== "free" ? ` (${strategy.min_tier})` : ""}
                </option>
              ))}
            </optgroup>
          ))}
        </select>
        <ChevronDown className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
      </div>

      {/* Selected strategy detail (only shown when using the full backend catalog) */}
      {!usingFallback &&
        (() => {
          const selected = groups
            .flatMap((g) => g.strategies)
            .find((s) => s.strategy_type === value);
          if (!selected) return null;
          return (
            <div className="rounded-lg border border-border/60 bg-muted/30 p-3 text-sm">
              <div className="flex items-center gap-2">
                <span className={cn("font-medium", biasColor(selected.bias))}>
                  {biasLabel(selected.bias)}
                </span>
                <span className="text-muted-foreground">·</span>
                <span className="text-muted-foreground">
                  {selected.leg_count} leg{selected.leg_count !== 1 ? "s" : ""}
                </span>
                {tierBadge(selected.min_tier)}
              </div>
              <p className="mt-1 text-muted-foreground">
                {selected.short_description}
              </p>
              <p className="mt-1 text-xs text-muted-foreground">
                Max loss: {selected.max_loss_description}
              </p>
            </div>
          );
        })()}

      {error ? <p id="strategyType-error" className="text-sm text-destructive">{error}</p> : null}
    </div>
  );
}
