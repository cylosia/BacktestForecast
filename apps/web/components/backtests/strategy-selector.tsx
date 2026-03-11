import { ChevronDown } from "lucide-react";
import type { StrategyCatalogGroup, StrategyType } from "@/lib/backtests/types";
import { Label } from "@/components/ui/label";
import { cn } from "@/lib/utils";

type S = { strategy_type: StrategyType; label: string; short_description: string; category: string; bias: string; leg_count: number; min_tier: string; max_loss_description: string; notes: string; tags: string[] };
const s = (strategy_type: string, label: string, short_description: string, category: string, bias: string, leg_count: number, min_tier: string, max_loss_description: string): S => ({ strategy_type: strategy_type as StrategyType, label, short_description, category, bias, leg_count, min_tier, max_loss_description, notes: "", tags: [] });

const FALLBACK_GROUPS: StrategyCatalogGroup[] = [
  { category: "single_leg", category_label: "Single-leg", strategies: [
    s("long_call", "Long Call", "Buy a call option", "single_leg", "bullish", 1, "free", "Premium paid"),
    s("long_put", "Long Put", "Buy a put option", "single_leg", "bearish", 1, "free", "Premium paid"),
    s("naked_call", "Naked Call", "Sell a call without shares", "single_leg", "bearish", 1, "premium", "Unlimited"),
    s("naked_put", "Naked Put", "Sell a put without collateral", "single_leg", "bullish", 1, "premium", "Strike × 100 minus premium"),
  ]},
  { category: "income", category_label: "Income strategies", strategies: [
    s("covered_call", "Covered Call", "Own shares + sell call", "income", "neutral", 2, "free", "Stock decline minus premium"),
    s("cash_secured_put", "Cash-Secured Put", "Sell put backed by cash", "income", "bullish", 1, "free", "Strike minus premium"),
    s("wheel_strategy", "Wheel Strategy", "Cycle puts and covered calls", "income", "neutral", 2, "premium", "Stock decline minus premium"),
    s("collar", "Collar", "Shares + sell call + buy put", "income", "neutral", 3, "premium", "Stock to put strike minus net cost"),
    s("covered_strangle", "Covered Strangle", "Shares + sell call + sell put", "income", "neutral", 3, "premium", "Stock decline minus credit"),
  ]},
  { category: "vertical_spread", category_label: "Vertical spreads", strategies: [
    s("bull_call_debit_spread", "Bull Call Spread", "Debit call spread", "vertical_spread", "bullish", 2, "free", "Net debit"),
    s("bear_put_debit_spread", "Bear Put Spread", "Debit put spread", "vertical_spread", "bearish", 2, "free", "Net debit"),
    s("bull_put_credit_spread", "Bull Put Credit Spread", "Credit put spread", "vertical_spread", "bullish", 2, "premium", "Width minus credit"),
    s("bear_call_credit_spread", "Bear Call Credit Spread", "Credit call spread", "vertical_spread", "bearish", 2, "premium", "Width minus credit"),
  ]},
  { category: "multi_leg", category_label: "Multi-leg", strategies: [
    s("iron_condor", "Iron Condor", "OTM put spread + OTM call spread", "multi_leg", "neutral", 4, "premium", "Widest side minus credit"),
    s("long_straddle", "Long Straddle", "ATM call + ATM put", "multi_leg", "neutral", 2, "premium", "Total debit"),
    s("long_strangle", "Long Strangle", "OTM call + OTM put", "multi_leg", "neutral", 2, "premium", "Total debit"),
    s("calendar_spread", "Calendar Spread", "Near-term sell + far-term buy", "multi_leg", "neutral", 2, "premium", "Net debit"),
    s("butterfly", "Butterfly Spread", "Three-strike range-bound", "multi_leg", "neutral", 3, "premium", "Net debit"),
  ]},
  { category: "short_volatility", category_label: "Short volatility", strategies: [
    s("short_straddle", "Short Straddle", "Sell ATM call + ATM put", "short_volatility", "neutral", 2, "premium", "Unlimited"),
    s("short_strangle", "Short Strangle", "Sell OTM call + OTM put", "short_volatility", "neutral", 2, "premium", "Unlimited"),
    s("iron_butterfly", "Iron Butterfly", "Short straddle + long wings", "short_volatility", "neutral", 4, "premium", "Wing width minus credit"),
  ]},
  { category: "diagonal", category_label: "Diagonal & calendar", strategies: [
    s("poor_mans_covered_call", "Poor Man's Covered Call", "Deep ITM LEAPS call + sell short call", "diagonal", "bullish", 2, "premium", "Net debit"),
    s("diagonal_spread", "Diagonal Spread", "Different strikes + expirations", "diagonal", "bullish", 2, "premium", "Net debit"),
    s("double_diagonal", "Double Diagonal", "Call diagonal + put diagonal", "diagonal", "neutral", 4, "premium", "Net debit"),
  ]},
  { category: "ratio", category_label: "Ratio spreads", strategies: [
    s("ratio_call_backspread", "Ratio Call Backspread", "Sell 1 call, buy 2 higher", "ratio", "bullish", 3, "premium", "Width plus net debit"),
    s("ratio_put_backspread", "Ratio Put Backspread", "Sell 1 put, buy 2 lower", "ratio", "bearish", 3, "premium", "Width plus net debit"),
  ]},
  { category: "synthetic", category_label: "Synthetic & exotic", strategies: [
    s("synthetic_put", "Synthetic Put", "Short stock + long call", "synthetic", "bearish", 2, "premium", "Premium + strike gap"),
    s("reverse_conversion", "Reverse Conversion", "Short stock + long call + short put", "synthetic", "neutral", 3, "premium", "Net cost + stock-strike gap"),
    s("jade_lizard", "Jade Lizard", "Short put + short call spread", "synthetic", "neutral", 3, "premium", "Downside: put strike minus credit"),
  ]},
  { category: "custom", category_label: "Custom strategies", strategies: [
    s("custom_2_leg", "Custom 2-Leg", "User-defined 2-leg combo", "custom", "directional", 2, "premium", "Depends on config"),
    s("custom_3_leg", "Custom 3-Leg", "User-defined 3-leg combo", "custom", "directional", 3, "premium", "Depends on config"),
    s("custom_4_leg", "Custom 4-Leg", "User-defined 4-leg combo", "custom", "directional", 4, "premium", "Depends on config"),
    s("custom_5_leg", "Custom 5-Leg", "User-defined 5-leg combo", "custom", "directional", 5, "premium", "Depends on config"),
    s("custom_6_leg", "Custom 6-Leg", "User-defined 6-leg combo", "custom", "directional", 6, "premium", "Depends on config"),
    s("custom_8_leg", "Custom 8-Leg", "User-defined 8-leg combo", "custom", "directional", 8, "premium", "Depends on config"),
  ]},
];

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
  const groups = catalogGroups && catalogGroups.length > 0 ? catalogGroups : FALLBACK_GROUPS;

  return (
    <div className="space-y-2">
      <Label htmlFor="strategyType">Strategy</Label>
      <div className="relative">
        <select
          id="strategyType"
          className="flex h-10 w-full appearance-none rounded-lg border border-input bg-background px-3 py-2 pr-10 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
          value={value}
          onChange={(e) => onChange(e.target.value as StrategyType)}
        >
          {groups.map((group) => (
            <optgroup key={group.category} label={group.category_label}>
              {group.strategies.map((strategy) => (
                <option key={strategy.strategy_type} value={strategy.strategy_type}>
                  {strategy.label}
                  {strategy.min_tier !== "free" ? ` (${strategy.min_tier})` : ""}
                </option>
              ))}
            </optgroup>
          ))}
        </select>
        <ChevronDown className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
      </div>

      {/* Selected strategy detail */}
      {groups.flatMap((g) => g.strategies).map((strategy) => {
        if (strategy.strategy_type !== value) return null;
        return (
          <div
            key={strategy.strategy_type}
            className="rounded-lg border border-border/60 bg-muted/30 p-3 text-sm"
          >
            <div className="flex items-center gap-2">
              <span className={cn("font-medium", biasColor(strategy.bias))}>
                {biasLabel(strategy.bias)}
              </span>
              <span className="text-muted-foreground">·</span>
              <span className="text-muted-foreground">
                {strategy.leg_count} leg{strategy.leg_count !== 1 ? "s" : ""}
              </span>
              {tierBadge(strategy.min_tier)}
            </div>
            <p className="mt-1 text-muted-foreground">{strategy.short_description}</p>
            <p className="mt-1 text-xs text-muted-foreground">
              Max loss: {strategy.max_loss_description}
            </p>
          </div>
        );
      })}

      {error ? <p className="text-sm text-destructive">{error}</p> : null}
    </div>
  );
}
