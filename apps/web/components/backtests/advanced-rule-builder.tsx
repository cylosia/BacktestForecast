import type {
  ComparisonOperator,
  IndicatorCrossDirection,
  IndicatorTrendDirection,
} from "@backtestforecast/api-client";
import {
  ADVANCED_INDICATOR_OPTIONS,
  ADVANCED_RULE_OPTIONS,
  createDefaultAdvancedRule,
  createDefaultIndicatorSeries,
  type EditableAdvancedRule,
  type EditableIndicatorSeries,
} from "@/lib/backtests/advanced-rules";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select } from "@/components/ui/select";

const COMPARISON_OPTIONS: Array<{ value: ComparisonOperator; label: string }> = [
  { value: "lt", label: "Less than" },
  { value: "lte", label: "Less than or equal" },
  { value: "gt", label: "Greater than" },
  { value: "gte", label: "Greater than or equal" },
];

const TREND_OPTIONS: Array<{ value: IndicatorTrendDirection; label: string }> = [
  { value: "rising", label: "Rising" },
  { value: "falling", label: "Falling" },
];

const CROSS_OPTIONS: Array<{ value: IndicatorCrossDirection; label: string }> = [
  { value: "crosses_above", label: "Crosses above" },
  { value: "crosses_below", label: "Crosses below" },
];

function SeriesFields({
  label,
  series,
  onChange,
}: {
  label: string;
  series: EditableIndicatorSeries;
  onChange: (next: EditableIndicatorSeries) => void;
}) {
  const update = (patch: Partial<EditableIndicatorSeries>) => onChange({ ...series, ...patch });

  return (
    <div className="rounded-lg border border-border/60 p-3 space-y-3">
      <div className="space-y-2">
        <Label>{label}</Label>
        <Select
          value={series.indicator}
          options={ADVANCED_INDICATOR_OPTIONS}
          onChange={(event) => onChange(createDefaultIndicatorSeries(event.target.value as EditableIndicatorSeries["indicator"]))}
        />
      </div>

      {["rsi", "sma", "ema", "cci", "roc", "mfi", "adx", "williams_r"].includes(series.indicator) ? (
        <div className="space-y-2">
          <Label>Period</Label>
          <Input inputMode="numeric" value={series.period} onChange={(event) => update({ period: event.target.value })} />
        </div>
      ) : null}

      {["macd_line", "macd_signal", "macd_histogram"].includes(series.indicator) ? (
        <div className="grid gap-3 sm:grid-cols-3">
          <div className="space-y-2">
            <Label>Fast</Label>
            <Input inputMode="numeric" value={series.fastPeriod} onChange={(event) => update({ fastPeriod: event.target.value })} />
          </div>
          <div className="space-y-2">
            <Label>Slow</Label>
            <Input inputMode="numeric" value={series.slowPeriod} onChange={(event) => update({ slowPeriod: event.target.value })} />
          </div>
          <div className="space-y-2">
            <Label>Signal</Label>
            <Input inputMode="numeric" value={series.signalPeriod} onChange={(event) => update({ signalPeriod: event.target.value })} />
          </div>
        </div>
      ) : null}

      {series.indicator === "bollinger_band" ? (
        <div className="grid gap-3 sm:grid-cols-3">
          <div className="space-y-2">
            <Label>Band</Label>
            <Select
              value={series.band}
              options={[
                { value: "lower", label: "Lower" },
                { value: "middle", label: "Middle" },
                { value: "upper", label: "Upper" },
              ]}
              onChange={(event) => update({ band: event.target.value as EditableIndicatorSeries["band"] })}
            />
          </div>
          <div className="space-y-2">
            <Label>Period</Label>
            <Input inputMode="numeric" value={series.period} onChange={(event) => update({ period: event.target.value })} />
          </div>
          <div className="space-y-2">
            <Label>Std dev</Label>
            <Input inputMode="decimal" value={series.standardDeviations} onChange={(event) => update({ standardDeviations: event.target.value })} />
          </div>
        </div>
      ) : null}

      {["iv_rank", "iv_percentile"].includes(series.indicator) ? (
        <div className="space-y-2">
          <Label>Lookback days</Label>
          <Input inputMode="numeric" value={series.lookbackDays} onChange={(event) => update({ lookbackDays: event.target.value })} />
        </div>
      ) : null}

      {series.indicator === "volume_ratio" ? (
        <div className="space-y-2">
          <Label>Lookback period</Label>
          <Input inputMode="numeric" value={series.lookbackPeriod} onChange={(event) => update({ lookbackPeriod: event.target.value })} />
        </div>
      ) : null}

      {["stochastic_k", "stochastic_d"].includes(series.indicator) ? (
        <div className="grid gap-3 sm:grid-cols-3">
          <div className="space-y-2">
            <Label>%K period</Label>
            <Input inputMode="numeric" value={series.kPeriod} onChange={(event) => update({ kPeriod: event.target.value })} />
          </div>
          <div className="space-y-2">
            <Label>%D period</Label>
            <Input inputMode="numeric" value={series.dPeriod} onChange={(event) => update({ dPeriod: event.target.value })} />
          </div>
          <div className="space-y-2">
            <Label>Smoothing</Label>
            <Input inputMode="numeric" value={series.smoothK} onChange={(event) => update({ smoothK: event.target.value })} />
          </div>
        </div>
      ) : null}
    </div>
  );
}

export function AdvancedRuleBuilder({
  value,
  error,
  onChange,
}: {
  value: EditableAdvancedRule[];
  error?: string;
  onChange: (rules: EditableAdvancedRule[]) => void;
}) {
  const updateRule = (index: number, next: EditableAdvancedRule) => {
    onChange(value.map((rule, candidateIndex) => (candidateIndex === index ? next : rule)));
  };

  const removeRule = (index: number) => {
    onChange(value.filter((_, candidateIndex) => candidateIndex !== index));
  };

  const addRule = () => {
    onChange([...value, createDefaultAdvancedRule()]);
  };

  return (
    <div className="rounded-xl border border-border/70 p-4 space-y-4">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <p className="font-medium">Advanced generic rule builder</p>
          <p className="text-sm text-muted-foreground">
            Build StrategyQuant-style indicator rules using thresholds, trends, level crosses, series crosses, and persistence checks.
          </p>
        </div>
        <Button type="button" variant="outline" size="sm" onClick={addRule}>
          Add advanced rule
        </Button>
      </div>

      {value.length === 0 ? (
        <p className="rounded-lg border border-dashed border-border/60 p-4 text-sm text-muted-foreground">
          No advanced rules configured. Use the classic controls above or add a generic rule here.
        </p>
      ) : null}

      {value.map((rule, index) => (
        <div key={rule.id} className="rounded-xl border border-border/70 bg-muted/30 p-4 space-y-4">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div className="space-y-2">
              <Label>{`Advanced rule ${index + 1}`}</Label>
              <Select
                value={rule.type}
                options={ADVANCED_RULE_OPTIONS}
                onChange={(event) => {
                  const next = createDefaultAdvancedRule(event.target.value as EditableAdvancedRule["type"]);
                  updateRule(index, { ...next, id: rule.id });
                }}
              />
            </div>
            <Button type="button" variant="ghost" size="sm" onClick={() => removeRule(index)}>
              Remove
            </Button>
          </div>

          {rule.type === "indicator_series_cross" ? (
            <>
              <div className="space-y-2 lg:max-w-sm">
                <Label>Direction</Label>
                <Select
                  value={rule.direction as IndicatorCrossDirection}
                  options={CROSS_OPTIONS}
                  onChange={(event) => updateRule(index, { ...rule, direction: event.target.value as IndicatorCrossDirection })}
                />
              </div>
              <div className="grid gap-4 lg:grid-cols-2">
                <SeriesFields
                  label="Left series"
                  series={rule.leftSeries}
                  onChange={(next) => updateRule(index, { ...rule, leftSeries: next })}
                />
                <SeriesFields
                  label="Right series"
                  series={rule.rightSeries}
                  onChange={(next) => updateRule(index, { ...rule, rightSeries: next })}
                />
              </div>
            </>
          ) : (
            <>
              <SeriesFields
                label="Series"
                series={rule.series}
                onChange={(next) => updateRule(index, { ...rule, series: next })}
              />

              {rule.type === "indicator_trend" ? (
                <div className="grid gap-3 sm:grid-cols-2">
                  <div className="space-y-2">
                    <Label>Direction</Label>
                    <Select
                      value={rule.direction as IndicatorTrendDirection}
                      options={TREND_OPTIONS}
                      onChange={(event) => updateRule(index, { ...rule, direction: event.target.value as IndicatorTrendDirection })}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>Bars</Label>
                    <Input inputMode="numeric" value={rule.bars} onChange={(event) => updateRule(index, { ...rule, bars: event.target.value })} />
                  </div>
                </div>
              ) : null}

              {rule.type === "indicator_level_cross" ? (
                <div className="grid gap-3 sm:grid-cols-2">
                  <div className="space-y-2">
                    <Label>Direction</Label>
                    <Select
                      value={rule.direction as IndicatorCrossDirection}
                      options={CROSS_OPTIONS}
                      onChange={(event) => updateRule(index, { ...rule, direction: event.target.value as IndicatorCrossDirection })}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>Level</Label>
                    <Input inputMode="decimal" value={rule.level} onChange={(event) => updateRule(index, { ...rule, level: event.target.value })} />
                  </div>
                </div>
              ) : null}

              {rule.type === "indicator_threshold" ? (
                <div className="grid gap-3 sm:grid-cols-2">
                  <div className="space-y-2">
                    <Label>Operator</Label>
                    <Select
                      value={rule.operator}
                      options={COMPARISON_OPTIONS}
                      onChange={(event) => updateRule(index, { ...rule, operator: event.target.value as ComparisonOperator })}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>Level</Label>
                    <Input inputMode="decimal" value={rule.level} onChange={(event) => updateRule(index, { ...rule, level: event.target.value })} />
                  </div>
                </div>
              ) : null}

              {rule.type === "indicator_persistence" ? (
                <div className="grid gap-3 sm:grid-cols-3">
                  <div className="space-y-2">
                    <Label>Operator</Label>
                    <Select
                      value={rule.operator}
                      options={COMPARISON_OPTIONS}
                      onChange={(event) => updateRule(index, { ...rule, operator: event.target.value as ComparisonOperator })}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>Level</Label>
                    <Input inputMode="decimal" value={rule.level} onChange={(event) => updateRule(index, { ...rule, level: event.target.value })} />
                  </div>
                  <div className="space-y-2">
                    <Label>Bars</Label>
                    <Input inputMode="numeric" value={rule.bars} onChange={(event) => updateRule(index, { ...rule, bars: event.target.value })} />
                  </div>
                </div>
              ) : null}
            </>
          )}
        </div>
      ))}

      {error ? <p className="text-sm text-destructive">{error}</p> : null}
    </div>
  );
}
