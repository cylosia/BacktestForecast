import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

interface TimeframeValues {
  startDate: string;
  endDate: string;
  targetDte: string;
  dteToleranceDays: string;
  maxHoldingDays: string;
}

interface TimeframeErrors {
  startDate?: string;
  endDate?: string;
  targetDte?: string;
  dteToleranceDays?: string;
  maxHoldingDays?: string;
}

export function TimeframeControls({
  values,
  errors,
  onChange,
}: {
  values: TimeframeValues;
  errors: TimeframeErrors;
  onChange: (patch: Partial<TimeframeValues>) => void;
}) {
  return (
    <div className="space-y-4">
      <div className="grid gap-4 sm:grid-cols-2">
        <div className="space-y-2">
          <Label htmlFor="startDate">Start date</Label>
          <Input
            id="startDate"
            type="date"
            value={values.startDate}
            onChange={(event) => onChange({ startDate: event.target.value })}
          />
          {errors.startDate ? <p className="text-sm text-destructive">{errors.startDate}</p> : null}
        </div>

        <div className="space-y-2">
          <Label htmlFor="endDate">End date</Label>
          <Input
            id="endDate"
            type="date"
            value={values.endDate}
            onChange={(event) => onChange({ endDate: event.target.value })}
          />
          {errors.endDate ? <p className="text-sm text-destructive">{errors.endDate}</p> : null}
        </div>
      </div>

      <div className="grid gap-4 sm:grid-cols-3">
        <div className="space-y-2">
          <Label htmlFor="targetDte">Target DTE</Label>
          <Input
            id="targetDte"
            inputMode="numeric"
            value={values.targetDte}
            onChange={(event) => onChange({ targetDte: event.target.value })}
          />
          <p className="text-xs text-muted-foreground">7 to 365 calendar days.</p>
          {errors.targetDte ? <p className="text-sm text-destructive">{errors.targetDte}</p> : null}
        </div>

        <div className="space-y-2">
          <Label htmlFor="dteToleranceDays">DTE tolerance</Label>
          <Input
            id="dteToleranceDays"
            inputMode="numeric"
            value={values.dteToleranceDays}
            onChange={(event) => onChange({ dteToleranceDays: event.target.value })}
          />
          <p className="text-xs text-muted-foreground">0 to 60 days around the target expiration.</p>
          {errors.dteToleranceDays ? (
            <p className="text-sm text-destructive">{errors.dteToleranceDays}</p>
          ) : null}
        </div>

        <div className="space-y-2">
          <Label htmlFor="maxHoldingDays">Max holding days</Label>
          <Input
            id="maxHoldingDays"
            inputMode="numeric"
            value={values.maxHoldingDays}
            onChange={(event) => onChange({ maxHoldingDays: event.target.value })}
          />
          <p className="text-xs text-muted-foreground">1 to 120 calendar days.</p>
          {errors.maxHoldingDays ? (
            <p className="text-sm text-destructive">{errors.maxHoldingDays}</p>
          ) : null}
        </div>
      </div>
    </div>
  );
}
