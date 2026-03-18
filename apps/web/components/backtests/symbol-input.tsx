import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export function SymbolInput({
  value,
  error,
  onChange,
}: {
  value: string;
  error?: string;
  onChange: (value: string) => void;
}) {
  return (
    <div className="space-y-2">
      <Label htmlFor="symbol">Symbol</Label>
      <Input
        id="symbol"
        autoComplete="off"
        maxLength={16}
        placeholder="SPY"
        value={value}
        aria-invalid={!!error}
        aria-describedby={error ? "symbol-error" : undefined}
        onChange={(event) => onChange(event.target.value.toUpperCase().replace(/[^A-Z0-9./^-]/g, ""))}
      />
      <p className="text-xs text-muted-foreground">Use an underlying ticker like SPY, QQQ, or AAPL.</p>
      {error ? <p id="symbol-error" className="text-sm text-destructive">{error}</p> : null}
    </div>
  );
}
