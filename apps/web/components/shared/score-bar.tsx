export function ScoreBar({ score, max }: { score: number; max: number }) {
  const pct = max > 0 ? Math.max(0, Math.min((score / max) * 100, 100)) : 0;
  return (
    <div className="h-2 w-full overflow-hidden rounded-full bg-muted">
      <div
        className="h-full rounded-full bg-primary transition-all"
        style={{ width: `${pct}%` }}
      />
    </div>
  );
}
