export function ScoreBar({ score, max }: { score: number | null | undefined; max: number | null | undefined }) {
  const safeScore = Number.isFinite(score as number) ? (score as number) : 0;
  const safeMax = Number.isFinite(max as number) && (max as number) > 0 ? (max as number) : 0;
  const pct = safeMax > 0 ? Math.max(0, Math.min((safeScore / safeMax) * 100, 100)) : 0;
  return (
    <div
      className="h-2 w-full overflow-hidden rounded-full bg-muted"
      role="progressbar"
      aria-label={`Score ${safeScore} of ${safeMax}`}
      aria-valuenow={safeScore}
      aria-valuemin={0}
      aria-valuemax={safeMax}
    >
      <div
        className="h-full rounded-full bg-primary transition-all"
        style={{ width: `${pct}%` }}
      />
    </div>
  );
}
