export function ScoreBar({ score, max }: { score: number | null | undefined; max: number | null | undefined }) {
  const safeScore = typeof score === "number" && Number.isFinite(score) ? score : 0;
  const safeMax = typeof max === "number" && Number.isFinite(max) && max > 0 ? max : 0;
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
