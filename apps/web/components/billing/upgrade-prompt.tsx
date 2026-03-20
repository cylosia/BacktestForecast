import Link from "next/link";
import { ArrowRight, Sparkles } from "lucide-react";
import { Button } from "@/components/ui/button";

export { isPlanLimitError } from "@/lib/billing/errors";

const TIER_LABELS: Record<string, string> = {
  pro: "Pro",
  premium: "Premium",
};

export function UpgradePrompt({
  message,
  requiredTier,
  className,
}: {
  message: string;
  requiredTier?: string;
  className?: string;
}) {
  const tierLabel = requiredTier ? TIER_LABELS[requiredTier] : undefined;
  const heading = tierLabel
    ? `Upgrade to ${tierLabel} to unlock this feature`
    : "Plan limit reached";

  return (
    <div
      className={`rounded-xl border border-primary/30 bg-primary/5 p-4 text-sm ${className ?? ""}`}
    >
      <div className="flex items-start gap-3">
        <Sparkles aria-hidden="true" className="mt-0.5 h-4 w-4 text-primary" />
        <div className="flex-1 space-y-3">
          <div>
            <p className="font-medium text-primary">{heading}</p>
            <p className="mt-1 text-foreground/80">{message}</p>
          </div>
          <div className="flex flex-wrap gap-2">
            <Button asChild size="sm">
              <Link href="/pricing">
                View plans
                <ArrowRight className="h-3.5 w-3.5" />
              </Link>
            </Button>
            <Button asChild size="sm" variant="outline">
              <Link href="/app/settings/billing">Billing settings</Link>
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
