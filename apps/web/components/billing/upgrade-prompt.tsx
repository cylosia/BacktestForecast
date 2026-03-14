import Link from "next/link";
import { ArrowRight, Sparkles } from "lucide-react";
import { Button } from "@/components/ui/button";

const PLAN_ERROR_CODES = new Set(["quota_exceeded", "feature_locked"]);

/**
 * Returns true if an API error code indicates a plan-limit issue that
 * can be resolved by upgrading. Use this to decide whether to show
 * an UpgradePrompt instead of a generic error message.
 */
export function isPlanLimitError(code: string | undefined): boolean {
  return code !== undefined && PLAN_ERROR_CODES.has(code);
}

export function UpgradePrompt({
  message,
  className,
}: {
  message: string;
  className?: string;
}) {
  return (
    <div
      className={`rounded-xl border border-primary/30 bg-primary/5 p-4 text-sm ${className ?? ""}`}
    >
      <div className="flex items-start gap-3">
        <Sparkles aria-hidden="true" className="mt-0.5 h-4 w-4 text-primary" />
        <div className="flex-1 space-y-3">
          <div>
            <p className="font-medium text-primary">Plan limit reached</p>
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
