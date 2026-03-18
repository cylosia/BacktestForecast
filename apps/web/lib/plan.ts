// Canonical PlanTier is in @backtestforecast/api-client. This local definition
// exists for modules that cannot import from the package.
export type PlanTier = "free" | "pro" | "premium";

export function planLabel(planTier: PlanTier | string): string {
  if (planTier === "premium") return "Premium";
  if (planTier === "pro") return "Pro";
  return "Free";
}

export function planBadgeVariant(planTier: PlanTier | string): "success" | "default" | "secondary" {
  if (planTier === "premium") return "success";
  if (planTier === "pro") return "default";
  return "secondary";
}
