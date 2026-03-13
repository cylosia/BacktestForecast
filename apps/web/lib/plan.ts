export function planLabel(planTier: string): string {
  if (planTier === "premium") return "Premium";
  if (planTier === "pro") return "Pro";
  return "Free";
}

export function planBadgeVariant(planTier: string): "success" | "default" | "secondary" {
  if (planTier === "premium") return "success";
  if (planTier === "pro") return "default";
  return "secondary";
}
