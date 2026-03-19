import type { PlanTier } from "@backtestforecast/api-client";

export function planLabel(planTier: PlanTier): string {
  switch (planTier) {
    case "premium": return "Premium";
    case "pro": return "Pro";
    case "free": return "Free";
    default: {
      const raw = planTier as string;
      if (process.env.NODE_ENV !== "production") {
        console.warn(`Unknown plan tier: ${raw}`);
      }
      return raw.charAt(0).toUpperCase() + raw.slice(1);
    }
  }
}

export function planBadgeVariant(planTier: PlanTier): "success" | "default" | "secondary" {
  switch (planTier) {
    case "premium": return "success";
    case "pro": return "default";
    case "free": return "secondary";
    default: {
      const raw = planTier as string;
      if (process.env.NODE_ENV !== "production") {
        console.warn(`Unknown plan tier: ${raw}`);
      }
      return "secondary";
    }
  }
}
