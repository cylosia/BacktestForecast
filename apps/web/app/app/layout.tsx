import Link from "next/link";
import { UserButton } from "@clerk/nextjs";
import { Activity, BarChart3, Bookmark, CreditCard, History, Microscope, PlusCircle, ScanSearch, TrendingUp, Zap } from "lucide-react";
import { AppNavLink } from "@/components/app-nav-link";
import { Badge } from "@/components/ui/badge";
import { getCurrentUser } from "@/lib/api/server";

function planBadgeVariant(planTier: string) {
  if (planTier === "premium") {
    return "success" as const;
  }
  if (planTier === "pro") {
    return "default" as const;
  }
  return "secondary" as const;
}

function planLabel(planTier: string) {
  if (planTier === "premium") {
    return "Premium";
  }
  if (planTier === "pro") {
    return "Pro";
  }
  return "Free";
}

export default async function AppLayout({ children }: { children: React.ReactNode }) {
  let planTier = "free";

  try {
    const user = await getCurrentUser();
    planTier = user.plan_tier;
  } catch {
    planTier = "free";
  }

  return (
    <div className="min-h-screen bg-background">
      <header className="sticky top-0 z-40 border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/85">
        <div className="mx-auto flex max-w-7xl items-center justify-between gap-6 px-4 py-4 sm:px-6 lg:px-8">
          <div className="flex items-center gap-3">
            <div className="rounded-xl bg-primary/10 p-2 text-primary">
              <Activity className="h-5 w-5" />
            </div>
            <div>
              <Link className="font-semibold tracking-tight" href="/app/dashboard">
                BacktestForecast
              </Link>
              <div className="mt-1 flex items-center gap-2 text-xs text-muted-foreground">
                <span>Research workspace</span>
                <Badge variant={planBadgeVariant(planTier)}>{planLabel(planTier)}</Badge>
              </div>
            </div>
          </div>

          <div className="hidden items-center gap-2 md:flex">
            <AppNavLink href="/app/dashboard" icon={BarChart3}>
              Dashboard
            </AppNavLink>
            <AppNavLink href="/app/backtests" icon={History}>
              History
            </AppNavLink>
            <AppNavLink href="/app/backtests/new" icon={PlusCircle}>
              New backtest
            </AppNavLink>
            <AppNavLink href="/app/templates" icon={Bookmark}>
              Templates
            </AppNavLink>
            <AppNavLink href="/app/daily-picks" icon={Zap}>
              Daily Picks
            </AppNavLink>
            <AppNavLink href="/app/analysis" icon={Microscope}>
              Deep Analysis
            </AppNavLink>
            <AppNavLink href="/app/scanner" icon={ScanSearch}>
              Scanner
            </AppNavLink>
            <AppNavLink href="/app/forecasts" icon={TrendingUp}>
              Forecasts
            </AppNavLink>
            <AppNavLink href="/app/settings/billing" icon={CreditCard}>
              Billing
            </AppNavLink>
          </div>

          <UserButton showName />
        </div>
      </header>

      <main className="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">{children}</main>
    </div>
  );
}
