import Link from "next/link";
import { UserButton } from "@clerk/nextjs";
import { Activity, BarChart3, Bookmark, CreditCard, History, Microscope, PlusCircle, ScanSearch, TrendingUp, Zap } from "lucide-react";
import { AppNavLink } from "@/components/app-nav-link";
import { MobileNav, type NavItem } from "@/components/mobile-nav";
import { Badge } from "@/components/ui/badge";
import { getCurrentUser } from "@/lib/api/server";
import { planBadgeVariant, planLabel } from "@/lib/plan";

const NAV_ITEMS: NavItem[] = [
  { href: "/app/dashboard", icon: BarChart3, label: "Dashboard" },
  { href: "/app/backtests", icon: History, label: "History" },
  { href: "/app/backtests/new", icon: PlusCircle, label: "New backtest" },
  { href: "/app/templates", icon: Bookmark, label: "Templates" },
  { href: "/app/daily-picks", icon: Zap, label: "Daily Picks" },
  { href: "/app/analysis", icon: Microscope, label: "Deep Analysis" },
  { href: "/app/scanner", icon: ScanSearch, label: "Scanner" },
  { href: "/app/forecasts", icon: TrendingUp, label: "Forecasts" },
  { href: "/app/settings/billing", icon: CreditCard, label: "Billing" },
];

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
            {NAV_ITEMS.map((item) => (
              <AppNavLink key={item.href} href={item.href} icon={item.icon}>
                {item.label}
              </AppNavLink>
            ))}
          </div>

          <div className="flex items-center gap-2">
            <UserButton showName />
            <MobileNav items={NAV_ITEMS} />
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">{children}</main>
    </div>
  );
}
