import Link from "next/link";
import { ArrowRight, BarChart3, CreditCard, Download, ScanSearch, Sparkles } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

const features = [
  {
    title: "Manual backtests with plan-aware limits",
    description: "Signed-in users can create backtests, inspect detailed results, and stay inside backend-enforced monthly quotas.",
    icon: Sparkles,
  },
  {
    title: "Scanner and forecasting tiers",
    description: "Pro unlocks the basic scanner and historical-analog forecasting. Premium expands scan breadth and advanced combinations.",
    icon: ScanSearch,
  },
  {
    title: "Exports and audit-ready billing",
    description: "CSV and PDF exports, Stripe-backed subscriptions, and persistent entitlement state are wired into the same account model.",
    icon: Download,
  },
  {
    title: "Production-style workspace",
    description: "Clerk-protected routes, structured API errors, request IDs, and clean dashboard flows keep the first product slice practical.",
    icon: BarChart3,
  },
] as const;

export default function HomePage() {
  return (
    <main className="mx-auto flex min-h-screen max-w-6xl flex-col px-6 py-10 sm:py-16">
      <div className="grid gap-10 lg:grid-cols-[1.15fr_0.85fr] lg:items-center">
        <div className="space-y-6">
          <div className="space-y-3">
            <p className="text-sm font-medium uppercase tracking-[0.2em] text-muted-foreground">
              BacktestForecast.com
            </p>
            <h1 className="max-w-3xl text-4xl font-semibold tracking-tight sm:text-5xl">
              Options research with backtests, scanner workflows, forecasts, and subscription-backed feature gates.
            </h1>
            <p className="max-w-2xl text-base text-muted-foreground sm:text-lg">
              The current slice includes authenticated backtesting, scanner infrastructure, probabilistic forecast ranges, export access, and Stripe-backed plan enforcement without changing the core modular architecture.
            </p>
          </div>

          <div className="flex flex-wrap gap-3">
            <Button asChild size="lg">
              <Link href="/sign-in">
                Sign in to continue
                <ArrowRight className="h-4 w-4" />
              </Link>
            </Button>
            <Button asChild size="lg" variant="outline">
              <Link href="/pricing">
                <CreditCard className="h-4 w-4" />
                View pricing
              </Link>
            </Button>
          </div>
        </div>

        <Card className="border-border/80 shadow-sm">
          <CardHeader>
            <CardTitle>What the product supports now</CardTitle>
            <CardDescription>
              The UI and API now share plan-aware billing, export, and entitlement flows.
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-4">
            {features.map((feature) => {
              const Icon = feature.icon;
              return (
                <div
                  key={feature.title}
                  className="flex items-start gap-4 rounded-lg border border-border/70 bg-background/70 p-4"
                >
                  <div className="rounded-lg bg-primary/10 p-2 text-primary">
                    <Icon className="h-5 w-5" />
                  </div>
                  <div className="space-y-1">
                    <p className="font-medium">{feature.title}</p>
                    <p className="text-sm text-muted-foreground">{feature.description}</p>
                  </div>
                </div>
              );
            })}
          </CardContent>
        </Card>
      </div>
    </main>
  );
}
