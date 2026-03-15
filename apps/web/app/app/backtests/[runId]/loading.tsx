import { Card, CardContent, CardHeader } from "@/components/ui/card";

export default function BacktestDetailLoading() {
  return (
    <div className="space-y-6 animate-pulse">
      <div>
        <div className="h-4 w-32 rounded bg-muted" />
        <div className="mt-2 h-8 w-64 rounded bg-muted" />
      </div>
      <Card>
        <CardHeader>
          <div className="h-5 w-48 rounded bg-muted" />
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="h-4 w-full rounded bg-muted" />
          <div className="h-4 w-3/4 rounded bg-muted" />
          <div className="h-4 w-1/2 rounded bg-muted" />
        </CardContent>
      </Card>
      <Card>
        <CardHeader>
          <div className="h-5 w-36 rounded bg-muted" />
        </CardHeader>
        <CardContent>
          <div className="h-64 w-full rounded bg-muted" />
        </CardContent>
      </Card>
    </div>
  );
}
