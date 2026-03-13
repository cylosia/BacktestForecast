import { Loader2 } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";

export default function AnalysisLoading() {
  return (
    <div className="space-y-6">
      <div>
        <div className="h-4 w-32 animate-pulse rounded bg-muted" />
        <div className="mt-3 h-8 w-56 animate-pulse rounded bg-muted" />
        <div className="mt-3 h-4 w-96 animate-pulse rounded bg-muted" />
      </div>
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Loader2 className="h-4 w-4 animate-spin" />
            Loading analysis…
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-3">
            <div className="h-10 w-full animate-pulse rounded bg-muted" />
            <div className="h-10 w-full animate-pulse rounded bg-muted" />
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
