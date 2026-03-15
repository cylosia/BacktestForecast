import { Card, CardContent, CardHeader } from "@/components/ui/card";

export default function NewScanLoading() {
  return (
    <div className="space-y-6 animate-pulse">
      <div>
        <div className="h-4 w-24 rounded bg-muted" />
        <div className="mt-2 h-8 w-48 rounded bg-muted" />
      </div>
      <Card>
        <CardHeader>
          <div className="h-5 w-40 rounded bg-muted" />
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="h-10 w-full rounded bg-muted" />
          <div className="h-10 w-full rounded bg-muted" />
          <div className="h-10 w-full rounded bg-muted" />
        </CardContent>
      </Card>
    </div>
  );
}
