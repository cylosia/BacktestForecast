import { Skeleton } from "@/components/ui/skeleton";

export default function ForecastsLoading() {
  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <Skeleton className="h-10 w-64" />
        <Skeleton className="h-5 w-96" />
      </div>
      <Skeleton className="h-52 rounded-xl" />
    </div>
  );
}
