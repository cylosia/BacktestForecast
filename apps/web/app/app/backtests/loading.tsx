import { Skeleton } from "@/components/ui/skeleton";

export default function BacktestsLoading() {
  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <Skeleton className="h-10 w-48" />
        <Skeleton className="h-5 w-80" />
      </div>
      <Skeleton className="h-96 rounded-xl" />
    </div>
  );
}
