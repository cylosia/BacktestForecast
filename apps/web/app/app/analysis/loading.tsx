import { Skeleton } from "@/components/ui/skeleton";

export default function AnalysisLoading() {
  return (
    <div className="space-y-6">
      <div className="space-y-2">
        <Skeleton className="h-4 w-28" />
        <Skeleton className="h-10 w-56" />
        <Skeleton className="h-5 w-96" />
      </div>
      <Skeleton className="h-28 rounded-xl" />
    </div>
  );
}
