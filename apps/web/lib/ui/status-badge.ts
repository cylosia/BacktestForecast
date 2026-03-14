export function statusBadgeVariant(status: string): "default" | "success" | "destructive" | "secondary" {
  switch (status) {
    case "succeeded":
      return "success";
    case "failed":
      return "destructive";
    case "running":
      return "default";
    default:
      return "secondary";
  }
}
