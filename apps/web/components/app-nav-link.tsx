"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";

export function AppNavLink({
  href,
  icon,
  children,
}: {
  href: string;
  icon: React.ReactNode;
  children: React.ReactNode;
}) {
  const pathname = usePathname();
  const isExact = pathname === href;
  const isChildRoute = !isExact && href !== "/app" && pathname.startsWith(href + "/");
  const childSegment = isChildRoute ? pathname.slice(href.length + 1).split("/")[0] : "";
  // Child segments that should NOT activate the parent nav link.
  // Update this list when adding new child routes to nav sections.
  const isActive = isExact || (isChildRoute && childSegment !== "new" && childSegment !== "compare");

  return (
    <Link
      href={href}
      aria-current={isActive ? "page" : undefined}
      className={cn(
        "inline-flex items-center gap-2 rounded-lg px-3 py-2 text-sm font-medium transition-colors",
        isActive
          ? "bg-primary text-primary-foreground"
          : "text-muted-foreground hover:bg-accent hover:text-accent-foreground",
      )}
    >
      {icon}
      <span>{children}</span>
    </Link>
  );
}
