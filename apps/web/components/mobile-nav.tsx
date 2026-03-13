"use client";

import { useEffect, useState } from "react";
import { usePathname } from "next/navigation";
import { Menu, X } from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { AppNavLink } from "@/components/app-nav-link";
import { Button } from "@/components/ui/button";

export interface NavItem {
  href: string;
  icon: LucideIcon;
  label: string;
}

export function MobileNav({ items }: { items: NavItem[] }) {
  const [open, setOpen] = useState(false);
  const pathname = usePathname();

  useEffect(() => {
    setOpen(false);
  }, [pathname]);

  useEffect(() => {
    if (open) {
      document.body.style.overflow = "hidden";
      return () => {
        document.body.style.overflow = "";
      };
    }
  }, [open]);

  return (
    <div className="md:hidden">
      <Button
        variant="ghost"
        size="icon"
        aria-label="Open navigation menu"
        onClick={() => setOpen(true)}
      >
        <Menu className="h-5 w-5" />
      </Button>

      {open && (
        <>
          <div
            className="fixed inset-0 z-50 bg-black/40 backdrop-blur-sm"
            onClick={() => setOpen(false)}
            aria-hidden
          />
          <nav
            className="fixed inset-y-0 left-0 z-50 flex w-72 flex-col gap-1 overflow-y-auto bg-background p-4 shadow-lg animate-in slide-in-from-left duration-200"
            aria-label="Mobile navigation"
          >
            <div className="mb-4 flex items-center justify-between">
              <span className="text-sm font-semibold tracking-tight">Navigation</span>
              <Button
                variant="ghost"
                size="icon"
                aria-label="Close navigation menu"
                onClick={() => setOpen(false)}
              >
                <X className="h-5 w-5" />
              </Button>
            </div>
            {items.map((item) => (
              <AppNavLink key={item.href} href={item.href} icon={item.icon}>
                {item.label}
              </AppNavLink>
            ))}
          </nav>
        </>
      )}
    </div>
  );
}
