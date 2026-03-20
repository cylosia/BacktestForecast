import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function daysAgo(n: number): string {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - n);
  return d.toISOString().slice(0, 10);
}

/** Return today's date in US Eastern time as YYYY-MM-DD. */
export function todayET(): string {
  const etOffsetMs = 5 * 60 * 60 * 1000;
  const nowEt = new Date(Date.now() - etOffsetMs);
  return nowEt.toISOString().slice(0, 10);
}

export function daysAgoET(n: number): string {
  const etOffsetMs = 5 * 60 * 60 * 1000;
  const d = new Date(Date.now() - etOffsetMs);
  d.setUTCDate(d.getUTCDate() - n);
  return d.toISOString().slice(0, 10);
}
