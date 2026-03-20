import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

const NEW_YORK_TIME_ZONE = "America/New_York";
const NEW_YORK_DATE_FORMATTER = new Intl.DateTimeFormat("en-CA", {
  timeZone: NEW_YORK_TIME_ZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function daysAgo(n: number): string {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - n);
  return d.toISOString().slice(0, 10);
}

function formatDateInNewYork(date: Date): string {
  return NEW_YORK_DATE_FORMATTER.format(date);
}

function dateFromIsoString(isoDate: string): Date {
  return new Date(`${isoDate}T12:00:00Z`);
}

function isWeekendInNewYork(isoDate: string): boolean {
  const weekday = new Intl.DateTimeFormat("en-US", {
    timeZone: NEW_YORK_TIME_ZONE,
    weekday: "short",
  }).format(dateFromIsoString(isoDate));

  return weekday === "Sat" || weekday === "Sun";
}

export function marketDateTodayET(now: Date = new Date()): string {
  const date = dateFromIsoString(formatDateInNewYork(now));
  while (isWeekendInNewYork(formatDateInNewYork(date))) {
    date.setUTCDate(date.getUTCDate() - 1);
  }
  return formatDateInNewYork(date);
}

/** Return today's date in America/New_York as YYYY-MM-DD. */
export function todayET(now: Date = new Date()): string {
  return formatDateInNewYork(now);
}

export function daysAgoET(n: number, now: Date = new Date()): string {
  const date = dateFromIsoString(marketDateTodayET(now));
  date.setUTCDate(date.getUTCDate() - n);
  return formatDateInNewYork(date);
}
