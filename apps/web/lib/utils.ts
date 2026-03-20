import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

const EASTERN_TIME_ZONE = "America/New_York";
const EASTERN_DATE_FORMATTER = new Intl.DateTimeFormat("en-CA", {
  timeZone: EASTERN_TIME_ZONE,
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

function formatDateParts(parts: Intl.DateTimeFormatPart[]): string {
  const lookup = new Map(parts.map((part) => [part.type, part.value]));
  const year = lookup.get("year");
  const month = lookup.get("month");
  const day = lookup.get("day");

  if (!year || !month || !day) {
    throw new Error("Unable to format US Eastern date.");
  }

  return `${year}-${month}-${day}`;
}

/** Return the current market date in US Eastern time as YYYY-MM-DD. */
export function currentEasternDate(now: Date = new Date()): string {
  return formatDateParts(EASTERN_DATE_FORMATTER.formatToParts(now));
}

/** Return today's date in US Eastern time as YYYY-MM-DD. */
export function todayET(now: Date = new Date()): string {
  return currentEasternDate(now);
}

export function daysAgoET(n: number, now: Date = new Date()): string {
  const easternToday = currentEasternDate(now);
  const d = new Date(`${easternToday}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() - n);
  return d.toISOString().slice(0, 10);
}
