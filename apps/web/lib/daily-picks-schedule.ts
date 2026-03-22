const DEFAULT_HOUR_UTC = 6;
const DEFAULT_MINUTE_UTC = 0;

function toBoundedInt(raw: string | undefined, fallback: number, min: number, max: number): number {
  const parsed = Number.parseInt(raw ?? "", 10);
  if (!Number.isFinite(parsed) || parsed < min || parsed > max) return fallback;
  return parsed;
}

export function getDailyPicksScheduleUtc(): { hour: number; minute: number } {
  return {
    hour: toBoundedInt(process.env.DAILY_PICKS_PIPELINE_HOUR_UTC, DEFAULT_HOUR_UTC, 0, 23),
    minute: toBoundedInt(process.env.DAILY_PICKS_PIPELINE_MINUTE_UTC, DEFAULT_MINUTE_UTC, 0, 59),
  };
}

export function formatUtcScheduleLabel(hour: number, minute: number): string {
  const meridiem = hour >= 12 ? "PM" : "AM";
  const hour12 = hour % 12 || 12;
  const minuteText = `${minute}`.padStart(2, "0");
  return `${hour12}:${minuteText} ${meridiem} UTC`;
}

export function getDailyPicksScheduleLabel(): string {
  const { hour, minute } = getDailyPicksScheduleUtc();
  return formatUtcScheduleLabel(hour, minute);
}
