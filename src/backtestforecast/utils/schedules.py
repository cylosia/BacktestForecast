from __future__ import annotations


def format_utc_schedule_label(hour: int, minute: int) -> str:
    bounded_hour = hour % 24
    bounded_minute = max(0, min(minute, 59))
    meridiem = "PM" if bounded_hour >= 12 else "AM"
    hour12 = bounded_hour % 12 or 12
    return f"{hour12}:{bounded_minute:02d} {meridiem} UTC"
