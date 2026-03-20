import { describe, expect, it } from "vitest";
import { daysAgoET, marketDateTodayET, todayET } from "@/lib/utils";

describe("America/New_York date utilities", () => {
  it("uses DST-aware New York conversion for a summer timestamp", () => {
    const now = new Date("2025-07-14T03:30:00Z");

    expect(todayET(now)).toBe("2025-07-13");
    expect(marketDateTodayET(now)).toBe("2025-07-11");
    expect(daysAgoET(3, now)).toBe("2025-07-08");
  });

  it("uses standard-time New York conversion for a winter timestamp", () => {
    const now = new Date("2025-01-13T04:30:00Z");

    expect(todayET(now)).toBe("2025-01-12");
    expect(marketDateTodayET(now)).toBe("2025-01-10");
    expect(daysAgoET(3, now)).toBe("2025-01-07");
  });
});
