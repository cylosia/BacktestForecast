import { describe, expect, it } from "vitest";

import { mapBacktestFieldErrors } from "@/lib/backtests/validation";

describe("mapBacktestFieldErrors", () => {
  it("maps backend snake_case field locations to backtest form keys", () => {
    expect(
      mapBacktestFieldErrors([
        { loc: ["body", "symbol"], msg: "Symbol invalid." },
        { loc: ["body", "risk_free_rate"], msg: "Rate invalid." },
      ]),
    ).toEqual({
      symbol: "Symbol invalid.",
      riskFreeRate: "Rate invalid.",
    });
  });

  it("ignores unknown field paths", () => {
    expect(
      mapBacktestFieldErrors([{ loc: ["body", "unknown_field"], msg: "Ignored." }]),
    ).toEqual({});
  });
});
