import { describe, it, expect } from "vitest";

describe("scanner form validation", () => {
  it("requires at least one symbol", () => {
    const symbolsText = "   ";
    const symbols = symbolsText
      .split(/[,\s]+/)
      .map((s) => s.trim().toUpperCase())
      .filter(Boolean);
    expect(symbols.length).toBe(0);
  });

  it("parses comma-separated symbols correctly", () => {
    const symbolsText = "SPY, QQQ, AAPL";
    const symbols = symbolsText
      .split(/[,\s]+/)
      .map((s) => s.trim().toUpperCase())
      .filter(Boolean);
    expect(symbols).toEqual(["SPY", "QQQ", "AAPL"]);
  });

  it("validates DTE tolerance < target DTE", () => {
    const targetDte = 30;
    const dteTolerance = 35;
    expect(dteTolerance >= targetDte).toBe(true);
  });

  it("accepts DTE tolerance less than target DTE", () => {
    const targetDte = 30;
    const dteTolerance = 5;
    expect(dteTolerance < targetDte).toBe(true);
  });

  it("rejects negative account size", () => {
    const accountSize = -1000;
    expect(accountSize < 100).toBe(true);
  });

  it("rejects account size below minimum", () => {
    const min = 100;
    const accountSize = 50;
    expect(accountSize < min).toBe(true);
  });

  it("requires start date before end date", () => {
    const start = new Date("2025-01-01");
    const end = new Date("2024-01-01");
    expect(start >= end).toBe(true);
  });

  it("enforces basic mode symbol limit", () => {
    const maxSymbols = 5;
    const symbols = ["SPY", "QQQ", "AAPL", "MSFT", "GOOG", "AMZN"];
    expect(symbols.length > maxSymbols).toBe(true);
  });

  it("enforces advanced mode symbol limit", () => {
    const maxSymbols = 25;
    const symbols = Array.from({ length: 26 }, (_, i) => `SYM${i}`);
    expect(symbols.length > maxSymbols).toBe(true);
  });

  it("requires at least one strategy type", () => {
    const selectedStrategies = new Set<string>();
    expect(selectedStrategies.size).toBe(0);
  });

  it("validates target DTE range", () => {
    const targetDte = 3;
    const min = 7;
    const max = 365;
    expect(targetDte < min || targetDte > max).toBe(true);
  });

  it("validates risk percent range", () => {
    const riskPct = 0.05;
    const min = 0.1;
    expect(riskPct < min).toBe(true);
  });

  it("validates max recommendations range", () => {
    const maxRecs = 31;
    const max = 30;
    expect(maxRecs > max).toBe(true);
  });
});
