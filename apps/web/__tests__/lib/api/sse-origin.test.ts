import { describe, expect, it } from "vitest";

import {
  getAllowedSseProxyOrigin,
  isAllowedSseProxyOrigin,
  resolveSseProxyRequestOrigin,
} from "@/lib/api/sse-origin";

describe("sse origin policy", () => {
  it("normalizes the configured app origin", () => {
    expect(getAllowedSseProxyOrigin("http://localhost:3000/")).toBe("http://localhost:3000");
    expect(getAllowedSseProxyOrigin("https://app.example.com/path")).toBe("https://app.example.com");
  });

  it("prefers Origin and falls back to Referer", () => {
    expect(
      resolveSseProxyRequestOrigin(
        new Headers({
          origin: "https://app.example.com",
          referer: "https://ignored.example.com/events",
        }),
      ),
    ).toBe("https://app.example.com");

    expect(
      resolveSseProxyRequestOrigin(
        new Headers({
          referer: "https://app.example.com/events/backtests/123",
        }),
      ),
    ).toBe("https://app.example.com");
  });

  it("rejects requests missing both Origin and Referer", () => {
    expect(isAllowedSseProxyOrigin(new Headers(), "https://app.example.com")).toBe(false);
  });

  it("rejects malformed or cross-origin evidence", () => {
    expect(
      isAllowedSseProxyOrigin(
        new Headers({ origin: "not a url" }),
        "https://app.example.com",
      ),
    ).toBe(false);

    expect(
      isAllowedSseProxyOrigin(
        new Headers({ referer: "https://evil.example.com/events" }),
        "https://app.example.com",
      ),
    ).toBe(false);
  });

  it("accepts same-origin evidence", () => {
    expect(
      isAllowedSseProxyOrigin(
        new Headers({ origin: "https://app.example.com" }),
        "https://app.example.com",
      ),
    ).toBe(true);
  });
});
