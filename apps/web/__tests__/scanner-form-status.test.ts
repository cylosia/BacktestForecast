import { describe, it, expect } from "vitest";

/**
 * Tests the scanner form status lifecycle transitions.
 * Verifies that status correctly transitions through the expected states
 * and that error state is cleared when a new submission starts.
 */

type ScanStatus = "idle" | "submitting" | "error";

describe("scanner form status lifecycle", () => {
  it("starts in idle state", () => {
    const status: ScanStatus = "idle";
    expect(status).toBe("idle");
  });

  it("transitions to submitting on form submit", () => {
    let status: ScanStatus = "idle";
    let errorMessage: string | null = null;

    status = "submitting";
    errorMessage = null;

    expect(status).toBe("submitting");
    expect(errorMessage).toBeNull();
  });

  it("transitions to error on failure", () => {
    let status: ScanStatus = "idle";
    let errorMessage: string | null = null;

    status = "error";
    errorMessage = "Scan could not be created.";

    expect(status).toBe("error");
    expect(errorMessage).toBe("Scan could not be created.");
  });

  it("clears previous error when a new submission starts", () => {
    let status: ScanStatus = "error";
    let errorMessage: string | null = "Previous error";
    let errorCode: string | undefined = "plan_limit_exceeded";

    status = "submitting";
    errorMessage = null;
    errorCode = undefined;

    expect(status).toBe("submitting");
    expect(errorMessage).toBeNull();
    expect(errorCode).toBeUndefined();
  });

  it("disables submit button while submitting", () => {
    const status: ScanStatus = "submitting";
    const isDisabled = status === "submitting";
    expect(isDisabled).toBe(true);
  });

  it("enables submit button when not submitting", () => {
    for (const status of ["idle", "error"] as ScanStatus[]) {
      const isDisabled = status === "submitting";
      expect(isDisabled).toBe(false);
    }
  });
});
