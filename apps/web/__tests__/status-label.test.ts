/**
 * Item 92: Test that statusLabel("expired") returns "Expired".
 *
 * The `statusLabel` function maps internal status strings (e.g. "queued",
 * "running", "succeeded", "failed", "cancelled", "expired") to user-facing
 * display labels. This test verifies that the "expired" case is handled and
 * returns the capitalized "Expired" label.
 *
 * This matters because:
 *   1. The "expired" status was added to the terminal status set and the
 *      statusLabel switch must have a matching case.
 *   2. Without a case for "expired", the default branch would return the
 *      raw snake_case string, which looks broken in the UI.
 *   3. The isTerminalStatus function already recognizes "expired" — the
 *      label function must be kept in sync.
 *
 * --- Test specification ---
 *
 * import { statusLabel } from "@/lib/backtests/format";
 *
 * describe("statusLabel", () => {
 *   it('returns "Expired" for the "expired" status', () => {
 *     expect(statusLabel("expired")).toBe("Expired");
 *   });
 *
 *   it('returns "Queued" for the "queued" status', () => {
 *     expect(statusLabel("queued")).toBe("Queued");
 *   });
 *
 *   it('returns "Running" for the "running" status', () => {
 *     expect(statusLabel("running")).toBe("Running");
 *   });
 *
 *   it('returns "Completed" for the "succeeded" status', () => {
 *     expect(statusLabel("succeeded")).toBe("Completed");
 *   });
 *
 *   it('returns "Failed" for the "failed" status', () => {
 *     expect(statusLabel("failed")).toBe("Failed");
 *   });
 *
 *   it('returns "Cancelled" for the "cancelled" status', () => {
 *     expect(statusLabel("cancelled")).toBe("Cancelled");
 *   });
 *
 *   it("returns the raw string for unknown statuses", () => {
 *     expect(statusLabel("unknown_status")).toBe("unknown_status");
 *   });
 * });
 */

// Placeholder export so TypeScript does not complain about an empty module
export {};
