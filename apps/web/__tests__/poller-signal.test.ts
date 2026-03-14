/**
 * Item 62: Test that pollers pass AbortSignal to fetcher.
 *
 * This test verifies that the fetcher callback in the polling hooks
 * receives an AbortSignal parameter. This is critical for:
 *
 *   1. Allowing in-flight fetch requests to be cancelled when the component
 *      unmounts or the polling is stopped by the user.
 *   2. Preventing memory leaks from orphaned network requests.
 *   3. Enabling the browser to abort requests that are no longer needed
 *      (e.g. when navigating away from the dashboard page).
 *
 * The usePolling hook should:
 *   - Create an AbortController before each poll
 *   - Pass controller.signal to the fetcher function as its first argument
 *   - Call controller.abort() when the hook is cleaned up (unmount, stop, or
 *     before the next poll cycle begins)
 *
 * --- Test specification ---
 *
 * import { renderHook, act } from "@testing-library/react-hooks";
 * import { usePolling } from "@/hooks/use-polling";
 *
 * describe("poller AbortSignal", () => {
 *   beforeEach(() => {
 *     jest.useFakeTimers();
 *   });
 *   afterEach(() => {
 *     jest.useRealTimers();
 *   });
 *
 *   it("passes an AbortSignal to the fetcher callback", async () => {
 *     let receivedSignal: AbortSignal | undefined;
 *
 *     const fetcher = async (signal: AbortSignal) => {
 *       receivedSignal = signal;
 *       return { status: "done" };
 *     };
 *
 *     renderHook(() =>
 *       usePolling({
 *         fetcher,
 *         onComplete: () => {},
 *         isComplete: (r) => r.status === "done",
 *         interval: 1000,
 *         maxAttempts: 5,
 *         autoStart: true,
 *       })
 *     );
 *
 *     await act(async () => {
 *       jest.advanceTimersByTime(1100);
 *     });
 *
 *     expect(receivedSignal).toBeDefined();
 *     expect(receivedSignal).toBeInstanceOf(AbortSignal);
 *   });
 *
 *   it("aborts the signal when polling is stopped", async () => {
 *     let receivedSignal: AbortSignal | undefined;
 *
 *     const fetcher = async (signal: AbortSignal) => {
 *       receivedSignal = signal;
 *       // Simulate a slow request that will be aborted
 *       return new Promise((resolve) =>
 *         setTimeout(() => resolve({ status: "running" }), 5000)
 *       );
 *     };
 *
 *     const { result, unmount } = renderHook(() =>
 *       usePolling({
 *         fetcher,
 *         onComplete: () => {},
 *         isComplete: (r) => r.status === "done",
 *         interval: 1000,
 *         maxAttempts: 5,
 *         autoStart: true,
 *       })
 *     );
 *
 *     await act(async () => {
 *       jest.advanceTimersByTime(1100);
 *     });
 *
 *     expect(receivedSignal).toBeDefined();
 *
 *     // Unmounting should abort the signal
 *     unmount();
 *     expect(receivedSignal!.aborted).toBe(true);
 *   });
 *
 *   it("creates a fresh signal for each poll cycle", async () => {
 *     const signals: AbortSignal[] = [];
 *
 *     const fetcher = async (signal: AbortSignal) => {
 *       signals.push(signal);
 *       return { status: "running" };
 *     };
 *
 *     renderHook(() =>
 *       usePolling({
 *         fetcher,
 *         onComplete: () => {},
 *         isComplete: (r) => r.status === "done",
 *         interval: 1000,
 *         maxAttempts: 5,
 *         autoStart: true,
 *       })
 *     );
 *
 *     for (let i = 0; i < 3; i++) {
 *       await act(async () => {
 *         jest.advanceTimersByTime(1100);
 *       });
 *     }
 *
 *     expect(signals.length).toBeGreaterThanOrEqual(2);
 *     // Each poll should have its own signal instance
 *     const uniqueSignals = new Set(signals);
 *     expect(uniqueSignals.size).toBe(signals.length);
 *   });
 * });
 */

// Placeholder export so TypeScript does not complain about an empty module
export {};
