/**
 * Item 69: usePolling maxAttempts boundary — no off-by-one.
 *
 * This test verifies that usePolling performs exactly `maxAttempts` polls
 * before timing out, not `maxAttempts - 1`.
 *
 * The hook logic:
 *   1. First poll is the initial call (attemptsRef starts at 0)
 *   2. After each non-complete poll, attemptsRef increments: next = attemptsRef + 1
 *   3. If next >= maxAttempts → status = "timeout" (no more polls)
 *   4. Otherwise → schedule next poll
 *
 * With maxAttempts = 3:
 *   Poll 1: attemptsRef=0 → fetch → not complete → next=1 → 1 < 3 → schedule
 *   Poll 2: attemptsRef=1 → fetch → not complete → next=2 → 2 < 3 → schedule
 *   Poll 3: attemptsRef=2 → fetch → not complete → next=3 → 3 >= 3 → timeout
 *   Total polls = 3 = maxAttempts ✓
 *
 * NOTE: This project does not currently have a frontend test runner configured
 * (e.g. jest or vitest). The test below is written as a specification that
 * can be executed once a test runner is set up. To run:
 *   1. Install vitest: `npm install -D vitest @testing-library/react @testing-library/react-hooks`
 *   2. Run: `npx vitest run __tests__/use-polling.test.ts`
 *
 * --- Test specification ---
 *
 * import { renderHook, act } from "@testing-library/react-hooks";
 * import { usePolling } from "@/hooks/use-polling";
 *
 * describe("usePolling maxAttempts boundary", () => {
 *   beforeEach(() => {
 *     jest.useFakeTimers();
 *   });
 *   afterEach(() => {
 *     jest.useRealTimers();
 *   });
 *
 *   it("performs exactly maxAttempts polls before timeout", async () => {
 *     const maxAttempts = 3;
 *     let fetchCount = 0;
 *
 *     const fetcher = async (_signal: AbortSignal) => {
 *       fetchCount++;
 *       return { status: "running" };
 *     };
 *
 *     const { result } = renderHook(() =>
 *       usePolling({
 *         fetcher,
 *         onComplete: () => {},
 *         isComplete: (r) => r.status === "done",
 *         interval: 1000,
 *         maxAttempts,
 *         autoStart: true,
 *       })
 *     );
 *
 *     // After all timers flush, fetch should have been called exactly maxAttempts times
 *     for (let i = 0; i < maxAttempts + 2; i++) {
 *       await act(async () => {
 *         jest.advanceTimersByTime(1100);
 *       });
 *     }
 *
 *     expect(fetchCount).toBe(maxAttempts);
 *     expect(result.current.status).toBe("timeout");
 *     expect(result.current.attempts).toBe(maxAttempts);
 *   });
 *
 *   it("completes before maxAttempts if isComplete returns true", async () => {
 *     const maxAttempts = 5;
 *     let fetchCount = 0;
 *
 *     const fetcher = async (_signal: AbortSignal) => {
 *       fetchCount++;
 *       return { status: fetchCount >= 2 ? "done" : "running" };
 *     };
 *
 *     const { result } = renderHook(() =>
 *       usePolling({
 *         fetcher,
 *         onComplete: () => {},
 *         isComplete: (r) => r.status === "done",
 *         interval: 1000,
 *         maxAttempts,
 *         autoStart: true,
 *       })
 *     );
 *
 *     for (let i = 0; i < maxAttempts + 2; i++) {
 *       await act(async () => {
 *         jest.advanceTimersByTime(1100);
 *       });
 *     }
 *
 *     expect(fetchCount).toBe(2);
 *     expect(result.current.status).toBe("done");
 *   });
 *
 *   it("does not poll maxAttempts-1 times (off-by-one guard)", async () => {
 *     // This is the critical assertion: with maxAttempts=1, the hook should
 *     // poll exactly once (not zero times).
 *     const maxAttempts = 1;
 *     let fetchCount = 0;
 *
 *     const fetcher = async (_signal: AbortSignal) => {
 *       fetchCount++;
 *       return { status: "running" };
 *     };
 *
 *     const { result } = renderHook(() =>
 *       usePolling({
 *         fetcher,
 *         onComplete: () => {},
 *         isComplete: (r) => r.status === "done",
 *         interval: 1000,
 *         maxAttempts,
 *         autoStart: true,
 *       })
 *     );
 *
 *     for (let i = 0; i < 5; i++) {
 *       await act(async () => {
 *         jest.advanceTimersByTime(1100);
 *       });
 *     }
 *
 *     expect(fetchCount).toBe(1); // Not 0!
 *     expect(result.current.status).toBe("timeout");
 *   });
 * });
 */

// Placeholder export so TypeScript does not complain about an empty module
export {};
