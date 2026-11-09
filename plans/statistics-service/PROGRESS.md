# Statistics Service Progress

## ST-4: cumulative planner estimate allowance — implemented

- Behavior: each planner metadata stream may request at most 16 storage estimates. The counter survives detach/reattach of planner metadata session state. Once exhausted, a valid estimate request returns `Unknown` without aborting the invocation, allowing fixed optimizer heuristics. The separate per-estimate scan bound remains 16,384 candidate rows; cancellation and deadline errors remain terminal.
- Changed code and contract: [planner metadata session](../../engine/src/transaction.rs), [statistics service](../../specs/statistics-service.md), and [planner budget](../../specs/planner-compiler.md).
- Verification: budget exhaustion unit test confirms the allowance rejects the next request without advancing the counter; locked workspace build, format check, warning-denied Clippy, and full workspace tests passed. Commit integration reported 128 passed and 3 cgroup-gated ignored tests.
- Remaining: exercise allowance exhaustion through a real planner stream; overall planner search budgets, asynchronous task draining, cache validity, and bounded maintenance collection remain open.
