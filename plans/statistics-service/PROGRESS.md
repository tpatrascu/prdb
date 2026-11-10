# Statistics Service Progress

## ST-2 / WI-7: repeatable authorized-estimate cost harness — measured

- Artifact: [`statistics_estimate.rs`](../../engine/examples/statistics_estimate.rs) seeds a temporary READY redb database with a deterministic 8,192-row INTEGER relation, constructs an explicitly authorized projection, runs 11 estimates over fresh invocation snapshots, verifies each exact summary, and reports median/p95 call latency. Bootstrap and seed writes are outside the timed interval.
- Measurement: on Rust 1.95.0, redb 4.4.0, x86_64 Linux, release build: median 8.655 ms and p95 12.073 ms. The run measured the storage estimate API only; planner IPC, SQL planning, compilation, and query execution were excluded. This is a single host-local sample, not a performance target.
- Verification: `cargo run --release -p prdb-engine --example statistics_estimate --locked --offline` completed and verified the row/cell counts. Locked workspace build, format check, warning-denied Clippy, serial workspace tests (129 passed, 3 cgroup-dependent ignored), and maintained TinyCC SDK suite passed.
- Remaining: broader estimate-cost variation, plan-quality evidence, mixed-workload benchmarks, cache validity, and maintenance collection remain open.

## ST-4: candidate-budget boundary publishes no partial estimate — verified

- Contract: authorized scan estimates may return an exact summary below the 16,384-candidate budget, but must return `Unknown` once the budget is reached, without exposing partial counts.
- Regression: a real-redb fixture commits 16,383 readable rows and verifies their exact VALUE population; it then commits the 16,384th row and verifies a fresh snapshot receives `Unknown`.
- Verification: the focused boundary regression passed. Locked workspace build, formatting, warning-denied Clippy, serial workspace tests (129 passed, 3 cgroup-dependent ignored), and maintained TinyCC SDK suite passed in the configured Nix shell.
- Remaining: asynchronous planner-task draining, cache validity, bounded maintenance collection, and mixed-workload evidence remain open.

## ST-4: cumulative planner estimate allowance — implemented

- Behavior: each planner metadata stream may request at most 16 storage estimates. The counter survives detach/reattach of planner metadata session state. Once exhausted, a valid estimate request returns `Unknown` without aborting the invocation, allowing fixed optimizer heuristics. The separate per-estimate scan bound remains 16,384 candidate rows; cancellation and deadline errors remain terminal.
- Changed code and contract: [planner metadata session](../../engine/src/transaction.rs), [statistics service](../../specs/statistics-service.md), and [planner budget](../../specs/planner-compiler.md).
- Verification: the budget exhaustion unit test confirms the allowance rejects the next estimate without advancing the counter. The planner IPC integration test builds a typed table through storage DDL, requests 16 exact summaries over `serve_planner`, receives `Unknown` on estimate 17, then successfully describes the relation again and confirms the invocation remains active. Locked workspace build, format check, warning-denied Clippy, and serial full workspace tests passed; commit integration reported 129 passed and 3 cgroup-gated ignored tests. A parallel full-test run reproduced the existing `InvalidAudit` flake in `storage_manager_creates_and_drops_check_constraint`; the serial rerun passed.
- Remaining: overall planner search budgets, asynchronous task draining, cache validity, and bounded maintenance collection remain open. Supervisor provisioning of the planner stream is also not implemented.
