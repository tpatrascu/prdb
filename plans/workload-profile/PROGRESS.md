# Workload and Integration Progress

## WI-7 dependency: authorized-estimate baseline cost — measured

- Harness: [`statistics_estimate.rs`](../../engine/examples/statistics_estimate.rs) builds a temporary READY redb database with 8,192 deterministic INTEGER rows, verifies an authorized exact summary, and reports repeated storage-estimate latency separately from setup.
- Profile and result: Rust 1.95.0, redb 4.4.0, x86_64 Linux release build; 11 estimates yielded 8.655 ms median and 12.073 ms p95. The result is a local baseline, not a service-level latency claim or configured target.
- Verification: release example completed; locked workspace build, format check, warning-denied Clippy, serial workspace tests (129 passed, 3 cgroup-dependent ignored), and maintained TinyCC SDK suite passed.
- Remaining: cross-process analytical saturation, operational latency under concurrent SQL analytics, maintenance pressure, memory/copy accounting, and repeatable target-hardware measurements remain open.
