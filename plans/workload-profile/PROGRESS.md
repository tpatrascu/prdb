# Workload and Integration Progress

## WI-7 dependency: authorized-estimate baseline cost — measured

- Harness: [`statistics_estimate.rs`](../../engine/examples/statistics_estimate.rs) builds a temporary READY redb database with 8,192 deterministic INTEGER rows, verifies an authorized exact summary, and reports repeated storage-estimate latency separately from setup.
- Profile and result: Rust 1.95.0, redb 4.4.0, x86_64 Linux release build; 11 estimates yielded 8.655 ms median and 12.073 ms p95. The result is a local baseline, not a service-level latency claim or configured target.
- Verification: release example completed; locked workspace build, format check, warning-denied Clippy, serial workspace tests (129 passed, 3 cgroup-dependent ignored), and maintained TinyCC SDK suite passed.
- Remaining: cross-process analytical saturation, operational latency under concurrent SQL analytics, maintenance pressure, memory/copy accounting, and repeatable target-hardware measurements remain open.

## WI-7 dependency: commit latency under retained-snapshot pressure — measured

- Harness: [`storage_pressure.rs`](../../engine/examples/storage_pressure.rs) records 256 operational-only durable commits, resets the four rows, then repeats 256 commits while a retained authorized snapshot performs full scans and point reads. Commit latency covers `commit()` only; row creation, encoding, and staging are excluded. The benchmark raises the managed reader scan-work limit to 2,000,000 so the single retained invocation can sustain this synthetic pressure interval.
- Profile and result: Rust 1.95.0, redb 4.4.0, x86_64 Linux release build. Operational-only p50/p95/p99 was 6.169/11.787/17.016 ms. During mixed load it was 9.004/11.184/16.042 ms; the retained snapshot completed 8,557 full authorized scans and 34,228 point reads while preserving its original values. The full mixed writer phase took 0.582 s (440.2 commits/s).
- Scope: one local run with four 32 KiB rows and four writer threads. The p95 difference is within the noise possible from a single run; this does not establish production SQL contention, service-level latency, or a target. Repeatable runs, SQL joins, maintenance pressure, and target-hardware profiles remain open.
