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

## WI-7 dependency: repeatability sample for storage-pressure latency — measured

- Method: ran the already-built release harness five times sequentially. Each run created and removed a fresh temporary READY database and measured 256 commits in each phase. These are independent database runs on one host, not independent host or filesystem samples.
- Summary: the median of the five run-level p50/p95/p99 values was 5.910/12.708/18.127 ms operational-only and 8.921/13.383/19.172 ms under retained-snapshot scans. Across runs, the operational-only ranges were 5.644–5.960/11.646–15.286/15.699–21.524 ms; mixed-load ranges were 8.697–9.162/11.213–16.845/16.406–34.279 ms. Mixed throughput ranged from 415.3 to 450.7 commits/s, while the snapshot completed 8,297–9,124 scans per run.
- Interpretation: the repeated sample supports a higher median commit latency under this synthetic scan loop on this host, with a wide mixed p99 range. Keep it as local comparative evidence only; workload diversity, longer runs, host variation, SQL joins/sorts, maintenance, and target-hardware profiles remain open.
