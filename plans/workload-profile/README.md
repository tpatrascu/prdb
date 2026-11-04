# Workloads, Integration and Release Evidence Plan

## Contract and Dependencies

Validate the [architecture](../../specs/architecture.md) and [operational/analytical workload](../../specs/workload-profile.md) across the [component roadmap](../README.md). Each component owns its local correctness suite; this plan owns cross-process vertical slices, scheduling evidence and release coverage. Add integration tests as interfaces appear, not after implementing every component.

## Decisions Before Implementation

Record supported hardware/kernel/filesystem/toolchain/redb profiles, deterministic workload datasets/seeds, resource classes/allowances, queue/age/memory/output defaults and benchmark methodology. Set quantitative targets only after repeatable measurements. Select scan-resistant cache/prefetch and maintenance policies supported by redb; do not implement a separate buffer manager or I/O subsystem.

## Integration Increments

| ID | Observable contract and evidence |
| --- | --- |
| WI-1 | Toolchain/profile and copied IPC fixtures work; real TinyCC constructors load only after fail-closed confinement. Missing features, role/version mismatch and crashes fail safely |
| WI-2 | Protected bootstrap plus real control administration creates two users and a table/policies. Different cell views survive hostile direct IPC attempts, forged handles and SDK bypass |
| WI-3 | Concurrent invocations read/stage writes, see own writes and preserve snapshots. Same-row conflicts abort wholly; disjoint writes commit; authority changes, constraints, recovery/status and retention safety use real redb |
| WI-4 | Real TCP helper uploads/seals/starts an immutable TinyCC artifact, sequential SDK SQL calls plan/execute, supervisor relays provisional output and storage confirms durable completion. Caught statement errors abort all writes |
| WI-5 | Disconnect/client/gateway/supervisor/storage failures around every commit transition reconcile by authenticated ID. Forged terminal bytes, incomplete frames or missing history never establish success/rollback; no automatic replay |
| WI-6 | Complete supported schema/identity/permission DDL and scalar/SQL matrix. Include unique/FK races, index publication crashes, bootstrap interruption, offline upgrade preservation and unsupported-format rejection |
| WI-7 | Under saturated analytics/output/planning/maintenance, bounded queues reserve progress for point reads, commit, cancellation and reclamation. Client class labels cannot exceed configured authority; memory/history pressure rejects or aborts explicitly |
| WI-8 | Required analytical SQL executes equijoins, grouping, COUNT/SUM/MIN/MAX/AVG, sorting/HAVING with precise null/redaction semantics and bounded hash/sort memory; no uncontrolled spill or privileged hidden-data aggregate |

WI-2 and WI-3 jointly establish the first usable data milestone; a read-only slice is intermediate evidence. WI-6 is required for full initial scope and WI-8 for analytical support. General serializability, distributed execution, window/recursive SQL, secure remote transports, shared-memory transport and process reuse remain separate future work.

## Test and Benchmark Harness

Use maintained C unit executables for SDK parsing/ownership. Run query libraries and hostile fixtures only in the real sandbox harness. Integration uses real binaries, local Unix channels, TCP sockets, TinyCC artifacts and isolated redb directories; mocks are fault controls rather than persistence/isolation evidence. Deterministic barriers/deadlines cover both race orders; model/property/fuzz tests record seeds and turn minimized failures into regressions.

Measure SDK release build separately from cold/warm per-query compile/link, load, first result, execution and durable commit. Record point/range latency, write throughput, conflict rate, commit p50/p95/p99, redb bytes/commit, copy bytes, memory and retained history. Compare operational-only with simultaneous scans/joins/sorts, scoped estimates, index builds and supported maintenance. Measure sampling/estimation cost and plan quality independently; upstream benchmarks are not project results.

Crash tests compare committed rows/indexes/catalog/audit/outcomes after restart. Fault injection tests the selected durable flush contract; process kills alone do not prove power-loss guarantees. Paired hidden-data tests check authorized outputs/estimate content while documenting timing, constraint-outcome and arbitrary-worker correctness limits.

## Completion and Handoff

Maintain feature/spec-to-test coverage with real test paths, exact commands/profile/results and unverified gates. Run relevant component checks then the required workspace/release/target gates from AGENTS.md. Missing tools or skipped confinement are unresolved gates. Do not claim production/security audit readiness from prototype tests. First behavior: a repeatable confined-artifact launch harness with one successful fixture and constructor-time denial, after toolchain and sandbox prerequisites.
