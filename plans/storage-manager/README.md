# Storage Manager Implementation Plan

## Purpose and Status

This plan guides a future agent implementing the trusted Rust storage manager. It is a dependency-ordered backlog, not evidence of completed implementation. At plan creation the repository contains specifications and no Cargo workspace or runtime harness. Inspect the checkout before acting; do not assume this remains true.

Follow [repository instructions](../../AGENTS.md) and the [TDD development guide](../../INTERATED_DEVELOPMENT.md). Normative behavior belongs in [specs](../../specs/README.md); these documents describe implementation sequence and acceptance evidence. Resolve contradictions by refining the affected specs explicitly, not by silently changing behavior in this plan.

## Reading Order

1. [Boundaries and prerequisite decisions](01-boundaries-and-decisions.md): modules, interfaces, ownership, and unresolved contracts.
2. [Storage foundation](02-storage-foundation.md): P0–P3, toolchain, codecs, reference I/O, WAL, pages, and minimal catalog.
3. [Transactions and authorized access](03-transactions-and-access.md): P4–P6, MVCC, commit, policy evaluation, and IPC.
4. [Catalog, recovery maintenance, and I/O](04-catalog-and-operations.md): P7–P10, DDL, checkpoints, io_uring, and scoped statistics.
5. [Acceptance and integration](05-validation-and-integration.md): P11, fault matrix, boundary tests, and performance evidence.
6. [Agent execution and handoff](06-agent-workflow.md): task selection, progress records, and completion rules.

## Dependency Order

```text
P0 contracts/toolchain -> P1 codecs -> P2 reference I/O/WAL
                                      -> P3 pages/catalog primitives
                                      -> P4 durable concurrent transactions
                                      -> P5 authorization/control bootstrap
                                      -> P6 role-bound IPC
                                      -> P7 complete initial DDL
                                      -> P8 checkpoints/reclamation/lifecycle
                                      -> P9 io_uring conformance
                                      -> P10 estimates/observability/scheduling
                                      -> P11 integrated acceptance
```

The order is a conservative implementation path. Add checkpoint primitives required for bootstrap in P3–P5; P8 completes concurrent checkpointing and retention. Begin io_uring ownership design in P0/P2, with the real backend in P9. Add tests and quotas throughout, not only in P11. P4 allows concurrent staging/readers with serialized commit; it is not a single-writer execution model.

P1–P4 are internal development increments with no normal query listener. The first usable data milestone requires P4–P6 together: real authorization, concurrent writes, durability, and process-bound admission. Complete initial storage-manager support additionally requires P7–P11. A reference backend is useful development infrastructure but does not complete the selected io_uring work.

## Scope and Integration Dependencies

Storage owns row-oriented pages, B-trees, MVCC, WAL, recovery, dictionary, policies, transaction outcomes, trusted estimates, and administrative operations. Operational latency is primary; bounded analytical scans share the same permissions and snapshots.

The gateway/client network protocol, supervisor, sandbox, TinyCC SDK, SQL library, and optimizer are adjacent components. Define and test their storage contracts here, but do not implement whole adjacent components merely to finish a storage unit test. Final integration requires their real implementations; test adapters cannot substitute for confinement or authentication.

Required analytical SQL joins/grouping/sorting remain a project follow-on under the [workload profile](../../specs/workload-profile.md). Storage completion supplies bounded authorized batches and estimates; it does not claim those SQL operators exist.

Optional direct I/O, sealed shared batches, group commit, delta WAL, serializable isolation, concurrent statements within one invocation, and online schema rewrites are outside this baseline. Measure copying and WAL amplification without introducing those features prematurely.
