# Storage Manager Implementation Plan with redb

## Purpose

Implement the trusted Rust storage manager on the pinned [redb backend](../../specs/redb-storage.md). redb owns physical pages, B+trees, allocation, MVCC read snapshots, atomic persistence, and recovery. The engine owns relational values, catalogs, SQL indexes, authorization, invocation overlays, conflict policy, durable outcomes, and the protocol boundaries.

Read [AGENTS.md](../../AGENTS.md), the [development guide](../../INTERATED_DEVELOPMENT.md), [redb storage](../../specs/redb-storage.md), [concurrency](../../specs/concurrency-mvcc.md), and [authorization](../../specs/authorization.md). The current phase documents below describe the redb implementation sequence; they replace the earlier custom page/WAL sequence.

## Dependencies and Phase Documents

Follow the [repository roadmap](../README.md). The [toolchain plan](../toolchain/README.md) owns reproducible builds; [transactions](../concurrency-mvcc/README.md) owns snapshot/overlay/conflict state; [authorization](../authorization/README.md) owns runtime row/cell enforcement; [dictionary](../data-dictionary/README.md) owns catalog payloads and dependencies. Those components live within trusted storage where specified; separate plans do not imply new processes or duplicate implementations.

- [Boundary decisions and module ownership](01-boundaries-and-decisions.md)
- [redb foundation and codecs](02-storage-foundation.md)
- [Transactions and authorized access](03-transactions-and-access.md)
- [Catalog and operational integration](04-catalog-and-operations.md)
- [Validation and integration](05-validation-and-integration.md)
- [Increment execution and handoff](06-agent-workflow.md)

This plan also owns [memory lifetimes](../../specs/memory-storage.md), [logical index-key encoding](../../specs/index-key-encoding.md), [physical metadata boundaries](../../specs/physical-metadata.md), [durability](../../specs/wal-recovery.md), and the delegated [storage format](../../specs/storage-format.md), [checkpoint](../../specs/checkpoint-format.md) and [file I/O](../../specs/disk-io.md) contracts. There are no independent custom physical-storage implementations to schedule.

Before codec work, reconcile stale physical version references, suffix/size limits and separator language in adjacent catalog/index/MVCC documents with redb-storage.md. Preserve normative SQL tuple-order examples while explicitly choosing the redb key mapping. Plans identify these decisions; they do not silently redefine persisted bytes.

## Sequence

1. **Pin and evaluate redb.** Choose exact crate version, durable commit mode, supported Rust/platform profile, licensing/upgrade policy, API value/key limits, transaction and guard lifetimes. Run independent API experiments in a disposable harness. Do not add other database-file code.
2. **Define engine codecs and table schema.** Freeze named redb table definitions and versioned row/catalog/index/outcome/audit value encodings. Keep catalog payloads and SQL logical index keys; remove assumptions about custom pages, physical B-tree separators and 16 KiB allocation.
3. **Implement read snapshots and overlay.** Admission retains a bounded redb read transaction with identity/schema/policy context. Implement own-write overlay and authorized scans. Prove repeatable committed reads, own-write visibility, and no cross-invocation leakage.
4. **Implement final write path.** At finalization open one redb write transaction; re-read current versions; validate row conflicts, permission epochs and constraints; merge patches against current values; atomically update row, index, catalog, required audit, and outcome tables; commit durably. Test competing transactions, write skew, abort, ambiguous outcome and restart.
5. **Implement complete catalog and controls.** Bootstrap, DDL, users/roles, policies, metadata, audit, and recovery validation operate as logical records in redb, never in its private physical tables.
6. **Integrate processes and protocols.** Exercise storage only through supervisor-bound IPC and gateway admission/result flow. Run hostile artifacts only inside the sandbox. Add status lookup after lost network or supervisor replies.
7. **Measure and harden.** Test retained-reader pressure, operational transactions during analytical scans, redb compaction/reclamation, selected durability mode, filesystem behavior, copy costs, and version upgrade/rejection. Reassess performance on the target Linux profile; do not assume upstream benchmarks transfer.

Run small TDD increments within each stage. Record actual red failures, exact redb version/profile, tests/commands, spec decisions, and remaining evidence. A mock redb adapter is useful for unit faults but cannot establish persistence or recovery.

Use task IDs SM-1 through SM-7 for these sequence items and smaller suffixes for individual observable behaviors. First behavior: an isolated real-redb write/abort/reopen fixture on the pinned release, after toolchain setup. Follow it with an independently checked valid/invalid engine key/value fixture.

## Memory and Durability Acceptance

Keep redb guards and borrowed values inside their owning transaction/task. Own validated overlay inputs and bounded sanitized output buffers; do not hold guards across IPC/network writes. C result batches borrow immutable authorized values until explicit release. Test cancellation/disconnect while storage/estimate tasks hold pins, retained-reader pressure and old-schema decoding. Physical reclamation/compaction uses supported redb operations, with engine age/number budgets; never silently replace snapshots.

Write COMMITTED outcomes atomically with all associated records. Persist definitive ABORTED outcomes through a durable transaction when justified; absence or expired retention means UNKNOWN/EXPIRED. Set explicit outcome/audit limits, test commit ambiguity/reopen, and record selected filesystem/durability-mode assumptions. No separate engine WAL/checkpoint or direct writer can establish an alternative outcome.

Copied local batches are the baseline. Optional sealed sanitized-memfd batches require an explicit negotiated capability and independent sealing/ownership/adversarial tests from memory-storage.md before enabling them. Raw redb pages and writable worker/storage mappings remain forbidden.

## Exit Conditions

The storage slice is complete only when real redb-backed tests show atomic rows/indexes/catalog/outcomes, concurrent snapshots and staged writers, correct authorization, durable commit/reopen behavior, bounded long-reader pressure, and no executor access to redb. Gateway/sandbox end-to-end evidence is required for the integrated service.
