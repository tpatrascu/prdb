# Storage Manager Implementation Plan with redb

## Purpose

Implement the trusted Rust storage manager on the pinned [redb backend](../../specs/redb-storage.md). redb owns physical pages, B+trees, allocation, MVCC read snapshots, atomic persistence, and recovery. The engine owns relational values, catalogs, SQL indexes, authorization, invocation overlays, conflict policy, durable outcomes, and the protocol boundaries.

Read [AGENTS.md](../../AGENTS.md), the [development guide](../../INTERATED_DEVELOPMENT.md), [redb storage](../../specs/redb-storage.md), [concurrency](../../specs/concurrency-mvcc.md), and [authorization](../../specs/authorization.md). This plan supersedes the earlier custom page/WAL implementation sequence in phase documents 01–05; those documents are retained as history and must not guide implementation.

## Sequence

1. **Pin and evaluate redb.** Choose exact crate version, durable commit mode, supported Rust/platform profile, licensing/upgrade policy, API value/key limits, transaction and guard lifetimes. Run independent API experiments in a disposable harness. Do not add other database-file code.
2. **Define engine codecs and table schema.** Freeze named redb table definitions and versioned row/catalog/index/outcome/audit value encodings. Keep catalog payloads and SQL logical index keys; remove assumptions about custom pages, physical B-tree separators and 16 KiB allocation.
3. **Implement read snapshots and overlay.** Admission retains a bounded redb read transaction with identity/schema/policy context. Implement own-write overlay and authorized scans. Prove repeatable committed reads, own-write visibility, and no cross-invocation leakage.
4. **Implement final write path.** At finalization open one redb write transaction; re-read current versions; validate row conflicts, permission epochs and constraints; merge patches against current values; atomically update row, index, catalog, required audit, and outcome tables; commit durably. Test competing transactions, write skew, abort, ambiguous outcome and restart.
5. **Implement complete catalog and controls.** Bootstrap, DDL, users/roles, policies, metadata, audit, and recovery validation operate as logical records in redb, never in its private physical tables.
6. **Integrate processes and protocols.** Exercise storage only through supervisor-bound IPC and gateway admission/result flow. Run hostile artifacts only inside the sandbox. Add status lookup after lost network or supervisor replies.
7. **Measure and harden.** Test retained-reader pressure, operational transactions during analytical scans, redb compaction/reclamation, selected durability mode, filesystem behavior, copy costs, and version upgrade/rejection. Reassess performance on the target Linux profile; do not assume upstream benchmarks transfer.

Run small TDD increments within each stage. Record actual red failures, exact redb version/profile, tests/commands, spec decisions, and remaining evidence. A mock redb adapter is useful for unit faults but cannot establish persistence or recovery.

## Exit Conditions

The storage slice is complete only when real redb-backed tests show atomic rows/indexes/catalog/outcomes, concurrent snapshots and staged writers, correct authorization, durable commit/reopen behavior, bounded long-reader pressure, and no executor access to redb. Gateway/sandbox end-to-end evidence is required for the integrated service.
