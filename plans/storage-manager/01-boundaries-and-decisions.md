# redb Boundary and Decisions

This file replaces the earlier custom physical-store module map. Storage modules should wrap redb and implement engine semantics; they must not reproduce its page manager, B-tree, allocator, WAL, checkpoint, or file I/O.

| Engine module | Responsibility |
| --- | --- |
| backend | Own redb Database and typed table handles; enforce one storage owner |
| codec | Versioned row, catalog, SQL-key, outcome, and audit values |
| txn | Snapshot lifetime, private overlays, row-version validation, commit state |
| auth/policy | Principal/grant binding, row/cell policy checks, epoch revalidation |
| catalog/ddl | Logical metadata, constraints, index definitions and migrations |
| access | Authorized scans, redaction and sanitized batches |
| runtime | Budgets, bounded queues, admission, status and observability |

Before coding, decide exact redb crate version, durability configuration, filesystem profile, API key/value limits, compaction behavior, and upgrade path. Freeze the named-table/key/value schema in redb-storage.md and add fixtures. Check whether indexes can use the exact byte ordering defined in index-key-encoding.md; alter the engine logical encoding if the upstream Key contract requires it. Do not preserve a 2,048-byte limit unless redb supports it and it is useful as a deliberate SQL limit.

The catalog payload encoding remains an engine value codec, but its former ROW1/page wrapper and physical version-chain assumptions are retired. Define stable redb keys and transactional deletion/tombstone semantics before implementation. Authentication verifier and expression IR encodings remain independent unresolved contracts.

Concurrency rule: one invocation owns a redb read snapshot plus a private overlay. No redb write transaction is held while native code runs. Finalization opens the serialized redb writer, validates latest row versions/epochs, merges changes and commits all related records atomically. Snapshot admission, commit/cancel boundary, and row conflict linearization require deterministic schedule tests.
