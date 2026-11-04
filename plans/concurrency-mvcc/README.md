# Transactions and MVCC Implementation Plan

## Contract and Dependencies

Implement [snapshot-isolated invocation transactions](../../specs/concurrency-mvcc.md) within the [storage manager](../storage-manager/README.md). Storage owns the redb adapter/codecs; this plan owns transaction state and validation. Depend on real [identity](../ddl-identity/README.md), [authorization](../authorization/README.md), [catalog epochs](../data-dictionary/README.md) and supervisor-only finalization via [IPC](../ipc-protocol/README.md).

Concurrent reads and private write staging are initial requirements. redb's serialized writer is used only for final validation/publication, never held while native code runs. Snapshot isolation permits write skew; row-level conflicts do not promise serializability.

## Decisions Before Implementation

Freeze admission snapshot/commit-sequence capture, row/base-version tokens, overlay/write-generation semantics, statement input freezing, lock/fence order, dependency epoch scope and synchronized cancellation barrier. Specify durable ABORTED persistence, ambiguous commit recovery, retained status/expiry limits and context/task pin ownership in the relevant specs. Resolve stale custom page/WAL terminology in adjacent specs before encoding physical assumptions.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| MV-1 | Repeated committed reads stay stable while another invocation commits; other private writes remain invisible | Admission-retained redb read transaction, context-bound snapshot/schema/policy and task-owned pins |
| MV-2 | Sequential insert/patch/delete reads see own successful writes without revisiting mutation outputs | Bounded owned overlay, private-write generation, fixed statement input list and closed-input-before-stage ordering |
| MV-3 | Barrier-coordinated same-row updates conflict atomically; disjoint rows stage concurrently and both commit | Serialized final redb writer, current base-version checks and complete patch/index publication; documented write-skew regression |
| MV-4 | Concurrent unique/FK and changed schema/credential/policy dependencies reject the whole stale write set | Latest-state plus final-overlay checks under the publication gate, conservative epochs, no user-edited policy inputs |
| MV-5 | Any statement error, unfinished stream, crash or accepted pre-barrier cancellation discards all writes | ACTIVE/COMMITTING/COMMITTED/ABORTED state machine, drain/freeze, no prefix commit and no executor commit operation |
| MV-6 | Both cancel/commit interleavings yield definite abort before commit begins or pending/too-late afterward | Synchronized irreversible redb-commit boundary; no fabricated rollback on lost replies |
| MV-7 | Kill/reopen and injected ambiguous commit reveal data and COMMITTED outcome together | Atomic row/index/catalog/audit/outcome publication; durable abort separately persisted when justified, authorized status returns UNKNOWN/EXPIRED for absent retained evidence |
| MV-8 | Worker exit during an in-flight scan/estimate cannot release history early; old-reader pressure remains bounded | Pin registry/drain ownership, age/write-set/history limits and explicit abort/reject rather than snapshot refresh |

## Acceptance and Handoff

Use explicit barriers/test hooks and deadlines, not timing sleeps, to cover both race orders. Run real pinned redb and multi-process supervisor/storage tests; mocks cover faults only. Separate process-crash recovery evidence from filesystem/power-loss claims. First behavior: two real redb read snapshots demonstrating repeatability during a committed write, then the private overlay.
