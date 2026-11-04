# Boundaries and Prerequisite Decisions

## Proposed Code Organization

Create only modules needed by the next test. A starting layout is one `crates/storage-manager` library with a thin service binary when P6 needs it; paths and crate names are proposals until recorded in the workspace. Separate protocol types into a small shared crate only when a second component consumes them.

| Module | Owned behavior | Must not own |
| --- | --- | --- |
| `format` | Checked page/row/key/catalog/WAL codecs | Native struct serialization or process state |
| `io` | Explicit-offset operations, completion ownership, sync dependencies | Commit authorization |
| `buffer`, `allocation` | Immutable page images, pins, generations, bounded allocation | Executor-visible pointers |
| `heap`, `btree` | Version records, overflow, row directory, access paths | Permission bypasses |
| `wal`, `recovery` | Record framing, durable frontier, redo, checkpoints | User callbacks |
| `txn` | Snapshots, overlays, dependency validation, publication, outcomes | Client-selected timestamps |
| `catalog`, `ddl` | Bootstrap descriptors, dictionary, dependencies, fences | SQL executed inside native libraries |
| `auth`, `policy` | Current identity validation, grants, bounded policy IR | Trust in SDK checks |
| `access`, `protocol` | Role-bound dispatch, handles, authorized scans/batches | Raw page export |
| `statistics`, `runtime` | Scoped estimates, budgets, queues, audit/telemetry | Global statistics for ordinary callers |

Keep error categories bounded and structured. Trusted internal diagnostics and externally visible errors need separate formatting paths. Rust ownership boundaries should make it difficult to pass a page image to a response serializer accidentally.

## Interfaces to Establish

These are conceptual contracts, not frozen Rust signatures:

- `StorageIo`: read/write at an explicit offset, sync file/directory, create/open internal files, complete/cancel/drain operations. Each pending operation owns its buffers and file-generation reference through terminal completion.
- `PageStore`: pin a validated image; prepare an unpublished replacement; publish a complete committed set. Page identifiers include file/page generations.
- `TransactionContext`: server-assigned snapshot, principal/grant, pinned catalog/policy dependencies, private writes, write generation, budgets, and state.
- `AuthorizedAccess`: resolve relation, open/advance/close scan, stage insert/patch/delete. Inputs are logical/scoped handles; outputs contain only authorized bytes.
- `CommitCoordinator`: supervisor-only finalization, autonomous abort, submission barrier, durable publication, authenticated outcome lookup.
- `AdminService`: authenticated bounded schema/identity/permission requests with atomic audit and dependency validation.
- `PlanningService`: independently scoped metadata/estimate requests, no executor scan capability and no privileged sampling callback.

Specify authority, ownership, limits, cancellation, and error effects for each operation before exposing it. Production context construction must require verified admission; test constructors remain unavailable in production builds.

## Decision Register

The catalog, checkpoint, and key formats below are now specified; implement and verify them rather than choosing replacements. Resolve each remaining open item just before its first dependent implementation. Update the linked normative spec and add independent fixtures/tests. Record the decision, rationale, affected versions, and evidence; do not leave alternative formats implemented simultaneously.

| Decision | Needed by | Source / acceptance requirement |
| --- | --- | --- |
| Rust version, dependencies, Linux target, TinyCC profile for integration | P0 | [Toolchain](../../specs/toolchain.md); reproducible lockfile, no guessed installed versions |
| Catalog tags/family keys and bootstrap mappings: specified | P1/P3 | [Catalog payload v1](../../specs/catalog-payloads.md); implement codecs and executable golden fixtures. Allocation payloads remain open. |
| Checkpoint records/manifest: specified; other WAL details open | P1/P2 | [Checkpoint format v1](../../specs/checkpoint-format.md); implement its framing/fixtures. Abort payload, ordinary transaction sequence origin/flags/classes still require clarification in [WAL](../../specs/wal-recovery.md). |
| Root feature/lifecycle codes and writer identity: specified | P1/P3 | [Checkpoint format v1](../../specs/checkpoint-format.md); writer ID is in the hash-bound manifest; validate version compatibility independently. |
| Exact tuple/suffix/separator encoding: specified | P1/P3 | [Index-key encoding v1](../../specs/index-key-encoding.md); turn examples into fixtures and compare bytewise order with independent typed comparison. |
| Scalar date/time ranges, wire values, predicate IR and error evaluation order | P1/P5/P6 | [DDL](../../specs/ddl-schema.md), [authorization](../../specs/authorization.md); no index-dependent authorization semantics |
| Token generation/verifier profile, setup handoff, expiry clock semantics | P5 | [Identity](../../specs/ddl-identity.md), lifecycle; only verifiers persist, restart/revocation tested |
| Frame/statement/private-write limits, recovery bounds, lock order | Before corresponding allocation/lock | [IPC](../../specs/ipc-protocol.md), [memory](../../specs/memory-storage.md); checked arithmetic and explicit overload errors |
| Outcome retention/expiry markers and checkpoint retention accounting | P4/P8 | [Physical metadata](../../specs/physical-metadata.md); absence never implies abort |
| io_uring crate, kernel/filesystem matrix, queue/reserve sizes | P9 | [Disk I/O](../../specs/disk-io.md); identical durability suite on both backends |

Do not describe format v1 as complete while required payload schemas remain undefined. No older-format migration is required without a declared source format, but unsupported-version rejection is mandatory.

## Synchronization and Ownership Review

Before P4, write the actual acquisition graph for context state, DDL fences, commit gate, catalog/page-directory latches, and I/O queues. Acquire multiple DDL fences in stable object-ID order. Do not wait for a DDL fence while holding the commit gate; a builder must be able to reach publication. Never hold page latches while waiting on user output or storage sync. Keep queue capacity for the work needed to release resources.

Define linearization points for snapshot admission, freezing writes, accepting cancellation, submitting COMMIT, publishing the committed frontier, and advancing the reclamation horizon. Published images and kernel-owned buffers remain immutable; task cancellation does not release I/O ownership. Include optimizer and maintenance pins in the same horizon accounting.

At commit, merge row changes into the latest committed page images after validation. A stale private copy of a shared page must not overwrite another transaction's unrelated row changes. Precompute outside the gate only with explicit dependency checks inside it.
