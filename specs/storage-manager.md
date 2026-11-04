# Storage Manager

## Responsibilities and Trust

The storage manager is the sole authority for database access. It owns database files, MVCC versions, private write sets, transaction outcomes, logs, policies, execution contexts, and cursor state. Implement the engine and request path in Rust, primarily safe Rust, with isolated reviewed unsafe code where required. Keep native loading outside this process.

It must remain secure when a worker sends arbitrary messages and when the planner produces an invalid or malicious plan. Authorization must not depend on following a prescribed plan.

## Operational Storage and I/O

Follow [storage format v1](storage-format.md) for row-oriented heap/version records, overflow, and B-tree access. [Memory storage](memory-storage.md) defines pinned immutable page images, transaction overlays, sanitized output, and copy accounting. [WAL v1](wal-recovery.md) defines no-steal full-page redo and checkpoints; [disk I/O](disk-io.md) defines io_uring ownership and sync ordering.

Prioritize operational queues/working sets while supporting bounded analytical scans. Do not allow scan prefetch or long snapshots to starve commits/reclamation. Borrow validated internal fields under pins and gather authorized output directly into its final buffer; never share heap/index pages with executors.

## Data Access

Expose authorized schema/relation resolution and bounded scans over connection-scoped table handles. Resolve logical references independently for optimizer and executor connections; returned plans cannot transfer handles or mint permissions. Validate identity binding, grant, requested columns, predicates, cursor ownership, snapshot lifetime, and limits independently.

For each batch:

1. Select candidate row versions from the pinned MVCC snapshot overlaid with the invocation's successful private writes, using a stable statement input view.
2. Apply trusted row-visibility policies.
3. Evaluate per-cell readability for projected fields and every field used by a predicate or other supported operation. An unprojected field is not exempt from authorization.
4. Apply supported operations according to authorized-view semantics.
5. Serialize only authorized values, null states, and redaction markers.

Never expose raw storage pages, database file descriptors, hidden payloads, or native callbacks to workers. Indexes and pushdown must preserve the same semantics as the reference scan, including authorization errors.

## Writes and Commit

Stage inserts, updates, and deletes in a private invocation write set. Storage independently validates mutation targets, changed-cell permissions, proposed rows, and constraints. Never accept client-supplied versions as proof of ownership or authorization. Preserve hidden unchanged cells internally.

Implement the [MVCC commit protocol](concurrency-mvcc.md): optimistic row conflict checks, policy-dependency validation, durable logging, and atomic row/index publication under an initial commit gate. Only supervisor finalization may request commit. Statement errors poison the whole transaction; cancellation/abort discards staged writes. Commit outcomes survive restart and support authenticated status lookup.

## Catalog and Administrative DDL

Own versioned schemas, types, table/column identities, constraints, index definitions, users/roles, token verifiers, grants, and policies. Accept administrative definitions only on a separately authenticated control channel, revalidate actor authority at commit, and reject executor-role DDL messages. A client-side parser or SDK version never substitutes for storage validation.

Implement [schema DDL](ddl-schema.md) fences, dependency checks, validation scans, index publication, and schema-epoch checks. Retain retired catalog/physical versions for old snapshots. Apply [identity](ddl-identity.md) and [permission](ddl-permissions.md) epoch changes atomically. Never expose credential verifiers or protected policy metadata to ordinary schema discovery.

## Dictionary, Lifecycle, and Recovery Metadata

Implement the record families, version-aware lookups, dependency edges, and cache keys in [data dictionary](data-dictionary.md). Public catalog access uses [metadata discovery](metadata-discovery.md); even database administrators cannot retrieve credential verifiers through inspection APIs. Catalog caches are not permission capabilities, and correctness must survive lost invalidation notifications.

Use [catalog lifecycle](catalog-lifecycle.md) for bootstrap, startup integrity checks, and offline upgrades. Own [physical metadata](physical-metadata.md) for storage maps, allocation generations, checkpoint roots, and durable outcomes. Expose live state through [operational metadata](operational-metadata.md), not the transaction's logical snapshot. User data, catalog changes, and their required recovery metadata must publish coherently.

## Policies and Metadata

Use a bounded trusted policy evaluator. Policy inputs may contain hidden attributes but cannot leak them in results or diagnostics. Metadata provided to planning must be authorized too.

Own the logical [statistics service](statistics-service.md), including trusted collection, protected catalogs, scoped estimation, and refresh scheduling. Expose authorized schema, logical access paths, and bounded estimates on a separate planning channel. Global samples, histograms, counts, and private cost inputs stay internal; a numeric answer does not remove disclosure risk.

Initially derive scoped summaries from the pinned authorized view within a budget, returning unknown when unavailable. Later collection may use background maintenance and caches; require explicit snapshot, policy, and population validity. Never execute planner-supplied native functions on samples. Private statistics may guide internal storage access, with timing leakage remaining a documented limitation, but cannot bypass authorized-view semantics or be exported as hidden-data-derived recommendations.

## Resource Ownership

Associate the snapshot with the invocation context and cursors with both that context and their owning statement/request. Sequential calls share the transaction and cumulative budgets; statement success closes only its resources, while a statement error aborts the invocation transaction. Limit open cursors, request complexity, response bytes, cumulative storage work, and estimation work. Keep maintenance budgets separate from query-triggered work. Closing a context invalidates handles and cancels pending work; release pins and memory only after in-flight storage tasks drain. Bound private writes and retained versions as well as result buffers. Reclaim obsolete versions using the active-snapshot frontier; never silently advance an old snapshot. A worker crash must not corrupt persistent state or affect unrelated contexts.

## Initial Scope

Start with concurrent MVCC reads and writes, reference scans, bounded mutation APIs, and durable commits; no arbitrary expression callbacks. The concurrency specification defines isolation and publication semantics. Version/page layouts and WAL framing are specified in storage format/WAL v1; remaining payload fixtures and synchronization primitives must be finalized during implementation. Initial schema/constraint support follows schema DDL, including primary/unique keys, CHECK, NOT NULL, and RESTRICT foreign keys. Validate cross-row constraints against concurrent commits under final serialization. General online rewrites, cascading actions, deferrable constraints, and native callbacks remain deferred.

## Validation

Test cross-context handles, unauthorized fields, malformed predicates, oversized requests, stale cursors, cancellation, and policy-cache isolation. Verify serialized batches contain no hidden bytes, including padding and unused capacity. Differentially compare future optimized access paths with the reference authorized scan.
