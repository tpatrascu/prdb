# Database Engine Specifications

These documents capture the current research design for an operational database with analytical query support. Low-latency transactional workloads drive the storage and scheduling choices; analytics uses the same authorized MVCC data under bounded resources. They are not claims about an implemented or audited system. Client-compiled query libraries, an SDK bundling basic SQL and component interfaces, Linux-first native execution, OS-process isolation without a VM, true cell-specific permissions, and concurrent reads/writes from the initial implementation are project requirements. User queries and the C SDK use TinyCC; the database engine uses Rust. Other choices are proposed baselines unless explicitly described otherwise.

## Documents

- [Toolchain decision](toolchain.md): TinyCC/C SDK, Rust engine, ABI, packaging, and validation implications.
- [Workload profile](workload-profile.md): operational priorities, analytical scope, and mixed-workload scheduling.
- [Storage format decision](storage-format.md): physical layout is delegated to redb.
- [redb storage backend](redb-storage.md): embedded database responsibilities and engine transaction boundary.
- [Catalog payload encoding v1](catalog-payloads.md): canonical envelopes, field tags, family keys, and bootstrap mappings.
- [Checkpoint format decision](checkpoint-format.md): physical checkpointing is delegated to redb.
- [Index-key encoding v1](index-key-encoding.md): exact ordered tuple bytes, version suffixes, and examples.
- [In-memory storage](memory-storage.md): buffer/transaction lifetimes and security-constrained copy reduction.
- [Transaction durability](wal-recovery.md): redb-backed atomic commits and recovery ownership.
- [Disk I/O decision](disk-io.md): redb owns database-file I/O; no engine io_uring path.
- [Architecture](architecture.md): goals, trust boundaries, execution flow, and milestones.
- [Concurrency and MVCC](concurrency-mvcc.md): snapshot isolation, concurrent writes, commit/recovery, and reclamation.
- [Data dictionary](data-dictionary.md): catalog records, IDs, indexes, dependencies, and caches.
- [Metadata discovery](metadata-discovery.md): filtered APIs and built-in `information_schema` relations.
- [Catalog lifecycle](catalog-lifecycle.md): bootstrap, startup validation, format versions, and offline upgrades.
- [Physical metadata](physical-metadata.md): redb owns physical state; engine owns logical outcomes and audit.
- [Operational metadata](operational-metadata.md): live registries, monitoring, cancellation, and audit.
- [Schema DDL](ddl-schema.md): schemas, tables, types, constraints, indexes, and concurrent catalog changes.
- [Users and roles](ddl-identity.md): database identities, membership, credentials, and admission.
- [Permission DDL](ddl-permissions.md): grants, default-deny row/cell policies, and composition.
- [Authorization](authorization.md): row visibility, cell permissions, redaction, and query semantics.
- [Client gateway](client-gateway.md): dedicated TCP listener, transport-independent admission, and bounded result relay.
- [Client network protocol v1](client-protocol.md): plaintext TCP frames, uploads, provisional output, and durable outcomes.
- [Supervisor](supervisor.md): execution identity, process lifecycle, and resource budgets.
- [Storage manager](storage-manager.md): trusted data access, policy enforcement, and snapshots.
- [Optimizer and compilation boundary](planner-compiler.md): server planning versus client compilation.
- [Statistics service](statistics-service.md): trusted collection, scoped estimates, and statistics disclosure.
- [Executor](query-worker.md): untrusted execution and the native extension contract.
- [Client SDK](client-sdk.md): TinyCC compilation, component contracts, and artifact packaging.
- [SQL library](sql-library.md): SDK SQL frontend, initial language subset, and plan execution.
- [IPC protocol](ipc-protocol.md): scoped handles, bounded requests, and authorized batches.
- [Linux sandbox](linux-sandbox.md): confinement requirements and launch ordering.

## Baseline and Terminology

- **Invocation:** one client-built query library run, with one disposable executor, MVCC transaction snapshot plus private writes, effective grant, deadline, and cumulative budgets.
- **SQL statement:** one SDK SQL call within an invocation. Sequential calls share one transaction; only one statement/request stream is active at a time initially. Any statement error aborts the transaction in the baseline.
- **Executor:** the sandboxed native host, also called the query worker in earlier discussions. `query-worker.md` retains its filename.
- **Optimizer:** the isolated planning service, also called the planner. It returns structured physical plans, not native libraries. `planner-compiler.md` retains its historical filename; compilation runs in the client.
- **Query library:** the client-built `query.so`, linked with a prebuilt SDK SQL/runtime archive in the proposed baseline. A physical plan is data, not this native artifact.

Administrative DDL is a separate authenticated catalog transaction, not a query-library statement. It includes schema, identity, role, and permission changes; catalog authority does not automatically grant executor data access.

The SDK's C ABI, IPC messages, structured plan format, and artifact manifest are distinct versioned contracts. SDK definitions do not grant service access. The selected toolchain is TinyCC for C queries/SDK code and Rust for the engine; versions and target profiles still require pinning and validation.

## Reading and Updating

Start with the architecture and authorization documents. Component specifications describe responsibilities, interfaces, failures, validation, and open questions. Names and API examples are conceptual; wire formats and implementation module layout remain unspecified. TinyCC is the selected user compiler; exact versions and supported targets are not yet pinned.

Record decisions consistently across affected documents. Keep unresolved security assumptions explicit.

Follow the [iterated development guide](../INTERATED_DEVELOPMENT.md) to implement these contracts through small TDD cycles while refining the specs.

The [storage-manager implementation plan](../plans/storage-manager/README.md) breaks that component into dependency-ordered tasks, prerequisite decisions, and acceptance gates for future implementation agents.
