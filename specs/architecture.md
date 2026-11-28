# Architecture

## Goals and Status

Design a local database engine that executes user query libraries compiled in the client using an SDK, with Linux OS processes for isolation and no VM. The established toolchain is TinyCC for C user queries and SDK SQL/runtime code, and Rust for database-engine processes; see the [toolchain decision](toolchain.md). Permissions must support different access to the same column across visible rows. This is a research design, not an implementation or security guarantee.

## Workload and Storage Direction

This is primarily an operational database: optimize for short transactions, point/range access, concurrent small writes, and predictable durable commit latency. Analytical queries are supported over the same authorized MVCC data, with scan-resistant caching and bounded analytical CPU/I/O/history budgets. See [workload profile](workload-profile.md) for the explicit SQL analytics roadmap; the first parser slice remains intentionally small.

Use [redb](redb-storage.md) for embedded persistence, physical B+trees, MVCC read snapshots, atomic commits, allocation, and recovery. The Rust engine retains logical SQL indexes, value codecs, authorization, private invocation overlays, and commit validation. It does not implement a second WAL/page store or access redb internals. Borrow values only inside trusted storage; copy only authorized values into executor output.

## Security Invariant

An executor can obtain only data authorized for its invocation context, regardless of the instructions it executes or the protocol messages it sends. Once a value is delivered, assume the worker can disclose it through permitted output.

The host kernel, client gateway, supervisor, storage manager, trusted policy evaluator, and storage-owned statistics service form the security boundary. The gateway handles plaintext credentials/results but has no database-file or executor-launch authority. redb is a library inside storage, not a separately exposed service. Treat the executor, submitted query library, and bundled SDK as compromised from launch. Treat optimizer output as untrusted for authorization purposes. An optimizer may still affect result correctness and disclose any statistics it receives.

## Components

```mermaid
flowchart TD
    Client[Client compiler and submission SDK] <-->|Framed plaintext TCP| Gateway[Dedicated client gateway]
    Gateway <-->|Private admission and result IPC| Supervisor[Supervisor]
    Supervisor --> Planner[Isolated optimizer]
    Admin[Authenticated database administrator] -->|Install, list, remove| SDKRepo[Instance SDK library repository]
    Supervisor --> SDKRepo
    SDKRepo --> Worker[Executor loads query.so and staged SQL SDK library]
    Worker -->|Structured planning request| Planner
    Planner -->|Physical plan| Worker
    Supervisor --> Storage[Trusted storage manager]
    Storage --> Stats[Trusted statistics service and private catalog]
    Planner -->|Bounded estimation requests| Stats
    Stats -->|Authorized metadata and scoped estimates| Planner
    Worker <-->|Scoped IPC; authorized batches| Storage
    Storage --> Redb[redb embedded database]
    Worker --> Output[Bounded local result channel]
    Output --> Relay[Supervisor result relay]
    Relay -->|Provisional data and storage-confirmed terminal outcome| Gateway
```

The [client gateway](client-gateway.md) is a dedicated Rust process and owns the client listener. The initial [client protocol](client-protocol.md) is framed unencrypted TCP, bound to loopback by default; secure/alternative transports are future gateway adapters. Executors never connect directly to network clients. No additional intermediary process beyond this gateway is required.

The supervisor and storage manager may initially share a process; neither owns the public TCP listener. The optimizer runs separately from both storage and the executor. A thread provides concurrency but no memory isolation. Native query libraries must never be loaded into the storage manager.

The executor (query worker) is a generic native host. For a hand-written native program, the client compiles user code into a loadable query library using the [client SDK](client-sdk.md), dynamically linked to a compatible versioned [SQL library](sql-library.md), and the supervisor resolves the manifest's exact SDK release from the instance [SDK library repository](sdk-library-repository.md) and stages it beside `query.so` for the invocation. For ordinary SQL text submissions (`submission_kind: "sql_text"`), the client uploads no artifact or manifest at all; the supervisor instead resolves the server-configured default SDK release and stages only that library, which the executor's SQL batch entry points are called from directly. A database administrator installs SDK releases in the instance SDK library repository either way. The executor loads the library only after confinement; the supervisor never loads native SDK code. The SQL library is not a separate server-selected query entry point. The optimizer returns physical plans and does not compile the user program.

## Execution Flow

1. For a native-artifact submission, the client uses its persistent per-instance SDK selection to compile and link query.so against a matching local SDK library; for a plain SQL-text submission, the client instead submits SQL text, parameters, and scope directly with no compiled artifact. Either way, the client authenticates through the gateway and uploads the bounded data into a supervisor-owned reservation. The client receives an invocation ID before explicitly starting execution. Uploading pins no MVCC snapshot. At SEAL the supervisor validates query ELF metadata as data for a native artifact, or validates only scope/parameter/SQL-data bounds for SQL text. At START it resolves and pins the exact SDK release (the manifest-declared release for a native artifact, or the server-configured default release for SQL text) from the admin-managed instance repository, revalidates identity with storage, and computes a proposed invocation-specific grant narrower than or equal to that identity's authority.
2. The storage manager creates a server-side context binding identity, scope, MVCC data/schema snapshot, private write set, policy version and inputs, and budgets.
3. The supervisor provisions separate planner-to-storage estimation and executor-to-planner submission channels bound to the same execution authority. It stages query.so and the selected repository SDK library in the worker's private read/execute-only artifact directory, then launches a confined executor with scoped storage access, approved runtime resources, and bounded output. A missing release or compatibility mismatch fails closed before worker launch.
4. For a native artifact, the executor loads the submitted query library after confinement and invokes its versioned entry point with host services and parameters; user code can call SDK SQL functions or scoped lower-level storage APIs. For a plain SQL-text submission, the executor instead `dlsym`s the staged SQL engine's own batch entry points directly, with no foreign entry point or ABI descriptor to validate. Either way, SQL calls parse and submit structured requests to the separate optimizer, which validates them and returns physical plans using scoped estimates or fixed heuristics.
5. The SQL library initially interprets the plan and requests batches through host services; user code may also issue direct storage requests. Sequential statements share the invocation transaction and cumulative budgets, observe successful prior writes, and permit one active statement/request stream at a time. Storage independently validates every request and applies row and cell policies before transmission. Compilation of user code has already happened in the client; server-side native plan generation is outside the baseline.
6. Successful library return triggers storage commit; only a durable commit decision permits success. Errors, cancellation, or timeout accepted before irreversible COMMIT submission abort all private writes; later races require resolving the durable outcome. After the terminal transaction outcome, close channels and release resources once in-flight tasks drain. Statement success alone does not commit. Output is relayed through the supervisor and gateway as provisional network data frames. Only a storage-confirmed terminal outcome establishes commit; this does not prove that untrusted code computed honestly. Disconnect/gateway failure follows the same cancellation barrier and authenticated status-reconciliation rules.

Direct storage calls can bypass SQL planning but must use the same scoped protocol and sandbox. Standalone executable support is a possible later adapter; the primary submission format is a client-built shared library.

## Concurrency and Transactions

Use [MVCC with snapshot isolation](concurrency-mvcc.md) from the first implementation, supporting concurrent readers and writers. Each invocation owns one transaction: a fixed committed snapshot plus its private writes, shared by SQL and direct SDK calls. Storage performs optimistic row-level conflict checks and serializes final commit validation/publication. This does not provide serializable isolation or general cross-row invariant enforcement.

On successful user-library return, the supervisor requests commit through a privileged control channel. Storage durably commits all writes or aborts the entire transaction. Any statement error aborts the transaction initially; savepoints are deferred. Success is reported only after commit, and lost replies are reconciled through authenticated transaction status. See the concurrency spec for policy revalidation, cancellation races, logging, bounded scheduling, and version reclamation.

## Schema, Users, and Permission Administration

The client submission SDK provides a separate authenticated control-mode gateway connection for [schema DDL](ddl-schema.md), [users/roles](ddl-identity.md), and [permission DDL](ddl-permissions.md). Storage independently validates definitions and current administrative authority. Executors cannot issue DDL or carry admission credentials. Database users authenticate initially with revocable database tokens; roles supply inherited grants and policy applicability.

Tables have mandatory default-deny row and cell policies in addition to object/column grants. Owners administer definitions but have no automatic data bypass. Initial schema support includes tables, scalar types, primary/unique/foreign-key/check constraints, B-tree indexes, and bounded alterations. Global constraint outcomes can reveal hidden key existence; this is an explicit limitation alongside timing inference.

Catalog changes participate in MVCC/durable ordering. DDL fences affected write commits while preserving pinned reader schemas; long builds do not hold the serialized redb writer. Identity, grant, and policy changes affect admission and write-commit epoch checks. See the DDL specs for scope and examples.

## Data Dictionary and Metadata

The storage-owned [data dictionary](data-dictionary.md) defines versioned logical objects, identities, grants, policies, dependencies, and cache validity. [Metadata discovery](metadata-discovery.md) exposes bounded authorized projections through IPC and built-in `information_schema` relations; raw catalogs are never executor tables.

[Catalog lifecycle](catalog-lifecycle.md) specifies bootstrap without SQL, validated startup, independent format versions, and offline upgrades. [Physical metadata](physical-metadata.md) describes redb ownership and engine durable outcomes. [Operational metadata](operational-metadata.md) exposes authenticated live status outside invocation MVCC snapshots. These classes intentionally have distinct consistency and disclosure rules.

## Statistics Boundary

Collection and optimization are separate responsibilities. Trusted storage routines may collect private global statistics, but the isolated optimizer receives only summaries derived from its execution's authorized view or public configuration. A numeric cost or selectivity can reveal private data just as a histogram can. Do not grant global statistics access merely because planning runs in another process.

The [statistics service](statistics-service.md) starts as a module within storage. Scoped on-demand estimates have independent work budgets. Global maintenance, cache reuse, and sampling are later optimizations with explicit validity and disclosure rules. A privileged optimizer is an alternative trust model, not the baseline.

## Guarantees and Limits

- Authorization applies to the principal-specific view, including storage-side operations.
- Confinement does not establish correct query results: malicious workers can fabricate or omit results.
- The kernel remains trusted; kernel exploits and comprehensive timing/cache side-channel prevention are outside the initial prototype's guarantees.
- Hidden-data inference through authorized outputs, errors, statistics, and execution time remains a research concern. Explicit redaction reveals that a field is unavailable.
- Revocation cannot erase values already delivered.

## Initial Milestone

Build a concurrent read/write prototype with MVCC, durable atomic commit, DDL-created tables/indexes, two database users with role membership, row-dependent field permissions, a client-built query library linked with SDK SQL support and a physical-plan interpreter, copied IPC batches, and a disposable executor per invocation. Add bounded authorized-view estimates with heuristic fallback; global statistics collection is not required initially. Demonstrate a malicious query library using the same storage protocol.

Validate isolation and authorization for client-built libraries before expanding compilation backends. Measure batch-transfer and policy-evaluation overhead before introducing shared memory. Exercise concurrent updates, conflicts, rollback, policy changes, recovery, and reclamation from the first milestone. SQL joins/grouping/aggregation/sorting are the required analytical follow-on in the workload profile, with mixed-workload validation. Serializable isolation, broad Linux executable compatibility, and privileged aggregates over hidden values remain separate future work.

## Open Decisions

- Credential protocol/cryptography details and the representation of invocation-specific grants.
- Supported Linux kernel features and deployment privilege requirements.
- Result-correctness guarantees for optimizer plans, SDK execution, and arbitrary user query libraries.
- Policy expression implementation, dependency validation, and immediate read revocation.
- Exact redb release/durable mode/platform profile, engine codec fixtures, and exact analytical SQL semantics.
