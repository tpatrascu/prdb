# Database Implementation Plans

These plans cover every component and supporting contract in the [specification index](../specs/README.md). They describe future work, not implemented features or passing checks. Normative behavior stays in `specs/`; resolve ambiguities there before making tests depend on them. Read [AGENTS.md](../AGENTS.md) and [INTERATED_DEVELOPMENT.md](../INTERATED_DEVELOPMENT.md) before implementation.

## Component Plans

| Plan | Responsibility |
| --- | --- |
| [Toolchain](toolchain/README.md) | Reproducible Rust/TinyCC builds, target profile, interface generation and release artifacts |
| [Internal IPC](ipc-protocol/README.md) | Versioned local transport, roles, scoped handles and sanitized batches |
| [Linux sandbox](linux-sandbox/README.md) | Fail-closed launch and confinement before constructors |
| [Executor](query-worker/README.md) | Disposable native host, ABI callbacks and invocation completion |
| [Supervisor](supervisor/README.md) | Admission, immutable staging, process lifecycle and result/outcome relay |
| [Client protocol](client-protocol/README.md) | External framing, upload/control state machines and reconciliation |
| [Client gateway](client-gateway/README.md) | Dedicated TCP process and bounded gateway-service adapter |
| [Client SDK](client-sdk/README.md) | C runtime bindings, compiler wrapper and separate submission/admin helpers |
| [SQL library](sql-library/README.md) | C frontend, expressions, interpreter, writes and required analytics |
| [Optimizer](planner-compiler/README.md) | Isolated Rust binding/planning with scoped metadata and estimates |
| [Statistics](statistics-service/README.md) | Storage-owned authorized estimates and later private collection |
| [Storage manager](storage-manager/README.md) | redb integration, engine codecs, logical indexes and durable publication |
| [Transactions](concurrency-mvcc/README.md) | Snapshots, private overlays, conflict validation and outcome races |
| [Authorization](authorization/README.md) | Trusted row/cell evaluation and mutation permission enforcement |
| [Data dictionary](data-dictionary/README.md) | Catalog records, canonical payloads, indexes and dependency epochs |
| [Catalog lifecycle](catalog-lifecycle/README.md) | Initialization, validated startup and offline upgrades |
| [Schema DDL](ddl-schema/README.md) | Types, table/index/constraint changes and DDL fences |
| [Identity DDL](ddl-identity/README.md) | Users, roles, credentials and administrative admission |
| [Permission DDL](ddl-permissions/README.md) | Grants, mandatory policies and atomic authority changes |
| [Metadata discovery](metadata-discovery/README.md) | Filtered discovery APIs and fixed SQL metadata relations |
| [Operational metadata](operational-metadata/README.md) | Live registries, authenticated monitoring and atomic audit |
| [Workloads and integration](workload-profile/README.md) | Cross-component release gates, resource scheduling and benchmarks |

## Dependency Roadmap

Use these stages as integration gates, with small red–green–refactor–specify cycles inside each. Start only the structures required for the next observable behavior; this index is not an instruction to generate every crate or interface at once.

| Stage | Work and dependency order | Required exit evidence |
| --- | --- | --- |
| 1 | Toolchain; first shared schema/codecs; disposable redb experiments | Exact pinned versions/profile, maintained C/Rust tests and redb atomic reopen fixtures; shared libraries compiled but not loaded outside confinement |
| 2 | Bounded IPC and host ABI; sandbox; executor; minimal supervisor launch | Real TinyCC artifact loads after confinement; hostile constructors, missing kernel features, crashes, version mismatch and forged frames fail safely |
| 3 | Dictionary and recoverable bootstrap; real identity/control admission; grants/policies; discovery and authorized reference scans | Two authenticated users receive different cell views; direct IPC and forged handles cannot widen authority |
| 4 | redb snapshots and overlays; transactions; row conflicts; durable commit/outcome publication | Concurrent staging and reads, own writes, all-or-nothing commit, stale-policy rejection, restart/status and retained-reader safety. Stages 3–4 jointly form the first usable data slice |
| 5 | Client framing/helper and gateway; supervisor relay; SDK SQL/parser; baseline optimizer and bounded estimates | Real TCP upload/seal/START of a TinyCC library, sequential reads/writes, durable commit and lost-reply reconciliation without replay |
| 6 | Complete schema, identity and permission DDL; logical indexes/constraints; metadata/admin/audit | Control-path coverage, unique/FK races, old readers versus DDL, stale writers and crash-safe index publication |
| 7 | Complete initial SQL/types, statistics fallback, scheduling and hardening | Feature-to-spec evidence, fuzz/model suites, bounded cancellation/pressure and repeatable operational measurements |
| 8 | Analytical operators and optimizer support; mixed workloads | Equijoins, grouping/aggregates, sorting/HAVING, redaction/null semantics, memory bounds and operational tail latency during analytics |

Stage 6 is required to complete the initial scope. Stage 8 is required to complete analytical support. Initial SQL alone completes neither. Minimal catalog/control functions land early; full DDL expands them later. Durability starts with the first writes. A serialized final commit gate permits concurrent invocation reads and staged writes; it does not permit holding a redb writer across native execution.

Schema generation and native ABI agreement precede cross-language integration. Metadata and statistics initially use authorized reference scans; optimized paths follow equivalence evidence. Bootstrap never depends on SQL, the optimizer or user code. Physical pages, B-trees, WAL, checkpoints, allocation and file I/O remain redb responsibilities.

## Specification Coverage

Each specification has a primary owning plan. Other plans link adjacent contracts; shared work is implemented once by the owner.

| Specification | Owning plan |
| --- | --- |
| [Architecture](../specs/architecture.md) | [Workloads and integration](workload-profile/README.md) and this roadmap |
| [Toolchain](../specs/toolchain.md) | [Toolchain](toolchain/README.md) |
| [Workload profile](../specs/workload-profile.md) | [Workloads and integration](workload-profile/README.md) |
| [Storage format](../specs/storage-format.md) | [Storage manager](storage-manager/README.md) |
| [redb storage](../specs/redb-storage.md) | [Storage manager](storage-manager/README.md) |
| [Memory storage](../specs/memory-storage.md) | [Storage manager](storage-manager/README.md) |
| [Transaction durability](../specs/wal-recovery.md) | [Storage manager](storage-manager/README.md) |
| [Checkpoint format](../specs/checkpoint-format.md) | [Storage manager](storage-manager/README.md) |
| [Disk I/O](../specs/disk-io.md) | [Storage manager](storage-manager/README.md) |
| [Physical metadata](../specs/physical-metadata.md) | [Storage manager](storage-manager/README.md) |
| [Index-key encoding](../specs/index-key-encoding.md) | [Storage manager](storage-manager/README.md) |
| [Storage manager](../specs/storage-manager.md) | [Storage manager](storage-manager/README.md) |
| [Concurrency and MVCC](../specs/concurrency-mvcc.md) | [Transactions](concurrency-mvcc/README.md) |
| [Authorization](../specs/authorization.md) | [Authorization](authorization/README.md) |
| [Data dictionary](../specs/data-dictionary.md) | [Data dictionary](data-dictionary/README.md) |
| [Catalog payloads](../specs/catalog-payloads.md) | [Data dictionary](data-dictionary/README.md) |
| [Catalog lifecycle](../specs/catalog-lifecycle.md) | [Catalog lifecycle](catalog-lifecycle/README.md) |
| [Schema DDL](../specs/ddl-schema.md) | [Schema DDL](ddl-schema/README.md) |
| [Identity DDL](../specs/ddl-identity.md) | [Identity DDL](ddl-identity/README.md) |
| [Permission DDL](../specs/ddl-permissions.md) | [Permission DDL](ddl-permissions/README.md) |
| [Metadata discovery](../specs/metadata-discovery.md) | [Metadata discovery](metadata-discovery/README.md) |
| [Operational metadata](../specs/operational-metadata.md) | [Operational metadata](operational-metadata/README.md) |
| [Client gateway](../specs/client-gateway.md) | [Client gateway](client-gateway/README.md) |
| [Client protocol](../specs/client-protocol.md) | [Client protocol](client-protocol/README.md) |
| [Supervisor](../specs/supervisor.md) | [Supervisor](supervisor/README.md) |
| [Optimizer](../specs/planner-compiler.md) | [Optimizer](planner-compiler/README.md) |
| [Statistics](../specs/statistics-service.md) | [Statistics](statistics-service/README.md) |
| [Executor](../specs/query-worker.md) | [Executor](query-worker/README.md) |
| [Client SDK](../specs/client-sdk.md) | [Client SDK](client-sdk/README.md) |
| [SQL library](../specs/sql-library.md) | [SQL library](sql-library/README.md) |
| [IPC](../specs/ipc-protocol.md) | [Internal IPC](ipc-protocol/README.md) |
| [Linux sandbox](../specs/linux-sandbox.md) | [Linux sandbox](linux-sandbox/README.md) |

## Execution and Handoff

Every increment records its task ID, normative spec sections, observable contract, failing assertion, implementation/spec decisions, exact commands/results, limitations and next smallest behavior. Track component progress in a `PROGRESS.md` beside its plan when implementation starts, using `not started`, `in progress`, `verified` or `blocked`. No task is verified by these documents alone. Preserve existing work and evidence when resuming.

Run the actual Rust and TinyCC checks in [AGENTS.md](../AGENTS.md) once their targets exist. Missing compilers, broken scaffolding or unavailable sandbox features are not meaningful red tests or passing integration gates. Use maintained C test executables for SDK unit tests; query libraries, constructors and hostile fixtures run only in the sandbox integration harness. Use deterministic barriers/deadlines for race tests and isolated directories for recovery tests. Process kills alone do not establish power-loss durability. Keep compilation, loading, execution and commit measurements separate.

Documentation changes require local link and consistency checks. Do not install compilers or report runtime evidence merely to validate these plans.
