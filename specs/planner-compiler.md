# Optimizer and Client Compilation Boundary

## Responsibilities and Isolation

Implement the optimizer in Rust and transform structured query requests into logical and physical plans. TinyCC produces the submitted C query library in the client using the prebuilt C SDK; the server optimizer does not compile that program. The executor's [SQL library](sql-library.md) owns SQL text parsing and operator execution; the planner validates its structured input, resolves authorized schema/types, and performs optimization. Run in a process separate from both the executor and storage manager: optimizer complexity, malformed inputs, and resource exhaustion must not compromise storage memory.

The planner is not an authorization authority. A compromised planner must not be able to mint execution grants or cause storage to release unauthorized values. Treat its plans as untrusted structured input. All client-built query artifacts are untrusted native code regardless of compiler or SDK provenance.

## Pipeline

```text
client: user C source + prebuilt C SDK -> TinyCC compiler/linker -> query.so
executor: query.so -> SDK SQL API -> structured planning request
optimizer: validated request -> logical plan -> physical plan
executor: SDK interpreter executes plan -> user code consumes results
```

The prebuilt SDK executes physical plans with an interpreter initially. There is no server compilation backend in the baseline; adding one requires an explicit future design decision.

Inputs are bounded structured requests from the executor, typed parameters as needed for planning, authorized schema, scoped estimates, and execution constraints. A provisioned submission channel binds requests to the existing execution context; input cannot change its identity or grant. Outputs require explicit versioning and validation. Plans carry logical relation/schema references, never optimizer-connection storage handles; the executor resolves them again through its scoped storage channel.

The initial structured request payload v1 is implemented as a bounded Rust decoder in `engine/src/optimizer_request.rs`; it is not yet connected to a process or worker channel. It is a single-table SELECT record: `u16 version=1`, `u8 statement=1`, `u8 reserved=0`, then schema and relation identifiers (`u8 byte_length` plus ASCII identifier bytes), `u16 projection_count` and that many identifiers, `u32 predicate_length` and prefix-encoded predicate bytes (zero length means absent), then `u8 limit_present` (0 or 1) and an optional `u32 limit`. Identifiers use `[A-Za-z_][A-Za-z0-9_]{0,62}`. There must be 1–128 projections. The full record is at most 64 KiB and predicate bytes at most 16 KiB.

Predicate node tags are `0x00` NULL; `0x01` Boolean followed by 0 or 1; `0x02` signed big-endian i64; `0x03` UTF-8 text with a big-endian u16 byte length (at most 4,096 bytes); `0x10` column identifier; `0x20` comparison followed by operator byte (0 equal, 1 not-equal, 2 less, 3 less-or-equal, 4 greater, 5 greater-or-equal) and two child nodes; `0x30` AND and `0x31` OR followed by a child count (2–16) and those child nodes; `0x32` NOT followed by one child. Predicates are bounded to 256 nodes and depth 32. Unknown tags, invalid values, truncation, and trailing bytes are rejected. This decoder checks structure and resource bounds only: it does not bind relation/column names, infer expression types, authorize access, or produce an executable plan. Unsupported literal types and all non-SELECT statement kinds are rejected in v1.

The in-process v1 binder's `bind_select_request_via_metadata` entry point resolves the request's exact qualified name through `PlannerMetadataConnection` on the admitted invocation before binding. A missing description and a description whose relation name does not match the requested name produce the same `RelationUnavailable` result. It binds the request to pinned schema ID/version, table ID/write epoch, and column IDs/ordinals/types/nullability; duplicate or malformed catalog metadata fails closed. Comparisons require matching known scalar types (or a NULL operand), and Boolean operators require Boolean expressions. The bound result retains the resolved schema/relation names plus stable catalog identities. This helper does not select a physical access path or create an executor grant. Planner IPC `describe_plan_relation` (`0x1204`) and executor IPC `resolve_plan_relation` (`0x1105`) return the same authorized stable identity without minting a handle. The executor-side endpoint lets the SQL library re-resolve a decoded plan against the same invocation snapshot before obtaining a scan capability.

The initial plan builder, `plan_select_v1`, lowers this bound SELECT to a linear `Scan -> Filter? -> Project -> Limit?` pipeline. The scan column set is the unique ordinal-sorted union of projected and predicate columns; projection order and duplicates remain intact. Every relation and column reference carries stable catalog IDs, schema version, and table write epoch, never a planner handle or permission. `validate_plan_v1` independently checks pipeline order, reference membership, Boolean filter types, duplicate/missing columns, and predicate bounds.

`encode_plan_v1` and `decode_plan_v1` implement the canonical version-1 plan payload (maximum 64 KiB). Its header is `u16 version=1`, `u8 kind=1` (SELECT), and `u8 reserved=0`, followed by schema name (`u8 length` and ASCII identifier), schema ID[16], schema version `u64`, relation name (`u8 length` and identifier), relation ID[16], table write epoch `u64`, and a `u8` node count (2–4). Node tags are `1` Scan (`u16` scan-column count and full column records), `2` Filter (one prefix predicate), `3` Project (`u16` projection count and full column records), and `4` Limit (`u32` row count). A column record is ID[16], ordinal `u32`, scalar type code `u8`, type parameters[4], and nullable `u8`. Type codes 1–11 and parameters match the IPC relation-description encoding. Scan columns are limited to 256 and projections to 128. Filter predicates use the request v1 node tags, except a column node carries its bound column ID[16] rather than a name. The Rust decoder resolves those IDs only against the preceding scan shape and runs the independent plan validator. `optimizer_service::optimize_select_v1` composes request binding, conservative lowering, and canonical plan encoding against an invocation-bound `PlannerMetadataConnection`; a real-redb test checks the result against pinned catalog identities. The `optimize_select` IPC operation (`0x1203`) and `OptimizerSubmission` role have shared generated constants and an allowlist. The `prdb-optimizer` executable serves that operation in a separate process, requests stable authorized relation metadata through `describe_plan_relation` (`0x1204`), and returns a validated plan or non-disclosing status. The supervisor launcher starts the optimizer beside the executor in the invocation cgroup, gives it separate submission and planner Unix sockets, and serves both planner streams concurrently with the executor's planner stream. The optimizer receives its own Landlock policy and seccomp filter. ABI v4 exposes a fixed-operation executor callback for bounded SELECT submissions. The C SDK's `prdb_sdk_optimizer_encode_select_v1` serializes an explicit-column single-relation SQL v2 AST, a caller-supplied schema identifier, and optional LIMIT into request v1. Its initial subset rejects `SELECT *` and every WHERE tree as unsupported; callers must keep those queries on the SQL execution path. `prdb_sdk_optimizer_decode_plan_v1` validates the `Scan -> Project -> optional Limit` subset. `prdb_sdk_optimizer_decode_plan_with_filter_v1` validates the complete v1 pipeline, including its bounded predicate tree, scanned column references, scalar compatibility, and Boolean root. Both reject malformed identities, types, order, references, lengths, and trailing bytes. Plan names and the validated prefix predicate borrow the complete payload. `prdb_sdk_optimizer_validate_executor_relation_v1` independently decodes a plan, asks executor storage to re-resolve it with `resolve_plan_relation` (`0x1105`), and compares stable identities and scanned columns. `prdb_sdk_optimizer_acquire_plan_relation_v1` obtains a normal executor relation handle only after that validation and checks its relation ID/name. `prdb_sdk_optimizer_open_plan_scan_v1` opens a full scan from the checked handle and skips cursor creation for `LIMIT 0`. The unfiltered batch consumer applies the plan projection and row limit. The bounded runner polls cancellation between pages, caps scan execution at 126 pages so cancellation, batch, setup and cursor-close calls fit the 256-request executor stream budget, honors LIMIT, and closes early cursors; filtered plan execution remains unsupported.

## Storage and Statistics Access

Use a dedicated planning channel for authorized schema, logical access paths, and bounded estimation requests. This is a richer interface than the worker scan API but grants no broader data authority. The storage-owned [statistics service](statistics-service.md) performs collection and policy-aware estimation; the optimizer consumes results and compares candidate plans.

Request cardinality, authorized output size, selectivity, and eventually distinct-count/correlation estimates as supported. Each estimate identifies its population and uncertainty. A readable-cell estimate cannot substitute for an estimate over every visible row. An estimate never proves a query is authorized.

The planner cannot request unrestricted refresh jobs, access raw global catalogs, or submit native estimation callbacks. Each planner stream has a cumulative allowance of 16 storage estimate requests; once exhausted, storage returns `Unknown` and planning may continue with fixed heuristics. This estimate allowance is separate from the existing 256-request planner stream cap and the invocation deadline. Exhaustion of the overall planning budget, cancellation, or deadline expiry terminates planning. Treat all returned metadata as potentially disclosed through plans, diagnostics, and executor output.

## Catalog and DDL Boundaries

Use only authorized catalog names/types and logical access paths from the pinned schema. Administrative DDL bypasses query optimization and goes to the validated control path; the optimizer cannot create tables, indexes, users, or policies. Plans bind stable catalog IDs and schema epochs. A published index is an optional storage access path, never a bypass for row/cell policies or version visibility.

Metadata binding also supports the fixed virtual relations in [metadata discovery](metadata-discovery.md). Their estimates and scans use filtered metadata only. The optimizer cannot inspect raw catalog records, physical roots, live cross-user registries, or privileged expression definitions. Cache validity follows [data dictionary](data-dictionary.md), including checks independent of notification delivery.

## Concurrent and Writable Plans

Produce read and mutation plans for the initial SQL subset. Plans contain logical targets and supported expressions, never permissions or commit decisions. Storage checks every staged mutation and commit independently. Plan input and estimates use the invocation snapshot plus its current private writes, identified by a transaction-local write generation. Replan/re-estimate after generation changes; no stale plan estimate can establish authorization.

Use bounded optimizer queues across invocations. A single invocation initially has one active planning/statement stream. Internal statistics requests must hold storage-owned snapshot pins until completion even if the executor exits. Do not let planner completion race transaction closure or change the invocation snapshot.

## Workload Priorities

Follow [workload profile](workload-profile.md): prioritize point/range access and predictable operational latency on the row store. The analytical follow-on chooses bounded join/group/sort plans over authorized batches and scoped estimates. Avoid unbounded optimization effort for small transactions. Planner estimates cannot authorize hidden-key index probes or hidden-data aggregation. Resource class and budgets are supplied by admission, not enlarged by plan output.

## Optimization Constraints

Optimize the authorized view. Never push a hidden-value predicate, user function, or native callback ahead of authorization. Storage must independently validate any requested pushdown. Error behavior for redacted inputs is part of semantics and must be preserved by rewrites.

Global histograms, samples, counts, and private-data-derived cost estimates are unavailable to the planner. Use scoped authorized-view summaries or conservative heuristics. Shared plan caches must not cross permission boundaries or reveal protected metadata. Historical estimates must remain valid for the current authorization and snapshot; staleness is not solely a performance issue.

## Native Artifacts and Budgets

User query artifacts are compiled in the client and loaded only in confined executors. The [SDK specification](client-sdk.md) defines packaging and version checks. Keep native ABI and IPC versions separate. Native SQL-plan generation by the server is outside the baseline and would require a separate design decision.

Bound server planning independently from execution and statistics estimation. Client compilation has its own latency and resource profile. A planning failure returns a bounded error and releases temporary resources; it must not fall back to executing user code inside storage.

## Validation and Limits

Test malformed queries, resource exhaustion, invalid output, unauthorized metadata requests, estimation probes, heuristic fallback, and rewrites involving redacted inputs. Compare SDK SQL execution with reference results on authorized data. Verify that changing only inaccessible data does not change released estimate content for a fixed authorized view.

Isolation protects data boundaries, not semantic correctness. Whether generated plans are trusted for correct results or require additional validation remains a separate research decision.
