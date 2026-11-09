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

Inputs are bounded structured requests from the executor, typed parameters as needed for planning, authorized schema, scoped estimates, and execution constraints. A provisioned submission channel binds requests to the existing execution context; input cannot change its identity or grant. Outputs require explicit versioning and validation. Plans carry logical relation/schema references, never optimizer-connection storage handles; the executor resolves them again through its scoped storage channel. Exact formats are undecided.

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
