# SDK Basic SQL Library

## Requirement and Placement

The client SDK includes a reusable basic SQL implementation that user query code can call. The client compiles its code into a shared library, proposed name query.so, linked with prebuilt SQL/runtime SDK code. The executor loads this user query library only after confinement. SQL is SDK functionality inside the loaded artifact, not a separate server-selected main library or trusted storage code. Implement this SDK library in C with the pinned TinyCC toolchain and ship it prebuilt. Expose a versioned C API; Rust engine services validate requests independently.

The library implements SQL parsing, expression semantics, and physical-plan execution. The separate planner/optimizer selects physical plans using authorized metadata and scoped estimates. This preserves the existing optimizer process boundary while keeping the SQL frontend and execution runtime in the library.

## Proposed Initial SQL Subset

Support a single-table read/write subset from the initial implementation:

- `SELECT` with explicit columns, literals, and aliases, including filtered built-in `information_schema` relations defined in [metadata discovery](metadata-discovery.md).
- `FROM` naming one authorized table.
- `WHERE` with supported comparisons, Boolean operators, and `IS NULL`.
- Typed parameters and `LIMIT` for reads.
- `INSERT ... VALUES`, `UPDATE ... SET ... WHERE`, and `DELETE ... WHERE` with the same bounded expression subset.
- Generated identities and explicitly writable user-supplied keys, subject to schema constraints and permission policies.

For example: `SELECT name FROM employees WHERE department_id = $1 LIMIT 100`.

[Schema DDL](ddl-schema.md) defines initial scalar types and identifier rules. Remaining expression/coercion details, wire encodings, and exact parameter grammar must be fixed before implementation; the example is illustrative. Reject unsupported syntax explicitly. The required analytical follow-on adds inner equijoins, grouping, COUNT/SUM/MIN/MAX/AVG, sorting, and HAVING under the [workload profile](workload-profile.md); these are not yet part of the first parser slice. General subqueries, explicit transaction-control statements, and user-defined SQL functions remain deferred. Schema/user/permission DDL is supported through the separate administrative SDK channel and is rejected inside user invocations. Existing design discussions of aggregates describe future semantics, not initial language support.

## Query Flow

1. The client builds query.so with the SDK and submits the artifact plus parameters. The supervisor creates its context and launches the confined executor.
2. The host invokes the user library entry point with host services and parameters. User code calls the SDK SQL API with SQL text and typed parameters; calls may be computed at runtime.
3. The library parses the request into a bounded structured representation and sends it through a host-provided planning submission channel.
4. The isolated planner validates the representation, resolves authorized schema/types, and constructs a physical plan using its own scoped statistics channel.
5. The library validates the returned plan format, resolves logical relation/schema references on its own storage channel, and executes supported operators using host-provided storage callbacks over scoped IPC. Optimizer-local handles are never reusable executor capabilities.
6. The SQL library returns result batches/errors to user code, which may process authorized values and emit results through bounded host callbacks. The invocation reports an explicit completion/error outcome.

Interpret SQL physical plans initially. The enclosing user query library is already native code compiled by the client; this does not require the server to compile SQL plans. Server-side native plan generation is outside the baseline. Keep parameter values bound to execution; they cannot modify identity or grants.

## Operational and Analytical Execution

Favor low-overhead point/range operations and small writes. Analytical operators consume borrowed authorized columnar transport batches from the row-oriented storage engine; no raw page or private-statistics access is implied. Bound hash/sort/aggregation memory and fail explicitly when budgets are exceeded until a secure scratch/spill design exists. Validate SQL null, overflow, and redaction behavior before each new operator is enabled; COUNT(*) covers visible rows only.

## Metadata Queries

Treat the fixed `information_schema` relations as read-only virtual storage scans. The Rust storage manager filters metadata before sending rows; the C SDK cannot broaden discovery by evaluating predicates locally. They share the invocation catalog snapshot and do not expose private definitions, credential records, or live operational state. General user-created SQL views remain deferred.

## Transactional Execution

All statements participate in the invocation's storage-managed MVCC transaction. Reads see the committed snapshot plus successful prior writes. Mutation plans use a fixed statement input view; close the prior result stream before starting another statement. Initially materialize a bounded target/value list, close the mutation input scan, then stage writes so the scan cannot revisit its own updates. Exceeding the budget aborts the invocation. Storage independently validates every row and cell and keeps all changes private until commit.

Any SQL/storage error aborts the invocation transaction in the baseline; catching an SDK error cannot make earlier writes committable. No savepoints or per-statement autocommit. Successful library return asks the supervisor to finalize, but only storage can decide commit. Mutation counts and result batches are provisional until durable commit. General `RETURNING` remains deferred. Storage enforces the constraints defined in schema DDL, including commit-time unique and foreign-key validation.

## ABI and Host Services

Expose a versioned C API for SQL initialization, statement execution, result consumption, cancellation notification, and cleanup. Keep it distinct from the user query entry-point ABI. Exact symbols and layouts remain open. Host services provide planning submission, scoped storage requests, result emission, and cancellation/deadline checks.

Use opaque handles, explicit buffer lengths/ownership, and compatible layouts; never exchange Rust-owned containers or trait objects across the ABI. No language exception or panic may unwind across the C boundary. Translate failures into reported transaction-aborting errors when possible; crashes or unrecoverable failures terminate the executor.

The library has no direct statistics-maintenance interface. Host callbacks are an integration contract, not a security boundary within the process: malicious code can bypass them. Planner and storage servers independently enforce roles, grants, validation, and budgets.

## Authorization and Results

Execute against the authorized view: invisible rows are absent and cells are `VALUE`, `NULL`, or `REDACTED`. Projection may return redaction markers. Supported predicates requiring redacted inputs fail rather than treating redaction as null or false. SQL null logic remains distinct from redaction. Define evaluation order and error behavior before optimization rules are added.

SQL result encoding must preserve redaction as a distinct state. User code must treat statement batches as provisional until statement success, because an error may arise after partial output. A statement error invalidates its stream and aborts the invocation transaction; user code cannot recover and commit earlier writes. Separately, the external client treats all emitted invocation output as provisional until confirmed durable commit. Neither boundary proves the honesty of arbitrary user code. Limits and pagination must not become authorization proofs or justify hidden-value pushdown.

## Lifecycle and Validation

Load only the submitted query artifact and declared supported runtime dependencies; arbitrary filesystem plugin discovery is not part of the prototype. ABI/version mismatch fails the request. Prefer process exit to unloading a library with live callbacks or buffers. Bound parsing, AST size, planning requests, memory, and output.

Validate syntax/type errors, unsupported statements, parameter binding, null/redaction behavior, malformed plans, ABI mismatch, constructor-time confinement, cancellation, and partial-output failure. Compare interpreter results against small authorized-view fixtures. None of these correctness checks replace malicious-worker containment tests.

Allow sequential SQL calls within one user invocation under the same MVCC snapshot, private write set, and cumulative budgets. Reentrant/nested execution and concurrent statements are deferred; start with one active statement at a time and one disposable executor per user invocation.

## Open Decisions

Parser implementation, precise SQL dialect/types, AST and physical-plan formats, result format, library entry points, and exact mutation expression support remain unresolved. The baseline abort-on-statement-error rule is fixed; savepoints are future work.
