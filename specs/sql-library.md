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

[Schema DDL](ddl-schema.md) defines initial scalar types and identifier rules. Remaining expression/coercion details and wire encodings must be fixed before their implementation; the example is illustrative. Reject unsupported syntax explicitly. The required analytical follow-on adds inner equijoins, grouping, COUNT/SUM/MIN/MAX/AVG, sorting, and HAVING under the [workload profile](workload-profile.md); these are not yet part of the first parser slice. General subqueries, explicit transaction-control statements, and user-defined SQL functions remain deferred. Schema/user/permission DDL is supported through the separate administrative SDK channel and is rejected inside user invocations. Existing design discussions of aggregates describe future semantics, not initial language support.

The parser currently accepts this deliberately narrow grammar:

```text
select       := SELECT projection FROM identifier
                [WHERE predicate]
                [LIMIT unsigned_integer] [;]
projection   := * | identifier (',' identifier)*
comparison   := '=' | '!=' | '<>' | '<' | '<=' | '>' | '>='
predicate   := identifier comparison parameter
             | identifier IS [NOT] NULL
parameter    := '$' unsigned_integer
identifier   := ASCII letter or '_' followed by ASCII letters, digits, or '_'
```

Keywords are case-insensitive; identifier spelling is preserved as source bytes for later binding. Whitespace is ASCII space, tab, CR, or LF. SQL input is bounded to 65,536 bytes, each identifier to 128 bytes, projection to 64 columns, parameter indices to 1–65,535, and LIMIT to 0–1,000,000. LIMIT 0 is valid. The parser accepts at most one predicate. Comparison predicates take only a positional parameter on the right side; `IS [NOT] NULL` predicates take no parameter. Parameter values are bound separately. Comments, quoted identifiers, aliases, schema qualification, literals, Boolean combinations, multiple statements, and other statement kinds are rejected (a non-SELECT statement reports UNSUPPORTED). The AST stores source offsets/lengths into the caller-owned immutable SQL buffer and allocates no strings. The predicate operator field uses `PRDB_SDK_SQL_COMPARE_IS_NULL` or `PRDB_SDK_SQL_COMPARE_IS_NOT_NULL` for null tests, and leaves `parameter_index` zero for them; those codes are predicate operators and are not valid inputs to the scalar comparison evaluator.

## Scalar Comparison Semantics

The C SDK's initial scalar evaluator follows the trusted policy evaluator's type and ordering rules. VALUE comparisons require identical scalar type codes; Decimal additionally requires equal declared scales, which the binder obtains from the pinned column/parameter types. Since NULL and REDACTED value views carry no type code, static type compatibility must be checked by the binder before evaluation. Fixed-width signed scalars compare as signed two's-complement values. Text and Varchar compare lexicographically by Unicode scalar value (equivalent to bytewise order for valid UTF-8), Bytea and Uuid compare lexicographically by bytes, and Boolean orders FALSE before TRUE. Numeric coercions, mixed Text/Varchar comparison, and collation-specific ordering are not implicit.

Comparison with SQL NULL yields UNKNOWN. REDACTED is not SQL NULL: any comparison or `IS NULL` requiring a REDACTED operand returns an evaluation error. `IS NULL` returns TRUE only for NULL and FALSE for VALUE. Boolean AND/OR/NOT use SQL three-valued truth tables; evaluation is left to right and short-circuits only on decisive FALSE for AND or TRUE for OR. Once a reached operand is REDACTED or malformed, evaluation fails rather than treating it as UNKNOWN. These rules do not authorize filtering hidden storage data: SQL operates only on sanitized executor batches.

The C SDK provides scalar comparison, `IS NULL`, Boolean-to-truth conversion, and pure AND/OR/NOT truth-table helpers in [`prdb_sdk_sql_v1.h`](../engine/include/prdb_sdk_sql_v1.h). `prdb_sdk_sql_eval_select_predicate_v1` connects one parsed predicate to a sanitized row-cell view and, for comparisons, its separately bound parameter view. It checks VALUE types against the resolved column type, applies the declared Decimal scale, returns SQL truth values including UNKNOWN for NULL comparisons, and propagates REDACTED errors. Null-test predicates require no parameter; comparison predicates require one. The pure AND/OR/NOT helpers combine already evaluated operands; they do not evaluate expression trees or invoke potentially skipped operands. Boolean combinations and connection to SQL statement execution remain future work.

The first typed-parameter binding helper takes the parsed SELECT record, a caller-owned one-based array of `prdb_sdk_value_v1` views, and the resolved column type (plus Decimal precision and scale when applicable). Decimal metadata must satisfy `1 <= p <= 38` and `0 <= s <= p`; a value's signed coefficient magnitude must not exceed `10^p - 1`. It selects the AST's `$n` entry, validates its record and bytes, rejects REDACTED values and type mismatches, and returns a borrowed copy of the selected view. SQL NULL is accepted and retains the expected type only in the binder context; the value record itself carries no type for NULL. No implicit coercions occur. The parameter array and underlying bytes remain caller-owned and immutable for the call and any later evaluation.

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

The initial parser API is [`prdb_sdk_sql_v1.h`](../engine/include/prdb_sdk_sql_v1.h). `prdb_sdk_sql_parse_select_v1` returns a sized/versioned AST record with source spans into the caller-owned SQL bytes; it copies no identifier strings. The record describes only the first grammar increment above and is not a plan request or an executable SQL statement. Empty SQL returns `SYNTAX_ERROR`; null pointers or an undersized output record return `INVALID_ARGUMENT`. The API reports an error byte offset when provided and writes the output record only on success. Keep this SQL API distinct from the user query entry-point ABI. Statement execution, result consumption, cancellation notification, and cleanup APIs remain future work. Host services will provide planning submission, scoped storage requests, result emission, and cancellation/deadline checks.

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

This initial SELECT parser does not settle the rest of the SQL dialect, types, coercions, AST extension/compatibility rules, physical-plan format, result format, SDK execution entry points, or mutation expression support. The baseline abort-on-statement-error rule is fixed; savepoints are future work.
