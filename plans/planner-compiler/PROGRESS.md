# Optimizer Progress

## OP-1 dependency: bounded structured SELECT request decoder

- Contract: [optimizer specification](../../specs/planner-compiler.md). The first request payload is a versioned, single-relation SELECT with explicit projections, optional bounded predicate tree, and optional LIMIT.
- Implementation: [`optimizer_request`](../../engine/src/optimizer_request.rs) decodes identifiers, record lengths, predicate tags and values into owned Rust data. It caps requests at 64 KiB, predicates at 16 KiB, projections at 128, text literals at 4,096 bytes, predicate nodes at 256, depth at 32, and boolean fan-in at 16. Unknown versions/statements/nodes, invalid booleans/operators/identifiers, truncation, and trailing bytes fail closed.
- Boundary: the decoder performs structural validation only. It has no storage access and does not bind names or types, authorize relations, choose plans, or grant execution authority. The C parser and worker channels do not call it yet.
- Regression: four unit tests cover a compound typed request, malformed outer records/identifiers, excessive collection bounds and unsupported nodes, and predicate byte/node/depth exhaustion.
- Verification: locked workspace build and format passed; warning-denied Clippy passed; serial locked workspace tests passed (129 commit integration tests and all other active suites passed; 3 cgroup-dependent commit tests and 1 disconnected-submission process test were ignored). `git diff --check` passed.
- Remaining: define and validate type semantics against authorized metadata, return only non-disclosing bind failures, produce and validate a logical/physical plan, integrate a bounded isolated optimizer process and its supervisor-bound submission channel, and execute plans through SDK operations.

## OP-2 dependency: bind SELECT against authorized relation shape

- Implementation: [`optimizer_binding`](../../engine/src/optimizer_binding.rs) decodes the v1 request, resolves its exact qualified name through the invocation-bound `PlannerMetadataConnection`, and binds schema ID/version, table ID/write epoch, and projection/predicate column IDs, ordinals, types, and nullability. It validates relation shape and expression type compatibility. Missing descriptions and relation-name mismatches both return `RelationUnavailable`.
- Boundary: the helper receives no principal, grant, raw catalog, or storage handle and creates no executor permission. Its only metadata source is the planner connection opened on the admitted transaction.
- Regression: three unit tests cover stable reference binding, non-disclosing relation absence/name mismatch, and unknown-column/type-incompatible rejection. A real-redb integration regression binds an authorized user table, checks schema ID/version, table ID/epoch, column ID/shape, and confirms a missing relation maps to `RelationUnavailable`.
- Verification: focused binder tests, the focused planner stream/metadata integration regression, locked workspace build, formatting, warning-denied Clippy, serial workspace tests, and `git diff --check` passed (132 commit integration tests passed; 3 cgroup-dependent commit tests and 1 process test ignored).
- Remaining: define additional literal coercions/SQL NULL semantics. Isolated optimizer process, versioned plan output, and supervisor-bound submission channel remain open.

## OP-3 dependency: lower a bound SELECT to a validated plan pipeline

- Implementation: [`optimizer_plan`](../../engine/src/optimizer_plan.rs) creates and canonically encodes a linear scan/filter/project/limit plan with schema ID/version, relation ID/write epoch, and stable column references. The scan reads the unique ordinal-sorted union of projection and predicate columns; output projection order and duplicates are preserved. The plan contains no planner handle, grant, or commit operation.
- Validation: `validate_plan_v1` and the byte decoder check the pipeline shape, relation identity, unique sorted scan columns, projection membership, filter references/types, and predicate depth/node/fan-in/text bounds. Column references in predicates resolve only through the decoded scan shape. The SDK and executor must repeat plan validation after channel transfer.
- Regression: eight unit tests verify pipeline construction/order, scan-column union, malformed order/duplicate limits, invalid filters/references/depth/types, canonical byte round-trip, and malformed version/type/truncated/trailing payload rejection.
- Verification: focused optimizer plan tests passed. Locked workspace build, format, warning-denied Clippy, serial workspace tests, and `git diff --check` passed (129 commit integration tests passed; 3 cgroup-dependent commit tests and 1 process test ignored).
- Remaining: assign/transfer the plan over the supervisor-bound channel, independently re-resolve the relation through executor storage, interpret the plan through the SDK, and implement mutation plans and optimizer process isolation.

## OP-3 dependency: compose invocation-bound optimizer service

- Behavior: one library entry point decodes, resolves and binds a structured request through the invocation's planner metadata connection, lowers it, and returns canonical plan bytes without transferring planner handles or authority.
- Implementation: [`optimizer_service`](../../engine/src/optimizer_service.rs) composes the existing binder and plan validator/encoder. The real-redb planner regression now invokes it, decodes the returned plan, and checks schema/table IDs and pinned epochs; unavailable relations map to a non-disclosing bind result.
- Verification: locked workspace build, format, warning-denied Clippy, serial workspace tests, and `git diff --check` passed (129 commit integration tests passed; 3 cgroup-dependent tests ignored).
- Remaining: bounded submission IPC, isolated optimizer process and supervisor channel, independent executor-side relation resolution, SDK interpretation, and mutation plans.

## OP-3 dependency: define optimizer submission payload and role

- Behavior: reserve one role-specific operation for SELECT submissions and accept a successful response only when it contains a complete independently validated plan; errors use a small non-disclosing status set.
- Implementation: operation `0x1203`, generated Rust/C constants, `OptimizerSubmission` role allowlisting, and canonical response payload encoder/decoder are defined in `optimizer_ipc`. Request structure remains the canonical SELECT v1 payload and its 64 KiB decoder. This only defines and validates payloads; no socket handler or executor callback is connected yet.
- Verification: generated ABI files matched the schema. Locked workspace build, format, warning-denied Clippy, serial workspace tests, and `git diff --check` passed (129 commit integration tests passed; 3 cgroup-dependent tests ignored; 81 Rust unit tests passed).
- Remaining: connect the submission socket to `optimize_select_v1`, launch the optimizer in its own process, independently re-resolve plan references through the executor channel, and interpret plans in the SDK.

## OP-3 dependency: serve optimizer submissions over a bounded socket

- Behavior: a provisioned optimizer-submission stream validates request IDs and role, returns canonical plan payloads, and gives the same unavailable status for hidden/missing relations. Malformed framing, duplicate IDs, limit exhaustion, transport failures, and storage transaction errors abort the invocation.
- Implementation: [`prdb-optimizer`](../../engine/src/bin/prdb-optimizer.rs) serves submissions in a separate process using only inherited submission and planner sockets. [`optimizer_process`](../../engine/src/optimizer_process.rs) validates each request, resolves stable authorized relation metadata through the planner socket, builds a plan, and validates the response before writing it. The previous in-process stream handler was removed. A real-redb subprocess integration regression checks successful and unavailable lookups.
- Boundary: the isolated process now has production supervisor launch, OS confinement, aggregate invocation resource limits, and a distinct planner metadata channel. The executor callback/channel is provisioned but not yet exposed through the query ABI.
- Verification: focused real-redb optimizer subprocess and protocol-loop tests passed. Generated ABI consistency, locked workspace build, format, warning-denied Clippy, serial workspace tests, and `git diff --check` passed (132 commit integration tests passed; 3 cgroup-dependent tests and 1 disconnected-submission process test ignored; 82 Rust unit tests passed).
- Remaining: expose optimizer submission through the executor query ABI, independently re-resolve plans in storage, and execute plans in the SDK.

## OP-3 dependency: C SDK optimizer response envelope — implemented

- Behavior: the C SDK can submit an already encoded bounded SELECT request through the ABI v4 optimizer callback and decode the canonical response version/status/length envelope. Successful responses expose borrowed plan bytes; errors expose no plan. Malformed versions, statuses, lengths, trailing bytes, oversized plans, and overlapping output/payload records are rejected.
- Implementation: [`prdb_sdk_optimizer_v1.h`](../../engine/include/prdb_sdk_optimizer_v1.h) and [`optimizer_v1.c`](../../engine/sdk/optimizer_v1.c) provide the status decoder and callback wrapper. The result record borrows plan bytes from the caller's response buffer.
- Regression: [`sdk_optimizer_v1.c`](../../engine/tests/fixtures/sdk_optimizer_v1.c) checks successful plans, unavailable status, callback I/O errors, response capacity behavior, and malformed payload rejection under TinyCC.
- Limitation: this step does not serialize a parsed SQL AST, validate plan structure, resolve relation IDs through executor storage, or execute plan operators.
- Verification: the full maintained TinyCC SDK suite passed, including this fixture. Workspace build, formatting, warning-denied Clippy, serial Rust tests, generated ABI check, and `git diff --check` also passed (129 commit integration tests; three cgroup-dependent tests ignored).
- Remaining: serialize the supported SQL AST into canonical optimizer requests, independently decode/validate plans, re-resolve the relation, and route physical operators through SDK executor calls.

## OP-3 dependency: encode simple SQL ASTs as optimizer requests — implemented

- Behavior: the C SDK serializes an explicit-column, single-relation SQL v2 SELECT AST into canonical optimizer request v1, with a caller-supplied schema and optional LIMIT.
- Boundary: `SELECT *` and every WHERE tree return `UNSUPPORTED`; the encoder does not silently discard either projection or predicate semantics. It checks ABI/version/AST shape, identifier and span bounds, request size, and output overlap; undersized buffers receive only the required length.
- Regression: the TinyCC optimizer fixture parses `SELECT name FROM items LIMIT 7;`, compares the encoded bytes to the canonical request, checks undersized-buffer behavior, and verifies star and parameterized predicates are rejected.
- Verification: the targeted fixture and full maintained TinyCC SDK suite passed. Rust 1.95 locked workspace build, format check, warning-denied Clippy, and serial workspace tests passed (82 library tests and 129 commit tests passed; 3 cgroup-dependent commit tests and 1 cgroup-dependent process test were ignored). Generated ABI consistency and `git diff --check` passed.
- Remaining: independently decode and validate plans in the executor, re-resolve plan relation identities through executor storage, and execute supported plan operators.

## OP-3 dependency: independently decode basic plans in the C SDK — implemented

- Behavior: the C SDK independently decodes canonical optimizer plan v1 for `Scan -> Project -> optional Limit`.
- Boundary: it validates framing, nonzero stable relation/column IDs and epochs, ASCII identifiers, scalar type encodings, scan-column ordering/uniqueness, projection membership, node order, and trailing bytes. Relation names borrow the input payload; column records are copied into the result. Filter nodes return `UNSUPPORTED` until their bounded prefix trees receive independent structural and type validation.
- Regression: the TinyCC fixture checks a hand-constructed plan, borrowed names and copied column metadata, projection-reference rejection, filter rejection, trailing-byte rejection, and undersized output records.
- Verification: targeted fixture and full maintained TinyCC SDK suite passed. Rust 1.95 locked workspace build, format check, warning-denied Clippy, and serial workspace tests passed (82 library tests and 129 commit tests passed; 3 cgroup-dependent commit tests and 1 cgroup-dependent process test were ignored). Generated ABI consistency and `git diff --check` passed.
- Remaining: decode/type-check filter predicates, independently resolve plan IDs through executor storage, and interpret validated nodes through SDK calls.

## OP-3 dependency: validate bounded filter plans in the C SDK — implemented

- Behavior: `prdb_sdk_optimizer_decode_plan_with_filter_v1` accepts the full canonical plan v1 pipeline and returns a borrowed raw prefix predicate view alongside the decoded scan/project records.
- Boundary: the decoder validates predicate tags and operators, UTF-8/text limits, column IDs against the scan shape, comparison operand compatibility, Boolean fan-in and NOT operands, depth 32/node 256 bounds, and a Boolean root. The existing basic decoder keeps its stable output record and returns `UNSUPPORTED` for filtered plans; callers use the new explicit filter-view entry point.
- Regression: TinyCC fixtures cover a valid nested AND/NOT comparison, invalid root type, incompatible comparison operands, invalid UTF-8, filter absence, and output capacity errors.
- Verification: targeted fixture and full maintained TinyCC SDK suite passed. Rust 1.95 locked workspace build, format check, warning-denied Clippy, and serial workspace tests passed (82 library tests and 129 commit tests passed; 3 cgroup-dependent commit tests and 1 cgroup-dependent process test were ignored). Generated ABI consistency and `git diff --check` passed.
- Remaining: independently resolve plan IDs through executor storage, interpret validated nodes through SDK calls, and connect the plan decoder to optimizer submission results.

## OP-3 dependency: expose stable authorized plan relation metadata

- Behavior: the optimizer can resolve exact qualified names to a pinned schema/table identity and declared column shape without receiving a storage handle or executor capability.
- Implementation: planner operation `describe_plan_relation` (`0x1204`) calls the invocation-bound metadata resolver directly and returns schema ID/version, table ID/write epoch and authorized columns. It does not allocate a planner handle. The existing planner channel regression checks returned identities against the real-redb catalog; role tests reject the operation on the optimizer submission role.
- Verification: focused planner channel regression passed. Generated ABI consistency, locked workspace build, format, warning-denied Clippy, serial workspace tests, and `git diff --check` passed (129 commit integration tests passed; 3 cgroup-dependent tests ignored; 81 Rust unit tests passed).
- Remaining: production process launch now gives the optimizer separate submission and planner metadata sockets; executor callback integration, executor-side re-resolution, and SDK interpretation remain.

## OP-3 dependency: re-resolve stable plan identity through executor storage

- Behavior: the executor independently checks a decoded plan's qualified relation against the same invocation-pinned authorized catalog and returns stable schema/table/column identities without creating a storage handle.
- Implementation: executor operation `resolve_plan_relation` (`0x1105`) reuses the versioned qualified-name request and the planner `describe_plan_relation` response encoding. Shared identity assembly validates catalog schema metadata for both roles. Executor-role allowlisting permits the operation while planner-role use is rejected; a real-storage IPC regression verifies the pinned identity and that hidden/missing relations are indistinguishable. C helper `prdb_sdk_optimizer_validate_executor_relation_v1` re-decodes the full plan, requests this metadata, and checks all stable identities plus each scanned column's ID, ordinal, type parameters, and nullability.
- Regression: TinyCC fixture checks successful identity matching, stale epoch rejection, scanned column ID mismatch, unavailable relation, and undersized response handling.
- Verification: Rust workspace and TinyCC suite passed for the preceding IPC increment. The current C helper's focused fixture passed; full workspace/TinyCC validation and diff checks are pending for this increment.
- Remaining: resolve the executor scan capability only after identity match, then interpret the validated operators.

## OP-3 dependency: acquire executor relation capability after plan identity validation

- Behavior: a plan receives an ordinary executor relation handle only after its stable identity and scanned shape match metadata resolved from the invocation's authorized snapshot.
- Implementation: `prdb_sdk_optimizer_acquire_plan_relation_v1` invokes the independent metadata validator first, then calls executor `resolve_relation` (`0x1101`) and checks that the returned relation ID/name still match the plan before reporting success. It returns the normal connection-local executor handle and full declared column shapes for subsequent scan setup.
- Regression: TinyCC fixture verifies both IPC calls on success, the returned kind-6 handle and relation shape, and rejection if the capability response returns a different relation ID.
- Verification: focused and full maintained TinyCC SDK suites passed; generated ABI consistency, documentation link checks, and `git diff --check` passed.
- Remaining: open a scan using the validated capability and interpret bounded plan operators.

## OP-3 dependency: open a scan only after plan validation

- Behavior: the SDK validates a physical plan through executor metadata, acquires the ordinary executor handle, and opens its full scan. A zero LIMIT produces an empty result without creating a cursor.
- Implementation: `prdb_sdk_optimizer_open_plan_scan_v1` composes plan decoding, stable-identity comparison, executor relation resolution, and full-scan open. It returns the checked relation metadata and connection-local cursor token in caller-owned buffers.
- Regression: TinyCC fixture verifies the metadata and capability requests precede `open_scan`, the returned cursor has kind 7, and LIMIT 0 performs no cursor request.
- Verification: focused and full maintained TinyCC SDK suites passed; generated ABI consistency, documentation link checks, and `git diff --check` passed.
- Remaining: consume bounded batches, evaluate Filter, project rows, honor nonzero LIMIT, and emit SQL results.

## OP-3 dependency: consume projected rows from bounded scan batches

- Behavior: a bounded C executor runs an unfiltered plan over immutable storage batches, emits projection-ordered values, polls cancellation, stops at LIMIT, and closes early cursors. Filter plans fail as unsupported before scan creation.
- Implementation: `prdb_sdk_optimizer_execute_unfiltered_plan_v1` composes validated metadata/capability/scan setup with a 126-page budget so two IPC calls per page plus setup and an early close stay within the 256-request executor stream limit. The SQL optimizer bridge additionally spends one request checking the returned plan against the submitted AST, so its effective page ceiling is 125. It polls cancellation, applies the stateful projection/LIMIT batch consumer, and closes the cursor on cancellation or early stop. Values borrow the current batch only during the result callback.
- Regression: TinyCC fixture checks source-ordinal projection, callback value/type, limit completion, request order, cursor close after early LIMIT, cancellation abort/cleanup, and rejection of filtered plans.
- Verification: focused and full maintained TinyCC SDK suites passed; generated ABI consistency, documentation link checks, and `git diff --check` passed.
- Remaining at this increment: evaluate Filter predicates, encode and emit SQL results, and add a real executor channel integration covering the plan-driven batch path. Filter evaluation is implemented in the following increment.

## OP-3 dependency: evaluate bounded Filter plans over scanned rows

- Behavior: the SDK evaluates validated plan predicates against each immutable scanned row before projection. SQL three-valued logic is preserved, including short-circuit behavior for AND/OR; only TRUE rows are emitted. Predicate errors such as REDACTED inputs fail plan execution and trigger normal cursor cleanup.
- Implementation: `prdb_sdk_optimizer_consume_plan_batch_v1` decodes the bounded prefix tree, resolves scanned columns by stable ID and ordinal, evaluates scalar comparisons through the shared SQL evaluator, and integrates with the bounded executor scan loop. False and UNKNOWN rows do not count toward LIMIT.
- Regression: TinyCC fixture checks that a matching row emits, a nonmatching row is skipped, and the bounded executor completes a Filter scan with early LIMIT and cursor close.
- Verification: focused and full maintained TinyCC SDK suites passed; generated ABI consistency, documentation link checks, and `git diff --check` passed.
- Remaining: submit WHERE predicates through the SQL optimizer bridge and add a real process/channel execution fixture. SQLR row emission is implemented in the following increment.

## OP-3 dependency: connect optimizer submission to unfiltered SQL execution

- Behavior: a parsed explicit-projection SELECT is encoded and submitted through ABI v4, its successful plan is checked against the submitted relation/projection order/LIMIT and executor-authorized column names, and the unfiltered plan is run through bounded storage pages. Unsupported predicates return to the existing SQL path without latching statement failure; optimizer or execution failures latch failure.
- Implementation: `prdb_sdk_optimizer_execute_select_ast_v1` composes canonical request encoding, the dedicated optimizer callback, response decoding, query-to-plan semantic checks, and the bounded executor plan runner. The extra metadata request is included in the 125-page budget.
- Regression: TinyCC fixture submits parsed SELECTs, checks optimizer/executor callback order and one projected result, accepts case-folded relation/projection spellings, rejects a changed projection before scanning, verifies WHERE fallback makes no optimizer call, and checks that optimizer error status latches statement failure.
- Verification: focused and full maintained TinyCC SDK suites passed; generated ABI consistency, documentation link checks, and `git diff --check` passed.
- Remaining: submit WHERE predicates through the SQL optimizer bridge, encode/emit SQL results, and exercise the path with a real optimizer/executor IPC invocation. Returned unfiltered plans are compared with the submitted SELECT and emitted as SQLR rows; the bridge leaves WHERE queries on the established SQL path.

## OP-3 dependency: launch and confine the optimizer with each invocation

- Behavior: production START launches optimizer and executor as separate confined processes in one invocation cgroup. The optimizer receives only its submission and planner sockets; storage serves its planner socket alongside executor planner and storage channels. Either launch failure cleans up the other process and invocation resources.
- Implementation: `ExecutorLaunchConfig` and `prdb-supervisor --optimizer` select the optimizer executable. The launcher stages independent command policies, starts and reaps both children, and retains the optimizer endpoint in the executor. Storage drains three worker channels concurrently and aborts all peers on handler failure. The optimizer installs `no_new_privs` and the worker seccomp filter before serving.
- Verification: the serial workspace suite passed (82 library unit tests; 129 commit integration tests; three cgroup-dependent commit tests and one cgroup-dependent process test ignored). After the final child-reaping change, workspace build, formatting, warning-denied Clippy, and all four launcher unit tests passed. Generated ABI consistency and `git diff --check` passed. End-to-end confined launch remains unverified because the host has no delegated cgroup test fixture.
- Remaining: route an optimizer request from the executor ABI, validate returned plans again against executor storage, and interpret SELECT plans through SDK operations.


## OP-3 dependency: emit projected optimizer rows as SQLR records

- Behavior: the supported optimizer SELECT path writes each projected row as a provisional SQLR v1 record using executor-authorized column names and declared types. Result callback failures fail the statement so the invocation cannot commit partial output.
- Implementation: `prdb_sdk_sql_write_projected_result_v1` encodes and writes a projected single-relation row in host-sized chunks. `prdb_sdk_optimizer_execute_select_ast_to_result_v1` adapts bounded plan execution to that writer.
- Regression: TinyCC fixture executes a submitted SELECT through ABI v4, captures the SQLR record, checks its projection name/type/state/bytes, and verifies a failed result callback latches statement failure.
- Verification: focused and full maintained TinyCC SDK suites passed; generated ABI consistency, documentation link checks, and `git diff --check` passed.
- Remaining: submit WHERE predicates through the optimizer bridge and add a real process/channel execution fixture.


## OP-3 dependency: encode bounded SQL predicates in optimizer requests

- Behavior: callers can serialize parsed single-relation SELECT predicates and supplied positional values into the existing optimizer request v1 format without changing the ABI wire version. Unsupported forms remain eligible for the existing SQL path.
- Implementation: `prdb_sdk_optimizer_encode_select_with_parameters_v1` serializes comparisons, Boolean AND/OR/NOT trees, and Boolean/INTEGER/TEXT/NULL values into the bounded prefix representation expected by the Rust decoder. The compatibility encoder continues to reject WHERE trees.
- Regression: TinyCC fixture checks an exact comparison request payload and rejects missing parameters; unfiltered request encoding remains byte-for-byte stable.
- Verification: focused and full maintained TinyCC SDK suites passed; generated ABI consistency, documentation link checks, and `git diff --check` passed.
- Remaining: route the validated parameterized bridge through a real optimizer/executor process and channel integration.


## OP-3 dependency: submit and execute validated parameterized WHERE plans

- Behavior: supported parameterized SELECT predicates are submitted to the optimizer, and the SDK rejects a returned Filter unless its operator tree, scanned column names, comparison operators, and values match the submitted AST and parameters. Matching plans execute through bounded filtered scans and can emit SQLR rows.
- Implementation: `prdb_sdk_optimizer_execute_select_ast_with_parameters_v1` composes parameterized request encoding, ABI v4 optimizer submission, executor metadata validation, AST-to-Filter comparison, filtered batch execution, and sticky statement failure. `prdb_sdk_optimizer_execute_select_ast_with_parameters_to_result_v1` emits provisional SQLR records for matching rows.
- Regression: TinyCC fixture checks successful filtered submission/execution/result emission and rejects a plan whose literal differs from the supplied parameter. Unsupported predicates remain on the existing SQL path.
- Verification: focused and full maintained TinyCC SDK suites passed; generated ABI consistency, documentation link checks, and `git diff --check` passed.
- Remaining: exercise the path through the confined optimizer/executor processes and channels; add bounded mutation plans.

## OP-3 dependency: verify filtered optimizer process output against executor storage

- Behavior: a parameterized equality SELECT crosses the real optimizer subprocess submission and planner channels; the returned Filter retains its bound column identity and literal, and executor storage independently resolves the plan's qualified relation and scanned column shape over its own IPC channel. The fixture then obtains a normal relation handle, opens a scan using the plan's scanned column IDs, and consumes a finished bounded batch.
- Regression: `planner_stream_estimate_allowance_falls_back_without_aborting_invocation` submits both an unfiltered request and `value = 7` with LIMIT, checks the decoded Filter node, compares schema/table identity plus every scanned column's ID, ordinal, type, and nullability with executor metadata, and confirms the empty fixture table returns no rows and no continuation. The existing unavailable-relation check remains on the optimizer process stream.
- Verification: focused Rust process/channel integration and format check passed. Workspace build, formatting, and warning-denied Clippy passed; the full workspace test run hit `InvalidAudit` in `storage_manager_creates_and_drops_check_constraint`, which passed alone, and the serial commit integration suite passed (129 passed, 3 cgroup-dependent tests ignored). The new scan assertion is being verified separately.
- Remaining: run the C SDK callback, Filter evaluation, and result path against a real confined optimizer/executor invocation; add bounded mutation plans.
