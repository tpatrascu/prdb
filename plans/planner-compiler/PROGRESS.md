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

## OP-3 dependency: expose stable authorized plan relation metadata

- Behavior: the optimizer can resolve exact qualified names to a pinned schema/table identity and declared column shape without receiving a storage handle or executor capability.
- Implementation: planner operation `describe_plan_relation` (`0x1204`) calls the invocation-bound metadata resolver directly and returns schema ID/version, table ID/write epoch and authorized columns. It does not allocate a planner handle. The existing planner channel regression checks returned identities against the real-redb catalog; role tests reject the operation on the optimizer submission role.
- Verification: focused planner channel regression passed. Generated ABI consistency, locked workspace build, format, warning-denied Clippy, serial workspace tests, and `git diff --check` passed (129 commit integration tests passed; 3 cgroup-dependent tests ignored; 81 Rust unit tests passed).
- Remaining: production process launch now gives the optimizer separate submission and planner metadata sockets; executor callback integration, executor-side re-resolution, and SDK interpretation remain.

## OP-3 dependency: launch and confine the optimizer with each invocation

- Behavior: production START launches optimizer and executor as separate confined processes in one invocation cgroup. The optimizer receives only its submission and planner sockets; storage serves its planner socket alongside executor planner and storage channels. Either launch failure cleans up the other process and invocation resources.
- Implementation: `ExecutorLaunchConfig` and `prdb-supervisor --optimizer` select the optimizer executable. The launcher stages independent command policies, starts and reaps both children, and retains the optimizer endpoint in the executor. Storage drains three worker channels concurrently and aborts all peers on handler failure. The optimizer installs `no_new_privs` and the worker seccomp filter before serving.
- Verification: the serial workspace suite passed (82 library unit tests; 129 commit integration tests; three cgroup-dependent commit tests and one cgroup-dependent process test ignored). After the final child-reaping change, workspace build, formatting, warning-denied Clippy, and all four launcher unit tests passed. Generated ABI consistency and `git diff --check` passed. End-to-end confined launch remains unverified because the host has no delegated cgroup test fixture.
- Remaining: route an optimizer request from the executor ABI, validate returned plans again against executor storage, and interpret SELECT plans through SDK operations.
