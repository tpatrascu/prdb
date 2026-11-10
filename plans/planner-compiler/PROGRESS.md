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

- Implementation: [`optimizer_plan`](../../engine/src/optimizer_plan.rs) creates a linear scan/filter/project/limit plan with schema ID/version, relation ID/write epoch, and stable column references. The scan reads the unique ordinal-sorted union of projection and predicate columns; output projection order and duplicates are preserved. The plan contains no planner handle, grant, or commit operation.
- Validation: `validate_plan_v1` checks the pipeline shape, relation identity, unique sorted scan columns, projection membership, filter references/types, and predicate depth/node/fan-in/text bounds. The SDK and executor must repeat plan validation after serialized process transfer is added.
- Regression: five unit tests verify pipeline construction and order, scan-column union, rejection of malformed order/duplicate limits, and rejection of non-Boolean, unscanned, or over-deep filters.
- Verification: focused optimizer plan tests passed. Locked workspace build, format, warning-denied Clippy, serial workspace tests, and `git diff --check` passed (129 commit integration tests passed; 3 cgroup-dependent commit tests and 1 process test ignored).
- Remaining: canonical plan encoding/decoding, executor-side independent resolution and interpretation, SDK integration, mutation plans, and optimizer process/channel integration.
