# Optimizer Progress

## OP-1 dependency: bounded structured SELECT request decoder

- Contract: [optimizer specification](../../specs/planner-compiler.md). The first request payload is a versioned, single-relation SELECT with explicit projections, optional bounded predicate tree, and optional LIMIT.
- Implementation: [`optimizer_request`](../../engine/src/optimizer_request.rs) decodes identifiers, record lengths, predicate tags and values into owned Rust data. It caps requests at 64 KiB, predicates at 16 KiB, projections at 128, text literals at 4,096 bytes, predicate nodes at 256, depth at 32, and boolean fan-in at 16. Unknown versions/statements/nodes, invalid booleans/operators/identifiers, truncation, and trailing bytes fail closed.
- Boundary: the decoder performs structural validation only. It has no storage access and does not bind names or types, authorize relations, choose plans, or grant execution authority. The C parser and worker channels do not call it yet.
- Regression: four unit tests cover a compound typed request, malformed outer records/identifiers, excessive collection bounds and unsupported nodes, and predicate byte/node/depth exhaustion.
- Verification: locked workspace build and format passed; warning-denied Clippy passed; serial locked workspace tests passed (129 commit integration tests and all other active suites passed; 3 cgroup-dependent commit tests and 1 disconnected-submission process test were ignored). `git diff --check` passed.
- Remaining: define and validate type semantics against authorized metadata, return only non-disclosing bind failures, produce and validate a logical/physical plan, integrate a bounded isolated optimizer process and its supervisor-bound submission channel, and execute plans through SDK operations.
