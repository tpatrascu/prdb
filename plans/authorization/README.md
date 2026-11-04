# Authorization Implementation Plan

## Contract and Dependencies

Implement trusted storage-side [row/cell semantics](../../specs/authorization.md) for SQL and direct IPC alike. Depend on [identity/context](../ddl-identity/README.md), [permission definitions](../ddl-permissions/README.md), [catalog](../data-dictionary/README.md) and [transaction validation](../concurrency-mvcc/README.md). Definition parsing belongs to permission DDL; runtime evaluation and sanitized access belong here.

## Decisions Before Implementation

Fix bounded typed policy IR, trusted context inputs, runtime error/null behavior and dependency tracking. Effective authority intersects caller permissions with invocation scope; ownership is administrative authority, not query access. Unsupported external policy inputs remain rejected until version pinning/revalidation exists. Define expression evaluation/error ordering consistently with SQL, optimizer and storage pushdown.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| AU-1 | Table-driven reference cases deny missing grants/ALLOW policies and combine ALLOW alternatives with every RESTRICT | Bounded trusted evaluator over pinned user/role/context; true alone allows, null/errors never allow; no owner/admin bypass |
| AU-2 | Two authenticated users see different rows/cells; denied variable-length values leave no bytes or lengths | Select MVCC/private version first, enforce row visibility then per-cell checks, and emit VALUE/NULL/REDACTED into sanitized IPC buffers |
| AU-3 | Hidden-value predicates, index probes and redacted required inputs cannot influence managed results improperly | Validate restricted expressions/access paths after authorization; compare optimized/batched access with independent per-cell/reference scans |
| AU-4 | Patching a readable/writable cell preserves hidden unchanged fields and checks old/new policy pairs | Storage builds full rows; one applicable ALLOW must pass both update checks, changed fields need read plus write rights, policy-driving fields remain immutable |
| AU-5 | Insert defaults/implicit nulls require grants/cell policies except generated identities; delete needs explicit row-delete rights | Apply defaults then creation policy/constraints, independently authorize later reads and prevent scope widening through self-modification |
| AU-6 | Policy/membership/token/schema changes race commit and abort stale writes while admitted reads retain bounded snapshots | Record authoritative dependency epochs/base versions and revalidate under serialized publication; no external inputs without a contract |
| AU-7 | Bypassing SDK, forging row handles or losing cache invalidation cannot widen access | Real hostile IPC clients in sandbox, complete cache validity domains and sanitized non-disclosing diagnostics |

## Acceptance and Handoff

Use real control-created users/policies rather than hard-coded production fixtures. Compare byte output and engine-managed errors across hidden-data variations, with constraint/conflict outcomes and timing inference documented as limits. Already delivered values cannot be revoked; immediate read revocation remains deferred. First behavior: an independent policy-composition truth table including no-ALLOW, null, restrict and update-pairing denial.
