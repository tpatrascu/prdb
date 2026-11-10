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

## Progress

- **AU-1: in progress.** The storage module has a bounded typed expression evaluator, a transitive membership-closure resolver (4096-edge bound, cycle/duplicate rejection), principal/role recipient matching, separate grant and invocation-scope checks, deny-by-default ALLOW/RESTRICT composition, SQL NULL behavior, and same-policy old/new UPDATE pairing. Integration cases cover missing grants/policies, restrictive intersections, role/principal recipients, NULL, expression errors/type mismatch, expression and membership limits. Grants, edges, and policies are still supplied as trusted inputs; catalog-derived loading and authenticated admission are not implemented yet.
- **AU-2: in progress.** `InvocationTransaction::read_authorized` chooses the pinned/private row, decodes it with a supplied pinned schema, requires schema USAGE and a relevant column grant before row visibility, applies row policy before cell checks, and returns VALUE/NULL/REDACTED cells. Real-redb coverage verifies principal-dependent redaction of a variable-length value and durable abort on policy evaluation error. The caller still supplies authority state; there is no authenticated control-created catalog path or IPC serialization test.
- **AU-4: in progress.** `stage_patch_authorized` checks table visibility/UPDATE plus SELECT and UPDATE grants/policies for every changed cell against the old and full proposed rows. It preserves unchanged hidden cells and rejects updates to the policy-driving columns supplied from the pinned policy dependency set. Real-redb tests cover authorized patch, hidden-field preservation, and dependency-field rejection. Insert/delete authorization and catalog-derived dependency loading remain absent.
- **AU-5: in progress.** `stage_insert_authorized` requires a fully materialized row, checks row creation plus every non-generated cell, and exempts trusted generated columns only from cell checks. `stage_delete_authorized` requires visible-row access and an explicit table DELETE grant/policy without requiring cell readability. Real-redb cases cover generated-field exemption, missing insert/delete grants, and atomic abort. Server default materialization, identity generation, and constraints are not implemented.
- **AU-6: in progress.** Authorized operations accept exact catalog dependency keys/bytes and verify them against their pinned snapshot before use; dependencies are automatically included in the final commit guards. A real-redb race changes the principal authority record after an authorized patch and confirms the data write aborts. The dependency list is still supplied by the caller; schema/policy/grant loading and complete dependency discovery are not implemented.
- **AU-3: partial.** `InvocationTransaction::probe_catalog_unique_index` supports a restricted unique equality case after pinned schema/index validation, refuses hidden or conditionally readable key cells so callers can use the reference scan, reconciles snapshot candidates with staged values/deletes, and returns only authorized sanitized rows. A catalog-derived principal entry point exists. It is not routed from SQL/optimizer execution, and broader redacted-predicate error equivalence is unverified. AU-7 remains not started; hostile service-boundary tests remain.
