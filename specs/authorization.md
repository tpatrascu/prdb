# Authorization and Cell Semantics

## Required Model

Support true cell-specific permissions: access to a field may differ between visible rows. Row visibility and cell readability are separate decisions.

```text
# Conceptual, language-neutral interfaces.
can_see_row(context, table, row) -> bool
can_read_cell(context, table, row, column) -> bool
```

Example: a manager can see employee names throughout their department but salaries only for direct reports. Employees outside the department may be entirely invisible.

## Policy Evaluation

The trusted storage manager evaluates policies using the execution context and relevant row attributes. Policies may inspect protected attributes without disclosing them to the worker. An ACL per cell is not required; ownership, memberships, classifications, and explicit grants can derive decisions.

Effective authority is the intersection of the caller's permissions and the execution grant. The worker cannot choose its principal or enlarge this grant. Trusted policy code must be bounded; arbitrary native policy callbacks are not part of the design.

## Grants and Policies

The [permission DDL](ddl-permissions.md) defines mandatory row/cell policies plus object/column grants. Effective access also requires the invocation scope and pinned [user/role](ddl-identity.md) identity. Every applicable ALLOW policy is an alternative; all applicable RESTRICT policies must pass, with no ALLOW meaning denial. For updates, an allowing policy must pass both old-row and new-row checks. Ownership grants administration, never an implicit executor data bypass.

Permission DDL also defines authorized schema visibility, metadata exposure, defaults, and per-column grant expansion. New columns receive no automatic field grants. These rules apply equally to SQL and direct storage requests.

## Authorized View

- Invisible rows are absent from the worker's view.
- Readable cells carry either a value or SQL `NULL`.
- Unreadable cells carry `REDACTED`, distinct from `NULL`.
- Redacted payloads contain no original value bytes, lengths, or storage offsets.

Redaction deliberately reveals that a field exists but is unavailable. Row handles are scoped references, not physical addresses or proof of authority.

## Computation Semantics

Query semantics operate over the authorized view. Hidden raw values cannot influence worker-visible predicates, sorting, joins, grouping, or aggregates through a storage-side operation. Authorization must precede any untrusted computation.

Proposed baseline: an engine-managed operation requiring a redacted input fails with an authorization error. Do not silently treat redaction as null or omit such cells. `SUM(salary)` over partially redacted salaries must not appear to be a complete total if aggregates are added; aggregates are outside the initial SQL subset. An explicit future operation may mean “sum readable salaries.”

This semantic rule governs engine execution and APIs; a malicious worker cannot be forced to report an error or compute honestly from the authorized data it already has. It must still never receive hidden values.

Do not expose diagnostics identifying hidden values or invisible rows. Define precise expression evaluation and error behavior before supporting complex SQL, including short-circuiting and partial results.

## Planning Statistics

Planning statistics obey the same effective grant as execution. The planner may receive summaries of the authorized view, including separate readable/null/redacted populations, but no global statistics or estimates based on hidden values. Scalar estimates, physical plans, diagnostics, and executor results can all disclose information. Runtime planning does not generate native artifacts in the baseline. See the [statistics service](statistics-service.md) for collection and disclosure rules.

Readable-only statistics are planning metadata, not a change to aggregate query semantics. They do not authorize silently dropping redacted cells from a query result.

## Mutation Permissions

Read and write rights are distinct. Initially an update requires row visibility and readability plus write permission for every changed cell. Inserts require insertion grants and cell policies for all non-generated inserted fields, including defaults, plus a full-row creation policy; deletes require visible-row access and an explicit row-delete grant. A writable cell is not permission to replace the entire row. Storage preserves hidden unchanged fields and validates the complete new row itself.

Mutation predicates and expressions must not inspect redacted values. Check both the old row and proposed row against applicable trusted policies. The invocation grant cannot expand through self-modification: policy-driving fields of existing rows are immutable through query writes initially, and inserts use trusted defaults or explicit creation-policy checks for those fields. Policy/ACL administration is outside user query writes and uses a trusted versioned control path. Use the MVCC snapshot plus prior private writes for data visibility, but use authoritative pinned policy/membership inputs for authorization. Record those dependencies and revalidate their epochs/versions at commit as specified in [concurrency and MVCC](concurrency-mvcc.md). A changed dependency aborts the write transaction.

Constraint/conflict errors omit protected values and identities, but their success/failure may still convey information. Primary/unique and foreign-key constraints are supported by schema DDL; successful/failed globally constrained writes may reveal hidden key existence. Do not claim that masking diagnostics alone eliminates this inference surface.

## Snapshots and Revocation

Proposed baseline: pin a consistent data/schema snapshot and policy context, including memberships and other policy inputs, for a bounded invocation lifetime. Sequential SQL statements and direct storage calls share this context and see successful prior private writes; finishing a statement does not refresh the committed snapshot or grant. MVCC selects visible versions before evaluating row and cell permissions. The mechanism for pinning external policy inputs remains open and must be defined before supporting them. Immediate read revocation is deferred; it would require invalidating access and terminating affected workers. Write commits separately validate that their policy dependencies have not changed, preventing stale permissions from authorizing new committed writes. Already delivered data cannot be revoked.

Cache authorization only with sufficient identity, grant, policy, and snapshot information to prevent stale or cross-principal reuse.

## Validation and Open Questions

Test mixed permissions within a column, null versus redaction, invisible rows, scope narrowing, policy changes, and attempted predicates over hidden values. Compare batched enforcement against a simple per-cell reference evaluator.

Open questions include explicit redaction inspection, further mitigation of error-based inference, richer policy expressions/context attributes, privileged reassignment, and controlled aggregates that intentionally use hidden data. Initial composition and schema visibility are defined in permission DDL.
