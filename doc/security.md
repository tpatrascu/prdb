# Security & Authorization Model

PRDB enforces security at the **Storage Manager** boundary and isolates untrusted query execution at the **OS process** level.

## 1. Core Principles

- **Default-Deny with No Implicit Bypass**: Neither table ownership nor administrative roles grant an implicit data-query bypass. Every table and cell access requires both an explicit grant and passing policies.
- **Invocation Scope Intersection**: Clients submit a versioned `requested_scope` (`.psc1`). The Storage Manager intersects this scope with the authenticated principal's catalog grants at transaction start. A missing capability in the intersection denies the operation.
- **Pinned Catalog Dependencies**: Transactions record exact `CAT1` catalog records (including negative lookups) for principals, role closures, grants, policies, and schemas. Concurrent catalog changes abort conflicting writes atomically.

## 2. Granular Access Control

### Column-Level Grants
Privileges (`SELECT`, `INSERT`, `UPDATE`, `REFERENCES`) are granted per column:
```sql
GRANT SELECT (id, name, department_id) ON TABLE app.employees TO ROLE analysts;
```
New columns receive no automatic field grants.

### Row-Level Policies (`CREATE ROW POLICY`)
Row policies control whether a row is visible (`can_see_row`):
- Invisible rows are completely absent from the executor's view.
- **ALLOW** policies act as alternatives (at least one must pass); **RESTRICT** policies must all pass.
- For `UPDATE`s, an allowing policy must pass **both** the old-row and proposed-row predicates.

### Cell-Level Policies (`CREATE CELL POLICY`)
Cell policies control whether a specific field in a visible row is readable (`can_read_cell`):
- Readable cells carry their value or SQL `NULL`.
- Unreadable cells carry **`REDACTED`**, which is distinct from `NULL`.
- **Zero-Leakage Redaction**: `REDACTED` payloads contain no original value bytes, lengths, or storage offsets.
- **Computation Semantics**: Engine-managed operations requiring a redacted input fail with an authorization error rather than silently treating `REDACTED` as `NULL`.

## 3. Mutation Permissions

Read and write rights are distinct:
- **INSERT**: Requires table/column `INSERT` grants, cell policies for all non-generated fields, and a full-row creation policy.
- **UPDATE**: Requires row visibility, readability, and `UPDATE` permission for every changed cell. Storage preserves hidden unchanged fields and validates the complete new row.
- **DELETE**: Requires row visibility, an explicit table `DELETE` grant, and a passing row `DELETE` policy (cell readability is not required).

## 4. OS-Level Confinement

Because `query.so` is native code compiled by the client, PRDB treats the executor as **untrusted**:
- **Disposable Process**: One executor process per invocation; connection-local handles cannot be reused across invocations.
- **cgroups v2**: The Supervisor enforces `cpu`, `memory`, and `pids` limits per invocation under a delegated cgroup parent (`PRDB_CGROUP_PARENT`).
- **Landlock**: Restricts filesystem visibility of the executor and services to only the validated runtime profile paths.
