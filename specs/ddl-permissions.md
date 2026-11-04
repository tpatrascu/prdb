# Permission Definition Language

## Model and Administrative Boundary

Permissions combine ordinary object/column grants with mandatory row and cell policies. Both layers default to denial. All tables enforce policies from creation; there is no enable/disable toggle or automatic owner/superuser bypass in query execution. An owner can administer policies and therefore is trusted to change access deliberately, but ownership alone is not a data-read grant.

Execute these statements only through the authenticated administrative channel described in [schema DDL](ddl-schema.md). Administration is independent of query invocations; the SDK exposes definitions/stubs, not embedded credentials or blanket capabilities. [Users and roles](ddl-identity.md) identify recipients.

## Ordinary Grants

```sql
GRANT USAGE ON SCHEMA app TO ROLE managers;
GRANT SELECT (id, department_id, manager_id, name, salary)
    ON TABLE app.employees TO ROLE managers;
GRANT UPDATE (salary) ON TABLE app.employees TO ROLE managers;
GRANT INSERT (department_id, manager_id, name, salary, active)
    ON TABLE app.employees TO ROLE loaders;
GRANT DELETE ON TABLE app.employees TO ROLE personnel_admins;
REVOKE UPDATE (salary) ON TABLE app.employees FROM ROLE managers;

GRANT MANAGE_USERS ON DATABASE TO USER account_admin;
GRANT CREATE ON SCHEMA app TO ROLE app_owner;
GRANT ALTER, INDEX, MANAGE_POLICIES, MANAGE_GRANTS
    ON TABLE app.employees TO ROLE app_owner;
```

Supported data privileges: schema `USAGE`; table/column `SELECT`, `INSERT`, `UPDATE`; table `DELETE`. A separate table/column `REFERENCES` privilege permits an administrator to define foreign keys against the specified target fields; it confers no SELECT access. Initial updates require both readable and writable changed cells. Read/write-only permission combinations must not silently expand each other. `SELECT ON TABLE` expands to the columns existing at grant time; future columns receive no implicit grants. The same explicit expansion applies to unqualified table `INSERT`/`UPDATE`/`REFERENCES` grants.

Schema `CREATE` permits creating tables and taking ownership. Object ownership includes `ALTER`, `INDEX`, `MANAGE_POLICIES`, and `MANAGE_GRANTS` on that object, not data privileges. Schema owners administer their schema, not automatic ownership of every table within it. `ALTER` covers rename/drop and ownership transfer; target owner existence and actor authority are checked. Only owners or `MANAGE_GRANTS` holders may change object grants; only owners or `MANAGE_POLICIES` holders may change policies. Only database administrators may grant system privileges. No grant option, implicit ownership transfer, or broad `ALL PRIVILEGES` shorthand initially.

Grant/revoke adds/removes the specified direct grant. Another inherited role grant can still provide authority; revoking one source is not an explicit deny. Policy restrictions provide mandatory intersections. Show grant provenance to authorized administrators.

## Policy Syntax

These are engine extensions, not standard SQL:

```text
CREATE ROW POLICY name ON schema.table
    FOR { SELECT | INSERT | UPDATE | DELETE }
    TO { USER name | ROLE name } [, ...]
    AS { ALLOW | RESTRICT }
    [USING (old_or_visible_row_expression)]
    [WITH CHECK (new_row_expression)];

CREATE CELL POLICY name ON schema.table (column [, ...])
    FOR { SELECT | INSERT | UPDATE }
    TO { USER name | ROLE name } [, ...]
    AS { ALLOW | RESTRICT }
    [USING (old_or_visible_row_expression)]
    [WITH CHECK (new_row_expression)];

DROP ROW POLICY name ON schema.table;
DROP CELL POLICY name ON schema.table;
```

`SELECT` and `DELETE` require `USING` only; `INSERT` requires `WITH CHECK` only; `UPDATE` requires both explicitly. `DELETE` has no cell policy because it removes the row. Bare field names bind to the visible/old row in `USING`, and the proposed full row in `WITH CHECK`; cross-old/new references are deferred. Policy names are unique per table across row/cell kinds. Replace a policy through atomic drop-and-create in an administrative batch; no missing-policy window is visible.

Predicates are bounded, typed, side-effect-free expressions using comparisons, Boolean operators, null checks, literals, and trusted context values. Initially expose `CURRENT_PRINCIPAL_ID()` and `HAS_ROLE('role_name')`; role names bind to catalog IDs at definition time. No caller-writable session variables, SQL subqueries, arbitrary joins, native code, network/filesystem access, or volatile functions. Runtime expression error fails closed and aborts the operation; null/unknown is not an allow decision.

Storage may inspect unreadable row fields to evaluate trusted policies. Those fields remain protected from the executor, optimizer callbacks, errors, and logs. The policy compiler records dependencies; query updates cannot change a policy-driving field on an existing row. Insert-time values require creation policies or trusted defaults. This restriction deliberately limits ownership reassignment until a privileged reassignment operation is designed.

## Composition and Enforcement

For each relevant row or cell operation, consider policies matching the principal's pinned user/role closure:

```text
policy_pass = (OR of applicable ALLOW results)
              AND (AND of applicable RESTRICT results)
```

No applicable `ALLOW` means false; no `RESTRICT` means true. Only predicate result true passes. For an update, each allowing policy must pass both its old and new predicates; do not combine an old-row grant from one policy with a new-row grant from another. All applicable restrictive policies must also pass both. Column lists are expanded into per-column policy targets; one column's allow does not authorize another.

- Row visibility: admitted scope, schema `USAGE`, at least one relevant SELECT field grant, and row `SELECT` policy pass.
- Readable cell: visible row, field `SELECT` grant, and cell `SELECT` policy pass. A declared but denied field in a visible row yields `REDACTED`, distinct from null; no values or lengths escape.
- Insert: admitted write scope, row `INSERT` policy pass, and insertion grants plus cell `INSERT` policy pass for every inserted non-generated field, including fields filled by defaults or implicit nulls. Trusted generated identities are exempt from a field INSERT grant/cell policy but still subject to row checks. No SELECT permission is implied by successful insertion; any later read is independently authorized.
- Update: visible old row, per-changed-cell read and UPDATE grants/policies, row `UPDATE` policy pass, and per-changed-cell `UPDATE` policy pass. Storage validates the full resulting row; hidden unchanged cells are preserved internally.
- Delete: visible old row, table DELETE grant, and row DELETE policy pass. No requirement to reveal every cell to delete the row.

Schema visibility is separate from cell readability: an admitted table may expose its declared column names/types to bind expressions, but not hidden values or policy text. A fully unauthorized relation returns a non-disclosing unavailable response. Exact column-existence errors and unsupported expressions follow the same authorization order across SDK and direct IPC paths.

## Complete Read/Update Example

Assume the table from schema DDL, provisioned user IDs in `manager_id`, and a `managers` role populated through identity DDL. This example makes employee names visible to managers and salaries visible/updatable only for their own direct reports:

```sql
GRANT USAGE ON SCHEMA app TO ROLE managers;
GRANT SELECT (id, department_id, manager_id, name, salary)
    ON TABLE app.employees TO ROLE managers;
GRANT UPDATE (salary) ON TABLE app.employees TO ROLE managers;

CREATE ROW POLICY employee_rows ON app.employees
    FOR SELECT TO ROLE managers AS ALLOW USING (TRUE);
CREATE CELL POLICY employee_identity ON app.employees (id, department_id, manager_id, name)
    FOR SELECT TO ROLE managers AS ALLOW USING (TRUE);
CREATE CELL POLICY own_salary_read ON app.employees (salary)
    FOR SELECT TO ROLE managers AS ALLOW
    USING (manager_id = CURRENT_PRINCIPAL_ID());
CREATE ROW POLICY own_employee_update ON app.employees
    FOR UPDATE TO ROLE managers AS ALLOW
    USING (manager_id = CURRENT_PRINCIPAL_ID())
    WITH CHECK (manager_id = CURRENT_PRINCIPAL_ID());
CREATE CELL POLICY own_salary_write ON app.employees (salary)
    FOR UPDATE TO ROLE managers AS ALLOW
    USING (manager_id = CURRENT_PRINCIPAL_ID())
    WITH CHECK (manager_id = CURRENT_PRINCIPAL_ID());
```

The same salary column is readable for some visible rows and redacted for others. No insert/delete authority results from these policies. To restrict row visibility by department, a later context-attribute extension may supply an authenticated department ID; do not substitute a user-supplied query parameter as authority. The initial policy language supports direct ownership/manager predicates without that extension.

## Metadata Interfaces

Use [metadata discovery](metadata-discovery.md) for exact schema visibility and inspection rules. Ordinary admitted-table discovery reveals declared column shape but omits expressions, ownership, and private constraints/index details. Built-in `information_schema` scans apply fixed storage-side filtering and reject grants, policy edits, and mutations. They do not provide access to raw system catalogs or live cross-user activity.

Credential verifiers and secrets are never returned by inspection, including administrative inspection. [Operational metadata](operational-metadata.md) limits global monitoring/audit to database administrators and keeps live observations outside invocation SQL snapshots.

## Versioning, Metadata, and Validation

Grants, role membership, and policies are MVCC catalog objects. Administrative changes atomically advance relevant authority/dependency epochs. New invocations use the new state; existing reads retain bounded snapshots, while pending write commits revalidate epochs and abort if relevant authority changed. Administrative actors' permissions are checked again at their own commit. Strong immediate read revocation remains deferred.

Ordinary catalog access reveals only admitted names/types and permitted logical access paths. Policy text, grant provenance, memberships, internal index diagnostics, global statistics, and nonsecret credential lifecycle metadata require explicit administrative authority; secrets and credential verifiers are never catalog-readable. DDL logs and explain output cannot expose protected row values.

Test default denial, missing row/cell policies, multiple roles, restrictive intersections, null predicates, update old/new pairing, hidden predicate inputs, new columns receiving no grant, immutable policy-driving fields, defaults during insert, owner queries without grants, and changes racing write commit.

Background: [PostgreSQL CREATE POLICY](https://www.postgresql.org/docs/current/sql-createpolicy.html) provides a useful distinction between old-row conditions and new-row checks. This engine adds mandatory cell policies, explicit clause requirements, and no query-owner bypass.
