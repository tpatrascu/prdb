# Metadata Discovery APIs and SQL Views

## Contract

Expose bounded logical metadata needed to bind queries and inspect authorized definitions. Never expose raw catalog relations, physical addresses, credential verifiers, global statistics, or unrestricted dependency graphs through an executor connection. Discovery is not permission to access data.

Use the caller's pinned catalog/policy snapshot for ordinary executor and optimizer discovery. Administrative discovery uses an authenticated control request with an explicit request-scoped catalog snapshot and current administrative authority. If authority is revoked, invalidate that administrative cursor rather than serving later pages under stale privilege.

## Visibility Rules

Ordinary discovery requires schema USAGE and invocation scope to list a schema. A table is discoverable when it lies within that scope and the user has an applicable SELECT/INSERT/UPDATE/DELETE grant. An administrative control request may additionally discover schemas/tables over which its actor has administrative authority. Query discovery does not gain administrator privileges from the caller's role: privileged inspection still requires the control channel.

For an admitted table, names, column ordinals, types, and nullability are bindable even when particular cells are unreadable. This reveals the declared shape intentionally, not data existence or values. Ordinary discovery omits defaults, check/policy expression text, ownership, grant provenance, and constraint/index details; these can encode protected literals or relationships. The optimizer separately receives approved logical access paths, without private counts, root IDs, or stored samples.

No catalog scan requires executing a table row policy to decide whether any row exists; metadata visibility is based on grants/scope, not data-dependent probing. Hidden and nonexistent objects return the same unavailable result where the caller lacks discovery authority. Error handling must not disclose a hidden namespace through suggestions or dependency names.

## API Surface

```text
list_schemas(page_limit, continuation?) -> visible_schema_page
list_relations(schema_ref, page_limit, continuation?) -> visible_relation_page
describe_columns(relation_ref, page_limit, continuation?) -> visible_column_page
resolve_relation(relation_ref) -> connection_local_handle, authorized_schema
admin_describe_object(object_ref, section, page_limit, continuation?) -> admin_page
```

The Rust storage slice `InvocationTransaction::list_authorized_relations` returns relation IDs and names from the invocation's pinned catalog. It requires schema USAGE and admits a relation when the invocation has an effective table-level SELECT, INSERT, UPDATE, or DELETE grant, or a column-level SELECT, INSERT, or UPDATE grant. It filters from catalog grants and scope without reading relation rows or evaluating row policies. It does not return defaults, policy text, ownership, grant provenance, schema versions, or hidden relation totals.

`InvocationTransaction::describe_authorized_columns` applies the same schema-usage and data-grant checks, then returns all declared columns for an admitted relation: stable column IDs, names, ordinals, scalar types, and nullability. Its single response is capped at 1 MiB; a shape that does not fit returns a response-size error rather than partial columns. The public paginated IPC adapter remains unimplemented.

`InvocationTransaction::describe_authorized_columns_page` returns at most 128 columns and at most 1 MiB per page, ordered by declared ordinal. Its continuation is valid only within the transaction that created it and binds the relation, pinned snapshot, authorization context, grants, and dependencies. It returns no partial column record when an individual column does not fit.

`InvocationTransaction::list_authorized_schemas` returns only schemas with an effective USAGE grant in the admitted scope. Pages contain at most 128 schema IDs and names and at most 1 MiB, ordered by ascending stable schema ID. The continuation is transaction-local and binds the invocation snapshot and authorization inputs; schemas without USAGE grants do not contribute names or total counts.

Relation pages contain at most 128 records and at most 1 MiB of encoded relation IDs and names. The initial implementation orders relation IDs in ascending byte order. Its opaque continuation is valid only in the transaction that created it and is bound to that invocation snapshot, schema, authorization context, grants, and dependencies. An unavailable schema and a nonexistent schema return the same unavailable result. A relation that cannot fit by itself in a page returns a bounded response-size error. Relation resolution and administrative metadata endpoints remain future increments.

Pages contain bounded typed records and opaque snapshot-bound continuation tokens. Tokens bind the connection, role, query/filter, and snapshot; they cannot resume against another principal or choose an older snapshot. Use deterministic logical ordering and do not return hidden total counts, ordinal gaps from hidden relations, or global catalog epochs. Column ordinal gaps caused by actual dropped columns may be visible as part of declared schema shape; they are not row counts.

Stable object IDs may be returned for visible objects; they convey no capability. Errors, expired cursors, changed administrative authority, cancellation, and response-size limits follow IPC rules. Recheck each resolution independently; do not accept optimizer-local handles in executor scans.

## Built-in SQL Metadata Relations

Reserve `information_schema` as a read-only built-in namespace. These relations are storage-provided virtual scans over filtered metadata, not user-created views and not a promise of full SQL-standard compatibility. Support the existing single-table SELECT/projection/predicate/LIMIT subset:

| Relation | Initial columns | Access |
| --- | --- | --- |
| `information_schema.schemata` | `schema_name` | Ordinary filtered discovery |
| `information_schema.tables` | `table_schema`, `table_name`, `table_type` | Ordinary filtered discovery |
| `information_schema.columns` | `table_schema`, `table_name`, `column_name`, `ordinal_position`, `data_type`, `is_nullable`, `character_maximum_length`, `numeric_precision`, `numeric_scale` | Ordinary filtered discovery |

```sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'app' AND table_name = 'employees';
```

Use SQL null only for genuinely inapplicable type parameters, not for redacted secrets. Filtering happens in storage before any user predicate or projection, so query rewrites cannot inspect hidden metadata rows. No user GRANT, policy alteration, INSERT, UPDATE, DELETE, or DDL is permitted on these virtual relations. Their fixed metadata-access rules are a reserved system interface, not an owner bypass for user tables.

Schemas/columns for the three built-in relations themselves are public interface definitions and appear consistently in discovery without recursively querying the same views. Privileged catalog sections use `admin_describe_object`, not extra secrets-bearing SQL views in executor processes. General user-created views remain deferred.

## Administrative Sections

Object owners or appropriately privileged administrators may inspect schema definitions, dependencies, constraints/index definitions, policies, and grant provenance for objects within their authority. Redact or omit references to objects outside their visibility rather than expanding access via a dependency edge. Full dependency diagnostics require authority over all revealed endpoints.

User/role administration requires the identity privileges defined in identity DDL. Token inventory returns nonsecret lifecycle metadata only; verifiers and secrets are never returned, including to database administrators. An administrative metadata response is bounded and auditable; it does not permit editing internal records directly.

## Validation

Test API/SQL discovery parity, restricted schemas, write-only principals, hidden versus nonexistent names, query predicates over metadata, empty tables versus hidden rows, continuation-token substitution, revocation between administrative pages, old snapshots after rename, and no secret expression text in ordinary output.
