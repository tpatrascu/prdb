# Metadata Discovery APIs and SQL Views

## Contract

Expose bounded logical metadata needed to bind queries and inspect authorized definitions. Never expose raw catalog relations, physical addresses, credential verifiers, global statistics, or unrestricted dependency graphs through an executor connection. Discovery is not permission to access data.

Use the caller's pinned catalog/policy snapshot for ordinary executor and optimizer discovery. Administrative discovery uses an authenticated control request with an explicit request-scoped catalog snapshot and current administrative authority. If authority is revoked, invalidate that administrative cursor rather than serving later pages under stale privilege.

## Visibility Rules

Ordinary discovery requires schema USAGE and invocation scope to list a user schema. The reserved `information_schema` namespace is public and does not require a user grant. A user table is discoverable when it lies within its visible schema and the user has an applicable SELECT/INSERT/UPDATE/DELETE grant. An administrative control request may additionally discover schemas/tables over which its actor has administrative authority. Query discovery does not gain administrator privileges from the caller's role: privileged inspection still requires the control channel.

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

`InvocationTransaction::resolve_authorized_relation` resolves an exact relation name by walking the same filtered relation pages, then independently rechecks authority before returning the relation ID, name, and declared column shape. Empty or over-limit names are invalid requests; the combined description is capped at 1 MiB and is never partial. An inaccessible relation and an absent relation return the same unavailable result. The returned description is not a row-access capability; every later read must authorize against its own request context.

`InvocationTransaction::list_authorized_schemas` returns schemas with an effective USAGE grant in the admitted scope and always includes the reserved public `information_schema` namespace. Pages contain at most 128 schema IDs and names and at most 1 MiB, ordered by ascending stable schema ID. The continuation is transaction-local and binds the invocation snapshot and authorization inputs; other schemas without USAGE grants do not contribute names or total counts.

`InvocationTransaction::scan_information_schema_schemata` projects those same bounded schema pages into `information_schema.schemata` rows containing only `schema_name`, including the public reserved namespace. It reuses the schema cursor and filtering path, so other hidden schemas do not appear. SQL planner/executor integration for the built-in relation remains unimplemented.

`InvocationTransaction::scan_information_schema_tables_in_schema` returns bounded `information_schema.tables` rows for one exact schema name. For user schemas it resolves the name through schema USAGE-filtered discovery and applies the usual relation data-grant filter; hidden and nonexistent schema names return the same unavailable result, while an authorized empty schema returns an empty page. The public `information_schema` namespace exposes the three fixed virtual relations with `table_type = VIEW`. User relation rows expose `table_schema`, `table_name`, and `table_type = BASE TABLE`. Both paths preserve relation-page ordering and continuation. SQL planner/executor integration remains unimplemented.

`InvocationTransaction::scan_information_schema_tables` walks authorized schemas, including the public built-in namespace, and returns one bounded relation page per visible schema. Pages follow schema-ID then relation-ID order, contain at most 128 rows and 1 MiB, and use a separate 16-byte, one-use cursor bound to the invocation. An empty visible schema may yield an empty page with a continuation; hidden schemas and relations produce no rows.

`InvocationTransaction::scan_information_schema_columns` walks authorized schemas, relations, and their declared column pages in schema-ID, relation-ID, and ordinal order, including the fixed public shapes of the built-in relations. Each page is bounded to one relation's column page, at most 128 rows and 1 MiB, and uses a separate 16-byte, one-use cursor retaining the schema, relation, and column continuations. An authorized relation with no declared columns may yield an empty page while advancing; hidden schemas and relations produce no rows.

`InvocationTransaction::scan_information_schema_columns_for_relation` returns bounded rows for one exact visible schema and relation. For user schemas it applies the same schema and relation discovery rules and pages the authorized declared shape. For the public built-in namespace it resolves one of the three fixed relation shapes without requiring catalog rows or grants. Hidden and nonexistent user relations return the same unavailable result. `ordinal_position` is one-based; `is_nullable` is `YES` or `NO`. `data_type` uses the uppercase spelling of the declared built-in type (`CHARACTER VARYING` and `TIMESTAMP WITH TIME ZONE` for those types). `character_maximum_length` is present only for VARCHAR; `numeric_precision` and `numeric_scale` are present for integer and decimal types, with integer decimal precisions 5/10/19 and scale 0. All three parameters are NULL for other types.

Schema, relation, column, all-schema `information_schema.tables`, and all-relation `information_schema.columns` continuations serialize as fixed-width 16-byte opaque tokens. Byte 0 identifies the cursor kind and bytes 1–15 come from the operating-system random source. The active invocation retains each token's position and authorization binding server-side; tokens are one-use and modified, replayed, cross-invocation, or cross-kind tokens are rejected.

`InvocationTransaction::load_catalog_schema_usage_grants` derives schema USAGE grants for the admitted principal and its pinned role closure directly from the catalog. It returns no table or column privileges and retains the matching schema, grant, membership, and grantor records as commit guards.

`InvocationTransaction::list_catalog_authorized_schemas` is the invocation-bound entry point: it derives that grant set and role closure from the pinned catalog, constructs the discovery context internally, and returns the same bounded USAGE-filtered schema pages. Clients cannot supply another principal or widen the grant scope through this path.

`InvocationTransaction::load_catalog_relation_discovery_grants` derives only schema USAGE and table/column data grants for one schema and the admitted principal's pinned role closure. It validates target columns and grantors, and retains matching schema, table, column, grant, grantor, and membership records as commit guards. `InvocationTransaction::list_catalog_authorized_relations` builds its authorization context from those catalog-derived grants and returns the same bounded relation pages; it accepts no caller-supplied identity or grant scope.

`InvocationTransaction::resolve_catalog_authorized_relation` resolves a qualified schema and relation name using only the admitted invocation's pinned catalog and role closure. It returns the independently authorized declared shape or the same unavailable result for hidden and absent names. It does not return a row-access capability.

`InvocationTransaction::open_executor_metadata_connection` creates an executor-scoped resolver session. `ExecutorMetadataConnection::resolve_relation` returns an opaque 16-byte handle and the visible description; the handle is bound to that session's saved qualified name and stable relation ID. Byte 0 identifies the handle kind and bytes 1–15 are OS-random. Revalidation resolves the saved name through the pinned catalog again and compares the stable ID. Explicit close or dropping the session invalidates its handles. A session may issue at most 256 handles over its lifetime. Handles identify references only and never authorize scans or writes by themselves.

`InvocationTransaction::resolve_authorized_schema` resolves an exact schema name by walking those filtered pages. Empty or over-limit names are invalid requests. A schema without USAGE and an absent schema return the same unavailable result. The returned ID is a logical reference only and grants no access by itself.

Relation pages contain at most 128 records and at most 1 MiB of encoded relation IDs and names. The initial implementation orders relation IDs in ascending byte order. Its opaque continuation is valid only in the transaction that created it and is bound to that invocation snapshot, schema, authorization context, grants, and dependencies. An unavailable schema and a nonexistent schema return the same unavailable result. A relation that cannot fit by itself in a page returns a bounded response-size error. Serialized IPC cursors, SQL metadata scans, and administrative metadata endpoints remain future increments.

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

The built-in rows in `information_schema.tables` use `table_type = VIEW`. Their stable order is `columns`, `schemata`, then `tables`; the same order defines the logical relation IDs used by storage discovery.

The current engine slice implements storage-side `information_schema.schemata`, schema-scoped and all-visible-schema `information_schema.tables`, and relation-scoped and all-visible-relation `information_schema.columns` projections through their corresponding `scan_information_schema_*` methods. The three built-in relations have fixed public self-descriptions: `schemata(schema_name TEXT NOT NULL)`, `tables(table_schema TEXT NOT NULL, table_name TEXT NOT NULL, table_type TEXT NOT NULL)`, and `columns(table_schema TEXT NOT NULL, table_name TEXT NOT NULL, column_name TEXT NOT NULL, ordinal_position INTEGER NOT NULL, data_type TEXT NOT NULL, is_nullable TEXT NOT NULL, character_maximum_length INTEGER NULL, numeric_precision INTEGER NULL, numeric_scale INTEGER NULL)`. These shapes are supplied statically and do not query the views recursively. SQL binding/execution remains future work.

## Administrative Sections

Object owners or appropriately privileged administrators may inspect schema definitions, dependencies, constraints/index definitions, policies, and grant provenance for objects within their authority. Redact or omit references to objects outside their visibility rather than expanding access via a dependency edge. Full dependency diagnostics require authority over all revealed endpoints.

User/role administration requires the identity privileges defined in identity DDL. Token inventory returns nonsecret lifecycle metadata only; verifiers and secrets are never returned, including to database administrators. An administrative metadata response is bounded and auditable; it does not permit editing internal records directly.

## Validation

Test API/SQL discovery parity, restricted schemas, write-only principals, hidden versus nonexistent names, query predicates over metadata, empty tables versus hidden rows, continuation-token substitution, revocation between administrative pages, old snapshots after rename, and no secret expression text in ordinary output.
