# Schema Definition Language

## Scope and Execution

This document defines proposed SQL-like schema DDL, not an implemented parser or PostgreSQL compatibility promise. Users and roles are defined in [identity DDL](ddl-identity.md); row/cell rules are defined in [permission DDL](ddl-permissions.md).

DDL runs through a separately authenticated administrative SDK/control channel. The client may parse text, but storage independently parses or validates a bounded structured definition and checks administrative privileges. Query executors cannot call this channel or execute DDL inside their invocation transaction. Definitions in the SDK confer no authority.

Each administrative submission contains one statement or an explicit atomic batch of DDL statements; it cannot mix user native execution or DML. Catalog changes, identity changes, and permissions share storage's durable commit ordering. Administrative requests have IDs, budgets, cancellation, and outcome lookup, just like transaction finalization.

## Names, Schemas, and Types

Initially one database per storage-manager instance, containing named schemas. `CREATE DATABASE`, federation, and cross-database references are future extensions.

`CREATE SCHEMA` is available through the v1 `schemas.create` authenticated control operation for normalized lowercase ASCII names of at most 128 bytes, with an existing user or role assigned as owner. The caller must have inherited database-scoped `CREATE_SCHEMA` or be a database administrator. Creation publishes the schema record, owned object record, audit event, and operation outcome in one commit. `DROP SCHEMA ... RESTRICT` is available through `schemas.drop`; the schema owner (including members of an owning role) or a database administrator may drop an empty schema. System schemas and schemas with child objects or remaining catalog dependencies are rejected. Storage checks emptiness again in the serialized write transaction before atomically deleting the schema and owned object records. The control operations accept structured arguments rather than SQL text; quoted mixed-case names, rename, and ownership transfer remain unimplemented.

The storage manager's initial `create_table` path accepts a structured schema ID, normalized table name, and 1–4096 ordered columns. Each column has a normalized name, one built-in scalar type, nullability, optional UUID generated-identity mode, and optional typed literal, NULL, or `CURRENT_PRINCIPAL_ID` default. Defaults are stored as immutable catalog expressions and evaluated by the existing insert path. Generated UUID identity cannot also have a default. It requires inherited `CREATE` on the schema or database-administrator membership, publishes the table, ordered columns, default expressions, owned-child dependencies, audit event, and outcome atomically, and rejects a duplicate table name in that schema. New tables receive no data grants. Built-in system schemas are reserved. The authenticated `tables.create` control operation exposes this structured path; SQL parsing, constraints in the same CREATE statement, and SDK integration remain separate work. Add constraints through the existing authorized constraint DDL path.

`ALTER TABLE ... ADD COLUMN` is available through `tables.add_column` with a table ID and one column definition. The table owner (including inherited membership in an owning role) or a database administrator may add a column to a non-system table. The new column is appended, receives no implicit grants, and cannot be a generated identity. A NOT NULL column requires a valid typed literal default; a nullable column may omit the default, which backfills NULL. `CURRENT_PRINCIPAL_ID` is rejected because a backfill must use one immutable value for all existing rows. The literal is recorded as both the initial insert default and immutable add-column default. Storage advances schema version and write epoch, rewrites every persisted tuple while preserving row IDs, and updates index definitions and row locators atomically under the table DDL fence. The rewrite is limited to 65,536 rows and 64 MiB of combined old and new tuple payloads; larger changes abort without publication. Existing snapshots retain the previous schema and rows.

`ALTER TABLE ... OWNER TO ...` is available through `tables.set_owner` with a table ID and the destination owner ID. The current table owner or a database administrator may transfer a non-system table to a live user or role. The table object's owner and object version change atomically with the table write epoch; tuple schema version, rows, indexes, and grants remain unchanged. Ownership gives the new owner table-level DDL authority, but grants no SELECT/INSERT/UPDATE/DELETE privileges. Existing owners and authorized roles may still transfer ownership while they retain authority.

`DROP TABLE ... RESTRICT` is available through `tables.drop`. The table owner (including inherited membership in an owning role) or a database administrator may request it. Built-in system schemas cannot be dropped through this operation. Incoming catalog dependencies reject the drop. In one authorized commit, storage removes the table, owned columns, constraints, indexes, policies, default/check/policy expressions, corresponding grants and dependency edges, row data, and physical index entries. The row scan is bounded by 65,536 rows and 64 MiB of row payloads per commit; larger drops fail without changing the catalog or data. Storage rechecks incoming dependencies in the serialized DDL path, and the normal catalog conflict checks protect the discovered delete set.

`ALTER TABLE ... DROP COLUMN ... RESTRICT` is available through `tables.drop_column` with table and column IDs. The table owner (including inherited membership in an owning role) or a database administrator may request it; built-in system schemas are rejected. Constraints, expressions, indexes, and policies that reference the column block the operation. Storage removes the column's own default expression, dependent grant records, and dependency edges, compacts surviving column ordinals, increments schema version and write epoch, and rewrites persisted tuples and surviving index keys in one fenced commit. The row scan is bounded by 65,536 rows and 64 MiB of combined old and rewritten tuple payloads; larger drops fail without publication. Existing rows retain their surviving values and row identities.

`ALTER TABLE ... RENAME TO ...` is available through `tables.rename` using a table ID and normalized new name. The table owner (including inherited membership in an owning role) or a database administrator may rename a non-system table. Name uniqueness within the schema is checked in the serialized final catalog. The stable table ID and tuple schema version do not change; storage increments the table write epoch and object version so stale writers are rejected while existing row tuples and index keys remain valid.

```sql
CREATE SCHEMA app AUTHORIZATION app_owner;
ALTER SCHEMA app RENAME TO application;
ALTER SCHEMA application OWNER TO app_owner;
DROP SCHEMA application RESTRICT;
```

Unquoted identifiers fold to lowercase; double-quoted identifiers preserve case. Require schema-qualified names in administrative definitions and stored dependencies. Resolve client unqualified names against an explicit admitted default schema, never an uncontrolled search path. Catalog record layouts, indexes, and dependencies are defined in [data dictionary](data-dictionary.md). Object and column identities are stable catalog IDs; renames do not retarget dependencies. New objects never reuse a dropped object's identity.

Initial scalar types: `BOOLEAN`, `SMALLINT`, `INTEGER`, `BIGINT`, `DECIMAL(p,s)` with `1 <= p <= 38` and `0 <= s <= p`, `TEXT`, `VARCHAR(n)`, `BYTEA`, `UUID`, `DATE`, and `TIMESTAMP WITH TIME ZONE`. Integers are signed with checked overflow. Decimal is exact; implicit lossy conversions fail. Text is valid UTF-8 with a deterministic binary collation initially; `VARCHAR(n)` limits Unicode scalar count. Timestamps normalize to UTC at microsecond precision. [redb storage](redb-storage.md) defines storage scalar encodings and supported DATE/TIMESTAMPTZ ranges; SQL input parsing and coercion still must enforce these same ranges. Arrays, JSON, enums, custom types, and locale collations are deferred.

## Tables, Defaults, and Constraints

```sql
CREATE TABLE app.employees (
    id UUID GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    department_id UUID NOT NULL,
    manager_id UUID NOT NULL,
    name TEXT NOT NULL,
    salary DECIMAL(12,2),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT salary_nonnegative CHECK (salary >= 0)
);
```

`UUID GENERATED ALWAYS AS IDENTITY` is this engine's extension: storage allocates an opaque non-sequential ID, rejects explicit overrides, and checks collisions. Every table has a stable internal identity even when its SQL primary key is user-supplied. No global sequence values are exposed by default.

Catalog-backed executor INSERT materializes generated identities only from DEFAULT, rejects caller-supplied identity values, and checks UUID collisions against the serialized final table state at commit. Updates reject patches to generated identity columns. The lower-level materialized-row helper remains a trusted internal boundary.

Supported constraints:

| Definition | Semantics |
| --- | --- |
| `NOT NULL` | Reject a null value. |
| `PRIMARY KEY (a, b)` | At most one per table; all fields non-null and the tuple unique. |
| `UNIQUE (a, b)` | Enforce tuple uniqueness; rows with any null key field do not conflict initially. |
| `CHECK (expression)` | Bounded deterministic row-local expression; false fails, true or SQL unknown passes. Use `NOT NULL` separately. |
| `FOREIGN KEY (...) REFERENCES schema.table (...)` | Referenced fields must name a primary/unique key in the same order and have identical built-in scalar types, including declared parameters. `MATCH SIMPLE`: any null referencing field exempts that row. |

Constraints operate on full storage values, not redacted projections. Only built-in typed expressions are allowed; no native functions, subqueries, or external callbacks. Defaults are typed literals or fixed trusted built-ins, initially generated identity and `CURRENT_PRINCIPAL_ID()`. Defaults are applied before creation policies and constraints, but do not bypass field/row permissions. `CURRENT_PRINCIPAL_ID()` is a server value, never a parameter chosen by the query.

For `CHECK`, evaluate the trusted, schema-bound, deterministic row-local expression over the complete proposed tuple. Accept TRUE and SQL UNKNOWN; reject FALSE. Evaluation is bounded and type/runtime errors abort the invocation. CHECK additions validate all existing rows before publication, within the row and byte limits in [permission DDL](ddl-permissions.md). A CHECK definition or any schema inputs used to bind it must be included in the transaction's guarded catalog dependencies.

Primary, unique, and foreign-key constraints are part of the initial schema design. Defining a foreign key requires source-table ALTER/creation authority and REFERENCES authority on the target key. Foreign keys initially support only `ON UPDATE RESTRICT` and `ON DELETE RESTRICT`; cascading actions and deferrable constraints are deferred. User-defined native constraints remain forbidden. `StorageManager::commit_authorized_foreign_key_ddl` authorizes FK add/drop on existing tables, and `StorageManager::commit_authorized_constraint_ddl` authorizes `NOT NULL`, CHECK, PRIMARY KEY, and UNIQUE add/drop through ownership or inherited ALTER. CHECK additions bind to the final table schema and validate existing rows before publication. PRIMARY KEY and UNIQUE additions atomically backfill their owned unique indexes from persisted rows; PRIMARY KEY columns must already be non-nullable. These routes accept structured catalog changes from the separately authenticated administrative caller, not SQL text from an executor.

Stage-time checks can reject obvious failures early. Before committing, storage validates uniqueness and referential integrity against the latest committed state plus the transaction's final private changes under commit serialization. Creating a foreign key also validates all existing non-NULL child tuples against the referenced key set before publishing the constraint. Validate both child insert/update and parent delete/key-change directions, including concurrent transactions. Abort the whole invocation on violation. This protects these declared constraints but does not make general snapshot-isolated transactions serializable.

The initial reference-scan implementation bounds combined persisted and staged row visits across foreign-key validation to 65,536 per commit and the encoded comparison-key sets for one FK check to 16 MiB. Exceeding either limit aborts the whole invocation; implementations must not skip validation or partially commit to satisfy the budget. An indexed validation strategy may replace the reference scans while preserving the same final-state and concurrency guarantees.

Constraint outcomes can reveal that a hidden conflicting/referenced key exists. Do not return protected values or row identities in diagnostics; nevertheless this logical inference is an explicit limitation of exposing globally constrained writes. Definitions and grants must be reviewed with that implication; redaction cannot eliminate it.

## Alter and Drop

```sql
ALTER TABLE app.employees ADD COLUMN title TEXT;
ALTER TABLE app.employees RENAME COLUMN title TO job_title;
ALTER TABLE app.employees ALTER COLUMN active SET DEFAULT TRUE;
ALTER TABLE app.employees ALTER COLUMN salary SET NOT NULL;
ALTER TABLE app.employees ADD CONSTRAINT employee_name UNIQUE (department_id, name);
ALTER TABLE app.employees DROP CONSTRAINT employee_name;
ALTER TABLE app.employees DROP COLUMN job_title RESTRICT;
ALTER TABLE app.employees RENAME TO staff;
ALTER TABLE app.staff OWNER TO app_owner;
DROP TABLE app.staff RESTRICT;
```

Support nullable column addition, typed literal defaults, rename, default changes, `SET/DROP NOT NULL`, `SET/DROP DEFAULT`, constraint add/drop, ownership transfer, and restricted column/table drop. Adding a non-null field requires a valid literal default and validating existing rows. Existing rows acquire the add-column default as of that DDL change; later default changes affect future inserts only. Context-dependent defaults are not allowed for backfilling existing rows. Type-changing rewrites, computed columns, partitioning, temporary tables, user-created views, triggers, stored procedures, explicit sequences, and `TRUNCATE` are deferred.

Changing nullability on an existing column is available through `tables.alter_column_nullability` with a table ID, column ID, and string `nullable` value (`"true"` or `"false"`). It preserves the tuple schema version because its encoded layout does not change; the table write epoch advances to invalidate stale writers. `SET NOT NULL` scans current persisted rows under the exclusive table fence and aborts with a constraint violation if any value is NULL. Validation is bounded to 65,536 rows and 64 MiB of row-record bytes per commit; larger tables fail without publication. `DROP NOT NULL` removes the matching internal constraint when present and marks the column nullable.

`ALTER TABLE ... RENAME COLUMN` is available through `tables.rename_column` with table ID, column ID, and normalized new name. The table owner (including inherited ownership through a role) or a database administrator may rename a column on a non-system table. The column ID, ordinal, type, nullability, defaults, dependencies, and tuple schema version remain unchanged; the table write epoch and column object version advance. The family 2 object name and family 6 column name update atomically, and table-local column-name uniqueness is checked against the serialized final catalog.

`tables.set_column_default` sets or replaces a current insert default on a non-generated column. PREPARE supplies `{ "table_id": id, "column_id": id, "type": type, "default": { ... } }`; the default uses the same typed literal, `null`, or `current_principal_id` forms as `tables.create`. The table owner (including inherited ownership through a role) or a database administrator may change defaults on a non-system table. The default mask and declared scalar type are checked, including UUID-only `CURRENT_PRINCIPAL_ID`; existing rows are not rewritten. `tables.drop_column_default` removes the current default. Both operations atomically update the column's current-default expression reference and table write epoch, while preserving schema version and the immutable add-column default reference. Replaced or dropped expression objects and column dependency edges are removed in the same commit. Setting an identical default or dropping an absent default is rejected.

Default drop mode is `RESTRICT`: dependencies prevent removing an object still referenced elsewhere. Dropping a column is blocked by its policies, constraints, and indexes. Dropping a table removes its owned column/index/constraint/policy definitions atomically, but is blocked by external references such as another table's foreign key. Dropping a schema requires it to be empty. No implicit cross-object cascade is supported. `IF EXISTS`/`IF NOT EXISTS` may suppress existence errors only after checking authority; they never reconcile incompatible definitions. Adding a column grants no field privileges automatically.

## Indexes

```sql
CREATE INDEX employees_department ON app.employees USING BTREE (department_id, name);
CREATE UNIQUE INDEX employees_unique_name ON app.employees (department_id, name);
DROP INDEX app.employees_department RESTRICT;
```

Index names live in the table's schema. Initial indexes are B-trees on one or more plain columns, ascending with nulls last. A unique index uses the same null/commit-validation semantics as `UNIQUE`. Primary/unique constraints own backing indexes; their indexes cannot be dropped independently while required. Expression, partial, covering, full-text, custom access methods, and `CREATE INDEX CONCURRENTLY` are deferred.

For ordinary index creation, removal, and rebuild, the storage manager accepts an actor identity only from a trusted authenticated administrative caller, then independently verifies that the actor is an enabled user with table ownership, an inherited owner role, an `INDEX` grant, or membership in the reserved database-administrator role. The commit guards the actor, role-membership, grant, and table-owner records used by that decision so concurrent revocation or ownership changes abort stale DDL. This narrow route handles index-definition and matching index-object records; every other administrative action needs its own permission check before control-channel exposure.

An index is an internal access path, not an authorization boundary or raw data API. Entries must resolve to MVCC-visible row versions and be subject to identical row/cell checks as scans. An index-only read is allowed only if storage can enforce those checks and snapshot visibility. Index definitions/size/statistics are disclosed only through authorized catalog/optimizer surfaces.

## DDL Concurrency and Recovery

Use per-table/schema DDL fences plus the serialized redb writer for final publication. Initial table alterations, constraint validation, and index builds block affected-table write commits while building/validating; unrelated tables and pinned readers can continue. Do not hold the redb writer during a long scan; build against a pinned snapshot and revalidate before publishing. Acquire multiple DDL fences in a deterministic object-ID order and apply deadlines to waits.

The Rust `StorageManager` provides coordinated invocation and catalog-commit entry points that share one per-database fence manager. The manager provides shared guards for table writers and exclusive guards for DDL. It sorts and deduplicates multi-table requests, admits each request for all tables atomically, gives waiting DDL priority over new writers on affected tables, and releases guards through RAII. A timeout leaves no partial fence held. Raw storage commit helpers remain unfenced trusted primitives; runtime paths must use the coordinated entry points.

After taking a fence, capture the latest committed table version, build/validate privately, then atomically publish the catalog and physical changes. Foreign-key creation fences both referencing and referenced tables during validation/publication. A coordinated index build scans a pinned read snapshot, rechecks its row keys and versions under the serialized writer, and atomically publishes the complete index with its catalog record. A coordinated index rebuild derives old and new keys from the pinned rows, validates the complete existing forward membership set and matching row/reverse locators, checks uniqueness, and replaces changed memberships with the updated definition atomically. A coordinated index drop validates the complete forward membership set and matching row/reverse locators, then removes both membership directions with the catalog record in the same transaction. Index DDL is bounded to 65,536 scanned rows, 65,536 created, removed, or replaced memberships, and 64 MiB of accounted row/key data per commit; oversized operations fail without publication. Creating or rebuilding an index on a populated table cannot be combined with a schema change to that table in the same DDL operation. Transactions based on an older affected-table schema/write epoch must abort rather than commit incompatible writes after the fence is released. Uncommitted private writes are not included in a build or rebuild. An index becomes usable only after durable complete publication; failed builds and rebuilds leave the prior catalog and physical index state intact. Already-admitted transactions never acquire the new index just because it exists physically; catalog snapshot visibility still applies.

Old read snapshots retain compatible catalog definitions and physical versions until their pins drain, including after rename/drop. Stored row versions also retain the schema definitions and add-column defaults needed for decoding until rewritten/reclaimed. New snapshots see the new schema; plans/handles are never silently rebound. Retain retired tables/indexes until no snapshot can use them, or explicitly abort an invocation under retention pressure. Initially use a conservative table schema/write epoch for any relevant DDL change.

Physical key/value limits come from the pinned [redb API](redb-storage.md). Keep the logical column/scalar limits only after validating they fit that API. Index key ordering follows [key encoding](index-key-encoding.md), adapted to the chosen redb key type. Oversize values reject atomically; never truncate keys.

Policy and membership changes need epoch publication and write revalidation, not full-table scans. A policy change taking a policy-driving field into use also freezes that field for future query updates. Durable recovery must restore the catalog, constraints, index validity, and data at one committed state.

## Discovery and Format Evolution

Built-in `information_schema` relations are fixed filtered storage interfaces defined in [metadata discovery](metadata-discovery.md), not user-created views. Reserve system namespaces against user DDL. Catalog bootstrap and storage-format upgrades are not ordinary ALTER statements; follow [catalog lifecycle](catalog-lifecycle.md). Diagnostic operational state is exposed separately from snapshot catalog discovery.

## Validation and References

Test duplicate definitions, dependency rejection, default/null semantics, simultaneous duplicate keys, both directions of foreign-key races, failed constraint validation, DDL versus pending writers, old snapshots after drop, crash during index publication, and inaccessible catalog diagnostics.

Conventional syntax references: [PostgreSQL CREATE TABLE](https://www.postgresql.org/docs/current/sql-createtable.html) and [CREATE INDEX](https://www.postgresql.org/docs/current/sql-createindex.html). Engine-specific authorization and concurrency semantics above take precedence.
