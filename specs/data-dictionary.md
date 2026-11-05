# Data Dictionary and System Catalog

## Ownership and Scope

The Rust storage manager owns the authoritative data dictionary. Catalog records describe logical objects, identities, permissions, and dependencies; they are not executor-accessible ordinary tables. Administrative DDL is the only external mutation path. Public metadata is a filtered projection defined in [metadata discovery](metadata-discovery.md).

Separate four metadata classes: transactional logical catalog; protected physical/recovery metadata; derived statistics/caches; and live operational state. They have different visibility, persistence, and consistency contracts. See [physical metadata](physical-metadata.md), [catalog lifecycle](catalog-lifecycle.md), and [operational metadata](operational-metadata.md).

## Common Record Model

Use opaque 128-bit object IDs, scoped to a persistent database UUID. IDs do not encode allocation order and are never reused after drop. Columns, policies, principals, constraints, and indexes have identities independent of names. Built-in identities are reserved and established by bootstrap. Internal versions use a separate monotonic commit sequence; do not expose that sequence through ordinary metadata APIs.

Each catalog record has a record-format tag, logical key, and typed payload. redb supplies snapshot visibility for prior values; catalog payloads do not encode begin/end commit visibility. Private administrative changes remain invisible until commit, and an old snapshot can still resolve definitions removed from the current catalog. Object lifecycle and non-reuse rules apply independently of redb's physical deletion. Persistent encoding uses explicit field tags/lengths, never Rust memory layouts; [catalog payload v1](catalog-payloads.md) fixes the envelope, family codes, typed fields, and canonical keys.

## Logical Catalog Relations

The following names describe internal record sets, not supported SQL tables. Listed keys are logical keys; redb provides snapshot visibility without an engine MVCC version suffix.

| Record set | Primary key | Required payload / relationships |
| --- | --- | --- |
| `database` | database UUID | Name, catalog version, built-in identity mapping, default configuration references |
| `objects` | object ID | Kind, parent namespace/table ID, normalized name, owner principal ID, definition epoch, lifecycle state |
| `schemas` | schema ID | Object reference, default name-resolution configuration |
| `types` | type ID | Built-in kind, parameter rules, comparison/encoding version, supported defaults/casts |
| `tables` | table ID | Schema ID, schema/write epoch, logical storage-map reference, stable row-identity policy |
| `columns` | column ID | Table ID, ordinal, name, type ID/parameters, nullable, default expression ID, generation mode |
| `expressions` | expression ID | Typed bounded IR, IR version, resolved object/column dependencies, purpose |
| `constraints` | constraint ID | Table ID, kind, ordered source columns, check expression or referenced key, action/null rules, validation state |
| `indexes` | index ID | Table ID, ordered columns, method, uniqueness/null rules, owning constraint, logical root reference, validity state |
| `principals` | principal ID | User/role kind, name, login state, authority epoch; stable ID used by authentication |
| `memberships` | member ID, role ID | Granting actor, membership version; directed edges with cycle checks |
| `grants` | grantee, object, privilege, field target | Direct grant provenance and epoch; field target is an explicit column ID or object scope |
| `policies` | policy ID | Table ID, row/cell kind, operation, ALLOW/RESTRICT, old/new expression IDs |
| `policy_targets` | policy ID, target kind, target ID | Recipient user/role IDs and optional expanded column targets |
| `dependencies` | dependent ID, referenced ID, edge kind | Ownership, expression reference, foreign-key target, backing-index, or authority dependency |
| `credential_verifiers` | token ID | User ID, verifier/algorithm version, expiry, revocation, label; protected authentication-only storage |

Parent `objects` records exist for named logical objects, including principals; child records must agree with their kind and lifetime. Composite grants and dependency rows need no separate public object identity. Constraint/index lifecycle is initially private BUILDING then atomically VALID, or discarded on failure; committed DROPPED definitions remain readable only to old snapshots.

Store policy recipient lists, ordered constraint/index columns, and expanded grants explicitly or in bounded child records. Do not hide unbounded lists in unchecked blobs. Plaintext credentials, user source code, and query parameters do not belong in the dictionary. Operational outcomes and statistics have separate stores described by their own specs.

## Catalog Indexes and Integrity

Provide version-aware lookups by object ID; `(namespace, kind/name-domain, normalized name)`; `(table ID, column name/ordinal)`; principal name; direct membership in both directions; grants by grantee/object; policies by table/operation; and dependency edges in both directions. Enforce the SQL naming domains, including shared user/role names and schema-local relation/index names. Built-in namespace reservations are enforced before user object creation.

Resolve each lookup at the invocation's catalog snapshot, not the latest catalog version. Validate referential integrity at catalog commit: no dangling owners/types/columns, duplicate live names, cycles in membership, invalid constraint references, or half-published table/index definitions. Default expressions and policies bind IDs, not names to be re-resolved later.

The storage engine can resolve a table's ordered schema by reading the pinned table record, scanning bounded column records for that table, sorting by ordinal, and resolving each referenced type record. Each column's own type parameters must be decoded and validated even when it shares a type ID with another column. It rejects gaps, duplicates, unsupported type profiles, invalid type parameters, and columns introduced after the table's schema version. This initial family scan is capped at 65,536 catalog records; exceeding the cap fails closed. The loader guards the table, matching column records, and referenced type records at final commit. DDL that adds, drops, or changes a column must also advance the table schema version atomically so a concurrent schema load cannot miss a new column.

Membership closure and grant reads use pinned membership, grant, and principal records. Any membership edge change for an existing member or direct grant change for an existing grantee must atomically advance that principal's authority epoch; otherwise the catalog transaction is rejected. Readers guard the records they use, so these changes cannot leave a stale role closure or grant set authorizing a write. Policy definition or target changes on an existing table must advance the policy authority epoch and the owning table write epoch in the same catalog transaction; constraint definition changes must also advance the owning table write epoch. These table epochs close phantom-definition windows for invocations that loaded their policy or constraint sets from a pinned snapshot.

The 4,096-edge membership limit applies to the final graph of an atomic catalog batch. Replacing an edge at the limit is valid in either submitted write order; a final graph above the limit is rejected.

The catalog publication bound of 4,096 target records per policy likewise applies to the final target set, after staged additions and removals. This bound does not replace the separate recipient, column, expression, and authorization validation requirements.

Every change to an existing principal record must strictly advance its authority epoch, even when no membership or grant changes in the same transaction. Restoring an older epoch could reproduce a previously guarded record while newly added authority edges remain present. An unchanged record is a no-op. Principal ID non-reuse after deletion requires separate lifecycle enforcement; monotonic updates alone do not establish it.

An existing table's schema version and write epoch never decrease, and a changed table record must advance at least one of them. In particular, a standalone update may not restore an older table record after a column, policy, or constraint change. An unchanged table record is a no-op. Table ID non-reuse after deletion remains a separate lifecycle requirement.

Catalog commits validate constraint definitions against the final catalog view: kind-specific required/absent fields, existing owning table and source columns, nonempty and cardinality-compatible ordered column lists, action/match codes, NOT NULL agreement with column nullability, CHECK expression purpose, primary/unique backing-index ownership and key shape, FK target-key and referenced-column agreement, and VALID state. Changes to tables, columns, expressions, constraints, or indexes revalidate all surviving constraints, with a 65,536-record family cap; removal of a referenced definition must remove or update its dependents atomically. Invocation loading recognizes NOT NULL only when its source column is non-nullable, and tuple encoding enforces that property for writes. This reference gate does not replace full DDL authorization, complete catalog graph validation, or runtime enforcement of unique and foreign-key constraints; those remain required before the corresponding DDL is exposed.

Bootstrap catalog indexes use trusted built-in definitions and the same storage publication guarantees. Missing/corrupt mandatory catalog records are fatal recovery errors, not reasons to silently recreate users or loosen permissions.

## Dependencies and DDL

Record dependencies during definition validation, in the same administrative transaction as the object. Drop/alter traverses reverse edges using a bounded deterministic walk. Recheck the traversed definitions and epochs at commit so concurrent DDL cannot introduce an unseen dependency. Large graphs can fail with an explicit work-limit error; never proceed with an incomplete dependency check.

Ownership edges identify objects removed with a parent, such as a table's own indexes/policies. Reference edges block RESTRICT drops, such as another table's foreign key. Column removal is blocked by applicable policies, expressions, and keys. Role/user removal checks ownership, grants, memberships, and policy recipients. Follow [schema DDL](ddl-schema.md) for exact drop behavior.

## Snapshots and Cache Invalidation

Maintain separate schema/write, authority, and statistics validity domains. Administrative commit atomically publishes new records, dependency changes, and affected epochs with its durable decision. Rows and catalog records visible to one snapshot must belong to the same committed history.

Cache immutable decoded definitions by database UUID, object ID, and definition version. Cache authorization/filtered metadata additionally by principal, effective grant, role/policy snapshot, and visibility dependencies. Plan/estimate caches include schema versions and transaction-local write generation where needed. A cache entry is never an authorization capability.

Commit notifications may evict entries proactively, but correctness cannot depend on receiving a notification. Check validity against the pinned context on lookup and current epochs at write/admin commit. A write invocation records the exact catalog bytes for each definition/epoch it uses, including absence for a lookup that must remain absent; final commit compares those guards with the latest catalog in the same serialized write transaction. A mismatch durably aborts the invocation without publishing its data writes. The access layer must record every schema and authority dependency it relies on; generic guards do not derive that dependency set or authorize an operation. Old pinned snapshots may continue using old definitions; epoch changes do not rewrite their view. Bound memory and hold version pins for in-flight work. Reclaim definitions only when relevant snapshots/tasks drain and no retained stored row version needs that schema to decode its fields or resolve an add-column default. See [redb storage](redb-storage.md).

## Acceptance Tests

Test rename without rebinding, drop/recreate with distinct IDs, old readers after DDL, atomic catalog/data publication, dependency races, missed invalidation notifications, role closure changes, cross-principal cache reuse, secret-free discovery, and corrupt catalog recovery. Compare indexed catalog lookups with a simple MVCC reference scan.
