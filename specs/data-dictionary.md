# Data Dictionary and System Catalog

## Ownership and Scope

The Rust storage manager owns the authoritative data dictionary. Catalog records describe logical objects, identities, permissions, and dependencies; they are not executor-accessible ordinary tables. Administrative DDL is the only external mutation path. Public metadata is a filtered projection defined in [metadata discovery](metadata-discovery.md).

Separate four metadata classes: transactional logical catalog; protected physical/recovery metadata; derived statistics/caches; and live operational state. They have different visibility, persistence, and consistency contracts. See [physical metadata](physical-metadata.md), [catalog lifecycle](catalog-lifecycle.md), and [operational metadata](operational-metadata.md).

## Common Record Model

Use opaque 128-bit object IDs, scoped to a persistent database UUID. IDs do not encode allocation order and are never reused after drop. Columns, policies, principals, constraints, and indexes have identities independent of names. Built-in identities are reserved and established by bootstrap. Internal versions use a separate monotonic commit sequence; do not expose that sequence through ordinary metadata APIs.

Every versioned record has a record-format tag, logical key, begin/end commit visibility, and typed payload. Private administrative changes remain invisible until commit. A tombstone closes the logical object's lifetime while old snapshots can still resolve its previous definitions. Persistent encoding uses explicit field tags/lengths, never Rust memory layouts; [catalog payload v1](catalog-payloads.md) fixes the envelope, family codes, typed fields, and canonical keys.

## Logical Catalog Relations

The following names describe internal record sets, not supported SQL tables. Listed keys are logical keys before the MVCC version suffix.

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

Bootstrap catalog indexes use trusted built-in definitions and the same storage publication guarantees. Missing/corrupt mandatory catalog records are fatal recovery errors, not reasons to silently recreate users or loosen permissions.

## Dependencies and DDL

Record dependencies during definition validation, in the same administrative transaction as the object. Drop/alter traverses reverse edges using a bounded deterministic walk. Recheck the traversed definitions and epochs at commit so concurrent DDL cannot introduce an unseen dependency. Large graphs can fail with an explicit work-limit error; never proceed with an incomplete dependency check.

Ownership edges identify objects removed with a parent, such as a table's own indexes/policies. Reference edges block RESTRICT drops, such as another table's foreign key. Column removal is blocked by applicable policies, expressions, and keys. Role/user removal checks ownership, grants, memberships, and policy recipients. Follow [schema DDL](ddl-schema.md) for exact drop behavior.

## Snapshots and Cache Invalidation

Maintain separate schema/write, authority, and statistics validity domains. Administrative commit atomically publishes new records, dependency changes, and affected epochs with its durable decision. Rows and catalog records visible to one snapshot must belong to the same committed history.

Cache immutable decoded definitions by database UUID, object ID, and definition version. Cache authorization/filtered metadata additionally by principal, effective grant, role/policy snapshot, and visibility dependencies. Plan/estimate caches include schema versions and transaction-local write generation where needed. A cache entry is never an authorization capability.

Commit notifications may evict entries proactively, but correctness cannot depend on receiving a notification. Check validity against the pinned context on lookup and current epochs at write/admin commit. Old pinned snapshots may continue using old definitions; epoch changes do not rewrite their view. Bound memory and hold version pins for in-flight work. Reclaim definitions only when relevant snapshots/tasks drain and no retained stored row version needs that schema to decode its fields or resolve an add-column default. See [storage format](storage-format.md).

## Acceptance Tests

Test rename without rebinding, drop/recreate with distinct IDs, old readers after DDL, atomic catalog/data publication, dependency races, missed invalidation notifications, role closure changes, cross-principal cache reuse, secret-free discovery, and corrupt catalog recovery. Compare indexed catalog lookups with a simple MVCC reference scan.
