# Storage Manager

## Responsibilities and Trust

The Rust storage manager is the sole database authority and the only process that opens the embedded [redb backend](redb-storage.md). It owns SQL semantics, typed row/catalog records, invocation transactions, authorization, constraints, private overlays, logical indexes, outcomes, and budgets. redb owns physical B+trees, page allocation, copy-on-write, physical reclamation, and crash recovery.

The executor is hostile. Authorization does not depend on a plan, index path, or SDK validation. No native client code runs inside storage. The supervisor provisions identities and requests finalization; only storage decides and durably records transaction outcomes.

## Access and Authorization

Resolve logical table/column IDs through the versioned catalog. At admission, pin a redb read transaction and authenticated schema/policy context. Each statement reads the pinned view overlaid with that invocation's staged writes. Before producing results, storage applies row visibility, predicate/cell authorization, and redaction semantics; encode only authorized values into bounded output.

Writes are private field patches until finalization. Storage checks readable/writable changed cells, row policies, constraints, and hidden unchanged-field preservation. At commit it revalidates row versions and authority epochs against the latest redb state. Indexes are access paths only; every candidate still passes visibility and permission checks.

## Commit and Concurrent Invocations

Allow concurrent redb read transactions and concurrent engine staging. Do not hold a redb write transaction across user execution or individual SQL statements. At successful library return, the supervisor requests finalization; storage obtains the serialized writer, validates all conflicts/constraints/authority, applies data/index/catalog/audit/outcome changes in one redb write transaction, and commits using the selected durable mode.

Any statement error aborts the whole invocation. Cancellation accepted before redb commit begins prevents publication. Once commit begins, report pending/too-late until storage establishes the durable outcome. On ambiguous failures, recover/reconcile; never infer ABORTED from a missing reply. See [MVCC](concurrency-mvcc.md) and [redb mapping](redb-storage.md).

## Catalog and Metadata

The engine owns the [data dictionary](data-dictionary.md), [catalog payloads](catalog-payloads.md), DDL, users, roles, grants, and true row-dependent cell policies. These logical records live in redb tables, not redb's inaccessible physical system tables. Public metadata and planning estimates remain permission-filtered. Required administrative audit records commit atomically with changes. After service authentication, owner-scoped durable outcome lookup is provided by `StorageManager::lookup_invocation_outcome` for lost-reply reconciliation.

## Resource and Process Boundaries

Bound active redb snapshots, transaction age, private overlay bytes, scans, output, and finalization work. `StorageManager` construction validates the recovered READY image before exposing admission; this validates storage integrity but does not authenticate a service connection. `begin_token_authenticated_invocation` verifies the versioned random-token profile using trusted UTC time and pins the verifier, user, and role-membership records for commit revalidation. `issue_token` uses the system CSPRNG, returns its secret once, and stores only its verifier; `revoke_token` uses the authorized identity DDL path. `list_tokens` reads a single snapshot and returns nonsecret metadata only, allowing self-inventory or cross-user inventory with `MANAGE_USERS`/database-admin authority. The identity-based admission method is only for a trusted caller that has already authenticated the user. The manager also requires a positive configured active-invocation limit and age, shares the snapshot cap across cloned handles, and reserves a slot before pinning the redb snapshot. Managed transactions expire at read/write/finalization checks; services must poll `check_deadline` while work is idle so expired transactions release their snapshot and admission slot. A long analytical reader can retain redb pages and delay reclamation; scheduling must protect operational transactions. Do not hold redb guards while waiting for network or executor output.

Only the gateway handles client TCP. Only the supervisor launches and owns executor processes. The executor receives local scoped IPC and sanitized values, never redb handles/descriptors. The optimizer receives authorized schema and scoped estimates, never raw database tables.

## Validation

Test redb snapshot/overlay semantics, concurrent staging and serialized finalization, same-row conflicts, distinct-row commits, write skew, atomic row/index/catalog/outcome updates, policy changes racing commit, hidden-field preservation, restart/outcome reconciliation, malformed IPC, long-reader pressure, and identical authorization behavior for scans and index lookups.
