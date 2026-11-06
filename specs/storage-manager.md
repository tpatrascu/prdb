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

The engine owns the [data dictionary](data-dictionary.md), [catalog payloads](catalog-payloads.md), DDL, users, roles, grants, and true row-dependent cell policies. These logical records live in redb tables, not redb's inaccessible physical system tables. Public metadata and planning estimates remain permission-filtered. Required administrative audit records commit atomically with changes. After service authentication, `StorageManager::get_invocation_status` returns owner-scoped live ACTIVE/COMMITTING state and falls back to retained durable outcomes after completion or restart. `StorageManager::list_own_invocations` returns bounded pages of only that owner's live entries; each continuation is owner- and owner-generation-bound and fails if one of that owner's live entries changes between pages. `StorageManager::list_runtime_invocations` provides current database administrators bounded pages of all live invocation entries after revalidating their authority on every request. Its cursor is bound to the administrator and process-local registry generation; no global count is returned. Unknown, cross-owner, and expired point lookups are indistinguishable. Registry state is best-effort, process-local, and shared by live manager handles for one redb `Database` object. Other global-monitoring surfaces and cancellation remain control-service work.

The trusted identity DDL route supports `DROP USER/ROLE ... RESTRICT` after bounded scans find no object owner/parent, membership, grant, policy-target, or dependency references. A user's active credentials are revoked in the same serialized commit that removes its principal and named object and inserts its permanent identity tombstone. The final catalog validators reject concurrent dangling references, and deletion cannot leave the database without a login-enabled administrator credential.

Token issue, revoke, and inventory requests use a monotonic 60-second sliding window per authenticated actor, with a default cap of 30 requests. Live manager handles for one redb `Database` object share the limiter; the actor-history map is capped at 4,096 entries and expired histories are pruned. If the map is full, requests for new actors fail closed. Invalid token-authentication attempts require gateway-level throttling because the storage API does not receive a client source identity.

## Resource and Process Boundaries

Bound active redb snapshots, transaction age, private overlay bytes, scans, output, and finalization work. `StorageInstance::open` acquires the exclusive local instance lock and validates a recovered READY image before a trusted service constructs `StorageManager`; it can then create a manager from that already validated image without repeating the startup scan. Direct `StorageManager` construction also validates READY state, but does not acquire the filesystem lock. Neither path authenticates a service connection. `bootstrap::initialize` creates an INITIALIZING instance, and `bootstrap::resume` rotates its bootstrap token after interrupted handoff. Both retain the instance lock until acknowledgement or drop. `begin_token_authenticated_invocation` verifies the versioned random-token profile using trusted UTC time and pins the verifier, user, and role-membership records for commit revalidation. `issue_token` uses the system CSPRNG, returns its secret once, and stores only its verifier; `revoke_token` uses the authorized identity DDL path. `list_tokens` reads a single snapshot and returns nonsecret metadata only, allowing self-inventory or cross-user inventory with `MANAGE_USERS`/database-admin authority. The identity-based admission method is only for a trusted caller that has already authenticated the user. The manager also requires a positive configured active-invocation limit and age, shares the snapshot cap across cloned handles, and reserves a slot before pinning the redb snapshot. Managed transactions expire at read/write/finalization checks; services must poll `check_deadline` while work is idle so expired transactions release their snapshot and admission slot. A long analytical reader can retain redb pages and delay reclamation; scheduling must protect operational transactions. Do not hold redb guards while waiting for network or executor output.

Only the gateway handles client TCP. Only the supervisor launches and owns executor processes. The executor receives local scoped IPC and sanitized values, never redb handles/descriptors. The optimizer receives authorized schema and scoped estimates, never raw database tables.

## Validation

Test redb snapshot/overlay semantics, concurrent staging and serialized finalization, same-row conflicts, distinct-row commits, write skew, atomic row/index/catalog/outcome updates, policy changes racing commit, hidden-field preservation, restart/outcome reconciliation, malformed IPC, long-reader pressure, and identical authorization behavior for scans and index lookups.
