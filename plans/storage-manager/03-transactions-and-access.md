# P4–P6: Durable Transactions and Authorized Access

## P4 — Concurrent MVCC and Commit

Sources: [MVCC](../../specs/concurrency-mvcc.md), [WAL](../../specs/wal-recovery.md), [physical metadata](../../specs/physical-metadata.md).

1. Register snapshots atomically with the committed frontier. Implement version-chain visibility and private write overlays; retain catalog versions as well as row versions.
2. Add invocation/statement state, sequential request enforcement, cumulative quotas, and transaction poisoning on any statement error. Materialize bounded mutation targets before staging; close the input stream so updates cannot revisit their own output.
3. Stage owned field patches, base-version identities, write generations, and dependency epochs. Coalesce repeated writes without losing conflict provenance. Other transactions must never read the overlay.
4. Implement finalization: stop admission to the context, drain admitted requests, freeze writes, take the commit gate, validate conflicts/dependencies, build latest-page replacements, assign sequence/LSNs, log/sync, publish all roots/images/frontier, then acknowledge.
5. Separate cancellation accepted before COMMIT submission from too-late/pending cancellation. After uncertain I/O, enter recovery-required state and prohibit further writes until resolution.
6. Implement authenticated-outcome storage primitives, including durable abort records and UNKNOWN/EXPIRED semantics. Read-only successful invocations also need a defined durable outcome path; finalize that contract before exposing success/status APIs.

**Red schedule suite:** same-row updates yield one conflict; distinct-row updates on the same page both survive; insert/update/delete overlays are visible only to their owner; a reader admitted during publication sees no partial commit; write skew remains possible as specified. Use barriers, not sleeps.

Inject failure before/after each WAL and publication transition. Test cancellation on both sides of the submission barrier, errors after earlier successful statements, unfinished mutation streams, lost replies, and outcome lookup after restart.

**Exit:** concurrent read/staging and durable atomic multi-row publication work with an internal verified-context fixture. This fixture is not a production admission mechanism. Authorization and network exposure remain gated on P5/P6.

## P5 — Identity, Bootstrap, and True Cell Permissions

Sources: [identity](../../specs/ddl-identity.md), [permission DDL](../../specs/ddl-permissions.md), [authorization](../../specs/authorization.md), [lifecycle](../../specs/catalog-lifecycle.md).

1. Implement minimal authenticated administrative operations to create users/roles, credentials, grants, and policies. Complete ordinary DDL breadth in P7; never hard-code demonstration identities into production.
2. Implement INITIALIZING/READY bootstrap with protected local setup ownership, verifier-only persistence, one-time secret handoff, and replacement of an unclaimed token after interrupted setup. READY cannot reenter unauthenticated setup.
3. Bind admitted contexts to verified principal, fixed execution grant, memberships, policy snapshot, expiry, and conservative authority/dependency epochs. Database ownership is administration, not a data-read bypass.
4. Implement bounded typed policy evaluation: mandatory default deny, alternative ALLOW rules, all RESTRICT rules, and paired old/new checks for an allowing update rule. Restrict query mutation of policy-driving fields as specified.
5. Implement the reference authorized scan: MVCC/overlay selection, row visibility, per-cell readability, authorized predicate semantics, final compact serialization. Validate every operation input column, not just projection columns.
6. Add write checks for insertion/defaulted fields, readable+writable changed cells, complete proposed-row validation, and explicit row deletion. Preserve hidden unchanged fields inside storage.
7. Revalidate schema/identity/token/membership/grant/policy dependencies under the commit gate. A stale allow decision aborts pending writes even when bounded reads retain their original snapshot.

**Red fixture:** one principal sees names in two rows but salary in only one; a second sees a different subset; a third sees neither row. Cover VALUE, SQL NULL, REDACTED, and invisible rows separately. Mutate only hidden payloads and verify serialized authorized output contains neither their values nor lengths/padding.

Test denied predicates, forged principal selection, policy self-escalation, revoke/disable/expiry racing commit, dropped invalidation notifications, hidden-field preservation, interrupted credential handoff, and required audit failure. Constraint outcomes retain the documented inference limitation.

**Exit:** production contexts originate in verified admission, no permissive default policy exists, and restart preserves identity/policy state. Minimal admin success includes its required audit/outbox entry in the same commit.

## P6 — Role-Bound Service and Batch Protocol

Sources: [IPC](../../specs/ipc-protocol.md), [metadata discovery](../../specs/metadata-discovery.md), [memory model](../../specs/memory-storage.md).

1. Freeze versioned frame layouts, bounded expressions, collection limits, and error categories before writing handlers. Reject size/depth/overflow errors before large allocations.
2. Add the thin service binary and copied Unix-socket transport. Supervisor-provisioned connections bind invocation and service role server-side; a role field supplied by a worker conveys no authority.
3. Implement logical resolution, scan/cursor operations, staged mutations, and scoped handle generations. Serialize statements/cursor advancement server-side even when hostile worker threads overlap requests.
4. Add supervisor-only finalization, separately authenticated administration/status, and separate planner capabilities. Reject executor DDL/finalization/statistics requests and cross-role handle reuse.
5. Encode sanitized column-oriented batches directly into owned final output buffers. Release storage-page pins before slow sends where possible; output ownership and budgets must survive cancellation without retaining the full cache.
6. Implement filtered discovery/continuations and fixed `information_schema` surfaces over the same catalog rules. Neither discovery nor errors reveal credential verifiers, physical addresses, private counts, or unapproved policy definitions.

**Red tests:** raw clients bypass SDK checks; steal another connection's handle; recycle a stale cursor; submit duplicate mutations; disconnect mid-stage; send malformed batches/expressions; block output while another transaction commits. Planning handles must fail on executor channels.

**Exit:** real Unix-socket tests enforce authority and lifetime checks. The P4–P6 data slice is usable only with real supervisor authentication/confinement integration; otherwise label cross-process release evidence pending. Never load test query libraries in the trusted storage process.
