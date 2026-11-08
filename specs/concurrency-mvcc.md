# Concurrency, MVCC, and Transactions

## Initial Scope and Isolation

Concurrent readers and writers are required from the first implementation. Use storage-managed multiversion concurrency control (MVCC) with snapshot isolation and optimistic write validation. One user-library invocation is one transaction, potentially containing sequential SQL and direct SDK calls. Different invocations execute concurrently; one invocation initially permits one active statement/request stream at a time.

Snapshot isolation prevents dirty reads and preserves the committed snapshot across calls, while exposing the transaction's own successful writes. It is not serializable: transactions updating different rows can exhibit write skew. General cross-row business invariants and serializable isolation are not promised. Cell-level authorization does not imply cell-level conflict detection; initially conflicting writes are detected at row granularity.

## Version Visibility

Storage allocates a transaction identity and a snapshot of the fully published commit sequence at admission. The executor receives an opaque context, not authority to choose timestamps or read historical snapshots.

Conceptually, a committed row version has a stable logical row ID and a visibility interval. Updates create new versions, deletes remove the current row, and inserts create fresh identities. redb provides physical snapshot visibility and page publication. The `rows_v1` record carries a nonzero row version in the RWR1 envelope; a successful writer assigns the engine commit sequence to each changed row, and commit validation compares the recorded base version with the latest record. [redb storage](redb-storage.md) defines the envelope, while [memory storage](memory-storage.md) defines snapshot and private-write ownership.

Each invocation owns a redb read-transaction snapshot and a private engine write overlay. The storage transaction API resolves reads from the latest staged insert/value/delete first and otherwise from the pinned snapshot; no other invocation sees staged writes before commit. Index lookups, scans, row handles, policy evaluation, and SQL execution must respect that same pinned snapshot. A row handle never means “read the latest committed row.” redb owns physical MVCC page retention; the engine enforces invocation policy and row-level conflicts.

`InvocationTransaction::scan_authorized` returns ordered row pages from that snapshot merged with the invocation's private overlay. It resolves each candidate through the overlay first, then applies the pinned SELECT row policy and per-cell projection before returning any cells. Each page authorizes at most 1,024 distinct candidate IDs; the snapshot and overlay each provide a bounded ordered prefix for the merge. A full page is conservatively incomplete, and an empty follow-up page confirms completion. Hidden rows and staged deletions advance a transaction-private opaque cursor, so continuation does not reveal hidden row IDs. The transaction pins the scan's schema and authorization inputs, accepts only its matching continuation cursor, and blocks further mutations or commit until the scan is completed or the invocation is aborted. A caller cannot change projection authority between pages.

A direct delete of a row already deleted in the invocation sees the staged absence and returns `NotFound`; under the baseline statement-error rule, this aborts the invocation and discards the earlier delete. SQL scans that match no rows may instead complete successfully without issuing such a direct delete.

Capture each statement's input view before it stages mutations, preventing an update scan from revisiting its own output. Initially materialize a bounded list of mutation targets and values, close the input scan, then stage changes. Exceeding the materialization budget aborts rather than partially committing. Disallow mutation while a previous cursor/result stream remains active. Closing the stream permits the next statement to observe successful writes.

## Commit Protocol

Transactions can scan, compute, and stage writes concurrently. redb serializes its write transactions; the engine validates and publishes through one redb transaction:

1. On successful library return, the supervisor stops new requests and asks storage to finalize the invocation. Executors cannot commit early through a raw protocol request.
2. Storage drains admitted work and verifies that statements completed successfully before freezing the private write set. Unfinished mutation streams or cancelled/failed work abort the transaction; finalization must not silently drop pending writes and commit a prefix.
3. Acquire the writer coordinator and begin a redb write transaction. Validate every updated/deleted row against its recorded base version: a concurrent committed change causes a conflict and aborts the whole transaction. Validate constraints, grants/policies, schema and authority dependencies against latest committed state.
4. Merge patches into current values. In the same redb transaction update all rows/indexes/catalog/audit/outcome data and the engine commit sequence. Commit using the selected durable mode; redb makes the state atomically visible.
5. Release the coordinator and report committed success only when the durable result is established. Abort discards every private write, never just a conflicting row.

The writer coordinator is a simple initial correctness mechanism. Keep validation bounded; prepare expensive work before opening the redb write transaction and verify dependencies inside it. Do not hold the writer while waiting for executor input, IPC output, or arbitrary user code.

Initial constraints follow [schema DDL](ddl-schema.md): generated or user-supplied primary keys, UNIQUE, NOT NULL, bounded row-local CHECK, and non-cascading foreign keys. Validate unique keys and both directions of foreign-key changes against the latest committed state plus final private changes inside the redb write transaction; abort on conflict/violation. The initial foreign-key reference scans visit at most 65,536 persisted and staged row records combined per commit and retain at most 16 MiB of encoded comparison keys for one FK check; exceeding either bound aborts. Supporting these declared constraints does not eliminate arbitrary write skew. Generated identities remain opaque, and constraint outcomes can disclose hidden key existence. Deferrable constraints and native constraint callbacks are unsupported.

## Authorization Under Concurrent Writes

Check write permission separately from read permission. Initial updates require a visible target row and readable, writable changed cells; inserts require insertion and per-field permission; deletes require an explicit row-delete grant. Storage constructs new rows and preserves unmodified hidden cells without sending them to the executor. Validate the complete proposed row with trusted policy rules.

Version policy records and database-resident membership inputs with the data snapshot. Policy-sensitive row changes must not grant new authority within the transaction: require old-row and new-row write checks and keep the admitted grant fixed. Initially policy-driving fields on existing rows (such as owner or manager identity) are immutable through query writes; inserts may initialize them only through trusted defaults or explicit creation-policy validation. Policy/ACL administration uses a separate trusted, versioned control path, not user-query writes. Relevant policy updates publish through the same commit ordering as data.

At commit, validate affected schema epochs, user/login/credential validity, role membership, grants, policy versions, and authorization dependencies used to allow writes. Token expiry/revocation or identity disable blocks pending writes even though admitted reads retain bounded snapshots. Start conservatively with epochs for relevant policy/dependency tables, validated under the same publication gate; a changed epoch aborts rather than trusting a stale allow decision. Row-dependent permission checks record their base versions as well. Policy evaluation reads authoritative snapshot inputs, never a user-edited replacement for membership/ACL data. Unsupported external policy sources must be rejected until versioned validation exists.

Read access remains pinned for the bounded invocation lifetime, so already delivered data is not revocable. A policy change can nevertheless prevent pending writes from committing. Permission/conflict failures must not expose private row contents or competing principals; conflict outcomes themselves remain a documented inference surface.

## Concurrent Catalog Changes

Administrative schema and permission transactions use the same durable publication order as data commits. Table changes, constraints, and index builds use DDL fences and schema/write epochs as defined in [schema DDL](ddl-schema.md). Old writers cannot publish against superseded definitions; old readers retain their pinned catalog/data until reclamation. User/role/grant/policy changes advance authority epochs and prevent stale write authorization without rewriting all data.

## Errors, Cancellation, and Retry

Use `ACTIVE -> COMMITTING -> COMMITTED` or `ACTIVE/COMMITTING -> ABORTED` before redb commit begins, with an internal submission barrier inside COMMITTING. For the baseline, any SQL/storage statement error aborts the entire invocation transaction; no savepoints or partial recovery are supported. User code cannot catch an error and later commit prior writes.

Cancellation and finalization race through a synchronized storage state transition. Cancellation accepted before redb write-transaction commit begins prevents commit. Managed transactions observe accepted cancellation at storage checkpoints; a pending audit decision is resolved before the checkpoint proceeds. The checkpoint discards staged work, records ABORTED, and releases the retained snapshot. The transition to COMMITTING occurs after validation and record staging, immediately before redb's write-transaction commit, so cancellation remains admissible during those earlier steps. Once the transition wins, cancellation is too late rather than a promised abort; the durable outcome must be resolved. After commit, data cannot be rolled back. An uncertain failure during logging/publication is resolved through recovery, never reported as a definite abort without evidence. Executor crashes before finalization abort; supervisor failure during commit may leave a committed transaction whose reply was lost.

Return an opaque invocation ID at admission and provide an authenticated status lookup to reconcile lost commit replies. `StorageManager::lookup_invocation_outcome` resolves only an unexpired outcome owned by the already authenticated principal; unknown, mismatched-owner, and expired outcomes all return absence. A committed transaction stays committed even if result delivery fails. Provisional output is not a commit receipt. Never automatically rerun arbitrary user libraries; clients may explicitly retry the whole invocation with a fresh snapshot after a known abort. Unknown outcome requires status reconciliation first. [Physical metadata](physical-metadata.md) defines retained durable outcomes and UNKNOWN/EXPIRED behavior; the concrete retention interval still requires configuration. Submission deduplication is not promised, and outcome lookup does not authorize automatic replay.

## Durability and Recovery

Use redb durable write transactions from the writable milestone. A successful acknowledgement requires commit under the selected redb durability mode. Recovery is redb-owned; logical rows, indexes, catalog, required audit and outcome must be written atomically. An ambiguous commit result requires recovery/status reconciliation.

## Metadata Consistency Domains

Catalog versions and dependency/authority epochs commit with logical changes as defined in [data dictionary](data-dictionary.md). [Physical metadata](physical-metadata.md) defines allocation generations, checkpoint/WAL ordering, and outcome retention. Root publication and reclamation must account for both old data and old catalog pins.

[Operational metadata](operational-metadata.md) is live control-plane observation, not a repeatable SQL snapshot. [Catalog upgrades](catalog-lifecycle.md) are offline and cannot mix live invocations across incompatible formats.

## Scheduling and Memory Safety

Use bounded storage/optimizer work queues and admission limits globally and per principal. Serialize cursor advancement and each invocation's mutation stream; reject concurrent use of a cursor or mutation stream rather than racing it. Request IDs and handle generations prevent late replies from being applied to reused objects. SDK calls are not initially thread-safe; arbitrary worker threads still face server-side enforcement.

Cancellation marks a context closed, prevents new work, and waits for existing storage tasks to release references before reclaiming memory. Storage tasks hold snapshot/version pins independently of executor liveness. Use a documented latch order and short critical sections for indexes, version chains, caches, and context tables. Slow clients must cause backpressure without holding global locks. Shard data structures only after measuring contention.

## Version Reclamation and Pressure

Track all active snapshot pins, including optimizer estimates and background maintenance. Reclaim an obsolete committed version only when no pinned snapshot or in-flight task can observe it; clean abandoned private versions separately. Publish the safe reclamation frontier consistently with snapshot admission. Never free versions merely because a worker exited while its storage task is still running.

Bound invocation age, private write-set bytes, pinned history, cursor buffers, and queued work. Under pressure, reject new work or abort selected old invocations with explicit errors; never silently advance their snapshots or reclaim visible history. Protect unaffected invocations and reserve progress capacity for cancellation, commit, and reclamation.

## Required Validation

- Concurrent readers/writers: [`invocation_snapshot_hides_private_writes_and_keeps_repeated_reads_stable`](../engine/tests/transaction.rs) verifies that one invocation cannot see another writer's private update or insert, keeps its original values after that writer commits, and a later invocation sees the published values. Keep this alongside [`invocation_sees_own_writes_and_commits_against_its_pinned_snapshot`](../engine/tests/transaction.rs), the same-row conflict and write-skew regressions, and [`statement_error_aborts_earlier_writes_and_prevents_finalization`](../engine/tests/transaction.rs).
- Two updates/deletes to one row: one conflicting transaction aborts; distinct-row writes can commit. The `snapshot_isolation_allows_documented_disjoint_row_write_skew` regression starts two invocations from one snapshot: each sees two `on-call` rows and takes a different row off call. Both disjoint writes may commit, leaving no one on call. This documents that snapshot isolation does not enforce cross-row invariants or promise serializability.
- Policy/membership change racing with commit: stale write authorization aborts; unreadable cells remain undisclosed.
- Multi-statement error, crash, or cancellation: [`statement_error_aborts_earlier_writes_and_prevents_finalization`](../engine/tests/transaction.rs), [`storage_manager_audits_cancellation_before_commit_and_preserves_terminal_outcomes`](../engine/tests/commit.rs), [`storage_manager_reports_live_and_durable_invocation_status_to_the_owner`](../engine/tests/commit.rs), and the real-redb `durable_commit_gate_tests` in [`commit.rs`](../engine/src/commit.rs) verify staged writes are discarded before commit; cancellation also releases its active snapshot slot, is accepted while a redb writer is held, and prevents publication when the final gate declines. Committed decisions survive lost replies and restart.
- Cursor races, late responses, cleanup during estimation, and long snapshots concurrent with reclamation.
- Crash injection before/after log flush and publication; recovered state and status agree.

## References and Open Decisions

[PostgreSQL's MVCC overview](https://www.postgresql.org/docs/18/mvcc-intro.html) describes versioned reads, and its [isolation documentation](https://www.postgresql.org/docs/18/transaction-iso.html) distinguishes snapshot isolation from serializability. These are background, not a requirement to copy its storage layout.

Open choices include the pinned redb release/durable mode, engine value fixtures, synchronization primitives, retention accounting, and outcome interval. redb serializes writes; the external contract remains snapshot isolation with engine row conflicts. Serializable isolation, finer write conflicts, savepoints, concurrent statements within one invocation, and online schema rewrites are future work. Initial administrative DDL is specified separately.
