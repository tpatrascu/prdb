# Concurrency, MVCC, and Transactions

## Initial Scope and Isolation

Concurrent readers and writers are required from the first implementation. Use storage-managed multiversion concurrency control (MVCC) with snapshot isolation and optimistic write validation. One user-library invocation is one transaction, potentially containing sequential SQL and direct SDK calls. Different invocations execute concurrently; one invocation initially permits one active statement/request stream at a time.

Snapshot isolation prevents dirty reads and preserves the committed snapshot across calls, while exposing the transaction's own successful writes. It is not serializable: transactions updating different rows can exhibit write skew. General cross-row business invariants and serializable isolation are not promised. Cell-level authorization does not imply cell-level conflict detection; initially conflicting writes are detected at row granularity.

## Version Visibility

Storage allocates a transaction identity and a snapshot of the fully published commit sequence at admission. The executor receives an opaque context, not authority to choose timestamps or read historical snapshots.

Conceptually, a committed row version has a stable logical row ID and visibility interval `[begin_commit, end_commit)`. A snapshot at `S` sees versions with `begin_commit <= S < end_commit`, excluding deletion tombstones. Updates create new versions, deletes publish tombstones, and inserts create fresh identities. [Storage format v1](storage-format.md) defines row records, version references, and page publication; [memory storage](memory-storage.md) defines pinned buffer/private-write ownership.

Each transaction owns a private write set. Reads overlay its latest successful writes on the committed snapshot; no other transaction sees those writes before commit. Index lookups, scans, row handles, policy evaluation, and SQL execution must all respect version visibility. A row handle never means “read the latest committed row.”

Capture each statement's input view before it stages mutations, preventing an update scan from revisiting its own output. Initially materialize a bounded list of mutation targets and values, close the input scan, then stage changes. Exceeding the materialization budget aborts rather than partially committing. Disallow mutation while a previous cursor/result stream remains active. Closing the stream permits the next statement to observe successful writes.

## Commit Protocol

Transactions can scan, compute, and stage writes concurrently. Initially serialize only final validation and publication through a storage-owned commit gate:

1. On successful library return, the supervisor stops new requests and asks storage to finalize the invocation. Executors cannot commit early through a raw protocol request.
2. Storage drains admitted work and verifies that statements completed successfully before freezing the private write set. Unfinished mutation streams or cancelled/failed work abort the transaction; finalization must not silently drop pending writes and commit a prefix.
3. Under the commit gate, confirm the transaction is active and within limits. Validate every updated/deleted row against its recorded base version: a concurrent committed change causes a conflict and aborts the whole transaction. Validate supported constraints, grant/policy validity, and authorization dependencies.
4. Allocate a commit sequence, durably record the complete transaction and commit decision, then atomically publish all row/index changes and the new committed frontier. No snapshot can observe partial publication.
5. Release the gate and report committed success. Abort discards every private write, never just a conflicting row.

The gate is a simple initial correctness mechanism, not a claim of lock-free operation. Keep validation bounded; perform expensive preparation before the gate and verify its dependencies inside it. No latch or commit gate may be held while waiting for executor input, IPC output, or arbitrary user code.

Initial constraints follow [schema DDL](ddl-schema.md): generated or user-supplied primary keys, UNIQUE, NOT NULL, bounded row-local CHECK, and non-cascading foreign keys. Validate unique keys and both directions of foreign-key changes against the latest committed state plus final private changes under the commit gate; abort on conflict/violation. Supporting these declared constraints does not eliminate arbitrary write skew. Generated identities remain opaque, and constraint outcomes can disclose hidden key existence. Deferrable constraints and native constraint callbacks are unsupported.

## Authorization Under Concurrent Writes

Check write permission separately from read permission. Initial updates require a visible target row and readable, writable changed cells; inserts require insertion and per-field permission; deletes require an explicit row-delete grant. Storage constructs new rows and preserves unmodified hidden cells without sending them to the executor. Validate the complete proposed row with trusted policy rules.

Version policy records and database-resident membership inputs with the data snapshot. Policy-sensitive row changes must not grant new authority within the transaction: require old-row and new-row write checks and keep the admitted grant fixed. Initially policy-driving fields on existing rows (such as owner or manager identity) are immutable through query writes; inserts may initialize them only through trusted defaults or explicit creation-policy validation. Policy/ACL administration uses a separate trusted, versioned control path, not user-query writes. Relevant policy updates publish through the same commit ordering as data.

At commit, validate affected schema epochs, user/login/credential validity, role membership, grants, policy versions, and authorization dependencies used to allow writes. Token expiry/revocation or identity disable blocks pending writes even though admitted reads retain bounded snapshots. Start conservatively with epochs for relevant policy/dependency tables, validated under the same publication gate; a changed epoch aborts rather than trusting a stale allow decision. Row-dependent permission checks record their base versions as well. Policy evaluation reads authoritative snapshot inputs, never a user-edited replacement for membership/ACL data. Unsupported external policy sources must be rejected until versioned validation exists.

Read access remains pinned for the bounded invocation lifetime, so already delivered data is not revocable. A policy change can nevertheless prevent pending writes from committing. Permission/conflict failures must not expose private row contents or competing principals; conflict outcomes themselves remain a documented inference surface.

## Concurrent Catalog Changes

Administrative schema and permission transactions use the same durable publication order as data commits. Table changes, constraints, and index builds use DDL fences and schema/write epochs as defined in [schema DDL](ddl-schema.md). Old writers cannot publish against superseded definitions; old readers retain their pinned catalog/data until reclamation. User/role/grant/policy changes advance authority epochs and prevent stale write authorization without rewriting all data.

## Errors, Cancellation, and Retry

Use `ACTIVE -> COMMITTING -> COMMITTED` or `ACTIVE/COMMITTING -> ABORTED` before irreversible COMMIT submission, with an internal submission barrier inside COMMITTING. For the baseline, any SQL/storage statement error aborts the entire invocation transaction; no savepoints or partial recovery are supported. User code cannot catch an error and later commit prior writes.

Cancellation and finalization race through a synchronized storage state transition. Cancellation accepted before WAL COMMIT submission prevents commit. Once submission begins, cancellation is pending/too-late rather than a promised abort; the durable outcome must be resolved. After commit, data cannot be rolled back. An uncertain failure during logging/publication is resolved through recovery, never reported as a definite abort without evidence. Executor crashes before finalization abort; supervisor failure during commit may leave a committed transaction whose reply was lost.

Return an opaque invocation ID at admission and provide an authenticated status lookup to reconcile lost commit replies. A committed transaction stays committed even if result delivery fails. Provisional output is not a commit receipt. Never automatically rerun arbitrary user libraries; clients may explicitly retry the whole invocation with a fresh snapshot after a known abort. Unknown outcome requires status reconciliation first. [Physical metadata](physical-metadata.md) defines retained durable outcomes and UNKNOWN/EXPIRED behavior; the concrete retention interval still requires configuration. Submission deduplication is not promised, and outcome lookup does not authorize automatic replay.

## Durability and Recovery

Use the [WAL v1 redo-only full-page protocol](wal-recovery.md) from the writable milestone. A successful commit acknowledgement requires its durable commit record. Recovery replays complete committed transactions and discards incomplete/uncommitted work, restoring indexes and transaction outcomes consistently. WAL framing, page layout, and checkpoint ordering are specified; remaining payload fixtures and later flush batching must preserve those contracts. io_uring write completion alone does not prove durability.

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

- Concurrent readers/writers: no dirty reads, stable repeated reads, own-write visibility, and no partial multi-row commits.
- Two updates/deletes to one row: one conflicting transaction aborts; distinct-row writes can commit. Include a write-skew case documenting the snapshot-isolation limit.
- Policy/membership change racing with commit: stale write authorization aborts; unreadable cells remain undisclosed.
- Multi-statement error, crash, or cancellation: all staged writes roll back before commit; committed decisions survive lost replies and restart.
- Cursor races, late responses, cleanup during estimation, and long snapshots concurrent with reclamation.
- Crash injection before/after log flush and publication; recovered state and status agree.

## References and Open Decisions

[PostgreSQL's MVCC overview](https://www.postgresql.org/docs/18/mvcc-intro.html) describes versioned reads, and its [isolation documentation](https://www.postgresql.org/docs/18/transaction-iso.html) distinguishes snapshot isolation from serializability. These are background, not a requirement to copy its storage layout.

Open implementation choices include remaining versioned payload fixtures, synchronization primitives, retention accounting, and concrete outcome-retention intervals. Page/row/WAL formats now have v1 specifications. Serializable isolation, finer write conflicts, savepoints, concurrent statements within one invocation, and online schema rewrites are future work. Initial administrative DDL is specified separately.
