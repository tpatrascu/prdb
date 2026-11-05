# redb Storage Backend

## Decision and Responsibility Split

Use [redb](https://github.com/cberner/redb) as the embedded persistent key/value engine inside the trusted Rust storage-manager process. Pin and review an exact release before implementation; this selects the upstream project, not a moving version. redb supplies its private file format, copy-on-write B+trees, allocation/reclamation, transactional persistence, crash recovery, ACID transactions, and MVCC read snapshots. Do not build a parallel heap/page manager, B-tree implementation, WAL, checkpoint/root format, or direct disk-I/O subsystem for database state.

The engine continues to own relational meaning and security: schemas, typed row serialization, SQL key encodings, MVCC transaction policy, optimistic conflict rules, constraints, cell permissions, users/roles, policies, catalog discovery, durable invocation outcomes, query budgets, and network/process boundaries. redb is an internal storage dependency, never a client API and never accessible to the executor, optimizer, or gateway.

Upstream documents an embedded ACID key/value store with copy-on-write B+trees, concurrent MVCC readers, and serialized writes; it describes a single writer at a time. Those properties support the design only if the engine stages invocation writes outside redb's writer transaction and validates at finalization. They do not automatically implement row-level conflicts, schema semantics, authorization, durable per-invocation status, or untrusted-process isolation. See upstream [README](https://github.com/cberner/redb) and [design](https://github.com/cberner/redb/blob/master/docs/design.md).

The current development dependency is exactly redb 4.4.0 (MIT OR Apache-2.0; upstream minimum Rust 1.90), with Rust 1.95.0 pinned for this workspace. Initial local tests use x86_64 Linux and ext4 at `/tmp`. For durable write experiments, explicitly select `Immediate` and enable two-phase commit; production use must apply and validate the same setting. A normal reopen test does not establish power-loss guarantees or validate a production filesystem. File-format upgrade policy, logical limits, recovery faults, and the supported release profile remain open; do not admit a production database on this evidence alone.

## Database Layout

One redb database file is owned by one storage-manager instance. The supervisor, gateway, executor, and planner do not open it. Use named redb tables with explicit versioned key/value codecs; table names and contents are engine-private. Start with:

| redb table | Key | Value |
| --- | --- | --- |
| engine_meta_v1 | fixed metadata name | database UUID, engine schema/version state, commit sequence, lifecycle |
| rows_v1 | table ID + row UUID | current serialized logical row and engine commit version |
| indexes_v1 | index ID + encoded SQL tuple + row UUID | fixed 32-byte ILC1 row UUID/version locator |
| catalog_v1 | canonical logical catalog key (including its family code) | versioned catalog payload from [catalog payloads](catalog-payloads.md) |
| outcomes_v1 | invocation ID | principal, terminal status, commit/version, retention state |
| audit_v1 | 16-byte event ID | bounded AUD1 required administrative audit/outbox payload |
| stats_v1 | statistics scope/key | disposable or rebuildable derived values, never authority |

The `engine_meta_v1` value at fixed key `format` uses the bounded ENG1 codec: exactly 46 bytes consisting of magic `ENG1`, little-endian metadata-format version 1, nonzero database UUID, nonzero catalog/expression/built-in-semantics versions, little-endian commit sequence (zero is valid for an empty database), one lifecycle byte (`UNINITIALIZED=0`, `INITIALIZING=1`, `READY=2`), and three zero reserved bytes. Structural decoding rejects unknown ENG1 versions, lifecycle values, nonzero reserved bytes, zero UUIDs/required versions, and any non-exact length. Before admitting or publishing a transaction, separately require the supported catalog, expression, and built-in-semantics versions, currently all 1; do not publish an abort record into an unsupported database either. ENG1's metadata version is independent of redb's file version and the versions it records. Other table payload codecs are still separate versioned contracts.

Each `rows_v1` value uses the bounded RWR1 envelope, maximum 1 MiB total: `RWR1`, little-endian value-format version 1, zero u16 flags, nonzero little-endian u64 row version, little-endian u32 payload length, then exactly that many bytes of schema-aware logical row payload. Reject unknown versions/flags, version zero, oversized records, and any length mismatch. The envelope supports row conflict validation; it does not define or validate the tuple payload, which must have its own schema-aware codec.

The schema-aware RWR1 payload uses the TUP1 tuple codec, maximum `1 MiB - 20` bytes. Its 20-byte header is magic `TUP1`, little-endian version 1, zero u16 flags, nonzero little-endian schema version u64, little-endian column count u16, and zero u16 reserved. Column count is 1–4096 and must equal the pinned table schema. Each field is `state u8, payload_length u32 LE, payload`; NULL is state 0 with length 0 and is valid only for nullable columns, VALUE is state 1 with a type-specific payload, and other states fail. No trailing bytes are allowed. Types and parameters come from the same pinned catalog schema as the invocation.

Scalar encoding v1 uses BOOLEAN one byte 0/1; signed integer types their fixed-width two's-complement little-endian bytes; DECIMAL(p,s) a signed i128 little-endian unscaled coefficient with exactly the schema scale and at most p decimal digits; TEXT and VARCHAR valid UTF-8 bytes (VARCHAR is bounded by Unicode scalar count); BYTEA arbitrary bytes; UUID the canonical 16 opaque bytes without host-endian conversion; DATE signed i32 days since 1970-01-01, supported for Gregorian years 0001–9999; TIMESTAMPTZ signed i64 UTC microseconds since epoch, supported from 0001-01-01T00:00:00Z through 9999-12-31T23:59:59.999999Z. Length, type, nullability, decimal precision/scale, UTF-8, VARCHAR count, and date/time ranges are validated before use. The row-record 1 MiB total limit applies after tuple encoding.

An `outcomes_v1` value uses the fixed 56-byte OUT1 envelope: magic `OUT1`, little-endian version 1, status byte (`COMMITTED=1`, `ABORTED=2`), retention byte (`RETAINED=1`), nonzero invocation ID, nonzero principal ID, little-endian commit sequence, and signed little-endian retention deadline in UTC microseconds. COMMITTED requires a nonzero commit sequence; ABORTED requires zero. Unknown statuses/retention values, zero IDs, invalid sequence/status combinations, nonpositive deadlines, and non-exact lengths fail closed. The row-commit path writes COMMITTED outcomes atomically with rows and engine metadata; proven conflicts and statement failures write ABORTED outcomes only after unpublished changes are discarded. The storage lookup helper hides records belonging to another principal and treats expired or missing records as absent; authenticated IPC integration and retention cleanup remain separate work.

This is a starting logical mapping; choose table definitions and key types from the pinned redb API. All composite keys use explicit byte encodings, never native Rust/C struct layouts. Keep private metadata out of ordinary discovery.

redb MVCC supplies historical physical snapshots. The engine's row value need not carry an old-version chain just to implement snapshot reads: a long-lived redb read transaction sees its pinned state. The latest committed row record can include a monotonically increasing row version for conflict checks and index maintenance. Retain schema definitions while any active snapshot or persisted row requires them. If the pinned redb API cannot keep required read transactions safely for bounded invocation lifetimes, stop and revise the transaction design before implementation.

## Invocation Transactions and Concurrent Writes

At admission, storage begins and retains a redb read transaction as the invocation snapshot, along with identity/schema/policy context. Reads borrow values from this snapshot and overlay that invocation's private staged inserts/patches/deletes. The overlay owns validated copies of incoming changes; it is never visible to another invocation. Do not open a redb write transaction per SQL statement.

At successful library return, the supervisor requests finalization. Storage acquires a bounded application commit coordinator, opens one redb write transaction, reads current committed rows/metadata, validates row versions, authorization epochs, uniqueness/FK constraints and limits, applies all rows/index/catalog/outcome changes atomically, and commits redb with the required durable mode. Concurrent invocations may read and stage simultaneously, but redb commits serialize. A conflict or validation failure aborts the entire invocation and publishes no write set. Avoid holding the coordinator while waiting for user code or network output.

Snapshot isolation is an engine guarantee to check against redb's snapshot behavior, not a claim derived from redb's serializable write transactions. The validation algorithm must preserve the specified row-level conflicts and the documented write-skew limit. Two transactions touching different rows must both be able to stage; final writes to different keys can commit sequentially. If two rows share an encoded index key, constraint checks run against the latest state inside the serialized write transaction.

COMMIT submission is redefined as entry into irreversible redb write-transaction commit, not submission of an engine-owned WAL record. Before crossing that boundary, accepted cancellation prevents publication. Once redb commit begins, return pending/too-late until the engine can establish the durable outcome. On an ambiguous error, close/reopen or otherwise use documented redb recovery/status behavior before deciding. In the same atomic redb commit, write outcomes_v1=COMMITTED with data changes so a crash cannot expose committed data without its corresponding status. Durable ABORTED outcomes also require an atomic redb write transaction; absence means UNKNOWN/EXPIRED, never proof of abort.

A process crash releases the retained read transaction, and redb recovers its own committed state on open. The engine still needs tests for storage-file errors, redb commit result ambiguity, transaction lifetime, shutdown and reopen, and outcome reconciliation. Do not add a second WAL or checkpoint around redb; doing so creates two durability authorities and cross-store atomicity problems.

## Authorization and Copy Boundaries

Only storage obtains redb tables/transactions. For each operation, enforce principal, grants, row policy, and per-cell policy before copying or encoding results for IPC. A redb access guard/borrow is trusted-process memory access, not permission to share database buffers with the executor. Gather only authorized cells into bounded response buffers. Hidden unchanged fields remain inside storage when applying write patches. Never expose redb handles, guards, database paths, native pointers, private metadata, or borrowed values to another process.

A held read transaction can retain old copy-on-write pages and grow disk/memory pressure. Bound invocation age, active readers, scans, and cumulative work. Apply admission limits and explicit cancellation/abort under pressure; do not silently replace a caller's snapshot with a newer one. Avoid holding redb value guards over IPC writes or slow network clients.

## Durability, I/O, and Operational Limits

Select redb's durable commit mode explicitly in the pinned API; do not use non-durable commits for acknowledged database changes. Evaluate the documented two-phase durable mode for the threat model and performance. Upstream describes one-phase checksum and two-phase durable strategies and states filesystem assumptions; record the chosen mode and tested filesystem/platform profile. redb checksums/file recovery are not cryptographic tamper protection. The database directory must remain private to the trusted storage service.

The engine delegates database-file I/O to redb. Benchmark redb's supported path and verify copy costs before claiming zero-copy disk I/O. Any future custom I/O integration requires a redb-supported backend with equivalent durability/recovery evidence, never a second writer to the same live file.

redb table/key APIs and size limits may differ from the former custom 16 KiB page, 2,048-byte B-tree key and overflow assumptions. Remove those physical limits unless retained as deliberate SQL-engine limits after verification against the pinned version. Keep only logical row/scalar/request limits specified by the engine, adjusted for actual redb constraints and testing.

## Upgrade, Compatibility, and Validation

Treat redb file-format compatibility and this engine's logical/catalog encoding compatibility as separate version domains. Pin and inspect a specific crate release's upgrade guarantees before an engine release. On open, validate engine metadata, expected tables/schema versions, database UUID, redb errors, and lifecycle before admitting clients. Unknown engine versions fail closed. Back up or use supported offline migration procedures before changing redb or catalog formats; never open a production file read-write with an unreviewed version.

Test concurrent retained snapshots plus staged writes, conflict validation, per-row patch merge, unique/FK races, one-write-transaction atomic data/index/catalog/outcome commits, durable commit/reopen recovery, deliberately aborted write transactions, large-reader pressure, compaction/reclamation behavior, and corruption/error handling with the pinned redb version. Test every enabled durability mode/profile. Benchmark operational point reads/writes and bounded analytical scans under concurrent readers. Do not infer project performance from upstream benchmarks.

## Explicitly Removed Engine-Owned Formats

The former custom [page/heap/B-tree format](storage-format.md), [WAL/checkpoint/root design](wal-recovery.md), [checkpoint manifest](checkpoint-format.md), and [direct disk I/O backend](disk-io.md) are superseded for database persistence. Their old bytes are not an additional format and there is no migration from a prior implementation of them. The [index-key encoding](index-key-encoding.md) remains an engine logical ordering contract where needed by SQL indexes, subject to the pinned redb key API; it no longer describes B-tree page separators or pages.
