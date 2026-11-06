# Physical Storage Metadata

redb exclusively owns the database file, physical B+trees/pages, allocation, copy-on-write roots, reclamation and storage recovery. The engine does not define or inspect physical page/file IDs, root slots, allocator trees, WAL positions, checkpoint descriptors, or page generations. [redb storage](redb-storage.md) is authoritative.

The engine stores logical catalog, row data, SQL index mappings, identities/permissions, durable outcomes and required audit events through versioned redb tables. Their transaction visibility comes from redb snapshots; logical formats are defined by [catalog payloads](catalog-payloads.md), [index key encoding](index-key-encoding.md), and [redb table mapping](redb-storage.md).

Rows, indexes, relevant catalog/authority changes, required audit entries, and terminal outcome commit atomically when they belong to one operation. Derived statistics/caches may be discarded only when they confer no authority and can be safely rebuilt. Long-running snapshots retain physical history under redb; bound their age and number.

Persist invocation outcome in the same redb write transaction as committed user data. A lost response is resolved from this table after reopen. Missing or expired history returns UNKNOWN/EXPIRED and never implies rollback. Required administrative audit/outbox records commit atomically with the administrative change; optional telemetry cannot undo a committed data operation.

Trusted storage maintenance prunes outcomes whose UTC retention deadline has passed. Each call scans at most 1,024 records and deletes expired records in one immediate, two-phase redb transaction. Its resume cursor is stored in `stats_v1` and committed atomically with deletions; it does not advance the logical commit sequence. A completed scan clears the cursor. An exact-size batch may need one empty follow-up call to confirm that the cycle is complete. The caller supplies trusted current time, and malformed cursors or outcome records fail the maintenance transaction without partial deletion.

On startup, open/recover using the pinned supported redb version, validate engine metadata and required logical tables, then validate catalog/authority invariants before exposing listeners. Corrupt or unsupported state fails closed. There is no independent engine checkpoint, WAL truncation, page reuse, or repair path.

Test atomic record-family updates, redb restart/recovery, snapshots concurrent with commits, bounded retention, old schema decoding, outcome status after lost replies, required audit recovery, and failure on unsupported engine/redb versions. Do not expose raw redb statistics to query planning.
