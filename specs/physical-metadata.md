# Physical Storage and Recovery Metadata

## Responsibility and Access

The Rust storage manager exclusively owns physical metadata: file identities, page/extent allocation, logical-to-physical mappings, index roots, WAL positions, checkpoints, and retirement state. Executors and optimizers receive logical references only. A database administrator may request bounded diagnostic summaries, never arbitrary memory/file access through query IPC.

Logical catalog IDs survive physical relocation. Physical identifiers include a generation so stale references cannot address a newly reused page/file. No physical layout or root pointer is an ABI shared with the C SDK.

## Required Record Families

| Family | Required fields / invariants |
| --- | --- |
| Database root | Magic, database UUID, format/feature versions, root generation, checkpoint descriptor reference, integrity checksum |
| File registry | Internal file ID/generation, purpose, owning storage object, format, lifecycle; no caller-provided filesystem paths |
| Storage map | Logical table/index ID and generation to versioned storage descriptors; relocations preserve logical identity |
| Allocation map | Extent/page identity, allocation generation, owner, reserved/live/retired/free state, retirement horizon |
| Page/record header | Kind/format, payload length, identity/generation, integrity data, last applied log position where applicable |
| Index descriptor | Logical index ID, root reference/generation, build state, schema compatibility, publication commit |
| Checkpoint descriptor | Generation, replay-start LSN, durable frontier, catalog/storage roots, allocation state references, checksum |
| Outcome log/store | Invocation ID, originating principal, durable commit/abort outcome and commit association, retention state |
| Administrative audit outbox | Action/correlation ID, actor/target IDs, bounded action/result, delivery state; committed with the catalog action |

[Storage format v1](storage-format.md) fixes 16 KiB pages, little-endian headers/records, CRC32C, root slots, and heap/B-tree layouts. [WAL v1](wal-recovery.md) fixes redo framing and ordering; [checkpoint format](checkpoint-format.md) fixes checkpoint WAL payloads, manifest sections, and root binding. [Catalog payloads](catalog-payloads.md) and [index keys](index-key-encoding.md) define logical encodings. Separate allocation/outcome/audit payload schemas and executable fixtures remain prerequisites. Implement all readers with explicit bounds and version checks. A page-local checksum or well-formed header does not establish that its ownership/reference is valid.

## Allocation and Reclamation

Allocate privately, record ownership, then publish reachability with the relevant transaction or maintenance commit. An aborted reservation is reclaimable only after no task references it. Reusing a page/file increments its generation; generation mismatch is an error, never a fallback to current contents.

Retire dropped tables/indexes and obsolete MVCC versions before freeing their physical resources. Reclamation requires both the logical snapshot horizon and drained storage-task pins. Maintenance jobs/checkpoints can also pin generations. Atomically coordinate snapshot admission with horizon advancement; never reclaim a version concurrently admitted readers can see.

Free-space maps are storage internals. A crash must not cause double allocation or resurrect retired objects. Recovery validates logged allocation/publication state and may rebuild derived free-space indexes conservatively; it must not guess that an unreachable committed object is disposable without a verified retention/root analysis.

## WAL and Atomic Publication

WAL covers data mutations, catalog changes, required administrative audit/outbox entries, index publication, allocation transitions needed for recovery, authority epochs, and final transaction outcomes. Records are bounded, versioned, checksummed, and associated with the database and transaction. Persist the full required log before a durable commit acknowledgement or flushing a dependent dirty page.

Recovery replays complete committed transactions and does not expose partial catalog/data/index state. Repeated replay must be idempotent. A failed or torn index build is not VALID merely because pages exist. Keep logical visibility publication ordered after the durable decision and ensure restart can complete publication after a crash in between.

The selected initial scheme is no-steal full-page after-image redo with explicit file/root records as defined in WAL v1; page deltas are a later format refinement. Do not equate an OS write completing with durable storage; define file and directory synchronization needed by the supported filesystem profile.

## Checkpoints and Root Selection

Baseline: briefly capture a committed frontier, root map, and dirty-page revisions at a WAL transaction boundary, then release the commit gate and flush/sync the captured obligations. WAL v1 specifies the concurrent-checkpoint replay interval, manifest, CHECKPOINT_END, and alternating root publication. Never hold the gate through the long flush or wait on user code. An incomplete checkpoint leaves the prior root and required WAL valid.

Use the two 4 KiB root records and generation/checksum protocol defined in storage format v1. Choose only the newest complete validated generation, not simply the numerically largest header. Older checkpoints plus retained WAL remain recovery paths until the new checkpoint is durably published and validated.

WAL truncation requires the minimum position still needed by recovery/checkpoints, unfinished supported maintenance, and durable outcome retention. Snapshot-visible old versions must remain in checkpointed storage or retained logs as the chosen version layout requires. A checkpoint does not by itself permit deleting history still visible to readers.

## Outcome Retention

Persist invocation outcome and owner with the commit decision. Expose an authenticated lookup with `ACTIVE`, `COMMITTED`, `ABORTED`, or `UNKNOWN/EXPIRED`; never turn missing history into a definite abort. Keep durable outcomes for a configured documented retention interval, independent of live-process state. If a retained token/credential is no longer valid, lookup requires another valid credential for the owner or an authorized administrator.

After retention expires, automatic retry remains unsafe because the transaction may have committed. Initial submission deduplication is not promised; an outcome ID is a lookup key, not a license to replay a native invocation. Persist enough metadata to enforce retention across restart and specify clock handling before release.

## Corruption and Tests

On invalid root references, ownership mismatches, unsupported encodings, missing committed objects, or irrecoverable log damage, stop affected operation/startup and preserve diagnostic evidence. Do not emit raw pages or secrets to ordinary clients. Checksums provide corruption detection, not encryption or authenticated storage.

Test torn writes, stale generations, double allocation, index root swaps, crashes around commit/checkpoint publication, recovery after catalog drops, long snapshot retention, orphaned private allocations, log truncation boundaries, and expired outcome ambiguity. Verify no public API reveals file paths, page IDs, private roots, or raw credential records.
