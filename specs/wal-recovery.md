# WAL Format and Recovery v1

## Decision

Use redo-only full-page after-images with no-steal publication: private/uncommitted page images never enter durable database files. Log one complete image per changed page, plus a durable transaction decision. This is intentionally simple but write-amplifying for an operational workload; delta logging and group commit require subsequent measured refinements. It is not an analytics-oriented append-only data model.

The storage commit gate initially serializes validation, WAL append/sync, and publication. Other invocations can read/compute/stage writes concurrently. Do not hold unrelated page latches or wait for user code while syncing. Group commit is not assumed by the initial algorithm.

## Segment and Record Framing

WAL segments are 64 MiB including a 4 KiB segment header. Header fields in order: magic `PRDBWAL1` (8), version u16=1, header bytes u16=4096, CRC32C u32, database UUID (16), segment number u64, previous segment number u64, required features u64, reserved to 4096. Segment zero uses previous=`UINT64_MAX`. Checksum covers the header with its checksum field zero.

LSN is a u64 byte position in the virtual segmented stream: `segment_number * 64 MiB + file_offset`. Segment headers consume address space but are not record payloads. Reject arithmetic overflow. Records are 8-byte aligned and never cross a segment boundary; zero-fill an unusable segment tail before continuing in the next segment. A nonzero malformed tail is not padding.

Each record has a 64-byte header:

| Offset | Bytes | Field |
| --- | --- | --- |
| 0 | 4 | `WAL1` magic |
| 4 | 2 | Version=1 |
| 6 | 2 | Record kind |
| 8 | 4 | Total bytes including aligned padding |
| 12 | 4 | Payload bytes |
| 16 | 8 | Record LSN |
| 24 | 16 | Transaction/maintenance UUID |
| 40 | 8 | Previous record LSN in that transaction, zero if none |
| 48 | 4 | Transaction-local sequence |
| 52 | 4 | Flags |
| 56 | 4 | CRC32C of header/payload/padding with this field zero |
| 60 | 4 | Reserved |

Maximum record size is 1 MiB; all padding is zero. Record kinds: BEGIN=1, FILE_DECLARE=2, PAGE_IMAGE=3, ROOT_SET=4, COMMIT=5, OUTCOME_ABORT=6, CHECKPOINT_BEGIN=7, CHECKPOINT_END=8. Unknown kinds/flags are unsupported, not silently skipped. Data-changing maintenance transactions use the same BEGIN/COMMIT discipline. Standalone CHECKPOINT_BEGIN/END markers use the distinct framing/sequence rules in [checkpoint format v1](checkpoint-format.md); they are not commit decisions.

## Payloads and Commit Ordering

BEGIN includes invocation ID (16), principal ID (16), assigned commit sequence u64, transaction class u32, reserved u32. FILE_DECLARE includes file ID/generation (two u64), owning object UUID (16), file kind u32, format u32; files are not addressed by externally provided paths. PAGE_IMAGE is exactly one 16 KiB page whose page LSN equals the enclosing record's LSN. Allocation transitions, logical metadata, index state, authority epochs, and audit/outcome store changes are represented by page images.

ROOT_SET contains a count u32, reserved u32, then entries `(logical object UUID, root page reference[40])`; it atomically updates logical root mappings, including catalog/storage bootstrap roots when applicable. COMMIT contains commit sequence u64, prior record count u32, reserved u32, and SHA-256 of the ordered complete prior transaction records (32). This checks transaction completeness, not authenticity. The invocation/principal from BEGIN also define the durable COMMITTED outcome during recovery, without circularly logging a record containing its own LSN.

Abort outcomes have a separate bounded standalone OUTCOME_ABORT record containing invocation/principal IDs and a reason code; sync before claiming durable ABORTED status. If no durable outcome can be established after crash, return UNKNOWN, never infer safe retry from missing data. Private row rollback needs no undo WAL because those pages were never flushed.

Finalization: validate under the gate; assign commit sequence; prepare images; append BEGIN and all required images/root records; append COMMIT; wait for all segment writes to finish; sync every touched WAL file plus any newly created segment directory entry; then publish all images/root changes and committed frontier; then acknowledge. A completed write CQE alone is not a durable decision.

After COMMIT has been submitted for durable storage, cancellation may race an irrevocable result. Stop accepting new cancellation-as-abort decisions at that transition; a later caller receives pending/terminal status, not guaranteed rollback. On sync/error ambiguity stop further writes and recover before deciding the outcome. Do not append an abort record for a possibly committed invocation.

## Redo and Torn Pages

Recovery starts from a validated checkpoint and scans records in LSN order, checking frame checksums, chains, sequence numbers, transaction digest, database identity, and commit ordering. Stage/revisit images until a complete COMMIT is validated; only then apply that transaction. Incomplete final transactions have no published effects. Bound recovery memory by replaying a validated transaction's LSN span if its images exceed memory budgets.

For a valid target page with matching identity/generation, skip an image only when the on-disk page LSN is at least the image LSN. For an invalid/torn page, restore a complete committed image; never trust an LSN read from an unchecked page. Replaying twice must produce identical data/catalog/index/outcome state. Root declarations and file allocation are replayed with the same transaction boundary.

A partial trailing record in the final active segment may be discarded only when no durable checkpoint/reference or committed transaction requires it. Corruption in the middle of retained history or a required committed span fails recovery. Disk loss of acknowledged WAL cannot be reclassified as an ordinary uncommitted tail. Recovery reports bounded diagnostics and opens no normal listeners until integrity checks pass.

## Checkpoints

At a transaction boundary, emit CHECKPOINT_BEGIN and capture committed frontier C, logical root map, and dirty-page revisions under a brief gate pause. Its LSN R is the replay start: no transaction's WAL is partially emitted across that boundary. Pin captured file/page generations and root descriptors until checkpoint completion or abandonment. Release the gate; flush each captured page to at least its captured revision, observing WAL-before-data and serialized per-address writes, then sync all affected data files and creation directory entries. Pages changed after C are safe because their complete WAL lies at or after R and remains retained.

Write the [checkpoint v1 manifest](checkpoint-format.md) with its exact ROOTS, FILES, and CATALOG_FAMILIES sections, including allocation-root references and CRC32C; sync it and its directory. Append/sync CHECKPOINT_END with the specified generation, C, R, UUID, length, and manifest hash. Then sync the alternate [root slot](storage-format.md). Only now may the checkpoint become the startup candidate. If a step fails, retain the old checkpoint and WAL; do not acknowledge incomplete checkpoint success.

Keep WAL sufficient for both valid root slots until safely retiring/replacing the older checkpoint, and for any recovery/maintenance/outcome requirements. Retain each checkpoint's manifests and required file generations as well; WAL retention alone does not make missing base files recoverable. Checkpointed storage retains all snapshot-visible MVCC/catalog versions; truncating WAL is not permission to reclaim them. Whole-segment deletion follows the minimum required LSN and durable directory updates. Outcome retention lives in checkpointed outcome storage, not merely in a live process.

## Required Tests

Golden headers/checksums, rollover/padding, short writes, multi-segment commits, sync errors, cancellation at each transition, torn data pages, partial tail versus mid-log corruption, repeatable redo, root swaps, checkpoints racing updates, and both-root retention. Inject process and storage faults separately; kill-only tests cannot establish power-loss guarantees. [Checkpoint](checkpoint-format.md), [catalog payload](catalog-payloads.md), and [key](index-key-encoding.md) schemas now have concrete encodings; executable golden fixtures are still required. Allocation/outcome/audit payloads and non-checkpoint WAL details remain explicit prerequisites where not yet defined.
