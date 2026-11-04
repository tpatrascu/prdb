# Checkpoint Manifest and WAL Records v1

## Scope and Invariants

This document fixes checkpoint payload/manifest version 1 under [WAL v1](wal-recovery.md). All integers are little-endian, reserved fields/flags are zero, UUIDs are opaque 16-byte identifiers, and page references use the existing 40-byte physical format. Checksums/hashes detect corruption, not hostile disk modification.

One checkpoint runs at a time. It captures committed frontier C at a WAL transaction boundary and starts replay at R, the CHECKPOINT_BEGIN record's LSN. No transaction straddles that capture boundary. New transactions may commit while captured pages flush. Their WAL at or after R remains available to bring the possibly newer physical pages and the captured logical root map into a coherent recovered state. Do not serve queries from the manifest's C before replay completes.

## Checkpoint WAL Payloads

CHECKPOINT_BEGIN (kind 7) has exactly 48 payload bytes:

| Offset | Bytes | Field |
| --- | --- | --- |
| 0 | 2 | Payload version=1 |
| 2 | 2 | Reserved |
| 4 | 4 | Flags=0 |
| 8 | 8 | Checkpoint generation |
| 16 | 8 | Captured committed frontier C |
| 24 | 8 | Replay start R; equals this record's header LSN |
| 32 | 16 | Checkpoint UUID |

CHECKPOINT_END (kind 8) has exactly 96 payload bytes. Bytes 0–47 repeat BEGIN's payload, then `manifest_length u64` at 48, SHA-256 of the entire final manifest (32 bytes) at 56, and reserved u64 at 88. Both records use the checkpoint UUID in the WAL header's transaction UUID field. BEGIN has sequence=0 and previous LSN=0; END has sequence=1 and previous LSN=R. They are standalone maintenance markers, **not** a BEGIN/COMMIT transaction and do not advance the commit sequence. They cannot substitute for a data/admin commit decision.

The generation is nonzero and strictly increases across attempts, including abandoned attempts; discover the maximum retained generation from validated roots/manifests/markers at startup before allocating a new one. UUID is nonzero and freshly generated. At most one successful END matches a BEGIN. Duplicate/conflicting complete markers are corruption. An interrupted BEGIN without END has no checkpoint-publication effect; it does not invalidate subsequent complete transaction WAL.

The total framed record lengths are 112 bytes for BEGIN and 160 for END, including the existing 64-byte WAL header. Normal WAL CRC and zero-padding rules apply.

## Manifest Header

Path: `checkpoint/<generation>.manifest`, using the internal canonical unsigned decimal generation without leading zeros. Maximum file length is 64 MiB; fail checkpoint construction without changing active roots if it would exceed this bound. Stream parsing/hashing within budgets; do not allocate from an unchecked count. The manifest has a 128-byte header:

| Offset | Bytes | Field |
| --- | --- | --- |
| 0 | 8 | ASCII `PRDBCKP1` |
| 8 | 2 | Manifest version=1 |
| 10 | 2 | Header bytes=128 |
| 12 | 4 | CRC32C of entire manifest with this field zero |
| 16 | 16 | Database UUID |
| 32 | 8 | Checkpoint generation |
| 40 | 8 | C |
| 48 | 8 | R |
| 56 | 16 | Checkpoint UUID |
| 72 | 8 | Total manifest bytes |
| 80 | 4 | Section count=3 |
| 84 | 4 | Flags=0 |
| 88 | 4 | Storage format version=1 |
| 92 | 4 | Catalog payload version=1 |
| 96 | 4 | WAL version=1 |
| 100 | 4 | Index-key version=1 |
| 104 | 8 | Required feature bits=0 |
| 112 | 16 | Writer build ID |

Writer build ID is a nonzero release/build identifier shared by binaries from the same build, recorded for operator diagnosis, not used as a compatibility or authority check. It resides in the manifest bound by the root's hash rather than enlarging the fixed root header. Supported format/feature fields determine compatibility. After computing and inserting CRC32C, compute SHA-256 over all final bytes, including that CRC; this avoids hash/checksum recursion.

## Section Directory and Entries

Three 32-byte directory entries start at offset 128 in ascending section-kind order. Each contains `kind u16, version u16=1, entry_bytes u32, count u64, offset u64, length u64`. Mandatory sections: ROOTS=1, FILES=2, CATALOG_FAMILIES=3. Unknown, repeated, or missing kinds fail validation. Entry count × entry_bytes must equal section length with checked arithmetic.

Sections follow the directory contiguously in kind order, beginning at offset 224. All entry sizes are multiples of eight, so no padding is needed. Empty FILES is permitted only for an uninitialized fixture, never a published READY database. Sections cannot overlap, contain gaps, or extend past the exact file length. ROOTS and CATALOG_FAMILIES are nonempty for a published database.

**ROOTS entries (64 bytes):** logical storage object ID[16], root kind u16, flags u16=0, reserved u32, root page reference[40]. Sort by raw object ID; no duplicate IDs. Root kinds: catalog directory=1, table row directory=2, SQL index=3, allocation map=4, outcome store=5, audit outbox=6, derived statistics=7. A root reference is non-null, with slot/slot-generation zero, and its page owner matches the storage object. Physical roots are protected metadata, not catalog-discovery values.

**FILES entries (64 bytes):** file ID u64, file generation u64, owning storage object ID[16], file kind u32, page-format version u32, minimum page count u64, lifecycle u32, reserved u32, retirement commit u64. Sort by `(file ID, generation)` numerically; reject duplicates. File kind data=1 (allows heap/B-tree/overflow pages for its owner) or allocation=2. Lifecycle LIVE=1/RETIRED=2. LIVE retirement commit is zero; RETIRED retirement commit is nonzero and at most C. File IDs/generations are nonzero. Minimum page count is the durable captured prefix at C, not the exact file size after later growth. The file length must be page-aligned and at least this count × 16 KiB; replay declarations/images account for later growth. Capture live and retired generations still required by roots/history. No path bytes are stored.

**CATALOG_FAMILIES entries (64 bytes):** family code u16, payload version u16=1, reserved u32, family storage object ID[16], root page reference[40]. Sort by family code; require exactly one entry for every family 1–16 from [catalog payloads](catalog-payloads.md). These are catalog primary lookup B-tree roots; their ownership, family key prefixes, and object IDs must agree with the catalog-directory root and database builtins mapping after recovery. This explicit bootstrap directory allows locating records without first running a SQL lookup or discovering the decoder from the catalog itself.

Allocation-map roots reference protected allocation pages; page ownership/generation/free/retired state is not inferred merely from file size. The allocation-page payload schema remains a separate implementation prerequisite. Outcome/audit roots preserve retained records, not just process-local state. Exactly one catalog-directory, outcome-store, and audit-outbox root is required in a READY database; at least one allocation-map root is required. Their storage IDs are persisted in ROOTS and cannot collide with any other root object. Table/index roots may be absent in an empty database.

## Publication and Root Binding

1. Under the commit gate at a transaction boundary, append BEGIN and capture root/family/file descriptors and dirty revision obligations at C. Pin the necessary generations; release the gate.
2. Flush each captured obligation to that revision or a newer committed revision, after its WAL is durable. Serialize writes to each physical address, sync affected files and creation directories, and keep WAL from R onward.
3. Construct the captured manifest, write it to a new generation-specific file, and sync file plus checkpoint directory. A partial file remains an unpublished orphan; never overwrite a manifest referenced by a root.
4. Append END with exact manifest size/hash and sync its WAL and any new segment directory entry.
5. Write and sync the inactive root slot. Root checkpoint generation, R, C, database UUID, format versions, features, and SHA-256 must equal the manifest. Only this durable root update publishes the checkpoint. Release pins when no other obligation retains them.

Root lifecycle values are INITIALIZING=1 and READY=2; uninitialized state has no valid root. Required root feature bits are zero in v1. Root generation is a separate increasing publication counter, not necessarily equal to checkpoint generation. A READY root requires successful bootstrap/handoff and catalog validation; checkpointing cannot change authentication state by itself.

For the first checkpoint, C may be zero only before any committed initialization transaction; a published READY checkpoint must include committed initialization. Empty/initializing examples are decoder fixtures, not permission to open service listeners.

## Recovery and Retention

Validate the candidate root and manifest checksum/hash, section structure, identities, and referenced base-file availability. Starting at R, require the matching BEGIN and a valid END matching the manifest before accepting that checkpoint as published; END can be found by bounded streaming replay, without storing its LSN in a circular manifest field. Reapply complete post-C transactions and root changes before validating final catalog/allocation relationships and admitting users. C may precede the latest replayed commit.

A later in-place data-page update may leave a page newer than C or torn; use retained committed WAL images under the normal redo rules. Do not reject a recoverable torn page before attempting redo. Conversely, do not treat an unsupported format, missing required WAL span, or valid root pointing at irrecoverable metadata as an excuse to discard acknowledged commits.

Keep BEGIN/END and every segment from each valid root's R through the active log, plus that root's manifest and required base-file generations. Only retire an older root's prerequisites once it is durably replaced/invalidated under a defined root-retirement operation; ordinary slot replacement suffices in the baseline. Incomplete checkpoint artifacts can be cleaned only after proving neither root nor retained maintenance references them. Retention is separate from MVCC reclamation and outcome expiry.

## Required Fixtures

The directory ends at byte 224. A synthetic manifest with one root, one file, and one family entry has section offsets 224/288/352 and total size 416; it tests framing only and must fail READY validation because required families/roots are missing. A manifest with four roots, one file, and sixteen families has offsets 224/480/544 and total size 1,568; its semantic validity still depends on actual referenced contents.

Add fixtures for exact BEGIN/END lengths, mismatched UUID/C/R/hash, duplicate directory entries, arithmetic overflow, zero/unknown features, truncated manifests, wrong-owner roots, missing families, newer file growth, interrupted publication at every sync, both-root retention, and redo of newer/torn pages. No runtime implementation or power-loss proof is claimed by these document examples.
