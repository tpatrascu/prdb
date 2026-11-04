# P0–P3: Storage Foundation

## P0 — Establish Contracts and Test Infrastructure

Read the [decision register](01-boundaries-and-decisions.md), [storage format](../../specs/storage-format.md), [WAL](../../specs/wal-recovery.md), and [memory model](../../specs/memory-storage.md).

1. Inspect existing files/toolchains; pin the Rust toolchain and minimal workspace dependencies when introducing code. Create a storage library and isolated temporary-directory test helper, not every proposed module.
2. Introduce distinct types for object IDs, row IDs, commit sequences, LSNs, and physical references. Keep untrusted wire representations separate from validated values.
3. Build deterministic schedule hooks and an I/O fault model. Distinguish submitted writes, completed writes, synced bytes, and durable directory entries; allow dropped unsynced writes, torn writes, short operations, reorderings, and errors.
4. Establish bounded test deadlines and seed recording. Hand-check fixture bytes and expected histories independently of the production encoder.

**Red starting point:** an invalid physical reference or overflowed offset is accepted by a minimal decoder. Scaffolding/compiler failures do not count as behavioral failures.

**Exit:** reproducible build/test commands exist; malformed values fail without allocation explosions; fault-model semantics and limits are documented. No storage durability claim yet.

## P1 — Freeze and Implement Versioned Codecs

Use [catalog payload v1](../../specs/catalog-payloads.md), [checkpoint format v1](../../specs/checkpoint-format.md), and [index-key encoding v1](../../specs/index-key-encoding.md) as the codec contracts; their documented byte examples still need executable fixtures.

Implement in small cycles: scalar values and references; 128-byte page header; heap slots and 112-byte row headers; field descriptors/overflow; B-tree headers/keys; 4 KiB root records; WAL segment/record headers; catalog/allocation/manifest payloads as prerequisites become concrete.

Use explicit endian readers, checked lengths, zeroed padding, and CRC32C with the checksum field zero. Retain the specified 16 KiB pages, 64-page extents, 64 MiB WAL segments, 8-byte WAL alignment, and record/value/key limits. Validate identity, kind, version, reserved fields, overlap, and generation before following references. Unknown required features fail closed.

**Red tests:** flip each header discriminator; truncate descriptors; overlap slots; overflow a length; feed invalid UTF-8 or oversized indexed values; compare negative/positive numeric and composite keys; introduce overflow cycles. Test NULL separately from the forbidden persisted REDACTED state.

**Exit:** independently reviewed golden fixtures round-trip byte-for-byte; malformed fixtures are rejected; key order matches typed SQL comparison including null rules; a decoder fuzz target operates with bounded memory. Add source/version notes to each fixture.

## P2 — Reference I/O, WAL Append, and Redo

1. Implement exclusive database-directory ownership and internally generated filenames. Define safe creation/open behavior and reject invalid existing directories. Do not expose normal listeners.
2. Implement the blocking reference backend behind the future completion-owned I/O contract. Handle short reads/writes, EOF, sync errors, file creation, and directory sync explicitly.
3. Implement WAL framing, record chains/digests, segment rollover and zero tail padding. Build vectored immutable header/page writes without concatenating every page into another large buffer.
4. Implement a transaction log batch with BEGIN, declarations, page images/root updates, and COMMIT. Separate appended and durable LSNs; advance durability only after required writes and file/directory syncs complete.
5. Implement bounded replay of complete committed batches, ignoring incomplete final transactions only under the specified tail rules. Restore torn pages from verified full-page images. Preserve uncertain outcomes rather than reporting an invented abort.

**Red tests:** acknowledge after write completion but before sync; lose a newly created unsynced WAL segment; tear a data page; lose a commit reply; corrupt a middle record; replay the same log twice; roll a multi-record transaction across a segment boundary.

**Exit:** synthetic page transactions survive modeled power loss iff durable; redo is idempotent; partial publication is never recovered; corrupt required history fails closed. This is internal recovery infrastructure, not an authenticated database milestone.

## P3 — Buffer Pool, Allocation, Rows, and Minimal Dictionary

1. Add bounded frame allocation, deduplicated misses, immutable pinned images, dirty revision tracking, and per-address write serialization. Test image lifetime independently from logical MVCC retention.
2. Add transactional allocation reservations with file/page/slot generations. Reuse only after all required references drain; define generation-overflow rejection.
3. Implement heap insert/version lookup, overflow chains, and row-directory B-tree search/insert/split/root replacement. Keep construction private until its WAL transaction commits. Begin reference scans without secondary-index shortcuts.
4. Implement only the built-in dictionary records needed to bootstrap identities, table/column definitions, authority, epochs, and physical roots. Use fixed engine descriptors, never SQL or SDK execution, to decode them.
5. Provide an internal initial checkpoint/root creation path for an empty/new database. Full online checkpoint orchestration follows in P8. Exercise dictionary and root decoding through restart.

**Red tests:** two misses duplicate a buffer; a cancelled task frees a pending read; page compaction changes a stable slot identity; an old reference resolves after reuse; a B-tree split exposes an incomplete root; a row outlives the schema required to decode it.

**Exit:** rows and minimal catalog are durable through the P2 machinery; borrowed values remain valid under pins; allocations are bounded; unpublished pages cannot be flushed; no raw page reaches a query-facing interface. Production bootstrap credential delivery and public admission remain gated on P5.
