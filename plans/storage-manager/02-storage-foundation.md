# redb Foundation

1. Pin redb and Rust; record license, supported target, durable mode, version compatibility, and tested filesystem assumptions.
2. Build a disposable storage adapter that opens a redb file, creates the exact engine tables, writes/reads typed fixtures, commits, aborts, and reopens.
3. Freeze bounded explicit encodings for row values, catalog values, index keys, metadata, outcomes and audit records. Do not serialize Rust structs or redb internals.
4. Verify guards/borrowed values cannot escape a read transaction and choose ownership conversions for private overlays and IPC.
5. Add red/green tests for malformed codecs, key sort order, failed/aborted transactions and reopen consistency.

Exit when codec fixtures and real redb transaction/reopen tests pass on the pinned release. This phase does not implement pages, B-trees, WAL, checkpoints, allocation, or disk I/O.
