# Disk I/O Decision

The engine accesses database files only through the pinned [redb backend](redb-storage.md). It does not implement a parallel io_uring ring, direct-I/O profile, page buffer pool, or database-file descriptor path. Do not assume redb uses io_uring or that its zero-copy key/value guards eliminate kernel or serialization copies.

Use the redb API's supported durability configuration and file ownership rules. No second engine writer or custom I/O backend may access the live database file. Evaluate operational latency and bounded scan behavior with the selected redb release on supported Linux filesystems; filesystem crash guarantees remain a separately tested deployment profile.

The sandbox continues to deny database access to executors. Any future custom storage backend must be an explicit redb-supported integration, with equivalent atomicity/recovery evidence and a separate design review.
