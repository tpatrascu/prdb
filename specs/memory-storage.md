# In-Memory Storage and Copy Boundaries

## redb Ownership

redb owns database page caches, copy-on-write page versions, read-transaction pins, and physical reclamation. The engine does not add a second page buffer pool. It may maintain bounded decoded-schema/policy caches and invocation-local write overlays, each with explicit validity and eviction rules. Cache entries never grant authority.

A redb read transaction anchors the invocation's committed snapshot. Its guards/borrows stay inside trusted storage and must not outlive the transaction. Copy and decode values into engine-owned typed representations only as needed; never pass guards, mapped bytes, redb handles, or database pointers to the gateway, executor, or optimizer.

## Transaction Memory

Each active invocation owns a bounded private overlay keyed by table/row identity: insert, field patch, or delete plus the expected base row version, schema/policy dependencies, and write generation. Copy validated IPC input into this overlay; do not retain pointers into client or executor buffers. Sequential SQL statements read the snapshot plus the invocation's own successful overlay.

At commit, merge patches into current rows inside the serialized redb write transaction after conflict and authorization revalidation. Do not replace a current row with a stale snapshot copy: concurrent edits to different fields or rows must not be lost. The overlay is discarded on abort. Bound bytes, rows, and age before allocation.

For the initial bounded overlay primitive, account for the owned encoded value bytes of each staged row and count distinct row identities. Replacement of a staged value charges only the new total, and a staged deletion consumes one row slot and zero value bytes. These are overlay limits, not full invocation memory budgets: identity storage, patch metadata, snapshot age, and cumulative work require additional accounting. A limit or arithmetic failure aborts and clears the whole overlay; subsequent operations fail until the invocation ends. This primitive does not itself authenticate requests, validate row schemas, or authorize writes.

Staged logical index mutations are also bounded: charge each distinct staged key against the invocation's configured byte budget, and allow at most 256 index-key changes per staged row. Replacing a staged membership for the same key charges it once. Exceeding either limit aborts and clears all staged rows and index changes.

## Safe Copy Reduction

| Boundary | Baseline | Constraint |
| --- | --- | --- |
| redb value to storage evaluation | Borrow through redb access guards when lifetime permits | Guard stays within trusted process and pinned read transaction |
| Executor input to private overlay | Validate and copy once | Executor memory is hostile and mutable |
| Overlay to redb | Encode final values in the commit path | All row/index/catalog/outcome changes share one atomic redb transaction |
| Stored row to result batch | Gather authorized cells directly into final bounded response | Never expose raw redb values or hidden lengths |
| Supervisor output to gateway | Bounded owned buffers may be relayed | Untrusted worker bytes are provisional application output |
| Gateway to client | Frame and stream under backpressure limits | Socket progress is not transaction success |

redb documents a zero-copy, thread-safe key/value API, but that does not mean network output can share stored values. Authorization compaction, typed serialization, and process isolation require a sanitized representation. Measure allocations and copied bytes before optimizing.

## Pressure and Lifetimes

Long read transactions retain historical copy-on-write pages and can delay redb reclamation. Bound concurrent snapshots, invocation age, analytical scans, and outstanding response bytes. `StorageManager` reserves from a shared per-handle active invocation cap before opening a snapshot; cloned handles share the cap, and the reservation follows the transaction through its snapshot lifetime. Managed transactions enforce their configured age at storage boundaries, and the service must poll the transaction deadline while it is idle. Slow clients must not hold redb guards or storage latches. On pressure, reject admission or cancel according to policy; never silently replace a snapshot.

Canceling an asynchronous storage task cannot release a guard still used by work or IPC. Drain in-flight tasks before ending the read transaction and releasing overlay/results. Test large readers, process cancellation, memory growth, and compaction/reclamation with the pinned redb release.

## Optional Shared Results

The baseline uses copied local IPC and TCP frames. A later sealed memfd may contain only sanitized invocation output and needs separate sandbox/capability review. It must not expose redb pages or become a generic database mapping.
