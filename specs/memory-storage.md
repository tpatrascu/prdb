# In-Memory Storage and Copy Boundaries

## Trusted Buffer Pool

Use a bounded Rust buffer pool keyed by database/file/page identity and allocation generation. A frame owns a 16 KiB aligned allocation, state (`EMPTY`, `READING`, `CLEAN`, `DIRTY`, `WRITING`, `FAILED`), immutable published page image, page LSN, dirty recLSN, pin/reference counts, and I/O generation. Alignment satisfies the selected I/O profile; a stable allocation is required for every outstanding kernel operation.

Deduplicate concurrent misses through a page-directory entry and wake waiters only after checksum/identity validation. Never expose a partially read page. Scans get bounded prefetch and a low-priority/scan admission class; protect operational hot pages and reserve capacity for WAL/commit/recovery.

Committed page images are immutable while borrowed or submitted for I/O. Updates build a private replacement image and atomically replace the directory's current image at publication. Old borrowed images remain alive until pins drain. Distinguish image memory lifetime from MVCC version lifetime: future readers may find old row versions inside a newer page image.

## Private Transactions and Decoding

Maintain a transaction-local arena with changed-row patches/new values, expected base-version identities, private row-directory overlay, dependency epochs, and a write generation. Never mutate published page bytes through a borrowed Rust slice. Read-your-own-writes resolves the overlay before the committed snapshot. Updates preserve hidden unchanged fields entirely inside storage.

Decode validated page records into borrowed byte slices and small scalar values, not one heap allocation per cell. Use explicit endian readers and checked arithmetic; on-disk alignment does not justify casting arbitrary bytes to Rust references. Borrowed views cannot outlive frame/version pins. Policy evaluation can borrow hidden fields but cannot send their backing page to the executor.

A field patch is copied/owned once on validated IPC ingress; do not retain pointers into mutable executor memory. Coalesce repeated updates in the private write set. At finalization, prepare one copy-on-write image per changed page, including row/index/catalog changes; reuse unchanged overflow storage where safe. Admission bounds private pages, value buffers, WAL bytes, and pinned history before allocating them.

## Explicit Copy Budget

| Boundary | Initial behavior | Safe reduction |
| --- | --- | --- |
| Disk to buffer pool | Read directly into the owned frame; buffered I/O may still copy through the kernel page cache | Registered buffers/direct I/O after profile validation |
| Page to trusted evaluation | Borrow validated field slices under pins | Avoid intermediate row/object materialization |
| Committed page to changed page | One private image copy per touched page | Later validated deltas/layout changes; no in-place mutation of borrowed images |
| Page to executor batch | Gather only authorized values once into a compact owned buffer | Write directly into final sanitized output; never share raw pages |
| WAL encoding | Own bounded headers plus immutable page image slices | Vectored writes avoid concatenating page payloads into another large buffer |
| Executor batch to SDK operators | Borrow immutable batch buffers until release | Explicit lifetime APIs; no per-cell copies |
| Executor write to storage | Copy/validate framed input into private owned state | Later sealed immutable ingress requires separate review |

“Low copy” is an objective measured in bytes, allocations, and retained-memory time; it is not an unconditional zero-copy promise. Checksumming, serialization headers, page replacement, and authorization compaction remain necessary work.

## Authorized Batches

Use column-oriented transport batches even though disk storage is row-oriented. Each batch has a bounded row count, column type descriptors, scoped row handles, per-cell VALUE/NULL/REDACTED states, and value offsets/lengths relative to the batch. All offsets are checked; null/redacted entries carry zero length and no source offset. Payloads contain only authorized bytes, with initialized padding. Do not expose hidden string lengths through sparse gaps or reusable buffer capacity.

The initial transport remains copied Unix-socket frames. Serialize directly into the final sanitized buffer and use bounded vectored sends where useful; retain buffers until send completion. A disconnect/slow consumer cannot pin the whole database cache: output buffers own their copied values and are independently budgeted.

An optional later transport may use a fresh memfd containing only one invocation's sanitized batch. After populating and unmapping writable views, require write/grow/shrink seals and seal-set finalization before passing its descriptor; the worker maps read-only. Never reuse a shared object for future secrets because a hostile worker can retain its descriptor. Acknowledgement is not proof it unmapped the object. Budget outstanding objects and process lifetime, validate seals, and keep raw database descriptors forbidden. See [Linux memfd documentation](https://man7.org/linux/man-pages/man2/memfd_create.2.html).

Writable shared memory between worker and storage is outside the baseline: it creates validation/use races. Shared batch transport requires an explicit capability-negotiated descriptor type, sandbox update, and adversarial tests before enabling it; unsupported hosts keep copied IPC.

## Concurrency and Failure

Use short page-directory latches, documented lock order, and I/O references independent of task futures. Dropping/cancelling a Rust future cannot free kernel-owned memory. Per-page writes are serialized by physical address/generation so an older image cannot finish after and overwrite a newer one. Dirty state clears only when the completed write covers the required image/LSN; later changes remain dirty.

Release pinned versions only after all readers, encoders, and I/O operations finish. Evict clean unpinned frames; flush eligible committed dirty frames only after their WAL is durable. Pressure causes bounded wait/admission failure or explicit invocation abort, never unsound lifetime shortcuts. Fault tests must cover cancellation during reads/writes, stale CQEs, frame reuse, slow output, and retained shared descriptors.
