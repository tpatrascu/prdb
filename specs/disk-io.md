# Disk I/O, io_uring, and Durability

## Backend Decision

Adopt io_uring as the preferred Linux disk-I/O backend, subject to conformance tests on the pinned kernel/filesystem profile. Keep an explicit blocking `pread`/`pwrite`/sync backend for correctness testing and supported hosts where io_uring is unavailable. Backend choice is reported at startup and does not change format, durability, permission, or sandbox requirements. No mid-flight silent fallback after uncertain writes.

Only the Rust storage process owns rings, database file descriptors, and registered buffers/files. Executors receive none of them; their seccomp policy denies io_uring setup/registration/submission. A shared kernel/user queue does not mean database data can be safely shared with untrusted processes.

## Submission and Buffer Ownership

Start with one bounded submission/completion owner and queues for WAL/commit, operational page reads, writes/checkpoints, and low-priority scans/maintenance. Keep reserved queue slots for progress and cancellation. Do not initially enable SQPOLL, IOPOLL, multishot operations, or completion suppression. Measure batching before adding more rings or polling threads.

Every request carries an opaque operation ID plus generation, file/page identity, buffer owner, offset, expected length, and completion state. Submit explicit offsets, not shared mutable file positions. Read directly into a stable frame allocation; keep write slices immutable and pinned through completion. Registered buffers are an optional bounded pool, not a reason to pin all database memory.

A cancelled future is not a completed kernel request. Wait for the original request's terminal completion and any cancellation bookkeeping before returning memory or reusing an operation ID. Late CQEs must not complete a newer request with a recycled ID. Check negative errors and short reads/writes; retry the remaining range only under the protocol's ordering and error policy. EOF is valid only where the format permits it.

## Explicit Durability Dependencies

io_uring operations may complete out of order. For the initial backend, wait for successful completion of all writes in a required WAL range before submitting its sync, then wait for successful sync completion before advancing the durable LSN. Submit no data-page flush whose page LSN exceeds the durable WAL frontier. Do not assume enqueue order, a write completion, or a linked chain automatically proves this protocol.

Apply the same dependency graph to data-file sync, checkpoint manifest creation, root-slot sync, and directory sync. Track newly created/extended files and segment rollover. If ring-based sync is unsupported for a required object, a blocking sync can implement that operation only with the same explicit ordering and ownership; surface the profile choice and test it.

Media/I/O errors after a possible commit force the engine into recovery-required state; never acknowledge a guessed rollback or rerun the write through another backend. Startup validates opcode/feature availability and configured limits. Missing I/O acceleration may select the declared fallback; missing required sandbox isolation still fails closed.

## Buffered and Direct I/O

Start with buffered I/O for both data and WAL. The userspace buffer pool may duplicate kernel page-cache data; io_uring does not eliminate that copy. Avoid mmap of mutable database files as a substitute for controlled buffer ownership/recovery.

An optional measured direct-I/O profile can read data into aligned frames to avoid page-cache duplication. Query the filesystem's alignment constraints and validate offsets, lengths, memory alignment, EOF handling, and sync behavior. Do not mix buffered and direct access to the same live file or treat direct I/O as durable without synchronization. A 16 KiB database page does not itself prove the profile is supported. WAL may remain buffered under a separately documented profile.

## Security-Constrained Low Copy

Use borrowed slices inside storage and vectored I/O for immutable WAL payloads. Authorize and compact values into a separate final output buffer before transmission; disk pages, index keys, overflow chains, and history remain private. Shared sanitized output is an optional extension specified in [memory storage](memory-storage.md), not disk zero-copy.

## Tests and Sources

Run the same fault-injection/recovery suite against both backends: short operations, reordered completions, cancellation, ring exhaustion, file reuse, segment rollover, failed sync, shutdown with in-flight buffers, and operational latency during scans. Validate memory/registration limits and measure CPU, copies, queue depth, tail latency, and fsync time independently.

Primary references: [io_uring overview](https://man7.org/linux/man-pages/man7/io_uring.7.html), [operation ordering and completion semantics](https://man7.org/linux/man-pages/man2/io_uring_enter.2.html), and [Linux direct-I/O constraints](https://man7.org/linux/man-pages/man2/open.2.html). Rust crate/version, supported kernel/filesystem matrix, queue sizes, and direct-I/O enablement remain implementation decisions.
