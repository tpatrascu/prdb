# Client SDK and Query Library Contract

## Required Model

The user writes a native query program in their client and compiles it into a loadable shared library using the SDK. The client submits that artifact and parameters. A disposable confined executor loads it and invokes its entry point. The SDK includes basic SQL support and public definitions for interacting with database components.

The user program may call SQL, process authorized results, or use lower-level scoped storage APIs. SQL is a convenience layer, not an authorization boundary. Compilation does not require database access or live statistics; optimization happens when SDK SQL calls run against an admitted execution context.

## SDK Contents

| Surface | SDK contents | Runtime authority |
| --- | --- | --- |
| Query entry point / executor | Versioned C ABI, host function table, parameters, result emission, errors, cancellation | Current invocation only |
| Storage manager | Schema/value types, redaction states, opaque handles, scan/cursor/mutation request definitions and stubs | Scoped reads and staged writes validated by storage |
| Optimizer | Structured request and physical-plan definitions, planning submission stub | Plans for the current context |
| SQL | Parser, expression semantics, physical-plan interpreter, result API | Same storage authority as user code |
| Client submission/network | TCP framing, authentication, artifact upload, compatibility manifest, START, provisional results, outcome/status/cancellation | Client-only gateway connection; caller-authenticated admission |
| Administration | Schema/user/role/policy DDL builders and token lifecycle calls | Separate authenticated control channel; never an executor capability |
| Statistics and other services | Versioned public protocol definitions and role annotations | Definitions confer no access; scoped estimation remains optimizer-only; global catalogs are private |

Generate language bindings from one protocol/schema source when selected. Do not expose process-local storage structures, raw page layouts, private catalogs, or server object pointers. Public definitions are contracts, not copies of server implementations. Services still validate messages from clients that ignore or modify the SDK.

## Client Network Boundary

Use the [network protocol](client-protocol.md) through the dedicated [gateway](client-gateway.md) for external submission and result delivery. Ship the transport/authentication client separately from the C SQL/runtime archive linked into query.so. It may be implemented as a Rust client library/helper with bindings; that choice does not change TinyCC query compilation. The query runtime receives no network sockets or admission credentials.

The client uploads/seals bytes before START, records the reserved invocation ID, and treats result frames as provisional until a supervisor-originated storage-confirmed terminal outcome. Network errors require authenticated status lookup, not automatic invocation replay. Multiple connections can run concurrent invocations; the initial TCP connection carries one operation/result stream at a time. Cancellation/status during streaming uses another authenticated connection.

## Packaging Baseline

Ship headers/bindings and prebuilt position-independent SQL/runtime libraries for supported Linux targets. Compile only user code and link the prebuilt SDK into query.so. Do not rebuild the SQL parser, interpreter, or database services on each query edit. Initially avoid whole-program optimization across the SDK boundary.

A separate SQL shared dependency is an optional packaging alternative, with explicit ABI/dependency matching. The baseline favors a statically linked SDK archive inside the dynamically loaded user artifact. Standard runtime dependencies must still be declared and supported; this is not a promise of a completely self-contained binary.

The artifact manifest declares target OS/architecture, CPU baseline, native ABI version, protocol compatibility, SDK version, entry-point identity, and dependencies. Treat the manifest as untrusted. Pin immutable artifact bytes for admission/loading, bound size, and reject incompatible targets. Hashes identify artifacts; they do not establish trust or permission. Do not execute constructors or load the artifact in a trusted service to inspect it.

## Buffer Lifetime and Workload

Borrow SDK result values from immutable authorized batches until explicit batch release; do not allocate/copy every cell by default. The C API must make ownership and invalidation visible, and copied input validation remains necessary at the trust boundary. Follow [memory storage](memory-storage.md); no API exposes disk-page pointers or ring buffers.

The first SDK value view is [`prdb_sdk_value_v1`](../engine/include/prdb_sdk_value_v1.h). It is a sized/versioned C record with a 32-bit state and type code, a borrowed byte pointer, and a 64-bit length. States are VALUE (0), NULL (1), and REDACTED (2); NULL and REDACTED have type code zero and no bytes. VALUE type codes 1 through 11 match the executor batch scalar codes, with fixed-width integers encoded big-endian, UUID as 16 bytes, Boolean as one byte containing zero or one, and text/varchar as UTF-8. Bytea is uninterpreted bytes. Empty text/bytea may use a null pointer with zero length. Values are capped at 1 MiB. The view never owns or frees its byte pointer; it stays valid only while its source batch remains borrowed. The validator checks record size/version, state/type, lengths, fixed widths, Boolean encoding, and UTF-8 validity. It cannot make an invalid native pointer safe to dereference. Owned batch result buffers and a release API remain future work.

Typed accessors expose Boolean, SmallInt, Integer, BigInt, Date, and TimestampTz values in host C scalar types. Signed integers convert from canonical two's-complement big-endian bytes without relying on implementation-defined unsigned-to-signed casts. The byte accessor returns the original borrowed payload and its length for text, varchar, bytea, decimal, UUID, or any other VALUE type. Access on NULL or REDACTED returns `NO_VALUE`; requesting the wrong scalar type returns `TYPE_MISMATCH`. Callers inspect the state separately to distinguish NULL from REDACTED. These helpers validate the record before reading, but the source bytes must still remain valid and unchanged for the duration of the call and any borrowed view use.

[`prdb_sdk_batch_v1`](../engine/include/prdb_sdk_batch_v1.h) opens the version-1 executor `next_batch` payload in place. Opening validates the complete payload, including its 1 MiB bound, optional continuation cursor kind, at most 128 rows, row-handle kind, at most 4096 cells per row, each cell representation, and exact end-of-payload. Row and cell iterators yield borrowed handles/value bytes without copying. The application retains the response buffer unchanged until all views and iterators are discarded; this API does not allocate a batch or provide a release function. A finished batch has no continuation cursor; otherwise it exposes the opaque 16-byte cursor for the next request. Row handles and cursors are opaque connection-scoped tokens, not row IDs or addresses.

The executor SDK exposes `prdb_sdk_executor_stage_insert_v1`, `prdb_sdk_executor_stage_update_v1`, and `prdb_sdk_executor_stage_delete_v1`. INSERT takes the full schema-order cell shape, with VALUE/NULL/DEFAULT states, writes into a caller-owned bounded request buffer, and returns only a validated kind-8 row handle. UPDATE accepts unique in-range column ordinals with VALUE/NULL data; DELETE takes a kind-8 handle. Both mutation acknowledgements must match version 1 and status 0. These functions serialize IPC only; storage revalidates invocation, snapshot, grants, policies, constraints, and row authority.

The SDK serves operational invocations first and also supports bounded analytical work. SQL analytical operators follow the [workload roadmap](workload-profile.md), run over authorized batches, and must not treat redaction as SQL null or omit denied inputs silently.

## Administrative SDK

Expose typed definitions/builders and text submission for [schema DDL](ddl-schema.md), [users/roles](ddl-identity.md), and [permission DDL](ddl-permissions.md). Administration occurs in the client over a separate authenticated control-mode gateway connection; user query libraries cannot inherit that connection or its tokens. Storage validates every definition and privilege independently. Do not embed credentials in compiled artifacts or logged SQL. Data SQL functions reject DDL in an invocation, even when its principal also has administrative privileges.

## Metadata SDK Surfaces

Provide typed discovery records and bounded paging for [metadata discovery](metadata-discovery.md), including SQL access to its built-in metadata relations. Keep administrative definition inspection and [live monitoring](operational-metadata.md) in the client control API, not executor host callbacks. Expose neither raw dictionary records nor physical roots/paths. Durable status has an explicit unknown/expired outcome so clients cannot mistake missing retained history for safe retry.

## Contract Versions

Negotiate the client TCP protocol and gateway-service protocol independently from the native entry-point/host ABI, internal IPC, and structured-request/physical-plan formats. The SQL SDK API and SDK release version also have explicit compatibility rules. A matching SDK version alone is not sufficient. Reject unsupported versions at their boundary; do not infer compatibility from language or compiler provenance. The manifest advertises compatibility, while runtime endpoints validate it independently.

## Entry-Point Contract

The Linux x86_64 entry-point ABI v1 is defined by [`prdb_query_abi_v1.h`](../engine/include/prdb_query_abi_v1.h), v2 by [`prdb_query_abi_v2.h`](../engine/include/prdb_query_abi_v2.h), and v3 by [`prdb_query_abi_v3.h`](../engine/include/prdb_query_abi_v3.h); Rust mirrors these contracts in `query_abi`. The executor prefers a v3 descriptor, then v2, then v1, and rejects an invalid preferred descriptor rather than falling back. A v3 query exports `prdb_query_descriptor_v3` and `prdb_query_main_v3`, with a host callback that latches SQL statement failure in trusted executor memory. After the entry point returns, any latched statement failure forces a nonzero worker exit even when C code caught the SQL status. The supervisor observes that exit and aborts the invocation. V2 remains available for non-SQL query libraries; v1 carries only parameters.

`prdb_invocation_v1` carries a borrowed parameter pointer and a `uint64_t` byte length, capped at 1 MiB by admission. The pointer may be NULL exactly when the length is zero; otherwise it points to the complete immutable parameter bytes and remains valid only for the entry-point call. The entry point returns zero for success and nonzero for failure. The host and library records use fixed-width fields and natural C alignment; on the supported x86_64 target, host table size/alignment are 8/4, invocation size/alignment are 24/8 with pointer and length at offsets 8 and 16, and descriptor size/alignment are 16/4. A query return code is not itself a transaction commit decision.

Use C-compatible fixed-layout structures, explicit sizes/versions, opaque handles, and explicit buffer ownership. No Rust containers, Go-managed objects, language exceptions, or panics may cross the ABI. Native pointers refer only to executor memory. Calls to other processes use validated serialization.

Constructors execute under confinement before entry-point checks can finish. The entire artifact, bundled SDK, and dependency code are untrusted. Host callbacks cannot constrain arbitrary machine code within the process; server-side roles and the OS sandbox enforce authority.

Allow sequential SQL/storage operations under one MVCC snapshot plus private writes and cumulative budgets. Initially permit one active statement/request stream and single-threaded SDK calls. All statement errors abort the entire transaction; no savepoints or per-statement commits exist. Successful library return requests finalization, but success requires storage-confirmed durable commit. Cancellation/failure accepted before redb commit begins aborts staged writes; after commit begins the outcome may be pending/uncertain and must be reconciled. After commit it cannot undo data. Results remain provisional until commit confirmation, which is not a correctness attestation. Use authenticated invocation-status lookup after lost replies; never automatically rerun a library with an unknown outcome. Exit the process rather than relying on safe unloading of arbitrary code.

## Concurrent Invocation Contract

Multiple clients and executors may run concurrently. Each invocation is one snapshot-isolated transaction; it sees its own staged writes but not others' uncommitted changes. Optimistic row conflicts abort the entire invocation. Snapshot isolation is not serializable. Status lookup, known-abort retries, cancellation, and version retention follow [concurrency and MVCC](concurrency-mvcc.md).

Read-only grants reject mutation calls. Public protocol definitions for commit do not grant the executor the supervisor's finalization role. No arbitrary timestamp selection or hidden historical reads are exposed.

## Language and Compilation Decision

Use C and TinyCC for user queries and SDK SQL/runtime code. Rust implements the database engine, including the executor host, supervisor, optimizer, storage, transactions, and authorization. This is an established choice, not an experiment among language candidates. See [toolchain implications](toolchain.md) for the component split and release gates.

Distribute a pinned TinyCC toolchain and prebuilt C SDK archive; compile only user source per edit. Keep administrative helpers separate from executor runtime code. TinyCC runs in a bounded client subprocess and emits query.so without executing it. Engine Rust compilation is never part of the per-query path.

The versioned C ABI is the interoperability boundary, not permission to use unspecified Rust layouts. C can corrupt the Rust executor host, so all trusted services remain outside that process. Other query languages/compilers are outside the initial supported SDK; artifact provenance never bypasses validation.

## Benchmark and Acceptance Plan

Measure cold and warm compile-plus-link time, one-line-edit turnaround, artifact size, load/startup time, and compile-to-first-result latency. Include SQL wrapper queries, small native loops, and mixed SQL/native work. Measure execution throughput too; reduced compile time may trade off generated-code quality.

Measure the selected TinyCC build and separate SDK release-build time from per-query compilation. Do not assume GCC-style optimization options produce equivalent results with TinyCC. Pin compiler versions, target, dependency packaging, and sandbox profile. No benchmark or toolchain implementation exists yet.

Validate cross-version rejection, malformed manifests, missing symbols, hostile constructors, dependency lookup restrictions, library crashes, protocol forgery, sequential-call budgets, and preservation of redaction states. Confirm SDK provenance never bypasses authorization.

## References

- [TinyCC documentation: shared-library output and compiler options](https://bellard.org/tcc/tcc-doc.html)
- [Toolchain specification](toolchain.md): Rust/C boundaries and implementation gates.

## Open Decisions

Select exact TinyCC/Rust versions, initial target architecture, manifest encoding, libc/runtime baseline, and exact versioned manifest/scope/administrative operation schemas. The initial artifact transport is the framed TCP upload protocol; secure transports are future adapters. Languages and the user compiler are fixed. Server-side native SQL-plan compilation and standalone executable submission are outside the initial path.
