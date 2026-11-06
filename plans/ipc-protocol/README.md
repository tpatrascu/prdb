# Internal IPC Implementation Plan

## Contract and Dependencies

Implement [local IPC](../../specs/ipc-protocol.md) over copied, bounded Unix-socket frames. Depend on [interface generation](../toolchain/README.md); integrate [supervisor](../supervisor/README.md), [storage](../storage-manager/README.md), [optimizer](../planner-compiler/README.md) and [gateway](../client-gateway/README.md) incrementally. External TCP framing has its [own plan](../client-protocol/README.md).

Server-provisioned connections bind role, invocation, identity and grant. Message fields and handle possession cannot select authority. No native struct dumps, database descriptors, native callbacks or generic descriptor transfer are allowed.

## Decisions Before Implementation

The frame envelope is specified and implemented in Rust: fixed 20-byte v1 header, big-endian fields, 1 MiB payload cap, and nonzero operation/request IDs. Still specify operation/error tags, bounded expression IR, collection/nesting sizes, outstanding requests/cursors, handle generations and request-ID rules. Define a role/operation matrix covering executor, planner, supervisor control, gateway service and administrative control. Freeze sanitized value/batch encodings with explicit VALUE/NULL/REDACTED states and initialized padding.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| IPC-1 | Hand-checked frames decode under split/coalesced reads; invalid lengths/versions fail before allocation | Rust bounded framing is implemented and tested; C codec parity and canonical cross-language error fixtures remain |
| IPC-2 | Executor attempts to finalize, estimate or administer fail even with validly encoded messages | Bind service roles on provisioned channels; implement allowlists and context lifecycle validation independently of SDK wrappers |
| IPC-3 | Cross-connection, stale-generation and closed-context handles fail without disclosing object existence | Resolve logical references independently per connection; add cursor/row ownership, duplicate IDs and late-response rejection |
| IPC-4 | Serialized batches omit invisible rows and every hidden payload byte/length/offset | Encode sanitized owned buffers after storage authorization; bounded next/close semantics, initialized capacity and reference-evaluator comparison |
| IPC-5 | Overlapping streams, disconnect, partial mutation and cancellation cannot commit a prefix | Enforce one invocation stream/cursor advancement at a time, terminal statement errors, drain pins and supervisor-only finalization |
| IPC-6 | Malformed requests, blocked readers and planner channel loss remain bounded across real processes | Add decoder/state-machine fuzzing, queue limits and deadlines; separately version gateway-service encoding rather than exposing arbitrary IPC tunneling |

## Acceptance and Handoff

Require C/Rust fixture parity and real local-socket role/handle tests. Integrate authenticated storage access and confined hostile protocol clients; a decoder unit suite alone does not prove authorization. Copied buffers are the baseline; a future negotiated sealed sanitized-memfd capability needs its own spec and adversarial gates before enabling descriptor transfer. First behavior: a bounded envelope with one valid fixture and an oversized-length rejection.
