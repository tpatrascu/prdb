# Internal IPC Implementation Plan

## Contract and Dependencies

Implement [local IPC](../../specs/ipc-protocol.md) over copied, bounded Unix-socket frames. Depend on [interface generation](../toolchain/README.md); integrate [supervisor](../supervisor/README.md), [storage](../storage-manager/README.md), [optimizer](../planner-compiler/README.md) and [gateway](../client-gateway/README.md) incrementally. External TCP framing has its [own plan](../client-protocol/README.md).

Server-provisioned connections bind role, invocation, identity and grant. Message fields and handle possession cannot select authority. No native struct dumps, database descriptors, native callbacks or generic descriptor transfer are allowed.

## Decisions Before Implementation

The frame envelope is implemented in Rust: fixed 20-byte v1 header, big-endian fields, 1 MiB payload cap, and nonzero operation/request IDs. Rust also defines fixed v1 operation tags, rejects unknown tags during frame decoding, and provides a bootstrap/executor/planner/supervisor/gateway/admin-control allowlist. Gateway and supervisor control loops enforce roles on bounded streams; executor/planner service provisioning, explicit error tags, bounded expression IR, collection/nesting sizes, outstanding cursors, handle generations, and sanitized batch encodings remain open.

The gateway submission payload mapping is specified in [IPC protocol](../../specs/ipc-protocol.md): one service context per client connection, bounded JSON forwarded without TCP headers, and a one-byte chunk-kind prefix. A role-checked Unix-stream loop now opens each client session with HELLO, returns typed START events, and cleans up staging/cancellation on channel failure. Listener provisioning and the network TCP gateway remain unimplemented.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| IPC-1 | Hand-checked frames decode under split/coalesced reads; invalid lengths/versions fail before allocation | Rust bounded framing and the TinyCC-built C codec consume shared generated header offsets, version, magic, size limits, operation tags, and v1 frame-validation status values. Both match the hand-checked frame fixture and the canonical error-status fixture |
| IPC-2 | Executor attempts to finalize, estimate or administer fail even with validly encoded messages | The role matrix and pure allowlist helper exist; bind roles to provisioned channels and enforce the helper in handlers, then add context lifecycle validation independently of SDK wrappers |
| IPC-3 | Cross-connection, stale-generation and closed-context handles fail without disclosing object existence | Resolve logical references independently per connection; add cursor/row ownership, duplicate IDs and late-response rejection |
| IPC-4 | Serialized batches omit invisible rows and every hidden payload byte/length/offset | Encode sanitized owned buffers after storage authorization; bounded next/close semantics, initialized capacity and reference-evaluator comparison |
| IPC-5 | Overlapping streams, disconnect, partial mutation and cancellation cannot commit a prefix | Enforce one invocation stream/cursor advancement at a time, terminal statement errors, drain pins and supervisor-only finalization |
| IPC-6 | Malformed requests, blocked readers and planner channel loss remain bounded across real processes | Add decoder/state-machine fuzzing, queue limits and deadlines; separately version gateway-service encoding rather than exposing arbitrary IPC tunneling |

## IPC-6 dependency: gateway submission dispatcher

- Behavior: gateway role authorizes each submission operation; strict AUTH, PREPARE, chunk, SEAL, START, STATUS, and CANCEL session handlers are invoked with per-connection state. START returns an in-process supervisor invocation/submission event while only the correlated acceptance payload is eligible for response encoding.
- Contract and implementation: [gateway submission payloads](../../specs/ipc-protocol.md) and [dispatcher](../../engine/src/supervisor_ipc.rs).
- Verification: real-redb submission lifecycle test passes through the operation dispatcher; warning-denied Clippy passes in the repository Nix shell.
- Gateway error handling: the gateway forwards only the five canonical sanitized v1 error payloads; other supervisor payloads close the client connection. See [IPC progress](PROGRESS.md).
- Remaining: provision per-client Unix sockets/generation references and launch/reap a confined worker.

## IPC-6 dependency: per-client gateway Unix service loop

- Behavior: one stream owns one `ClientConnectionSession`; request IDs remain strictly increasing across AUTH and submission operations. The loop returns the START acceptance and typed invocation event, then can resume after the caller handles execution. EOF and protocol/write failures discard unstarted staging or request cancellation for started work.
- Changed code and regression: [gateway session service loop](../../engine/src/supervisor_ipc.rs) and [real-redb UnixStream lifecycle](../../engine/tests/commit.rs).
- Verification: the focused UnixStream HELLO/AUTH/PREPARE/chunk/SEAL/STATUS/START/CANCEL regression passes.
- Remaining: provision authenticated listener sockets and a concurrent supervisor execution/result relay while monitoring disconnects.

## Acceptance and Handoff

Require C/Rust fixture parity and real local-socket role/handle tests. Integrate authenticated storage access and confined hostile protocol clients; a decoder unit suite alone does not prove authorization. Copied buffers are the baseline; a future negotiated sealed sanitized-memfd capability needs its own spec and adversarial gates before enabling descriptor transfer. First behavior: a bounded envelope with one valid fixture and an oversized-length rejection.
