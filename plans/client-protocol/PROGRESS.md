# Client Protocol Progress

## NP-1 dependency: bounded version-1 frame codec

- Behavior: `ClientFrameDecoder` incrementally accepts fragmented/coalesced bytes, validates the fixed PDBN 1.0 header and per-kind payload limits before allocation, rejects nonzero flags/invalid sequences/zero IDs, and reports partial-frame EOF. `ClientFrame::encode` emits the fixed little-endian header. Request-ID and streamed-response sequence helpers enforce monotonic request IDs and consecutive RESULT_DATA/TERMINAL sequence numbers.
- Contract: [client network protocol](../../specs/client-protocol.md).
- Changed code: [frame codec and sequence state](../../engine/src/client_protocol.rs) and [module export](../../engine/src/lib.rs).
- Verification: workspace format, build, warning-denied Clippy, and `git diff --check` pass in the repository Nix shell. Tests were not run.
- Remaining: strict JSON/base64 operation decoding, live TCP/gateway integration, fuzzing and golden fragmented-frame fixtures.

## NP-2 dependency: per-connection HELLO/AUTH state

- Behavior: `ClientConnectionSession` accepts one HELLO, requires storage authentication before authenticated operations, enforces query/control request-kind allowlists, derives query staging ownership from storage-authenticated identity, and revalidates token/login/role authority before each execution or control request. Its connection-scoped bearer-secret copy is zeroized on drop.
- Changed code and contract: [client session state](../../engine/src/client_protocol.rs) and [HELLO/AUTH mode rules](../../specs/client-protocol.md).
- Verification: repository Nix-shell formatting, workspace build, warning-denied Clippy, and `git diff --check` pass. Tests were not run.
- Remaining: strict JSON/base64 wire decoding and TCP listener/service integration.

## NP-2 verification: real-redb session revocation

- Evidence: the focused commit integration test authenticates a seeded token, checks HELLO/AUTH ordering and query mode, revalidates a protected request, then revokes the credential and confirms the next protected request fails while the session ID remains stable.
- Test: [real-redb session regression](../../engine/tests/commit.rs).
- Remaining: wire JSON/base64 decoding and network listener integration.

## NP-2 dependency: strict HELLO and AUTH decoding

- Behavior: HELLO/AUTH accept exact flat string schemas, reject duplicate/unknown fields and malformed JSON, and validate lowercase nonzero token IDs plus canonical padded base64 for the fixed-size credential. Decoded secrets zeroize on drop and do not implement `Debug`.
- Changed code and fixtures: [HELLO/AUTH decoder](../../engine/src/client_protocol.rs) and [control decoder regressions](../../engine/tests/client_protocol.rs).
- Verification: focused protocol suite passed: 4 tests, including duplicate/unknown keys, escapes, trailing bytes, unpaired surrogates, lowercase IDs, and canonical base64.
- Remaining: decode other operation payloads and connect the session state to a live gateway TCP listener/service.

## NP-2 dependency: handshake frame dispatcher

- Behavior: the connection session accepts HELLO as the first request, validates increasing request IDs, answers HELLO/AUTH/PING with bounded sequence-zero responses, and rejects pre-HELLO PING and repeated/out-of-order operations.
- Changed code: [handshake dispatcher](../../engine/src/client_protocol.rs) and [client session integration regression](../../engine/tests/commit.rs).
- Verification: focused real-redb handshake regression and full workspace tests pass; workspace format, build, and warning-denied Clippy also pass.
- Remaining: dispatch query/control operation schemas and connect to TCP and the supervisor service channel.

## NP-2 / SU-1 integration: frame AUTH through storage

- Behavior: the frame-level AUTH dispatcher decodes canonical credentials, calls storage, and returns only the opaque session ID; the real-redb fixture verifies the response correlates to the accepted request and contains no principal/token secret.
- Test: [real-redb HELLO/AUTH/PING frame flow](../../engine/tests/commit.rs).
- Verification: focused handshake regression passes; workspace formatting, build, warning-denied Clippy, and diff checks pass.
- Remaining: network listener and gateway-service IPC integration.

## NP-3 dependency: authenticated binary chunk dispatch

- Behavior: chunk payloads decode only for nonzero invocation IDs and 1–256 KiB data, preserving the little-endian offset. Authenticated query frames route artifact versus parameter chunks through the connection-bound staging owner and return the next accepted offset.
- Changed code and tests: [chunk decoder and dispatcher](../../engine/src/client_protocol.rs), [boundary tests](../../engine/tests/client_protocol.rs), and [real-redb AUTH/upload stream test](../../engine/tests/commit.rs).
- Verification: focused chunk decoder and handshake/upload tests pass in the repository Nix shell.
- Remaining: strict PREPARE/SEAL/START JSON schemas, TCP listener, and gateway-service IPC integration.

## NP-3 dependency: PREPARE and SEAL dispatch

- Behavior: PREPARE enforces exact scalar fields, canonical u64 strings, lowercase SHA-256, canonical base64, protocol byte ceilings, and a supported workload name before reserving authenticated staging. SEAL accepts only one canonical invocation ID and returns `prepared` after staging verifies lengths, digests, and versioned metadata.
- Changed code and tests: [PREPARE/SEAL decoders and handlers](../../engine/src/client_protocol.rs), [schema regressions](../../engine/tests/client_protocol.rs), and [real-redb session upload lifecycle](../../engine/tests/commit.rs).
- Verification: focused protocol tests and HELLO/AUTH/PREPARE/chunk/SEAL integration regression pass in the repository Nix shell.
- Remaining: START frame response dispatch, STATUS/CANCEL, TCP listener, and gateway-service IPC integration.

## NP-4 dependency: one-shot START frame acceptance

- Behavior: START accepts only the strict invocation-ID envelope, routes through the sealed staging transition and current storage-authority revalidation, then returns sequence-zero `accepted` with the admitted supervisor invocation.
- Changed code and test: [START dispatcher](../../engine/src/client_protocol.rs) and [real-redb deadline/revocation regression](../../engine/tests/commit.rs).
- Verification: focused real-redb START regression passes in the repository Nix shell.
- Remaining: launch and supervise the worker before streaming RESULT_DATA and a trusted terminal.

## NP-5 dependency: owner STATUS and CANCEL

- Behavior: query STATUS revalidates current authority and returns an owner's ephemeral prepared state or durable/in-process storage state, while keeping unknown, expired, and foreign IDs non-enumerable. CANCEL revalidates authority, frees unstarted staging without an outcome, or marks an active owned invocation cancelled under the commit barrier.
- Changed code and tests: [owner status and cancellation dispatch](../../engine/src/client_protocol.rs), [storage admission cancellation](../../engine/src/storage_manager.rs), and [real-redb authenticated lifecycle](../../engine/tests/commit.rs).
- Verification: focused authenticated status/cancel, active cancellation, and handshake regressions pass; warning-denied Clippy passes in the repository Nix shell.
- Remaining: control-mode administrator operations and gateway IPC/TCP service integration.

## NP-5 / IPC-6 dependency: gateway-role owner control dispatch

- Behavior: versioned gateway-role IPC status/cancel operations bind the principal from trusted channel setup, perform owner-scoped storage lookups, and return the same unknown result for unknown or foreign IDs. Executor channels are denied these operations.
- Changed code and regression: [gateway control IPC dispatcher](../../engine/src/supervisor_ipc.rs) and [role/non-enumeration test](../../engine/tests/commit.rs).
- Verification: focused real-redb dispatch test and warning-denied Clippy pass in the repository Nix shell.
- Remaining: provisioned gateway-service socket/session lifecycle and TCP connection server.

## NP-3/NP-4/NP-5 integration: submission operations through gateway IPC

- Behavior: the gateway IPC adapter carries AUTH, PREPARE, typed artifact/parameter chunks, SEAL, START, STATUS, and CANCEL through one client-bound session. START returns a typed trusted launch event and only exposes the accepted response for network delivery.
- Changed code and spec: [gateway IPC adapter](../../engine/src/supervisor_ipc.rs) and [payload mapping](../../specs/ipc-protocol.md).
- Verification: the real-redb handshake/submission regression covers the dispatcher lifecycle.
- Remaining: real service socket provisioning, TCP listener, result relay, and process lifecycle.

## NP-3 integration: authenticated TCP upload and seal — verified

- Behavior: the real supervisor and gateway binaries now carry HELLO, bootstrap-token AUTH, PMF1/PSC1 PREPARE, a TinyCC artifact chunk, and SEAL over one external TCP connection. The service validates artifact hashes, compatibility, requested scope and ELF dependencies, then reports `prepared` without loading the artifact.
- Regression: `supervisor_and_gateway_binaries_authenticate_upload_and_seal_tinycc_query_over_tcp` compiles a minimal ABI-v3 shared object with TinyCC and exercises the complete authenticated submission path through both binaries.
- Verification: `cargo test --workspace --locked` passed; this integration test was among the three passing supervisor binary tests. The two sandboxed START tests remain ignored because this host has no writable delegated cgroup.
- Remaining: exercise START, confined loading, result relay and durable terminal reconciliation on a host with a writable delegated cgroup.

## NP-5 integration: prepared STATUS and CANCEL over TCP — verified

- Behavior: through the same authenticated real-binary connection, STATUS reports a sealed reservation as `prepared`, CANCEL releases the unstarted reservation with `accepted`, and subsequent STATUS returns the same `unknown` response used for unavailable invocations.
- Regression: the TinyCC TCP submission test now covers that state transition and confirms the connection remains usable afterward with PING.
- Verification: focused supervisor binary regression, workspace build/format/warning-denied Clippy/tests, and the maintained TinyCC SDK suite passed. Workspace tests reported 109 commit tests passing and two cgroup-gated tests ignored.
- Verification update: the ignored `disconnected_submission_is_reconciled_after_gateway_restart` test now passes on a delegated-cgroup host. It starts a confined invocation, kills the gateway before terminal delivery, reads its ABORTED status through a replacement gateway, restarts the supervisor/storage process, and reads the same retained ABORTED result again.
- Remaining: exercise authenticated active invocation STATUS/CANCEL through the process boundary. The restarted-process test covers durable terminal STATUS and does not exercise cancellation through the active network protocol.

## NP-4 integration: START sandbox preflight rejection over TCP — verified

- Behavior: a valid sealed TinyCC submission cannot enter storage admission when the configured cgroup parent is an ordinary directory. The real supervisor/gateway path returns the generic `unavailable` error, and a new authenticated connection sees the invocation as `unknown`.
- Regression: the binary test uses its private temporary directory as the deliberately invalid cgroup parent, making the preflight failure deterministic without depending on host cgroup permissions. The artifact is never loaded.
- Verification: the focused real-binary test passes alongside full workspace build/format/Clippy/tests and maintained TinyCC SDK tests. Two positive sandbox START tests remain ignored pending writable cgroup delegation.
- Remaining: successful sandboxed START and terminal outcome relay remain gated on a conforming delegated-cgroup host.

## NP-6: authenticated ADMIN ordinary-index operations — implemented

- Contract: [client protocol](../../specs/client-protocol.md) defines exact PREPARE/START argument schemas for ordinary index create, rebuild, and drop. Operations remain bound to one authenticated control connection and START must use current storage authorization.
- Implementation: `ClientConnectionSession` strictly decodes `indexes.create`, `indexes.rebuild`, and `indexes.drop`, binds each PREPARE to a random pending operation ID, then routes START to the typed `StorageManager` index lifecycle methods with a correlated audit event.
- Regression: decoder coverage checks canonical requests for all three operations. The real-redb authenticated gateway lifecycle test prepares, starts, and commits create/rebuild/drop through the gateway dispatch path, then validates persisted catalog state.
- Verification: full workspace build, formatting, warning-denied Clippy, and tests passed in the configured Nix shell; the commit integration target reported 124 passed and 3 cgroup-dependent ignored.
- Remaining: expose a user-facing client API for these ADMIN commands and verify them through the separately managed process boundary; that client work remains in the SDK and gateway plans.
