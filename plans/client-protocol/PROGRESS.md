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
