# Internal IPC Progress

## IPC-3 dependency: generated supervisor control request validation — implemented

- Behavior: status and administrative-cancel requests are decoded and validated through their exact schema-generated operation layouts.
- Implementation: [supervisor control dispatcher](../../engine/src/supervisor_ipc.rs) no longer manually reads the common version prefix before invoking those decoders.
- Verification: focused owner-scoped status/cancel and control-socket tests passed; locked build, formatting, warning-denied Clippy, serial workspace tests (129 passed, 3 cgroup-dependent ignored), and TinyCC SDK suite passed.
- Remaining: role/handle tests across real processes, queue/deadline hardening, and worker lifecycle coverage remain open.

## IPC-6 dependency: generated terminal request encoders — implemented

- Behavior: the gateway sends the exact generated version-only request for finalize or abort and emits no terminal event for an unrelated operation.
- Changed code and regression: [supervisor terminal relay](../../engine/src/supervisor_ipc.rs) uses operation-specific generated encoders; the unit regression checks exact bytes and rejects status lookup as a terminal operation.
- Verification: focused regression, locked workspace build/format/warning-denied Clippy/serial tests and the maintained TinyCC SDK suite passed. Three cgroup-dependent tests remain ignored by default.
- Remaining: other hand-coded payloads, process-level queue/deadline hardening, and worker lifecycle coverage remain open.

## IPC-2 dependency: generated executor relation response prefixes — implemented

- Behavior: relation resolution encodes an exact unavailable variant or a successful fixed prefix followed by bounded authorized relation metadata.
- Implementation: [shared ABI schema](../../engine/abi/query_abi_constants.json) now defines both response prefixes; [executor IPC](../../engine/src/executor_ipc.rs) uses the generated codecs and keeps handle validation and variable-tail parsing in Rust.
- Regression and verification: exact-byte round-trip and malformed variant coverage pass, as do ABI generation checks, locked workspace build, formatting, warning-denied Clippy, serial workspace tests (129 passed, 3 cgroup-dependent ignored), and the maintained TinyCC SDK suite.
- Remaining: other IPC payload migrations, process-level queue/deadline hardening, and worker lifecycle coverage remain open.

## IPC-6 dependency: generated terminal acknowledgement validation — implemented

- Behavior: the gateway accepts storage finalize/abort acknowledgements only when the operation has its exact versioned response layout and a success status.
- Implementation: [supervisor terminal relay](../../engine/src/supervisor_ipc.rs) now uses shared schema-generated decoders rather than duplicating response widths and offsets.
- Regression and verification: valid responses and truncated, unsupported-version, nonzero-status, and trailing-byte responses are covered. The focused regression and full locked Rust/TinyCC validation suite passed; the serial workspace suite reported 129 passed and 3 cgroup-dependent ignored tests.
- Remaining: additional hand-coded IPC payload validation, process-level queue/deadline hardening, and worker lifecycle coverage remain open.

## IPC-6 dependency: canonical gateway error payload validation — implemented

- Behavior and contract: [`GatewayClientError`](../../specs/ipc-protocol.md) is a supervisor-to-gateway event whose payload must be one of the five fixed, sanitized v1 JSON errors. The gateway now compares the complete payload bytes against that allowlist before encoding a client ERROR frame; an unknown category, modified message, extra field, malformed JSON, or other payload closes the connection as a mismatched service response.
- Red evidence: `gateway_rejects_unsanitized_supervisor_error_payloads` failed before the change because the gateway wrote an 87-byte frame containing `secret principal detail` to the TCP client.
- Regression: the same test now verifies the client sees EOF and the gateway reports `MismatchedServiceResponse`. `gateway_bridge_returns_sanitized_supervisor_error_frames` verifies a canonical error still reaches the client unchanged.
- Verification: both focused gateway boundary tests passed. `cargo build --workspace --locked --offline`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`, and `cargo test --workspace --locked --offline -- --test-threads=1` passed in the pinned Nix shell. The full suite reported 3 cgroup-gated commit tests and 1 cgroup-gated metadata process test as ignored by default.
- Follow-up: the supervisor producer and gateway validator now share one typed `GatewayClientErrorCode` definition and canonical payload bytes, avoiding parallel JSON formatting and whitelist definitions. Unit coverage checks all five payloads and rejects missing fields or modified messages.
- Follow-up verification: canonical payload unit tests, the valid TCP relay regression, and the raw-payload rejection regression passed. Workspace build, formatting, warning-denied Clippy, and serial locked workspace tests passed; the serial suite reported 3 cgroup-gated commit tests and 1 cgroup-gated metadata process test as ignored.
- Schema follow-up: the five canonical payload strings now come from `query_abi_constants.json`; generation validates their exact v1 byte strings and emits both Rust constants and C string macros. Rust and TinyCC fixtures compare generated values to independent golden strings. The pre-generation TinyCC compile failed because the generated C macro was absent; the maintained suite passed after regeneration.
- Verification update: `gateway_rejects_error_frames_with_mismatched_request_ids` sends a canonical supervisor error with a different request ID and verifies that the gateway closes the TCP connection without forwarding the payload, reporting `MismatchedServiceResponse`.
- Verification update: `gateway_bridge_returns_sanitized_supervisor_error_frames` now relays and checks all five canonical v1 error categories over TCP, including request correlation and connection close after the error.
- Remaining: provision per-client Unix sockets/generation references, add queue/deadline and malformed-frame error coverage across separate processes, and complete worker lifecycle hardening.
