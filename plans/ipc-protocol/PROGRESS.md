# Internal IPC Progress

## IPC-6 dependency: canonical gateway error payload validation — implemented

- Behavior and contract: [`GatewayClientError`](../../specs/ipc-protocol.md) is a supervisor-to-gateway event whose payload must be one of the five fixed, sanitized v1 JSON errors. The gateway now compares the complete payload bytes against that allowlist before encoding a client ERROR frame; an unknown category, modified message, extra field, malformed JSON, or other payload closes the connection as a mismatched service response.
- Red evidence: `gateway_rejects_unsanitized_supervisor_error_payloads` failed before the change because the gateway wrote an 87-byte frame containing `secret principal detail` to the TCP client.
- Regression: the same test now verifies the client sees EOF and the gateway reports `MismatchedServiceResponse`. `gateway_bridge_returns_sanitized_supervisor_error_frames` verifies a canonical error still reaches the client unchanged.
- Verification: both focused gateway boundary tests passed. `cargo build --workspace --locked --offline`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked --offline -- -D warnings`, and `cargo test --workspace --locked --offline -- --test-threads=1` passed in the pinned Nix shell. The full suite reported 3 cgroup-gated commit tests and 1 cgroup-gated metadata process test as ignored by default.
- Follow-up: the supervisor producer and gateway validator now share one typed `GatewayClientErrorCode` definition and canonical payload bytes, avoiding parallel JSON formatting and whitelist definitions. Unit coverage checks all five payloads and rejects missing fields or modified messages.
- Follow-up verification: canonical payload unit tests, the valid TCP relay regression, and the raw-payload rejection regression passed. Workspace build, formatting, warning-denied Clippy, and serial locked workspace tests passed; the serial suite reported 3 cgroup-gated commit tests and 1 cgroup-gated metadata process test as ignored.
- Remaining: provision per-client Unix sockets/generation references, add queue/deadline and malformed-frame error coverage across separate processes, and complete worker lifecycle hardening.
- Next smallest behavior: exercise every canonical v1 category through the TCP bridge and reject incorrect request correlation before any error payload is forwarded.
