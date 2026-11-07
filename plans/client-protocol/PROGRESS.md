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
