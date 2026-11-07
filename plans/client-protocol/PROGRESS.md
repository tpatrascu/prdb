# Client Protocol Progress

## NP-1 dependency: bounded version-1 frame codec

- Behavior: `ClientFrameDecoder` incrementally accepts fragmented/coalesced bytes, validates the fixed PDBN 1.0 header and per-kind payload limits before allocation, rejects nonzero flags/invalid sequences/zero IDs, and reports partial-frame EOF. `ClientFrame::encode` emits the fixed little-endian header. Request-ID and streamed-response sequence helpers enforce monotonic request IDs and consecutive RESULT_DATA/TERMINAL sequence numbers.
- Contract: [client network protocol](../../specs/client-protocol.md).
- Changed code: [frame codec and sequence state](../../engine/src/client_protocol.rs) and [module export](../../engine/src/lib.rs).
- Verification: workspace format, build, warning-denied Clippy, and `git diff --check` pass in the repository Nix shell. Tests were not run.
- Remaining: strict JSON/base64 operation decoding, HELLO/AUTH/session state, live TCP/gateway integration, fuzzing and golden fragmented-frame fixtures.
