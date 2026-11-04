# Client Protocol Implementation Plan

## Contract and Dependencies

Implement the exact [network protocol v1](../../specs/client-protocol.md) in the [gateway](../client-gateway/README.md) and separate [submission helper](../client-sdk/README.md). Protocol codec/state tests can precede service integration; authenticated execution depends on [supervisor](../supervisor/README.md) and storage outcomes.

Preserve the specified 32-byte little-endian PDBN header, version 1.0, zero flags, 1 MiB hard payload maximum, 64 KiB JSON controls and 256 KiB chunk data. Request IDs increase, response streams have explicit sequences, and one connection has one operation/stream at a time.

## Decisions Before Implementation

Finish versioned manifest/scope and named administrative argument/result schemas in the owning specs, along with the credential profile. Do not invent unsupported operations behind generic dispatch. Decide bounded parser allocation and canonical JSON/base64 validation strategy without altering fixed v1 bytes.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| NP-1 | Golden frames survive every split point/coalescing; invalid lengths/version/flags close without resynchronization | Incremental bounded decoder and encoder, validation before allocation, sanitized bounded errors |
| NP-2 | Duplicate/unknown JSON keys, depth >16, noncanonical IDs/base64/u64 strings fail | Strict operation schemas, HELLO/AUTH/PING pre-auth bounds, immutable query/control modes and non-enumerating auth errors |
| NP-3 | PREPARE/chunks/SEAL reject gaps, duplicates, overflow, cross-session IDs and digest/count mismatch | Independent contiguous offsets and declared quota checks; 64 MiB artifact/1 MiB parameter ceilings or lower configured limits, empty-parameter digest and no snapshot during staging |
| NP-4 | Unsealed/repeated START fails; valid START emits sequence-0 acceptance then consecutive data and one terminal | Model state transitions and provisional results; START uncertainty never triggers automatic retransmission |
| NP-5 | Worker-like terminal bytes and EOF cannot establish transaction success; status is owner/admin protected | Authenticate STATUS/CANCEL on another connection, preserve unknown/expired, distinguish accepted/too-late cancellation and durable results |
| NP-6 | Control-mode read/prepare/start rejects mutation-as-read, repeated starts and unauthorized operations | Bounded server-classified admin allowlist, reserved operation ID and shared outcome namespace; lost secret delivery requires revoke/reissue |
| NP-7 | Real TCP disconnect/reconnect and gateway death around commit reconcile without result replay | Integrate helper/gateway/supervisor/storage, fuzz codecs and state transitions, test concurrent connections and slow clients |

## Acceptance and Handoff

Keep golden byte fixtures independent of encoders. Test all message kinds and forbidden transitions, sanitized diagnostics and current-authority revalidation. Plaintext loopback is the baseline; TLS/HTTP/multiplexing/resume/durable deduplication are separate future contracts. First behavior: decode a hand-checked header under fragmented input and reject an oversized payload before allocation.
