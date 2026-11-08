# Client SDK Progress

## SDK-1 dependency: canonical Rust PMF1 encoder — implemented

- Behavior: `encode_manifest_v1` emits the canonical PMF1 representation only for supported ABI/symbol pairs and bounded fields with sorted, unique dependency names. It round-trips through the decoder to keep the public encoder aligned with admission constraints.
- Regression: the manifest fixture round-trips through the public encoder/decoder and rejects unsorted dependency lists. The real TinyCC TCP submission regression uses the encoder to prepare a valid SEAL request.
- Verification: the encoder unit test, full workspace build/format/Clippy/tests and maintained TinyCC SDK test suite passed.
- Remaining: the C submission SDK and compiler wrapper still need to collect dependencies and encode manifests for client uploads.

## SDK-1 initial borrowed value view — implemented

- Contract: [`prdb_sdk_value_v1`](../../engine/include/prdb_sdk_value_v1.h) exposes a sized/versioned view with VALUE, NULL, and REDACTED states. Value type codes and canonical bytes match the executor batch protocol. The view borrows bytes from an immutable source batch; it never allocates or frees them.
- Validation: [`value_v1.c`](../../engine/sdk/value_v1.c) checks layout version/size, state/type, the 1 MiB bound, fixed-width types, Boolean values, null/redacted empty representations, and UTF-8 strings. Invalid pointers remain a native caller error and cannot be made safe by this validator.
- Regression: [`sdk_value_v1.c`](../../engine/tests/fixtures/sdk_value_v1.c) covers all states, pointer borrowing, integer widths, malformed null/redacted records, invalid Boolean, invalid UTF-8, unsupported version, and undersized records.
- Red/green evidence: before implementation, TinyCC compilation failed because `prdb_sdk_value_v1.h` did not exist. After implementation, `make -f engine/sdk/Makefile test` passed using TinyCC 0.9.27-unstable-2025-01-06, including archive creation with `tcc -ar` and execution of the maintained test.
- Contract: [`prdb_sdk_batch_v1`](../../engine/include/prdb_sdk_batch_v1.h) opens the executor next-batch wire payload in place, validates it in full, and provides zero-copy row/cell iterators. The caller retains the unchanged response bytes for the lifetime of all views; it owns and releases that buffer.
- Regression: [`sdk_batch_v1.c`](../../engine/tests/fixtures/sdk_batch_v1.c) covers a hand-checked row with VALUE/NULL/REDACTED cells, row/cell iteration, borrowed data, continuation cursor, truncation, unsupported wire version, invalid handle kind and oversized cell count. The test was first compiled before implementation and failed because `prdb_sdk_batch_v1.h` did not exist; it now passes against the TinyCC archive.
- Typed accessors: value state, Boolean, signed 16/32/64-bit scalars, Date, TimestampTz and borrowed raw payload accessors validate state/type and preserve NULL versus REDACTED. Signed conversion handles negative values and minimum representable boundaries without out-of-range signed casts.
- Regression: the expanded value fixture initially failed to compile because `prdb_sdk_value_state_v1` and the typed getters were absent; after implementation it passes against the TinyCC archive, including negative/minimum signed values, no-value statuses, type mismatch, and borrowed text bytes.
- Query host ABI v3: [`prdb_query_abi_v3.h`](../../engine/include/prdb_query_abi_v3.h) adds a sticky SQL statement-failure callback while preserving v1/v2 entry points. The trusted executor latches the callback and forces a nonzero worker exit after user entry returns, so a caught SQL API error still reaches supervisor abort handling. Rust/C layout and descriptor checks and the SDK SELECT fixture cover the new boundary.
- Remaining: add generated bindings and an owned batch-result lifetime API before claiming SDK-1 complete; complete the C SQL/runtime result format and cancellation surface; then implement the compiler wrapper, artifact manifest and client submission helper.
