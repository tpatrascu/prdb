# Client SDK Progress

## SDK-1 initial borrowed value view — implemented

- Contract: [`prdb_sdk_value_v1`](../../engine/include/prdb_sdk_value_v1.h) exposes a sized/versioned view with VALUE, NULL, and REDACTED states. Value type codes and canonical bytes match the executor batch protocol. The view borrows bytes from an immutable source batch; it never allocates or frees them.
- Validation: [`value_v1.c`](../../engine/sdk/value_v1.c) checks layout version/size, state/type, the 1 MiB bound, fixed-width types, Boolean values, null/redacted empty representations, and UTF-8 strings. Invalid pointers remain a native caller error and cannot be made safe by this validator.
- Regression: [`sdk_value_v1.c`](../../engine/tests/fixtures/sdk_value_v1.c) covers all states, pointer borrowing, integer widths, malformed null/redacted records, invalid Boolean, invalid UTF-8, unsupported version, and undersized records.
- Red/green evidence: before implementation, TinyCC compilation failed because `prdb_sdk_value_v1.h` did not exist. After implementation, `make -f engine/sdk/Makefile test` passed using TinyCC 0.9.27-unstable-2025-01-06, including archive creation with `tcc -ar` and execution of the maintained test.
- Contract: [`prdb_sdk_batch_v1`](../../engine/include/prdb_sdk_batch_v1.h) opens the executor next-batch wire payload in place, validates it in full, and provides zero-copy row/cell iterators. The caller retains the unchanged response bytes for the lifetime of all views; it owns and releases that buffer.
- Regression: [`sdk_batch_v1.c`](../../engine/tests/fixtures/sdk_batch_v1.c) covers a hand-checked row with VALUE/NULL/REDACTED cells, row/cell iteration, borrowed data, continuation cursor, truncation, unsupported wire version, invalid handle kind and oversized cell count. The test was first compiled before implementation and failed because `prdb_sdk_batch_v1.h` did not exist; it now passes against the TinyCC archive.
- Remaining: add generated/typed getters and an owned batch-result lifetime API before claiming SDK-1 complete; then implement the C SQL/runtime archive, compiler wrapper, artifact manifest and client submission helper.
