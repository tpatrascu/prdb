# Executor Progress

## EX-1 dependency: freeze the initial C entry-point ABI

- Behavior: Linux x86_64 query libraries use a sized/versioned descriptor, `prdb_query_main_v1`, a sized host record, and a sized invocation record containing immutable borrowed parameters. `BorrowedInvocationV1` ties the raw pointer to the parameter slice, uses NULL only for zero length, and enforces the 1 MiB cap. The initial host record exposes no callbacks; storage, planning, result, and cancellation services remain future ABI work.
- Contract and code: [query worker ABI](../../specs/query-worker.md), [client SDK entry point](../../specs/client-sdk.md), [public v1 header](../../engine/include/prdb_query_abi_v1.h), and [Rust ABI definitions](../../engine/src/query_abi.rs).
- Regression: Rust tests assert x86_64 C size/alignment/offsets, reject descriptor ABI, size, host-table, and invocation-record mismatches, and verify parameter pointer lifetime construction, empty NULL representation, and size bounds. A TinyCC `_Static_assert` fixture independently compiles the public header layout without loading a library.
- TDD evidence: the focused Rust ABI test first failed to compile because the `query_abi` module did not exist; it passes against the implemented records and validator.
- Verification: focused Rust tests passed (3/3). TinyCC 0.9.27-unstable-2025-01-06 compiled `query_abi_v1_layout.c` to an ELF object. The object was not linked into or loaded by a process. Full workspace formatting, build, warning-denied Clippy, all tests, TinyCC header compilation, and `git diff --check` passed after the bounded marshalling addition.
- Remaining: implement the clean executor host and confined load/descriptor/entry validation, define storage/result callbacks, and exercise compatible/mismatch artifacts only through the full sandbox harness.
