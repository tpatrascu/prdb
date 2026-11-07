# Client SDK Implementation Plan

## Contract and Dependencies

Implement the [SDK](../../specs/client-sdk.md) using the pinned [toolchain](../toolchain/README.md), generated [IPC](../ipc-protocol/README.md)/ABI contracts and [SQL library](../sql-library/README.md). Runtime C code linked into query.so and the client-only submission/authentication/admin helper are distinct packages and authority surfaces.

## Decisions Before Implementation

Freeze manifest/scope encodings and independent compatibility checks for target, CPU/libc, entry-point ABI, IPC, query/plan, SQL API and SDK release. Specify explicit buffer borrowing/release rules, compiler input restrictions and wrapper resource limits. Exact public symbols and Rust-helper bindings become normative before publishing headers.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| SDK-1 | Maintained C tests exercise sized/versioned APIs, value states and correct allocator release | Generate public headers/types, opaque handles and borrowed immutable batches; invalid lengths/layout versions fail explicitly |
| SDK-2 | One source edit recompiles only user code and links a reusable prebuilt TinyCC SDK archive | Bounded client compiler subprocess emitting query.so without execution; explicit source/include/link inputs and no credential-bearing environment |
| SDK-3 | Manifest mismatch/malformed scope is rejected without trusting compiler identity | Bounded packaging with target/dependencies/version declarations and digest; validate again independently at admission/loading |
| SDK-4 | Real helper uploads/seals/starts, records ID and distinguishes provisional bytes from durable completion | Implement TCP/authentication outside runtime archive, status/cancel via another connection and no automatic replay after uncertain START |
| SDK-5 | Credential/admin APIs and live monitoring are unavailable through executor callbacks or artifacts | Separate control connection/builders, typed allowlisted operations, secret-once handling and protected client credential storage |
| SDK-6 | Sequential SQL/direct calls preserve snapshot, own writes, error abort and cumulative budgets | Integrate scoped stubs, discovery paging, plan submission and SQL runtime; test actual sandboxed artifacts and storage role enforcement |
| SDK-7 | Clean packaged SDK works on each supported profile and rejects incompatible releases | Release manifest/header/archive checks, C/Rust parity and compile/edit/load/first-result/commit benchmarks |

## Acceptance and Handoff

Maintain C executable unit tests separately from query fixtures, which load only in the sandbox harness. Verify wrong allocator/lifetime use cannot compromise trusted services, NULL and REDACTED remain distinct, and administrative credentials never enter the artifact. Missing replies reconcile by status; missing history is not safe retry. First behavior, implemented: the versioned borrowed cell-value view and validator in `engine/include/prdb_sdk_value_v1.h` and `engine/sdk/value_v1.c`, with a maintained test built against the TinyCC archive by `make -f engine/sdk/Makefile test`. Batch handles, batch release callbacks, remaining typed getters, SQL/runtime, packaging and client helper are not implemented by this slice.
