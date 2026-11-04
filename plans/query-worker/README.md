# Executor Implementation Plan

## Contract and Dependencies

Implement the disposable Rust [executor](../../specs/query-worker.md), loading TinyCC query libraries only inside the [sandbox](../linux-sandbox/README.md). Depend on [toolchain/ABI](../toolchain/README.md), [IPC](../ipc-protocol/README.md) and provisioned [supervisor](../supervisor/README.md) channels. Integrate the [SDK](../client-sdk/README.md) and [SQL interpreter](../sql-library/README.md) after the minimal host path works.

## Decisions Before Implementation

Freeze sized/versioned entry-point and host tables, explicit statuses, parameter/result buffer ownership, null-pointer rules and allocator release pairs. Define callback failure/panic behavior, required symbols, dependency policy and cancellation completion signal. A bad pointer may kill the executor; it cannot be validated into a trusted in-process boundary.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| EX-1 | Confined compatible fixture invokes one entry point; missing symbol or ABI mismatch fails | Load immutable artifact bytes and declared dependencies after confinement; no inspection by loading in trusted processes |
| EX-2 | Maintained C ABI fixtures consume parameters/results and release with the originating allocator | Implement scoped host callbacks using generated types, explicit ownership and no unwinding/non-local jumps across FFI |
| EX-3 | Sequential direct storage calls share context while overlapping/reentrant calls fail | Bridge serialized local IPC; preserve opaque handles, redaction states, cancellation and cumulative budgets; storage repeats checks independently |
| EX-4 | Worker output spelling COMMITTED remains provisional application bytes | Emit only on bounded local output; supervisor receives an independent completion/error signal, never a worker terminal envelope |
| EX-5 | Caught statement error followed by successful return cannot commit prior writes | Notify trusted transaction state of failures, stop requests on finalization, and leave commit authority with supervisor/storage |
| EX-6 | Constructor/entry crashes, malformed pointers, loops and forced termination leave other invocations safe | Sandbox integration, process exit rather than unsafe unloading, disconnect cleanup and draining storage-owned pins |

## Acceptance and Handoff

Run real TinyCC artifacts through the sandbox and real IPC, including hostile code bypassing SDK callbacks. Verify no network/session credentials or database resources enter the host. Result correctness remains outside arbitrary-code guarantees. First behavior: a confined ABI-compatible fixture and mismatch rejection, after the sandbox gate passes.
