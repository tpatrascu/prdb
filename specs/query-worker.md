# Executor (Query Worker)

## Execution Model

A disposable sandboxed Rust executor process (also called the query worker) loads a C query library compiled by TinyCC in the client using the [SDK](client-sdk.md). The SDK bundles the [SQL library](sql-library.md), containing the SQL frontend and an initial physical-plan interpreter. The generic host invokes the user entry point and supplies lifecycle, planning submission, scoped storage, and result callbacks. User code may call SQL or lower-level scoped APIs. The optimizer does not produce the submitted query binary. Assume arbitrary hostile machine code can run in this process, regardless of the implementation language.

The prototype targets programs built for the database protocol/SDK, not unrestricted compatibility with existing Linux applications. Workers receive only scoped IPC, bounded output, approved runtime/artifact resources, and any explicitly permitted scratch space.

## Data Contract

Access database values only through the storage-manager protocol. Received batches represent visible rows and individually authorized cells. A worker has no database files, raw storage mappings, policy secrets, or channels belonging to other contexts.

The SQL library implements redaction-aware execution semantics. Malicious code can ignore those semantics and fabricate results, but must not obtain additional data. Output can disclose any values delivered to the worker; grant scope must reflect that fact. C code can corrupt the Rust host within this process; host callbacks are not an isolation boundary. Storage and optimizer endpoints enforce their own boundaries. Follow the FFI ownership and panic rules in the [toolchain specification](toolchain.md).

## Client Result Boundary

Emit arbitrary application bytes only to the provisioned bounded local output channel. The supervisor relays them to the [gateway](client-gateway.md), which sends provisional [network result frames](client-protocol.md). Workers have no client TCP socket or gateway session token. Bytes resembling status/commit messages remain application data; workers cannot mint terminal protocol outcomes. Storage-confirmed transaction decisions reach clients through the supervisor control path.

## Native Library ABI

The initial ABI is version 1 in [`prdb_query_abi_v1.h`](../engine/include/prdb_query_abi_v1.h): `prdb_query_descriptor_v1` declares the ABI and record sizes; `prdb_query_main_v1` accepts the sized host and invocation records. V1 carries immutable borrowed parameters and has no service callbacks yet. The entry point returns zero for success and nonzero for failure. The host must validate descriptor fields before invoking the entry point, but dynamic loading and constructors already occur inside the sandbox. Do not expose Rust `String`, `Vec`, trait objects, or unspecified layouts across the library boundary. Do not allow language exceptions or panics to unwind across the ABI. Report failures and abort the transaction when possible; otherwise terminate the executor.

Pointers passed within a worker are local ABI details, never storage-manager addresses. Storage requests use serialized IPC. ABI misuse may crash the worker; it must not compromise storage.

Accept only the bounded artifact and dependency profile described by the SDK contract. Never load user libraries in a supervisor or storage process for inspection. Confinement must apply before library loading because constructors can execute during loading. Runtime dependencies and loader behavior must be part of the sandbox design.

## Lifecycle and Validation

Use one disposable executor per user-library invocation, with sequential SDK calls under a shared MVCC snapshot, private write set, and cumulative budgets. Any statement failure aborts the transaction, even if user code catches it. Successful entry-point return requests supervisor finalization; it is not a commit decision. Cancellation or limits may terminate execution without cooperative cleanup. Storage-owned resources must therefore be recoverable on disconnect. Invocation results need an explicit completion/failure signal so interrupted output cannot be mistaken for success. Success requires storage-confirmed durable commit, but is not proof that the library computed correctly. The executor cannot issue an early commit. SDK calls are initially single-threaded; storage rejects overlapping mutation/cursor use even from hostile multithreaded code.

Test hostile protocol clients, library constructors, crashes, infinite loops, child-process attempts, forbidden filesystem/network access, and output floods. Verify the worker cannot access supervisor/storage memory or inherited secrets.

## Open Questions

The initial ABI target is Linux x86_64. The clean executor host, dynamic loader/dependency policy, storage and result callbacks, and permitted threading profile remain open implementation work. The user query is dynamically loadable; the baseline links a prebuilt SQL SDK archive into it. A separately packaged SQL shared dependency and standalone executable support are future options. Shared-memory batches and process reuse are deferred.
