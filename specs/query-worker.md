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

ABI v1 remains supported by [`prdb_query_abi_v1.h`](../engine/include/prdb_query_abi_v1.h): `prdb_query_descriptor_v1` declares the ABI and record sizes; `prdb_query_main_v1` accepts the sized host and invocation records. V1 carries immutable borrowed parameters and has no service callbacks. The callback surface for ABI v2 is defined in [`prdb_query_abi_v2.h`](../engine/include/prdb_query_abi_v2.h), with distinct descriptor and entry symbols so a host can reject unsupported versions before resolving an entry point. Its sized host table has executor request, planner request, and provisional result callbacks. IPC request and response buffers are each bounded by 1 MiB; result writes are bounded to 16 KiB per call and 64 MiB cumulatively per invocation. Empty buffers use a null pointer and zero length. A successful IPC callback returns the complete response length; any nonzero callback result is failure and response contents are unspecified. The invocation parameter record remains borrowed and immutable, with the existing 1 MiB bound. The executor host does not yet load v2 libraries or implement these callbacks.

Both entry points return zero for success and nonzero for failure. The host must validate every descriptor field and record size before invoking the entry point. Dynamic loading and constructors already occur inside the sandbox. Do not expose Rust `String`, `Vec`, trait objects, or unspecified layouts across the library boundary. Do not allow language exceptions or panics to unwind across the ABI. Report failures and abort the transaction when possible; otherwise terminate the executor. V2 callbacks are synchronous; storage and planner authority comes from their separately provisioned channels and server-side role allowlists, never from the operation code alone. The result callback emits application bytes only and cannot finalize or abort storage.

The Linux x86_64 `prdb-executor` host accepts `--library ABSOLUTE_PATH --parameters-fd FD --planner-fd FD --executor-fd FD --result-fd FD`. The trusted launcher passes separate planner, executor, and result Unix streams; the supervisor can take ownership of the result endpoint for relay. Before loading a library the host requires PID 1 in the private PID namespace, `no_new_privs`, and empty effective, permitted, inheritable, and bounding capability sets, then installs the worker seccomp filter. It resolves and validates the v1 descriptor before resolving the entry point and keeps the library mapped until process exit. The parameter descriptor is read with a 1 MiB bound; the worker passes borrowed immutable bytes and exits with code 0 only when the entry point returns zero. ABI v1 defines no supported callbacks for these descriptors, so query code cannot use them through the documented v1 host table. The v2 ABI header and Rust descriptor validator are contract scaffolding only; runtime v2 loading, callback dispatch, cumulative output enforcement, and result relay remain implementation work. A `--self-check` mode validates the worker context and installs seccomp without loading a library. The trusted launcher must still establish namespaces, Landlock, clean descriptors/environment, and the required cgroup before executing this binary.

Pointers passed within a worker are local ABI details, never storage-manager addresses. Storage requests use serialized IPC. ABI misuse may crash the worker; it must not compromise storage.

Accept only the bounded artifact and dependency profile described by the SDK contract. Never load user libraries in a supervisor or storage process for inspection. Confinement must apply before library loading because constructors can execute during loading. Runtime dependencies and loader behavior must be part of the sandbox design.

## Lifecycle and Validation

Use one disposable executor per user-library invocation, with sequential SDK calls under a shared MVCC snapshot, private write set, and cumulative budgets. Any statement failure aborts the transaction, even if user code catches it. Successful entry-point return requests supervisor finalization; it is not a commit decision. Cancellation or limits may terminate execution without cooperative cleanup. Storage-owned resources must therefore be recoverable on disconnect. Invocation results need an explicit completion/failure signal so interrupted output cannot be mistaken for success. Success requires storage-confirmed durable commit, but is not proof that the library computed correctly. The executor cannot issue an early commit. SDK calls are initially single-threaded; storage rejects overlapping mutation/cursor use even from hostile multithreaded code.

Test hostile protocol clients, library constructors, crashes, infinite loops, child-process attempts, forbidden filesystem/network access, and output floods. Verify the worker cannot access supervisor/storage memory or inherited secrets.

## Open Questions

The initial ABI target is Linux x86_64. Runtime v2 callback implementation, output quotas and relay, and the permitted threading profile remain open implementation work. The user query is dynamically loadable; the baseline links a prebuilt SQL SDK archive into it. A separately packaged SQL shared dependency and standalone executable support are future options. Shared-memory batches and process reuse are deferred.
