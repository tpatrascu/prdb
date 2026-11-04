# Toolchain Decision: TinyCC SDK and Rust Engine

## Established Decision

Use C and TinyCC for user query compilation and the query SDK, including its SQL parser/interpreter and runtime stubs. The separately packaged external network submission helper may use Rust with client bindings; it is not linked into query.so. Use Rust for database-engine processes and trusted implementation code. This replaces the earlier language-selection experiment; benchmarks validate the chosen design rather than select its languages.

| Component | Language / build | Execution location |
| --- | --- | --- |
| User query library | C compiled and linked with pinned TinyCC | Sandboxed executor |
| SDK SQL parser/interpreter and scoped runtime stubs | C, prebuilt with the supported TinyCC toolchain | Sandboxed executor; public C bindings also available to clients |
| External submission/network helper | Rust helper or client binding implementation | Client only; owns TCP/authentication, never included in query.so |
| Executor host and sandbox launcher | Rust | Executor / supervisor respectively |
| Client gateway | Rust | Separate plaintext TCP listener; future transport adapters |
| Storage, MVCC, WAL, indexes, catalog, policy evaluator, statistics | Rust | Trusted storage process |
| Optimizer and structured-plan validation | Rust | Separate optimizer process |
| Admission, authentication, administrative definition validation | Rust | Trusted engine control path |

Client application language remains unrestricted: it may invoke the SDK compiler wrapper or use the C API. Other query compilers/languages are not initially supported products even if they can generate compatible machine code. Engine authorization never depends on proving that TinyCC produced an artifact.

## Compilation and Distribution

Package a pinned TinyCC build, C headers, compiler support files, target configuration, and prebuilt SQL/runtime archives. Compile query C sources and link the archive into query.so. Keep client administration/credential helpers separate from the executor runtime archive; query artifacts receive no credential material or privileged channel.

The SQL SDK archive is built once per SDK/target release, not once per query. Engine Rust builds use Cargo with a pinned Rust toolchain and dependency lockfile once implementation exists. SDK headers, Rust protocol types, and serializers must derive from one versioned interface schema, with cross-language checks; the generator is not selected yet.

The production compilation path emits a file for submission; do not use TinyCC `-run` or execute query entry points in the client as part of compilation. Initially invoke the compiler as a separate bounded client process, rather than embedding libtcc in a credential-bearing process. Source includes and linker inputs can read local files: restrict compilation to explicit source/SDK inputs when compiling third-party code and never expose credentials in its environment or include paths.

Pin the exact TinyCC release/commit and distribution hashes. Select and test a Linux architecture/libc profile before publishing artifacts. Do not assume that modern system archives, all ELF relocations, TLS models, C extensions, or linker flags work with the selected build. The supported C dialect is a documented tested subset, initially conventional C99-style code; full C11/C17/C23, C++, OpenMP, and arbitrary compiler extensions are not promised.

## Packaging and Loader Implications

Keep the baseline of a prebuilt C SDK archive linked into the submitted shared library. Verify its shared-library relocation model with the pinned TinyCC linker; file suffixes or accepted flags do not prove loadability. Rust services are separate binaries, so normal query linking does not require consuming a Rust static archive. A Rust implementation of SDK SQL would be a later explicit change, not an implicit dependency.

Publish manifests containing architecture, CPU baseline, libc/runtime dependencies, TinyCC build identity, SDK release, host ABI, and protocol/plan versions. Compiler identity is diagnostic metadata, not a trust credential. Constrain loader paths and dependency exports, and verify all dependencies fit the sandbox. No ambient library discovery or automatic dependency downloads occur inside execution.

## C/Rust ABI

Use a versioned C ABI for query entry points and host callbacks. Rust exports `extern "C"` functions and `#[repr(C)]` records where appropriate. C headers use explicit-width integer/status fields, explicit lengths, sized/versioned function tables, and opaque handles. Avoid compiler-dependent enum widths, bitfields, variadic entry points, Rust references/containers/trait objects, or allocator-specific ownership across the boundary. Specify alignment and nullable pointers explicitly.

Pair each owned allocation with its originating allocator's release function. Never free a Rust allocation with C `free`, or adopt arbitrary C pointers into Rust owning containers. Validate lengths and representations before constructing Rust slices/references, but do not claim arbitrary hostile pointers can be made safe through bounds checks alone. A bad pointer can kill/corrupt the executor; the process boundary protects trusted services.

No panic or C non-local jump may cross an FFI frame. Document panic strategy per Rust binary; exported callbacks must return structured errors or terminate the executor without unwinding into C. Rust aborts are process failures, not recoverable SDK exceptions. IPC remains explicit serialization, never a native struct memory dump.

## Security and Reliability

C query/SDK code can corrupt the Rust executor host's memory. Therefore the whole executor is untrusted, including Rust callbacks and bundled SDK code. Keep the storage manager, optimizer, credentials, and commit-control authority outside that address space. TinyCC bounds instrumentation, warnings, and SDK provenance do not replace sandboxing or server-side authorization.

Implement trusted engine logic primarily in safe Rust. Isolate and review `unsafe` in sandbox syscalls, dynamic loading/FFI, and low-level storage operations with explicit invariants. Rust reduces memory-safety risks but does not prove correct permission checks, MVCC, deadlock freedom, durability, or protocol bounds. Keep those validations mandatory.

Malformed SQL parsed by the C SDK is still untrusted structured input to Rust optimization/storage. Rust must independently type-check/validate requests and policies; never execute C callbacks inside trusted processes. Credential operations remain client control APIs and must not become host callbacks available to query libraries.

## Performance and Validation

TinyCC is selected for interactive compilation latency, not maximum optimization of arbitrary native loops. No latency or throughput target is yet measured. Prebuilt SDK code avoids repeated parsing/compilation work; batching IPC remains important because compiler choice does not remove process-boundary cost. Measure compilation, linking, loading, first result, and committed completion separately.

Release gates include TinyCC shared-library/archive compatibility; C/Rust size/alignment/ownership tests; constructors under confinement; crashes and ABI mismatch; SQL/type/protocol parity; cross-version rejection; target/libc compatibility; and compile-to-commit performance. Fuzz the C parser and Rust decoders independently. Supplemental sanitizer builds may use another compiler in development without changing the supported TinyCC query path.

No toolchain has been installed, pinned, or benchmarked by this design decision. Exact compiler versions, target/libc baseline, header generator, build commands, and panic profiles remain implementation work. If shared-library compatibility fails, fix/document the toolchain profile rather than silently changing compilers or weakening confinement.

## References

[TinyCC's manual](https://bellard.org/tcc/tcc-doc.html) documents C compilation, shared-library output, and libtcc; its published manual describes an older release, so target support must be checked against the actual pinned build. [Rust linkage documentation](https://doc.rust-lang.org/reference/linkage.html) distinguishes Rust-specific outputs from C-compatible library artifacts; no Rust-native ABI is exposed by this design.
