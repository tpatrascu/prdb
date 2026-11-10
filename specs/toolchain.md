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

The locked x86_64-linux development shell in [`flake.nix`](../flake.nix) and [`flake.lock`](../flake.lock) uses Nixpkgs revision `718f670e845328e9c2cecdd832835712b2cbd065` (source NAR hash `sha256-Ee5BWo8sh3pGt70QUdRajnaginqFmWvrtdBc4gJzhks=`). It supplies Rust/Cargo/Clippy 1.95.0, rustfmt 1.9.0, TinyCC `0.9.27-unstable-2025-01-06`, GCC 15.3.0, binutils 2.46, GNU Make 4.4.1, and patchelf. The pinned Nixpkgs recipe identifies TinyCC upstream commit `f6385c05308f715bdd2c06336801193a21d69b50` and source hash `sha256-tO3N+NplYy8QUOC2N3x0CO5Ui75j9bQzLSZQF1HQyhY=`. Its compiler (`out`), support-library (`lib`), and development/header (`dev`) Nix outputs have NAR hashes `sha256-oi9Et4PGBXLBLusWV0sC3BaM+i9fMq1EJ3yP+ARWkHo=`, `sha256-cNCz1h00s4du1xueBlLmU7AJnkQKeBqT1EHotsv+Pmk=`, and `sha256-sAED9BgSwUcbxKVgc25NJz+efH72JjZ7XDIuG65aMas=` respectively. Enter the shell with `nix develop --locked`. The final shared-object link uses GNU `ld` with `-z noexecstack`: a direct TinyCC `-shared` output had no `PT_GNU_STACK` program header, and glibc rejected it inside the confined executor with `cannot enable executable stack as shared object requires`. This is an explicit linker compatibility workaround, not a substitute C compiler. The maintained Makefile exposes it as `SHARED_LINKER`; its default follows `CC`. The selected initial release-validation profile is `nixos-x86_64-glibc-2.42` with CPU baseline `x86_64-v1`. It was validated on the pinned NixOS host with glibc 2.42-100 (NAR hash `sha256-c9VTB0ezSOYH331yInQ5a7N73FFzdKcvzI6Jk9tFNSU=`); this evidence supports that exact profile only.

The SQL SDK archive is built once per SDK/target release, not once per query. Engine Rust builds use `rust-toolchain.toml` and `Cargo.lock` in the locked development shell. The shared schema, [`query_abi_constants.json`](../engine/abi/query_abi_constants.json), generates query ABI version/symbol/bound constants, executor/planner/supervisor/optimizer payload-version constants, v1-v4 C and Rust native record declarations, IPC operation tags, and IPC frame magic/version/header/field offsets; it also generates native size/alignment/field-offset assertions. Every declared IPC payload schema emits Rust and C encode/decode helpers for its fixed fields or variable-tail prefix. Callers continue to validate bounded collection and record tails with role-specific codecs. `make -f engine/sdk/Makefile abi-constants-check` checks generated files for drift.

The production compilation path emits a file for submission; do not use TinyCC `-run` or execute query entry points in the client as part of compilation. Initially invoke the compiler as a separate bounded client process, rather than embedding libtcc in a credential-bearing process. Source includes and linker inputs can read local files: restrict compilation to explicit source/SDK inputs when compiling third-party code and never expose credentials in its environment or include paths.

Pin the exact TinyCC release/commit and distribution hashes. The initial release-validation profile is Linux x86_64, CPU baseline `x86_64-v1`, and NixOS glibc 2.42-100. The maintained `release-profile` target builds Rust binaries with release panic strategy `abort`, builds the TinyCC SQL SDK archive and three maintained shared fixtures, strips build-shell RUNPATH entries from staged engine binaries, validates ELF architecture/dependencies/GLIBC symbol requirements/non-executable query stacks, and writes SHA-256 checksums. The profile's Rust binaries require at most GLIBC_2.39 and depend on `libgcc_s.so.1`, `libc.so.6`, and `ld-linux-x86-64.so.2`; the query fixtures require at most GLIBC_2.14 and depend only on `libc.so.6`. These checks do not establish compatibility with older glibc releases or other distributions. Do not assume that modern system archives, all ELF relocations, TLS models, C extensions, or linker flags work with the selected build. The supported C dialect is a documented tested subset, initially conventional C99-style code; full C11/C17/C23, C++, OpenMP, and arbitrary compiler extensions are not promised.

## Packaging and Loader Implications

Keep the baseline of a prebuilt C SDK archive linked into the submitted shared library. Verify its shared-library relocation model with the pinned TinyCC linker; file suffixes or accepted flags do not prove loadability. Rust services are separate binaries, so normal query linking does not require consuming a Rust static archive. A Rust implementation of SDK SQL would be a later explicit change, not an implicit dependency.

Publish canonical `PMF1` manifests as defined by the [client SDK contract](client-sdk.md), containing target, CPU/runtime profile, TinyCC build identity, SDK release, host ABI, protocol version, exported entry symbols, and dependencies. Compiler identity is diagnostic metadata, not a trust credential. SEAL validates the manifest against the service's configured profile and allowlist and checks the ELF dependency table without loading the library. The exact deployment CPU/runtime profile and dependency allowlist still require selection and validation. Constrain loader paths and dependency exports, and verify all dependencies fit the sandbox. No ambient library discovery or automatic dependency downloads occur inside execution.

## C/Rust ABI

Use a versioned C ABI for query entry points and host callbacks. Rust exports `extern "C"` functions and `#[repr(C)]` records where appropriate. C headers use explicit-width integer/status fields, explicit lengths, sized/versioned function tables, and opaque handles. Avoid compiler-dependent enum widths, bitfields, variadic entry points, Rust references/containers/trait objects, or allocator-specific ownership across the boundary. Specify alignment and nullable pointers explicitly.

Pair each owned allocation with its originating allocator's release function. Never free a Rust allocation with C `free`, or adopt arbitrary C pointers into Rust owning containers. Validate lengths and representations before constructing Rust slices/references, but do not claim arbitrary hostile pointers can be made safe through bounds checks alone. A bad pointer can kill/corrupt the executor; the process boundary protects trusted services.

No panic or C non-local jump may cross an FFI frame. All release binaries use Rust's `panic=abort` profile; debug and test builds retain the development unwind strategy. Exported callbacks must return structured errors or terminate the executor without unwinding into C. Rust aborts are process failures, not recoverable SDK exceptions. IPC remains explicit serialization, never a native struct memory dump.

## Security and Reliability

C query/SDK code can corrupt the Rust executor host's memory. Therefore the whole executor is untrusted, including Rust callbacks and bundled SDK code. Keep the storage manager, optimizer, credentials, and commit-control authority outside that address space. TinyCC bounds instrumentation, warnings, and SDK provenance do not replace sandboxing or server-side authorization.

Implement trusted engine logic primarily in safe Rust. Isolate and review `unsafe` in sandbox syscalls, dynamic loading/FFI, and low-level storage operations with explicit invariants. Rust reduces memory-safety risks but does not prove correct permission checks, MVCC, deadlock freedom, durability, or protocol bounds. Keep those validations mandatory.

Malformed SQL parsed by the C SDK is still untrusted structured input to Rust optimization/storage. Rust must independently type-check/validate requests and policies; never execute C callbacks inside trusted processes. Credential operations remain client control APIs and must not become host callbacks available to query libraries.

## Performance and Validation

TinyCC is selected for interactive compilation latency, not maximum optimization of arbitrary native loops. No latency or throughput target is yet measured. Prebuilt SDK code avoids repeated parsing/compilation work; batching IPC remains important because compiler choice does not remove process-boundary cost. Measure compilation, linking, loading, first result, and committed completion separately.

Release gates include TinyCC shared-library/archive compatibility; C/Rust size/alignment/ownership tests; constructors under confinement; crashes and ABI mismatch; SQL/type/protocol parity; cross-version rejection; target/libc compatibility; and compile-to-commit performance. Fuzz the C parser and Rust decoders independently. Supplemental sanitizer builds may use another compiler in development without changing the supported TinyCC query path.

The repository has a locked Nix development shell for the Rust/TinyCC development profile and an initial validated NixOS x86_64/glibc 2.42 release-validation target. `make -f engine/sdk/Makefile release-profile` stages un-RPATHed engine binaries, the TinyCC SDK archive and headers, and maintained query fixtures under `target/release-profile`; `PROFILE.txt` records dependencies and `SHA256SUMS` makes the staged tree checkable. `make -f engine/sdk/Makefile release-profile-package` creates a normalized tarball at `target/prdb-release-profile.tar.gz` and a detached `.sha256` file. This package is checkable, not cryptographically signed, and is not validated beyond the exact host profile. If shared-library compatibility fails, fix/document the toolchain profile rather than silently changing compilers or weakening confinement.

For fixed-width IPC payloads, the shared schema may request generated Rust and C encode/decode helpers. These helpers encode the schema version and fixed fields in network byte order, require the exact payload length and supported version when decoding, and are checked against independent wire examples. A schema may mark a helper as a prefix codec when fixed fields precede a variable tail; its decoder requires at least the prefix size and supported version, while the caller validates the remaining bytes with its bounded codec. Generated C codecs support u8, u16, u32, u64, i64, and 16-byte fields. Variable-length payloads still use explicit bounded codecs.

## References

[TinyCC's manual](https://bellard.org/tcc/tcc-doc.html) documents C compilation, shared-library output, and libtcc; its published manual describes an older release, so target support must be checked against the actual pinned build. [Rust linkage documentation](https://doc.rust-lang.org/reference/linkage.html) distinguishes Rust-specific outputs from C-compatible library artifacts; no Rust-native ABI is exposed by this design.
