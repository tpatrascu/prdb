# Toolchain Implementation Plan

## Contract and Dependencies

Implement the [TinyCC/Rust boundary](../../specs/toolchain.md) and reproducible release profile. This is stage 1 of the [roadmap](../README.md); later loading evidence depends on the [sandbox](../linux-sandbox/README.md), not merely a successful linker exit.

Rust owns engine processes. TinyCC builds C queries and the prebuilt SQL/runtime SDK archive. The separate submission helper may use Rust. No compiler substitution changes this acceptance path.

## Decisions Before Implementation

Record exact Rust and TinyCC versions/commit/hashes, Linux architecture/CPU/libc baseline, runtime support files, license/distribution requirements and per-binary panic strategies in the spec. Choose a single versioned interface-schema source and generator for C headers, Rust types and serializers; keep ABI, IPC, query/plan, manifest and SDK versions independent. Validate redb's Rust requirements with the storage plan before pinning.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| TC-1 | Maintained Rust/C smoke sources can build on the declared profile; unsupported/missing tools report setup failures | Introduce only the workspace needed by the first test, `rust-toolchain.toml` with rustfmt/clippy, and committed `Cargo.lock`. Install selected tools before checks and record `rustc --version`, `cargo --version`, `tcc -v` |
| TC-2 | Generated C/Rust fields agree on size/alignment and invalid versions are rejected; regenerate without drift | Add schema generation and hand-checked serialization fixtures. Native ABI layout tests remain distinct from explicit wire encoding tests |
| TC-3 | TinyCC builds a maintained C archive/test executable and links a shared fixture using that archive | Fix archive relocation, support-file and runtime dependency flags for the selected target. Reuse the archive across query edits; inspect files without executing constructors |
| TC-4 | Real sandbox harness accepts compatible fixtures and rejects ABI/target/dependency mismatch | Verify shared-library loading only after confinement, restricted dependency lookup and allocator ownership; compiler provenance grants no authority |
| TC-5 | Clean release rebuilds produce checkable manifests and compatible artifacts; tampered/missing support files fail | Package pinned compiler, headers, archives, runtime files and hashes; document actual build/test commands and supported dialect subset |

## Acceptance and Handoff

Run workspace build, formatting, clippy, tests and release validation from AGENTS.md once targets exist, plus the real TinyCC acceptance path and target/profile checks. Supplemental sanitizer compilers do not replace TinyCC. Record exact flags and profile, generated-interface parity, archive/shared-loader evidence, and cold/warm SDK-build versus per-query compile/link measurements. First behavior: a maintained smoke executable with separately recorded toolchain setup evidence.
