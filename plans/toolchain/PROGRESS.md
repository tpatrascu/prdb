# Toolchain Progress

## TC-1: locked Nix development shell — implemented

- Behavior: `nix develop --locked` provides the pinned x86_64-linux package set used for the engine and SDK checks. [`flake.nix`](../../flake.nix) and [`flake.lock`](../../flake.lock) pin Nixpkgs revision `718f670e845328e9c2cecdd832835712b2cbd065` with source NAR hash `sha256-Ee5BWo8sh3pGt70QUdRajnaginqFmWvrtdBc4gJzhks=`.
- Package versions: Rust/Cargo/Clippy 1.95.0, rustfmt 1.9.0, TinyCC `0.9.27-unstable-2025-01-06`, GCC 15.3.0, binutils 2.46, and GNU Make 4.4.1. `nix develop --offline --command` reproduced these versions from the lockfile.
- Verification: inside the locked shell, the maintained TinyCC SDK suite passed, `sandbox-query-fixture` built all three GNU-linked artifacts, and workspace build, format, warning-denied Clippy, and serial tests passed. Three cgroup-gated commit tests and one cgroup-gated metadata process test remained ignored; confined acceptance had already passed with this exact Nixpkgs source revision.
- Changed contract: [`specs/toolchain.md`](../../specs/toolchain.md) records the development profile; [`AGENTS.md`](../../AGENTS.md) points checks to the locked shell and maintained SDK targets.
- TinyCC provenance: the pinned recipe records upstream commit `f6385c05308f715bdd2c06336801193a21d69b50`, source hash `sha256-tO3N+NplYy8QUOC2N3x0CO5Ui75j9bQzLSZQF1HQyhY=`, and Nix NAR hashes for the compiler, runtime-support library, and development/header outputs. These are recorded in [`specs/toolchain.md`](../../specs/toolchain.md).
- Remaining: validate the release Linux/libc/runtime profile, extend schema generation to native fields/serializers, record release build commands and panic profiles, and package signed/checkable release artifacts. The locked shell is a development profile, not release artifact packaging.

## TC-2: generated query ABI constants — partial

- Behavior: [`query_abi_constants.json`](../../engine/abi/query_abi_constants.json) is the common source for query ABI versions, symbol names, callback failure code, and parameter/IPC/result bounds. [`generate_query_abi.py`](../../engine/tools/generate_query_abi.py) emits the C constants header and Rust constants module. `make -f engine/sdk/Makefile abi-constants` regenerates them; the maintained SDK `test` target checks drift.
- Compatibility: C record declarations and Rust `repr(C)` records remain unchanged. Existing Rust ABI layout/descriptor tests still compare the records against the C header contract and reject unsupported descriptor versions/sizes.
- Verification: generated-file drift check passed as part of the full maintained TinyCC SDK suite. The focused Rust/C query ABI test passed all six tests, including v1-v3 layout and descriptor validation.
- Remaining: generate native field declarations/layout assertions from the schema and extend cross-language parity to IPC encodings and serializers. This increment does not claim TC-2 complete.

## TC-1 / TC-3: maintained TinyCC SDK and shared-artifact link profile — partial

- Observed development tools: Rust/Cargo 1.95.0 with rustfmt and Clippy; TinyCC `0.9.27-unstable-2025-01-06` on x86_64 Linux. The SDK archive and C fixtures build with TinyCC `-Wall -Wextra -Werror`.
- Red evidence: `tcc -shared` linked the archive, but its output omitted `PT_GNU_STACK`; the confined executor's glibc loader rejected it with `cannot enable executable stack as shared object requires`.
- Implementation: maintained query C objects and the archive remain TinyCC-built. The fixture link step now names `SHARED_LINKER` explicitly and applies GNU linker's `-z noexecstack` option. The Makefile documents why the linker differs from TinyCC.
- Verification: `make -f engine/sdk/Makefile test`, `make -f engine/sdk/Makefile sandbox-query-fixture`, and the DML/SQL and restricted-metadata artifacts linked with the maintained GNU linker passed their confined acceptance tests. `readelf` confirms `GNU_STACK` is RW, with no execute permission. The direct TinyCC-linked shared artifact was rejected at `dlopen` in the sandbox; it was not loaded in a trusted process.
- Remaining: record TinyCC's upstream source/build identity and compiler support-file hashes, define the release Linux/libc profile, check generated-interface parity, and measure clean SDK builds and per-query compile/link separately. The locked Nix shell makes development checks reproducible but is not the release artifact package.
