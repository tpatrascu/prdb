# Toolchain Progress

## TC-2: generated executor open-scan request/response schemas — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) defines the open-scan request as version, relation handle, column count, and optional column-ID list; its response is version plus cursor handle.
- Implementation: the shared schema now models 16-byte fields and length-counted arrays in addition to u16 fields and UTF-8 tails. The generator emits Rust/C offsets and header lengths. Rust request/response codecs and TinyCC SDK request/response checks consume those generated constants.
- Regression: `executor_relation_socket_loop_ends_session_without_finalizing` exercises the Rust request path; the maintained `sdk_executor_v1.c` fixture checks exact request and response bytes.
- Verification: focused Rust/C regressions, generator drift, full workspace build/format/Clippy/serial tests (129 commit tests passed; 3 cgroup-dependent tests ignored), and the complete maintained TinyCC SDK suite passed.
- Remaining: field-level generation still covers only qualified relation resolution and open-scan payloads. Other IPC messages and serializer generation remain open.

## TC-2: generated executor relation request field schema — implemented

- Contract: [internal IPC](../../specs/ipc-protocol.md) and [toolchain](../../specs/toolchain.md). The variable-length executor relation-resolution request has a six-byte fixed header: big-endian version, schema-name length, and relation-name length, followed by the two UTF-8 names in that order.
- Implementation: `engine/abi/query_abi_constants.json` declares the fixed and length-delimited fields. The generator validates supported encodings, unique names, and length-field references, then emits matching Rust/C offsets and header size. Rust and TinyCC encoders use those generated values.
- Regression: the Rust and C executor fixtures retain hand-checked request bytes, independently checking the generated layout. Generator `--check` verifies outputs have no drift.
- Verification: generator drift check, locked workspace build, format, warning-denied Clippy, serial workspace tests (129 commit tests passed; 3 cgroup-dependent tests ignored), and the full maintained TinyCC SDK suite passed.
- Follow-up: the C planner `describe_relation` helper shares the same generated request field offsets and version instead of maintaining its own literals. Its TinyCC fixture compares the emitted request against an independent exact-byte example.
- Follow-up verification: the focused `sdk-planner-v1` TinyCC fixture passes after rebuilding the SDK archive. The Rust request codec is shared with the executor and its exact-byte transaction regression passes.
- Remaining: extend the schema and generation to other IPC request/response payloads and codecs. This increment does not generate serializers.

## TC-2: generated IPC payload version registry — implemented

- Contract: [Toolchain](../../specs/toolchain.md). The shared interface schema now owns the executor relation/batch, planner, supervisor, and optimizer response payload-version constants.
- Implementation: the existing generator emits matching `u16` values for Rust and C. The Rust executor, planner, supervisor, and optimizer codecs import the generated constants rather than defining local version values. Payload fields and serializers remain in their Rust modules; this increment does not claim full IPC schema generation.
- Verification: `make -f engine/sdk/Makefile abi-constants-check`, locked workspace build, format check, warning-denied Clippy, serial workspace tests, and `make -f engine/sdk/Makefile test` passed in the locked Nix development shell. The commit integration suite reported 129 passed and 3 cgroup-dependent tests ignored.
- Remaining: define/generate IPC payload field schemas and serializers, complete C/Rust serialization parity, and validate the release Linux/libc/runtime profile and release artifacts. TC-2 remains partial.

## TC-1: locked Nix development shell — implemented

- Behavior: `nix develop --locked` provides the pinned x86_64-linux package set used for the engine and SDK checks. [`flake.nix`](../../flake.nix) and [`flake.lock`](../../flake.lock) pin Nixpkgs revision `718f670e845328e9c2cecdd832835712b2cbd065` with source NAR hash `sha256-Ee5BWo8sh3pGt70QUdRajnaginqFmWvrtdBc4gJzhks=`.
- Package versions: Rust/Cargo/Clippy 1.95.0, rustfmt 1.9.0, TinyCC `0.9.27-unstable-2025-01-06`, GCC 15.3.0, binutils 2.46, and GNU Make 4.4.1. `nix develop --offline --command` reproduced these versions from the lockfile.
- Verification: inside the locked shell, the maintained TinyCC SDK suite passed, `sandbox-query-fixture` built all three GNU-linked artifacts, and workspace build, format, warning-denied Clippy, and serial tests passed. The four cgroup-gated acceptances that the default suite reports as ignored were also run individually and passed with the pinned runtime profile and a temporary delegated cgroup parent; see [Linux sandbox progress](../linux-sandbox/PROGRESS.md).
- Changed contract: [`specs/toolchain.md`](../../specs/toolchain.md) records the development profile; [`AGENTS.md`](../../AGENTS.md) points checks to the locked shell and maintained SDK targets.
- TinyCC provenance: the pinned recipe records upstream commit `f6385c05308f715bdd2c06336801193a21d69b50`, source hash `sha256-tO3N+NplYy8QUOC2N3x0CO5Ui75j9bQzLSZQF1HQyhY=`, and Nix NAR hashes for the compiler, runtime-support library, and development/header outputs. These are recorded in [`specs/toolchain.md`](../../specs/toolchain.md).
- Remaining: validate the release Linux/libc/runtime profile, extend schema generation to native fields/serializers, record release build commands and panic profiles, and package signed/checkable release artifacts. The locked shell is a development profile, not release artifact packaging.

## TC-2: generated query ABI constants — partial

- Behavior: [`query_abi_constants.json`](../../engine/abi/query_abi_constants.json) is the common source for query ABI versions, symbol names, callback failure code, parameter/result bounds, IPC operation tags, IPC frame magic/version/header size/field offsets, payload cap, malformed-frame status values, the canonical gateway error payload strings, and v1-v3 native record declarations, sizes, alignments, and field offsets. [`generate_query_abi.py`](../../engine/tools/generate_query_abi.py) emits C record headers, Rust records, constants, layout assertions, and C operation-tag cases. Rust and C frame codecs consume the generated layout and operation tags. `make -f engine/sdk/Makefile abi-constants` regenerates them; the maintained SDK `test` target checks drift.
- Compatibility: The v3 invocation remains an alias of the v2 record. Generated assertions fail C and Rust builds if native layouts diverge from the schema; descriptor tests also reject unsupported versions/sizes.
- Verification: generated-file drift check and the full maintained TinyCC SDK suite passed, including the TinyCC-built C frame codec fixture with exact-byte, coalesced-frame, cap, malformed-input, no-partial-output, generated frame error-status, and canonical gateway error-payload checks. Rust tests check the same generated payload strings and frame status values. Workspace build, formatting, warning-denied Clippy, and serial workspace tests passed; the serial suite reported 3 cgroup-gated commit tests and 1 cgroup-gated metadata process test as ignored. The four ignored-by-default acceptance tests were separately run and passed; see [Linux sandbox progress](../linux-sandbox/PROGRESS.md).
- Remaining: move the remaining IPC payload schemas and serializers into the shared interface schema. TC-2 remains partial.

## TC-1 / TC-3: maintained TinyCC SDK and shared-artifact link profile — partial

- Observed development tools: Rust/Cargo 1.95.0 with rustfmt and Clippy; TinyCC `0.9.27-unstable-2025-01-06` on x86_64 Linux. The SDK archive and C fixtures build with TinyCC `-Wall -Wextra -Werror`.
- Red evidence: `tcc -shared` linked the archive, but its output omitted `PT_GNU_STACK`; the confined executor's glibc loader rejected it with `cannot enable executable stack as shared object requires`.
- Implementation: maintained query C objects and the archive remain TinyCC-built. The fixture link step now names `SHARED_LINKER` explicitly and applies GNU linker's `-z noexecstack` option. The Makefile documents why the linker differs from TinyCC.
- Verification: `make -f engine/sdk/Makefile test`, `make -f engine/sdk/Makefile sandbox-query-fixture`, and the DML/SQL and restricted-metadata artifacts linked with the maintained GNU linker passed their confined acceptance tests. `readelf` confirms `GNU_STACK` is RW, with no execute permission. The direct TinyCC-linked shared artifact was rejected at `dlopen` in the sandbox; it was not loaded in a trusted process.
- Remaining: define the release Linux/libc profile and measure clean SDK builds and per-query compile/link separately. The locked Nix shell makes development checks reproducible but is not the release artifact package. TC-2 tracks generated-interface parity separately.
