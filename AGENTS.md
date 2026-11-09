# Repository Guidelines

## Project Structure & Scope

This repository specifies an operational database with analytical query support and contains an early Rust engine, a maintained TinyCC SDK, and a confined integration harness. Start with `specs/README.md`; follow `specs/toolchain.md` for the TinyCC/C SDK and Rust engine boundary. Keep related specifications consistent.

Follow [INTERATED_DEVELOPMENT.md](INTERATED_DEVELOPMENT.md) for incremental TDD, specification refinement, implementation milestones, and handoff evidence.

## Toolchain Setup

Rust is pinned in `rust-toolchain.toml` and dependencies in `Cargo.lock`; include `rustfmt` and `clippy`. Use the locked repository development shell from the root:

```sh
nix develop --locked
rustc --version
cargo --version
rustfmt --version
cargo clippy --version
tcc -v
```

The flake pins Nixpkgs revision `718f670e845328e9c2cecdd832835712b2cbd065`. Its x86_64-linux shell supplies Rust/Cargo/Clippy 1.95.0, rustfmt 1.9.0, TinyCC 0.9.27-unstable-2025-01-06, GCC 15.3.0, binutils 2.46, GNU Make 4.4.1, and Python 3 for ABI generation. `cc` is the GNU compiler Rust needs for linking; it does not replace TinyCC for SDK/query compilation. Nix store paths are machine-specific and can be garbage-collected; do not rely on hard-coded paths. Carry the pinned TinyCC source/support hashes into SDK release packaging and validate the Linux target/libc profile before release acceptance.

If the toolkit is missing, broken, or inaccessible, actively diagnose and restore a working, version-matched environment before implementation checks: inspect PATH and Nix, enter or provision the locked development shell, and resolve missing linkers or dependencies. Use authorized sandbox escalation when Nix daemon access is blocked. `nix-shell -p` may be used as a host-specific recovery tool if the locked shell cannot be built, but verify every version and document the deviation. Do not silently substitute GCC/Clang for TinyCC. If an external restriction still blocks repair, report the attempted repair and the exact blocker; never represent unrun checks as passing. Cargo may use `--offline` with `--locked` when all locked dependencies are cached; otherwise obtain the dependencies through the permitted network. Run Cargo operations that write the same target directory sequentially.

## Rust Build and Checks

Once `Cargo.toml` and `Cargo.lock` exist, run from the repository root:

```sh
cargo build --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
```

These build the engine, check formatting/lints, and run unit, integration, and library documentation tests. Use `cargo build --workspace --release --locked` for release validation. Add targeted feature/platform checks when introduced; do not assume all features are compatible. Document actual server startup commands when binaries exist.

## TinyCC Build and Tests

Use the maintained build targets:

```sh
make -f engine/sdk/Makefile test
make -f engine/sdk/Makefile sandbox-query-fixture
```

The following are templates for a new source outside those targets. Set each variable to a real path; create `BUILD_DIR` first. Repeat compilation for each SDK source and archive all resulting objects:

```sh
mkdir -p "$BUILD_DIR"
tcc -I"$SDK_INCLUDE" -c "$SDK_SOURCE" -o "$BUILD_DIR/sdk.o"
tcc -ar rcs "$BUILD_DIR/libsdk.a" "$BUILD_DIR/sdk.o"
tcc -shared -I"$SDK_INCLUDE" "$QUERY_SOURCE" "$BUILD_DIR/libsdk.a" -o "$BUILD_DIR/query.so"
tcc -I"$SDK_INCLUDE" "$SDK_TEST_SOURCE" "$BUILD_DIR/libsdk.a" -o "$BUILD_DIR/sdk-test"
"$BUILD_DIR/sdk-test"
```

Use only maintained SDK unit-test sources for the executable test. Add target/runtime flags from the validated toolchain profile; verify archive relocations and shared-library loading. Run query libraries, constructors, and hostile fixtures only through the sandboxed executor integration harness, never `tcc -run` or a trusted-process loader. Reuse prebuilt SDK archives between query edits.

## Style and Validation

Use rustfmt conventions for Rust and consistent C naming through the public headers. Keep unsafe Rust isolated and documented. Follow the versioned C ABI ownership rules; native structs are not IPC encodings.

Test ABI layout/ownership, malformed inputs, cell permissions, sandbox confinement, concurrent MVCC writes, conflicts, rollback, recovery, and reclamation. Benchmark compilation separately from execution. No coverage threshold exists. Documentation-only changes require link and consistency checks, not unavailable builds.

## Commits and Pull Requests

After completing each feature implementation, create a git commit for that feature.
Use an imperative subject that names the change, such as `Define SDK build profile`. Keep commit messages to the subject line; do not add validation summaries or unresolved-assumption text in commit bodies. Report validation separately in the handoff. Never report template commands as executed tests.
