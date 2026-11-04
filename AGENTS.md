# Repository Guidelines

## Project Structure & Scope

This repository specifies an operational database with analytical query support and currently contains design documents, not an implementation. Start with `specs/README.md`; follow `specs/toolchain.md` for the TinyCC/C SDK and Rust engine boundary. Keep related specifications consistent. Source directories, manifests, and test harnesses have not been created.

Follow [INTERATED_DEVELOPMENT.md](INTERATED_DEVELOPMENT.md) for incremental TDD, specification refinement, implementation milestones, and handoff evidence.

## Toolchain Setup

Pin Rust in `rust-toolchain.toml` and commit `Cargo.lock` when introducing the Cargo workspace. Include `rustfmt` and `clippy`. Pin TinyCC's exact build, support files, and Linux target/libc profile in SDK packaging.

Check tools with `rustc --version`, `cargo --version`, and `tcc -v`. Do not silently substitute GCC/Clang for TinyCC. No Rust or TinyCC installation is currently available on this checkout's PATH; install the selected versions before implementation checks.

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

The following are templates, not existing build targets. Set each variable to a real path; create `BUILD_DIR` first. Repeat compilation for each SDK source and archive all resulting objects:

```sh
mkdir -p "$BUILD_DIR"
tcc -I"$SDK_INCLUDE" -c "$SDK_SOURCE" -o "$BUILD_DIR/sdk.o"
tcc -ar rcs "$BUILD_DIR/libsdk.a" "$BUILD_DIR/sdk.o"
tcc -shared -I"$SDK_INCLUDE" "$QUERY_SOURCE" "$BUILD_DIR/libsdk.a" -o "$BUILD_DIR/query.so"
tcc -I"$SDK_INCLUDE" "$SDK_TEST_SOURCE" "$BUILD_DIR/libsdk.a" -o "$BUILD_DIR/sdk-test"
"$BUILD_DIR/sdk-test"
```

Use only maintained SDK unit-test sources for the executable test. Add target/runtime flags from the validated toolchain profile; verify archive relocations and shared-library loading. Run query libraries, constructors, and hostile fixtures only through the sandboxed executor integration harness, never `tcc -run` or a trusted-process loader. That harness does not exist yet. Reuse prebuilt SDK archives between query edits.

## Style and Validation

Use rustfmt conventions for Rust and consistent C naming through the public headers. Keep unsafe Rust isolated and documented. Follow the versioned C ABI ownership rules; native structs are not IPC encodings.

Test ABI layout/ownership, malformed inputs, cell permissions, sandbox confinement, concurrent MVCC writes, conflicts, rollback, recovery, and reclamation. Benchmark compilation separately from execution. No coverage threshold exists. Documentation-only changes require link and consistency checks, not unavailable builds.

## Commits and Pull Requests

Use imperative subjects, such as `Define SDK build profile`. Describe the change, validation performed, and unresolved assumptions; link relevant issues. Never report template commands as executed tests.
