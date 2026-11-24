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

## Test Data Outside the NFS Checkout

This repository checkout is on NFS. Run performance tests and other intensive tests with runtime test data outside the checkout, under `$HOME/tmp/prdb-tests`. Before starting one, create and export a run-specific scratch directory and direct temporary files there:

```sh
export PRDB_TEST_ROOT="$HOME/tmp/prdb-tests"
export PRDB_TEST_RUN_DIR="$PRDB_TEST_ROOT/runs/r$(date +%s)"
mkdir -p "$PRDB_TEST_ROOT/t" "$PRDB_TEST_RUN_DIR/data"
if [ ! -e "$HOME/t" ] && [ ! -L "$HOME/t" ]; then ln -s "$PRDB_TEST_ROOT/t" "$HOME/t"; fi
test "$(readlink -f "$HOME/t")" = "$(readlink -f "$PRDB_TEST_ROOT/t")"
export TMPDIR="$HOME/t"
```

Point the short `$HOME/t` alias at `$PRDB_TEST_ROOT/t` and use it for `TMPDIR` so temporary Unix socket paths stay below the platform limit; use the unique run directory for other per-run files. Use the run-specific directory for configured database instance files, query input/data files, spill files, logs, and other runtime data; remove it after collecting results. Tempfile-backed data uses the short external alias and is automatically unique per test. Copy compiled query libraries there when the harness consumes them as test inputs, while leaving their compiler outputs in the existing build directories. Cargo, TinyCC, and release-profile compiler output stays in its normal repository location. Do not set `CARGO_TARGET_DIR`, `BUILD_DIR`, or `RELEASE_PROFILE_DIR` to move compilation output for this policy. If an intensive test writes runtime data relative to its working directory or into the source/build tree instead of `TMPDIR` or a configured data root, update its harness to accept an external data root. Keep the source checkout as the code input; its NFS location must not be used for generated test data. See [`plans/sql-library/SANDBOX_TEST.md`](plans/sql-library/SANDBOX_TEST.md) for the sandbox procedure.

## Rust Build and Checks

Once `Cargo.toml` and `Cargo.lock` exist, run from the repository root:

```sh
cargo build --workspace --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
```

These build the engine and check formatting/lints. Run `cargo test --workspace --locked -- --test-threads=1` inside the delegated `systemd --user` service described below, with the fixture and runtime environment in [`plans/sql-library/SANDBOX_TEST.md`](plans/sql-library/SANDBOX_TEST.md); all sandbox integration tests are enabled. Use `cargo build --workspace --release --locked` for release validation. Add targeted feature/platform checks when introduced; do not assume all features are compatible. Document actual server startup commands when binaries exist.

During development, run focused tests and targeted checks for each iteration. Run the full workspace test suite at the end of a roadmap stage, after that stage's deliverables are implemented; do not run it after every iteration. Fix any failures found at that gate within the same stage, then rerun the relevant checks and full suite before marking the stage complete.

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

### Confined integration test host setup

The sandboxed query integration tests are part of the normal workspace test suite and are not ignored. Building `sandbox-query-fixture` only builds their TinyCC artifacts; it does not create or delegate a cgroup. Run the full Cargo test suite inside the delegated `systemd --user` service below, with artifact and runtime variables from `plans/sql-library/SANDBOX_TEST.md`. These tests need `PRDB_TEST_CGROUP_PARENT` to name an **absolute path to a writable delegated cgroup v2 parent**. That parent must expose and have enabled the `cpu`, `memory`, and `pids` controllers. The test creates per-invocation child cgroups beneath it.

This is normally rootless **after** systemd delegates a subtree to the test service. Run tests inside a dedicated `systemd --user` unit with `Delegate=cpu memory pids`; do not run them from a desktop application scope, which may expose only `memory` and `pids`. `engine/tools/with_delegated_cgroup.sh` moves the launcher into a child cgroup before enabling controllers, sets `PRDB_TEST_CGROUP_PARENT` and `PRDB_SYSTEMD_CGROUP_PARENT` to the writable unit cgroup, and executes the requested command. The setup is shared by user and system services.

First inspect the host mount from a shell with host visibility. On this machine, `findmnt` inside the Codex sandbox reports a read-only view, while the host mount is `cgroup2 rw`; the sandbox view is not a reason to remount the host hierarchy. If the host has no cgroup2 mount and is not already managed by systemd, an administrator can mount it with `sudo mount -t cgroup2 -o nsdelegate cgroup2 /sys/fs/cgroup`. Do not mount a second cgroup2 hierarchy or remount from inside a container/desktop sandbox. A host/runtime that exposes the mount read-only must grant a writable delegated subtree; sudo inside that same read-only mount cannot change it. Creating a directory or changing permissions at the cgroup root does not enable delegation.

Check the host mount and process cgroup, then run the built-in probe inside the delegated user service:

```sh
sudo findmnt -no FSTYPE,OPTIONS /sys/fs/cgroup
cat /proc/self/cgroup
systemd-run --user --wait --pipe --collect \
  --unit="prdb-cgroup-probe-$$.service" --slice=prdb-tests.slice \
  --property='Delegate=cpu memory pids' --property="WorkingDirectory=$PWD" \
  "$PWD/engine/tools/with_delegated_cgroup.sh" \
  sh -c 'export PRDB_TEST_ROOT="$HOME/tmp/prdb-tests"; \
    export PRDB_TEST_RUN_DIR="$PRDB_TEST_ROOT/runs/r$(date +%s)"; \
    mkdir -p "$PRDB_TEST_ROOT/t" "$PRDB_TEST_RUN_DIR/data"; \
    if [ ! -e "$HOME/t" ] && [ ! -L "$HOME/t" ]; then ln -s "$PRDB_TEST_ROOT/t" "$HOME/t"; fi; \
    test "$(readlink -f "$HOME/t")" = "$(readlink -f "$PRDB_TEST_ROOT/t")"; \
    export TMPDIR="$HOME/t"; \
    exec nix develop --no-update-lock-file -c cargo run --locked \
      --bin prdb-sandbox-probe -- "$PRDB_TEST_CGROUP_PARENT"'
```

The probe must report a writable cgroup v2 parent and `cpu memory pids` in both the available controllers and enabled child controllers. Probe success is a preflight, not end-to-end acceptance; run `cargo test --workspace --locked -- --test-threads=1` inside the same `systemd --user` wrapper with its documented fixture/runtime environment variables. The commit and metadata process integration tests run as part of this suite. Do not run them outside delegated cgroup setup or use a fake cgroup directory as confinement evidence.

The separate supervisor and gateway systemd units for user and system managers are maintained in [`contrib/systemd`](contrib/systemd/README.md). User units apply Landlock policies with `prdb-landlock-run` and require the paths for the selected runtime profile. These require a READY database instance and a runtime profile validated for the host; they do not initialize storage or infer runtime allowlists.

## Style and Validation

Use rustfmt conventions for Rust and consistent C naming through the public headers. Keep unsafe Rust isolated and documented. Follow the versioned C ABI ownership rules; native structs are not IPC encodings.

Test ABI layout/ownership, malformed inputs, cell permissions, sandbox confinement, concurrent MVCC writes, conflicts, rollback, recovery, and reclamation. Benchmark compilation separately from execution. No coverage threshold exists. Documentation-only changes require link and consistency checks, not unavailable builds.

Treat numeric query/operator/estimate, request, result, queue, admission, and maintenance bounds in the specs as provisional configurable defaults unless they are explicitly fixed by a wire/storage format or published ABI. Do not encode these defaults as final product capacity. Classify each bound, load workload budgets from trusted validated profiles, and retain independent cumulative/service ceilings. Before calling a bound production-ready, test an elevated profile and representative ERP/CRM scale, row widths, result sizes, skew, and concurrent operational load as described in [`specs/workload-profile.md`](specs/workload-profile.md#configurable-bounds-and-production-sizing). If a desired setting exceeds a format or implementation ceiling, version or redesign that interface and document the migration.

## Commits and Pull Requests

After completing each feature implementation, create a git commit for that feature.
Use an imperative subject that names the change, such as `Define SDK build profile`. Keep commit messages to the subject line; do not add validation summaries or unresolved-assumption text in commit bodies. Report validation separately in the handoff. Never report template commands as executed tests.

If Git cannot create `.git/index.lock` or another repository metadata file, inspect `.git` ownership and mode first. Repair stale ownership or mode so the current repository user can write there; do not leave repository metadata owned by root after elevated builds. If `.git` is already correctly owned and writable by the repository user but the sandbox mounts it read-only, retry the Git operation through authorized escalation as the repository user (for example, `sudo -u "$USER" git ...`), rather than changing ownership or running Git as root.
