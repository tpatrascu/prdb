# Linux Sandbox Progress

## SB-1: fail-closed host capability preflight

- Behavior: `linux_sandbox::probe` reports user-namespace configuration, Landlock ABI, seccomp `KILL_PROCESS`, and cgroup v2 availability/delegation. `require_supported` lists missing profile requirements; capability reporting does not launch a worker or claim confinement.
- Contract: [Linux sandbox profile](../../specs/linux-sandbox.md).
- Changed code: [capability probe](../../engine/src/linux_sandbox.rs) and [engine module export](../../engine/src/lib.rs).
- Host observation: this Linux 6.18.57 host permits a rootless user/network/mount/PID namespace probe and exposes seccomp actions. `/sys/fs/cgroup` is read-only to the current account, so the required writable delegated cgroup parent is unavailable here.
- Verification: workspace build, format, warning-denied Clippy, and `git diff --check` are recorded after implementation. No runtime confinement or worker-launch evidence is claimed.
- Remaining: test the Rust report on supported and unsupported profiles; implement and validate actual namespace, Landlock, seccomp, and cgroup setup in the launcher before loading any artifact.

## SB-1 dependency: host profile diagnostic

- Behavior: `prdb-sandbox-probe [CGROUP_PARENT]` prints the discovered capability report and exits nonzero with missing requirements when the host fails the preflight. It never starts a worker.
- Changed code: [probe binary](../../engine/src/bin/prdb-sandbox-probe.rs).
- Verification: Nix-shell formatting, workspace build, and warning-denied Clippy pass. Running `cargo run -p prdb-engine --bin prdb-sandbox-probe` reports Landlock ABI 7, user namespaces and seccomp available, cgroup v2 mounted with `cpu`, `memory`, and `pids` enabled, but rejects the host because `/sys/fs/cgroup` is not writable. Exit code is 1 as required for this unsupported launch profile; `git diff --check` passes.
- Remaining: this diagnostic is not a launch gate until the supervisor calls the same preflight and performs the full confinement setup.

## SB-5 dependency: cgroup v2 worker resource controls

- Behavior: `WorkerCgroup::create` requires a delegated parent with CPU, memory, and PID controllers enabled, creates a unique nonzero-invocation leaf, applies positive CPU quota/period, memory maximum, and process-count limits, and verifies process/kill controls exist. Process placement rejects PID zero; cleanup kills descendants and removes the leaf only after `pids.current` reaches zero. A failed control write must prevent launch or resource release.
- Contract and implementation: [cgroup worker limits](../../specs/linux-sandbox.md) and [Linux sandbox resource helpers](../../engine/src/linux_sandbox.rs).
- Changed regression: [fake cgroup control files verify encodings, validation, placement and kill request](../../engine/tests/linux_sandbox.rs).
- Observed red: the focused target initially failed to compile because `WorkerResourceLimits` and cgroup helper functions were absent.
- Verification: four fake-control tests validate positive limits, process placement, kill requests, rejection of an undelegated parent, and cleanup of a partially created leaf when kernel controls are absent. A failing cleanup assertion found that ordinary filesystem writes could create missing mock controls; writes now require the control file to exist. Full configured Nix-shell validation passed after the creation/rollback change: `cargo fmt --all -- --check`, `cargo build --workspace --locked`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked`, and `git diff --check`.
- Remaining: exercise leaf creation/cleanup against a delegated cgroup v2 mount; place the process before exec; and integrate cgroup termination with timeout/cancellation and terminal storage decisions. This host has no writable delegated cgroup parent, so no live cgroup launch evidence exists.
