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
