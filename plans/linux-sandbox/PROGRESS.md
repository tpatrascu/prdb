# Linux Sandbox Progress

## SB-1: fail-closed host capability preflight

- Behavior: `linux_sandbox::probe` reports user-namespace configuration, Landlock ABI, seccomp `KILL_PROCESS`, and cgroup v2 availability/delegation. `require_supported` lists missing profile requirements; capability reporting does not launch a worker or claim confinement.
- Contract: [Linux sandbox profile](../../specs/linux-sandbox.md).
- Changed code: [capability probe](../../engine/src/linux_sandbox.rs) and [engine module export](../../engine/src/lib.rs).
- Host observation: this Linux 6.18.57 host permits a rootless user/network/mount/PID namespace probe and exposes seccomp actions. `/sys/fs/cgroup` is read-only to the current account, so the required writable delegated cgroup parent is unavailable here.
- Verification: workspace build, format, warning-denied Clippy, and `git diff --check` are recorded after implementation. No runtime confinement or worker-launch evidence is claimed.
- Remaining: test the Rust report on supported and unsupported profiles; implement and validate actual namespace, Landlock, seccomp, and cgroup setup in the launcher before loading any artifact.
