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

- Behavior: `WorkerCgroup::create` requires a delegated parent with CPU, memory, and PID controllers enabled, creates a unique nonzero-invocation leaf, applies positive CPU quota/period, memory maximum, and process-count limits, and verifies process/kill controls exist. Process placement rejects PID zero; cleanup kills descendants and removes the leaf only after `pids.current` reaches zero. `SupervisorInvocation::observe_worker_exit_in_cgroup` keeps terminal authority closed until this cleanup succeeds. A failed control write must prevent launch or resource release.
- Contract and implementation: [cgroup worker limits](../../specs/linux-sandbox.md) and [Linux sandbox resource helpers](../../engine/src/linux_sandbox.rs).
- Changed regression: [fake cgroup control files verify encodings, validation, placement and kill request](../../engine/tests/linux_sandbox.rs).
- Observed red: the focused target initially failed to compile because `WorkerResourceLimits` and cgroup helper functions were absent.
- Verification: six tests validate positive limits, process placement before exec, kill requests, rejection of an undelegated parent, and fail-closed handling of non-cgroup directories. A cleanup regression found that writes could create missing mock controls; control writes now require existing kernel files. A capability-probe regression found that `cgroup.controllers` alone falsely identified an ordinary directory as cgroup v2; the probe now verifies the filesystem type, and cgroup creation checks the complete supported profile before creating a leaf. The real host probe reports cgroup v2 mounted with CPU/memory/PID controllers enabled, but rejects launch because `/sys/fs/cgroup` is not writable. Full configured Nix-shell validation passed: format check, workspace build, warning-denied Clippy, workspace tests, and `git diff --check`.
- Remaining: exercise leaf creation/cleanup against a delegated cgroup v2 mount and wire cgroup creation/placement into the confined worker launcher. This host has no writable delegated cgroup parent, so no live cgroup launch evidence exists.

## SB-3 dependency: Landlock filesystem restrictions

- Behavior: `LandlockPolicy` canonicalizes and combines explicit read, read/execute, and writable path rules. `LandlockRuleset` requires Landlock ABI 3, handles filesystem rights through truncate and reference, and installs the ruleset in a child-side pre-exec hook after setting `no_new_privs`. Add cgroup placement before this hook so placement succeeds before filesystem rights are narrowed.
- Contract and implementation: [Landlock launch ordering and profile](../../specs/linux-sandbox.md) and [Landlock policy and ruleset](../../engine/src/linux_sandbox.rs).
- Regression: a confined shell can write its marker in an allowed directory while `cat /etc/passwd` is denied before worker code can read it.
- Verification: the focused Landlock regression passed. Full Nix-shell validation passed: format check, workspace build, warning-denied Clippy, workspace tests, and `git diff --check`.
- Remaining: this Landlock setup is not yet integrated with a clean executor launcher, namespaces, descriptor scrubbing, network isolation, or seccomp. Full hostile-artifact launch validation remains outstanding.
