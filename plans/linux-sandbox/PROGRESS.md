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

## SB-2 dependency: rootless worker namespaces — implemented

- Behavior: `attach_namespaces_before_exec` creates a one-ID-mapped user namespace, denies setgroups, enters private mount/network/PID namespaces, marks mount propagation private, mounts a fresh PID-scoped `/proc`, clears capability and bounding sets, and sets `no_new_privs` before the final executable image runs. It forks a trusted outer reaper for PID namespace semantics; the reaper closes its inherited descriptors before waiting, while the worker child proceeds through later pre-exec hooks.
- Ordering: attach the invocation cgroup first, then namespaces, then Landlock and seccomp. The command handle tracks the reaper; the invocation cgroup is required to terminate the entire worker tree.
- Regression: [Linux namespace tests](../../engine/tests/linux_sandbox.rs) check distinct user/mount/network/PID namespace handles, worker PID 1, zero effective and bounding capabilities, `NoNewPrivs`, isolation from the supervisor PID in `/proc`, cgroup-hook ordering, and that `Command::spawn` returns before worker exit.
- Observed runtime: both focused namespace tests passed on the configured Linux host. They launch only harmless shell probes; no query artifact is loaded.
- Remaining: wire the hook chain into a clean executor launcher, provision filesystem/runtime resources, and exercise the full profile with a delegated cgroup parent and hostile fixtures.

## SB-2 dependency: clean worker exec environment — implemented

- Behavior: `prepare_clean_worker_command` clears inherited environment variables and registers a child hook that closes all descriptors above stderr except explicit IPC descriptors. It clears `FD_CLOEXEC` on preserved descriptors and fails spawn if a preserved descriptor is invalid or Linux `close_range` is unavailable.
- Ordering: call after cgroup, namespace, and Landlock hooks have registered their setup work; add only non-secret environment values after this helper. It runs before the worker's final `exec`.
- Regression: [clean worker command test](../../engine/tests/linux_sandbox.rs) verifies a kept close-on-exec descriptor survives, an unlisted descriptor is absent, and a supervisor environment variable is stripped.
- Verification: focused regression passed with Rust/Cargo 1.95.0. Full Nix-shell validation passed: formatting, workspace build, warning-denied Clippy, all workspace tests, and `git diff --check`.
- Remaining: wire this command setup into the clean executor launcher and validate that only the intended IPC descriptors are provisioned.

## SB-4 dependency: worker syscall filter — implemented for Linux x86_64

- Behavior: `install_worker_seccomp_filter` installs an initial allowlist in the current clean worker host, checks the x86_64 audit architecture before syscall numbers, sets `no_new_privs`, applies the filter to existing threads with TSYNC, and kills the process tree for any unlisted syscall. Unsupported targets return `Unsupported`.
- Ordering: call after the worker host's final `exec` and before loading the query library or creating threads. Do not install it in the supervisor or before the namespace hook's final exec.
- Regression: a disposable copy of the test executable confirms `getpid` remains available and a new socket syscall terminates the child with `SIGSYS`.
- Verification: the focused seccomp regression passed with Rust/Cargo 1.95.0. Full Nix-shell validation passed: formatting, workspace build, warning-denied Clippy, all workspace tests, and `git diff --check`.
- Remaining: integrate with the clean executor launcher, validate the allowlist against the supported Rust/libc/SDK runtime and admitted query behavior, and run hostile artifact fixtures only in that sandbox. This host still lacks the writable delegated cgroup parent needed for a complete launch.
