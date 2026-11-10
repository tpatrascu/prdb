# Linux Sandbox Progress

## SB-1: fail-closed host capability preflight

- Behavior: `linux_sandbox::probe` reports user-namespace configuration, Landlock ABI, seccomp `KILL_PROCESS`, and cgroup v2 availability/delegation. `require_supported` lists missing profile requirements; capability reporting does not launch a worker or claim confinement.
- Contract: [Linux sandbox profile](../../specs/linux-sandbox.md).
- Changed code: [capability probe](../../engine/src/linux_sandbox.rs) and [engine module export](../../engine/src/lib.rs).
- Host observation: this Linux 6.18.57 host permits a rootless user/network/mount/PID namespace probe and exposes seccomp actions. The desktop process sees `/sys/fs/cgroup` read-only; an authorized temporary parent inside the existing `user@1000.service` delegation made the required controllers writable for confined acceptance.
- Verification: `prdb-sandbox-probe` reported Landlock ABI 7, user namespaces and seccomp KILL_PROCESS available, and CPU/memory/PID cgroup controllers enabled on the delegated parent. The parent was removed after the acceptance runs. See the end-to-end acceptance record below.
- Remaining: exercise unsupported profiles and additional kernel/runtime combinations; probe success alone still does not prove all platform configurations.

## SB-1 dependency: host profile diagnostic

- Behavior: `prdb-sandbox-probe [CGROUP_PARENT]` prints the discovered capability report and exits nonzero with missing requirements when the host fails the preflight. It never starts a worker.
- Changed code: [probe binary](../../engine/src/bin/prdb-sandbox-probe.rs).
- Verification: the probe rejects `/sys/fs/cgroup` because it is not writable by the desktop account, and passes when pointed at the temporary delegated `user@1000.service` cgroup parent. The full confined executor acceptance below confirms the launch profile beyond the diagnostic.
- Remaining: cover unsupported capability combinations and additional host profiles; the worker admission path already calls the preflight before cgroup setup.

## SB-5 dependency: cgroup v2 worker resource controls

- Behavior: `WorkerCgroup::create` requires a delegated parent with CPU, memory, and PID controllers enabled, creates a unique nonzero-invocation leaf, applies positive CPU quota/period, memory maximum, and process-count limits, and verifies process/kill controls exist. Process placement rejects PID zero; cleanup kills descendants and removes the leaf only after `pids.current` reaches zero. `SupervisorInvocation::observe_worker_exit_in_cgroup` keeps terminal authority closed until this cleanup succeeds. A failed control write must prevent launch or resource release.
- Contract and implementation: [cgroup worker limits](../../specs/linux-sandbox.md) and [Linux sandbox resource helpers](../../engine/src/linux_sandbox.rs).
- Changed regression: [fake cgroup control files verify encodings, validation, placement and kill request](../../engine/tests/linux_sandbox.rs).
- Observed red: the focused target initially failed to compile because `WorkerResourceLimits` and cgroup helper functions were absent.
- Verification: six tests validate positive limits, process placement before exec, kill requests, rejection of an undelegated parent, and fail-closed handling of non-cgroup directories. A cleanup regression found that writes could create missing mock controls; control writes now require existing kernel files. A capability-probe regression found that `cgroup.controllers` alone falsely identified an ordinary directory as cgroup v2; the probe now verifies the filesystem type, and cgroup creation checks the complete supported profile before creating a leaf. The desktop account cannot write `/sys/fs/cgroup`; a temporary delegated parent under `user@1000.service` passed the probe and the live worker acceptance. Full configured Nix-shell validation passed: format check, workspace build, warning-denied Clippy, workspace tests, and `git diff --check`.
- Verification extension: live leaf creation, resource-limit application, pre-exec placement, kill/reap, and cleanup were exercised by the confined TinyCC acceptance tests using the temporary delegated parent. The test parent was empty and removed afterward.
- Remaining: validate controller behavior across supported deployment systemd configurations and stress cleanup under kernel/controller failures.

## SB-3 dependency: Landlock filesystem restrictions

- Behavior: `LandlockPolicy` canonicalizes and combines explicit read, read/execute, and writable path rules. `LandlockRuleset` requires Landlock ABI 3, handles filesystem rights through truncate and reference, and installs the ruleset in a child-side pre-exec hook after setting `no_new_privs`. Add cgroup placement before this hook so placement succeeds before filesystem rights are narrowed.
- Contract and implementation: [Landlock launch ordering and profile](../../specs/linux-sandbox.md) and [Landlock policy and ruleset](../../engine/src/linux_sandbox.rs).
- Regression: a confined shell can write its marker in an allowed directory while `cat /etc/passwd` is denied before worker code can read it.
- Verification: the focused Landlock regression passed. Full Nix-shell validation passed: format check, workspace build, warning-denied Clippy, workspace tests, and `git diff --check`.
- Remaining: broaden hostile-artifact coverage and validate the same policy across additional runtime profiles. The maintained confined SQL fixtures now traverse the complete launcher chain.

## SB-3: native constructor filesystem access denied — verified

- Behavior: Landlock must already constrain filesystem access while the query library's native constructor runs, before `prdb_query_main_v3` is called.
- Regression: `sql_v3_caught_error.c` now has a constructor that attempts to create and write a marker under `/tmp`. It reports a distinct result if the operation succeeds. The confined SQL error acceptance requires the ordinary entry-point output and confirms no marker file was created, then still verifies durable ABORTED after the caught SQL error.
- Verification: `sandboxed_v3_sql_error_latches_failure_and_durably_aborts_after_partial_output` passed through the delegated cgroup, clean executor, Landlock, seccomp, result relay, and storage outcome path on the current Linux x86_64 profile. `make -f engine/sdk/Makefile sandbox-query-fixture` built the artifact with TinyCC; the temporary cgroup leaf was removed by the test.
- Remaining: constructor probes for process access, dependency initialization, and additional kernel/runtime profiles remain open. File-write and host-loopback connection attempts are covered in the follow-up below.

## SB-3: native constructor filesystem and network restrictions — verified

- Behavior: filesystem and network namespace restrictions are active during native query-library constructors, before the query entry point.
- Regression: the maintained caught-error fixture's constructor attempts to create/write outside invocation scratch. A separately compiled variant also tries to connect to a host loopback listener. The confined acceptance checks the marker remains absent and accepts either a pre-entry worker termination or an explicit blocked-connection result; a successful connection emits a distinct failure payload.
- Verification: both `sandboxed_v3_sql_error_latches_failure_and_durably_aborts_after_partial_output` and `sandboxed_v3_constructor_cannot_connect_to_host_loopback` passed through the delegated cgroup, clean executor, Landlock, seccomp, and storage abort path. The TinyCC artifact build and full SDK suite passed. Locked workspace build, formatting, warning-denied Clippy, serial workspace tests (129 commit tests passed; 4 cgroup tests ignored), and `git diff --check` passed.
- Remaining: constructor process-access and dependency-initialization probes, more hostile syscall cases, and additional runtime/kernel profiles remain open.

## SB-2 dependency: rootless worker namespaces — implemented

- Behavior: `attach_namespaces_before_exec` creates a one-ID-mapped user namespace, denies setgroups, enters private mount/network/PID namespaces, marks mount propagation private, mounts a fresh PID-scoped `/proc`, clears capability and bounding sets, and sets `no_new_privs` before the final executable image runs. It forks a trusted outer reaper for PID namespace semantics; the reaper closes its inherited descriptors before waiting, while the worker child proceeds through later pre-exec hooks.
- Ordering: attach the invocation cgroup first, then namespaces, then Landlock and seccomp. The command handle tracks the reaper; the invocation cgroup is required to terminate the entire worker tree.
- Regression: [Linux namespace tests](../../engine/tests/linux_sandbox.rs) check distinct user/mount/network/PID namespace handles, worker PID 1, zero effective and bounding capabilities, `NoNewPrivs`, isolation from the supervisor PID in `/proc`, cgroup-hook ordering, and that `Command::spawn` returns before worker exit.
- Observed runtime: both focused namespace tests passed on the configured Linux host. They launch only harmless shell probes; no query artifact is loaded.
- Remaining: extend the maintained acceptance matrix to more hostile fixtures, resource pressure, and supported kernel/runtime profiles.

## SB-2 dependency: clean worker exec environment — implemented

- Behavior: `prepare_clean_worker_command` clears inherited environment variables and registers a child hook that closes all descriptors above stderr except explicit IPC descriptors. It clears `FD_CLOEXEC` on preserved descriptors and fails spawn if a preserved descriptor is invalid or Linux `close_range` is unavailable.
- Ordering: call after cgroup, namespace, and Landlock hooks have registered their setup work; add only non-secret environment values after this helper. It runs before the worker's final `exec`.
- Regression: [clean worker command test](../../engine/tests/linux_sandbox.rs) verifies a kept close-on-exec descriptor survives, an unlisted descriptor is absent, and a supervisor environment variable is stripped.
- Verification: focused regression passed with Rust/Cargo 1.95.0. Full Nix-shell validation passed: formatting, workspace build, warning-denied Clippy, all workspace tests, and `git diff --check`.
- Remaining: add descriptor-provisioning cases for future supervisor channels and additional worker runtime profiles.

## SB-2 / SB-3 dependency: ordered worker pre-exec configuration — implemented

- Behavior: `configure_worker_command` registers cgroup placement, namespace entry, Landlock installation, and clean environment/descriptor setup in the required order. It fails immediately if any hook cannot be prepared.
- Regression: the composed command test exercises the ordered setup chain with a worker shell, verifies an allowed scratch write, checks the environment is cleared, confirms an unlisted file read fails, and confirms the cgroup placement hook ran. Separate namespace tests inspect the namespace handles and PID view.
- TDD evidence: the focused test first failed to compile because the ordering API was absent; it passed after the helper assembled the existing hooks.
- Verification: focused composition regression passed with Rust/Cargo 1.95.0. Full Nix-shell validation passed: formatting, workspace build, warning-denied Clippy, all workspace tests, and `git diff --check`.
- Remaining: exercise hook failure cases at each chain position and add integration coverage for more invocation resource profiles.

## SB-4 dependency: worker syscall filter — implemented for Linux x86_64

- Behavior: `install_worker_seccomp_filter` installs an initial allowlist in the current clean worker host, checks the x86_64 audit architecture before syscall numbers, sets `no_new_privs`, applies the filter to existing threads with TSYNC, and kills the process tree for any unlisted syscall. Unsupported targets return `Unsupported`.
- Ordering: call after the worker host's final `exec` and before loading the query library or creating threads. Do not install it in the supervisor or before the namespace hook's final exec.
- Regression: a disposable copy of the test executable confirms `getpid` remains available and a new socket syscall terminates the child with `SIGSYS`.
- Verification: the focused seccomp regression passed with Rust/Cargo 1.95.0. Full Nix-shell validation passed: formatting, workspace build, warning-denied Clippy, all workspace tests, and `git diff --check`.
- Remaining: expand syscall compatibility checks to more admitted query behavior and hostile fixtures. The current verified profile is Linux x86_64 with the pinned NixOS glibc/TinyCC runtime.

## SB-2 / SB-3 / SB-4 / SB-5: confined TinyCC execution acceptance — verified

- Behavior: the supervisor admits a sealed artifact, creates and configures a per-invocation cgroup, places the executor before user code, applies private user/mount/network/PID namespaces, Landlock filesystem rules, a clean environment and descriptor set, and the x86_64 seccomp filter before loading the query library. Worker exit and cgroup cleanup precede the storage terminal decision.
- Verification environment: Linux 6.18.57 x86_64, Rust/Cargo 1.95.0, TinyCC `0.9.27-unstable-2025-01-06`, glibc 2.42-100, and GCC 15.3 `libgcc_s`. A root-level temporary parent passed preflight but the acceptance returned `EPERM`; moving the temporary parent under `user@1000.service` allowed the complete launch and cleanup. The temporary parent was removed after testing.
- Acceptance results: `sandboxed_v3_metadata_submission_commits_over_tcp`, `sandboxed_v3_sql_error_latches_failure_and_durably_aborts_after_partial_output`, `sandboxed_v3_sql_dml_sequence_is_visible_and_commits`, and `disconnected_submission_is_reconciled_after_gateway_restart` each passed individually through their documented exact opt-in commands. The SQL tests verified committed and aborted terminal outcomes; the disconnect test verified status reconciliation after a broken terminal relay.
- Remaining: test additional runtime/kernel profiles, broader hostile constructor and syscall fixtures, and resource exhaustion cases. These results establish the maintained Linux x86_64 fixtures, not an audit or all-platform proof.
