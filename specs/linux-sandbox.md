# Linux Sandbox

## Objective and Threat Model

Contain arbitrary native worker code using Linux OS mechanisms without a VM. The kernel and trusted launcher remain in the trusted computing base. A separate process alone is insufficient, and seccomp alone is not a complete sandbox.

## Required Properties

- No access to database files, user home directories, credentials, or unrelated application resources.
- No external network access or unauthorized IPC.
- No tracing, memory access, or signaling of trusted/unrelated processes.
- No privilege escalation; drop capabilities and establish `no_new_privs`.
- No worker access to the redb database file or storage backend handles/descriptors. Disk I/O belongs to trusted storage.
- Only explicitly provisioned descriptors and approved runtime resources.
- Bounded CPU, memory, process count, output, and wall time.

Use user, mount, network, and PID namespaces, filesystem restrictions, Landlock, and a narrow seccomp allowlist as complementary mechanisms. The initial launch profile requires Landlock ABI 1 or newer, seccomp filter support with `KILL_PROCESS`, and a writable delegated cgroup v2 parent with the `cpu`, `memory`, and `pids` controllers enabled. The `prdb-engine::linux_sandbox::probe` report is a preflight only; the launcher must still establish each restriction for the worker and fail closed if any setup operation fails. This profile requires rootless user namespaces and delegated cgroup v2 control; there is no weaker launch fallback.

## Launch Ordering

1. A trusted launch path creates the execution environment and scoped channels.
2. Establish filesystem/process/network isolation and resource controls.
3. Remove inherited descriptors, environment secrets, capabilities, and unwanted access.
4. Apply required restrictions before transferring control to any untrusted code.
5. In the clean executor host, load the admitted immutable query.so and supported dependencies only inside that confinement, then invoke its entry point.

Implementation must resolve loader/runtime syscall needs without an unconstrained startup window. Library constructors count as untrusted execution. Do not fork a worker from a secret-bearing storage address space and execute user code in the inherited image; use a clean execution image before user code runs.

## Artifact and Runtime Profile

The baseline executes client-built shared libraries linked with prebuilt SDK code; it does not run a compiler in the executor. Provision only the admitted artifact and declared supported dependencies. Constrain dynamic-loader search paths and remove loader-related environment overrides; neither the manifest nor SDK provenance grants filesystem access. A later standalone executable adapter must satisfy the same boundary but is outside the initial path. An optional sealed sanitized-batch memfd is a separate negotiated capability defined in [memory storage](memory-storage.md); it must not become generic descriptor or writable-shared-memory access.

The TinyCC-produced C library, prebuilt C SDK, libc dependencies, and Rust executor host must fit the chosen syscall, threading, memory, and dependency profile. Faster compilation is not grounds for weakening confinement. Native constructors and dependency initialization are part of untrusted execution, not an exemption from the sandbox.

## Capability Detection and Failure

Verify the minimum supported feature profile before launch. The capability probe verifies cgroup v2 using the filesystem type, not just a `cgroup.controllers` filename. Capability presence does not prove that namespace, Landlock, seccomp, or cgroup setup will succeed for a particular worker; each setup step must be checked at launch before transferring control to the executor. Fail closed when required restrictions are unavailable; never silently run with weaker isolation. Rootless deployment feasibility and cgroup delegation depend on the host configuration and require explicit validation.

The supervisor must clean up descendants and resources after crashes or timeouts and request transaction abort before commit. Killing a process does not undo a durable commit. Storage retains version pins until its own in-flight tasks drain. Sandbox limits do not replace storage-side request, write-set, and history-retention budgets.

Use one non-root cgroup v2 leaf per invocation. Before execution, configure positive `cpu.max`, `memory.max`, and `pids.max` values from the admitted resource budget, then place the worker in `cgroup.procs` before it can execute untrusted code. Use `WorkerCgroup::attach_command_before_exec` so placement occurs in the child between fork and exec; post-spawn placement is allowed only when the worker is stopped. On timeout, cancellation, or worker failure, write `1` to `cgroup.kill` and verify cleanup before releasing invocation resources. A missing control file or failed write is a launch/cleanup failure; do not continue without the corresponding limit. The Rust helpers create and configure an invocation leaf under a delegated parent and expose pre-exec placement, descendant termination, and empty-leaf cleanup. They are not yet wired into a confined launch, so the supervisor does not yet claim these controls are active for workers.

## Validation and Limits

Use malicious client-built query libraries to attempt filesystem reads, network access, descriptor discovery, process inspection, privilege changes, spawning, memory exhaustion, and unbounded output. Exercise constructor-time attacks and every supported deployment profile.

Kernel vulnerabilities and comprehensive microarchitectural/timing side-channel protection are outside the initial guarantee. Review exposed kernel interfaces and record residual inference risks as the design develops.

## References

- [Chromium Linux sandbox](https://chromium.googlesource.com/chromium/src/+/master/sandbox/linux)
- [Linux seccomp documentation](https://kernel.org/doc/html/latest/userspace-api/seccomp_filter.html)
- [Linux Landlock documentation](https://docs.kernel.org/userspace-api/landlock.html)
